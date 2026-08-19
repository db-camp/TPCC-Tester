use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::connection::client::RmdbClient;
use crate::connection::cursor::RmdbCursor;
use crate::data_gen::TpccDataGen;
use crate::error::TpccError;
use crate::report::BenchmarkResult;
use crate::transaction::{self, TransactionType};

struct SharedCounters {
    total_committed: AtomicU64,
    total_aborted: AtomicU64,
    new_order_count: AtomicU64,
    cancelled: AtomicBool,
}

pub struct BenchmarkExecutor {
    config: Config,
}

impl BenchmarkExecutor {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub async fn run(&self) -> Result<BenchmarkResult, TpccError> {
        let num_threads = self.config.threads;
        let txn_per_thread = self.config.transactions;
        let rw_ratio = self.config.rw_ratio;
        let txn_probs = self.config.txn_probs.clone();

        info!(
            "启动并发基准测试: {} 线程, 每线程 {} 事务",
            num_threads, txn_per_thread
        );

        let counters = Arc::new(SharedCounters {
            total_committed: AtomicU64::new(0),
            total_aborted: AtomicU64::new(0),
            new_order_count: AtomicU64::new(0),
            cancelled: AtomicBool::new(false),
        });

        // Ctrl+C handler
        let cancel_counters = counters.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("收到 Ctrl+C 信号，正在优雅关闭...");
            cancel_counters.cancelled.store(true, Ordering::Relaxed);
        });

        // Progress reporter
        let progress_counters = counters.clone();
        let progress_handle = tokio::spawn(async move {
            let start = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                if progress_counters.cancelled.load(Ordering::Relaxed) {
                    break;
                }
                let committed = progress_counters.total_committed.load(Ordering::Relaxed);
                let aborted = progress_counters.total_aborted.load(Ordering::Relaxed);
                let elapsed = start.elapsed().as_secs_f64();
                let tps = committed as f64 / elapsed;
                let total = committed + aborted;
                let success_rate = if total > 0 {
                    committed as f64 / total as f64 * 100.0
                } else {
                    0.0
                };
                info!(
                    "[进度] 已完成: {committed}, TPS: {tps:.1}, 成功率: {success_rate:.1}%, 耗时: {elapsed:.1}s"
                );
            }
        });

        let start_time = Instant::now();
        let mut join_set = JoinSet::new();

        // Per-thread timing data
        let thread_results: Arc<tokio::sync::Mutex<Vec<Vec<(TransactionType, f64, bool)>>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));

        for thread_id in 0..num_threads {
            let host = self.config.host.clone();
            let port = self.config.port;
            let mode = self.config.protocol_mode();
            let scale = self.config.scale_factor;
            let counters = counters.clone();
            let probs = txn_probs.clone();
            let results = thread_results.clone();

            join_set.spawn(async move {
                let mut thread_data: Vec<(TransactionType, f64, bool)> = Vec::new();

                // Create connection for this worker
                let client = match RmdbClient::connect(&host, port, mode).await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("[Worker {thread_id}] 连接失败: {e}");
                        return;
                    }
                };
                let mut cursor = RmdbCursor::new(client);
                let gen = TpccDataGen::new(scale);

                let mut consecutive_failures = 0u32;

                for txn_idx in 0..txn_per_thread {
                    if counters.cancelled.load(Ordering::Relaxed) {
                        break;
                    }

                    let txn_type = transaction::select_transaction_type(rw_ratio, &probs);
                    let txn_start = Instant::now();

                    // Retry loop (matches Python behavior: infinite retry until success)
                    loop {
                        if counters.cancelled.load(Ordering::Relaxed) {
                            break;
                        }

                        match transaction::execute_transaction(&mut cursor, &gen, txn_type).await {
                            Ok(true) => {
                                let elapsed = txn_start.elapsed().as_secs_f64();
                                counters.total_committed.fetch_add(1, Ordering::Relaxed);
                                if txn_type == TransactionType::NewOrder {
                                    counters.new_order_count.fetch_add(1, Ordering::Relaxed);
                                }
                                thread_data.push((txn_type, elapsed, true));
                                consecutive_failures = 0;

                                if elapsed > 30.0 {
                                    error!(
                                        "[Worker {thread_id}] 事务 {txn_idx} ({}) 耗时 {elapsed:.1}s",
                                        txn_type.name()
                                    );
                                } else if elapsed > 5.0 {
                                    warn!(
                                        "[Worker {thread_id}] 事务 {txn_idx} ({}) 耗时 {elapsed:.1}s",
                                        txn_type.name()
                                    );
                                }
                                break;
                            }
                            Ok(false) => {
                                counters.total_aborted.fetch_add(1, Ordering::Relaxed);
                                consecutive_failures += 1;
                                if consecutive_failures > 10 {
                                    warn!(
                                        "[Worker {thread_id}] 连续 {consecutive_failures} 次事务失败，请检查数据库状态"
                                    );
                                }
                                debug!(
                                    "[Worker {thread_id}] 事务 {} 重试 (attempt {})",
                                    txn_type.name(),
                                    consecutive_failures
                                );
                                // Continue retry
                            }
                            Err(TpccError::Connection(_))
                            | Err(TpccError::Io(_))
                            | Err(TpccError::Protocol(_)) => {
                                // 协议状态失步时连接不再可信，与断连一样重建
                                warn!("[Worker {thread_id}] 连接断开，尝试重连...");
                                // Attempt reconnect
                                match RmdbClient::connect(&host, port, mode).await {
                                    Ok(new_client) => {
                                        cursor = RmdbCursor::new(new_client);
                                        info!("[Worker {thread_id}] 重连成功");
                                    }
                                    Err(e) => {
                                        error!("[Worker {thread_id}] 重连失败: {e}");
                                        error!("[Worker {thread_id}] rmdb 进程可能已崩溃，请检查服务状态");
                                        results.lock().await.push(thread_data);
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("[Worker {thread_id}] 事务错误: {e}");
                                counters.total_aborted.fetch_add(1, Ordering::Relaxed);
                                consecutive_failures += 1;
                                // Continue retry
                            }
                        }
                    }
                }

                results.lock().await.push(thread_data);
            });
        }

        // Wait for all workers
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                error!("Worker panic: {e}");
            }
        }

        let total_duration = start_time.elapsed().as_secs_f64();
        counters.cancelled.store(true, Ordering::Relaxed);
        let _ = progress_handle.await;

        // Collect results
        let all_results = thread_results.lock().await;
        let flat: Vec<(TransactionType, f64, bool)> = all_results.iter().flat_map(|v| v.iter().copied()).collect();

        // Build result
        let total_committed = counters.total_committed.load(Ordering::Relaxed) as usize;
        let total_aborted = counters.total_aborted.load(Ordering::Relaxed) as usize;
        let new_order_count = counters.new_order_count.load(Ordering::Relaxed) as usize;

        let mut txn_breakdown = std::collections::HashMap::new();
        let mut txn_latencies: std::collections::HashMap<TransactionType, Vec<f64>> =
            std::collections::HashMap::new();

        for &(txn_type, latency, _success) in flat.iter() {
            *txn_breakdown.entry(txn_type).or_insert(0usize) += 1;
            txn_latencies.entry(txn_type).or_default().push(latency);
        }

        // Sort latencies for percentile calculation
        for latencies in txn_latencies.values_mut() {
            latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        }

        let total_transactions = total_committed;
        let avg_response_time = if !flat.is_empty() {
            flat.iter().map(|(_, lat, _)| *lat).sum::<f64>() / flat.len() as f64
        } else {
            0.0
        };
        let throughput_tps = if total_duration > 0.0 {
            total_committed as f64 / total_duration
        } else {
            0.0
        };
        let tpmc = if total_duration > 0.0 {
            new_order_count as f64 * 60.0 / total_duration
        } else {
            0.0
        };

        Ok(BenchmarkResult {
            total_transactions,
            successful_transactions: total_committed,
            failed_transactions: total_aborted,
            avg_response_time,
            throughput_tps,
            total_duration,
            tpmc,
            transaction_breakdown: txn_breakdown,
            transaction_latencies: txn_latencies,
            num_threads,
            txn_per_thread,
            scale_factor: self.config.scale_factor,
            rw_ratio,
        })
    }
}
