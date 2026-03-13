use std::collections::HashMap;

use crate::transaction::TransactionType;

pub struct BenchmarkResult {
    pub total_transactions: usize,
    pub successful_transactions: usize,
    pub failed_transactions: usize,
    pub avg_response_time: f64,
    pub throughput_tps: f64,
    pub total_duration: f64,
    pub tpmc: f64,
    pub transaction_breakdown: HashMap<TransactionType, usize>,
    pub transaction_latencies: HashMap<TransactionType, Vec<f64>>,
    pub num_threads: usize,
    pub txn_per_thread: usize,
    pub scale_factor: i32,
    pub rw_ratio: f64,
}

impl BenchmarkResult {
    pub fn print_report(&self) {
        println!();
        println!("============================================================");
        println!("TPC-C CONCURRENT BENCHMARK RESULTS");
        println!("============================================================");
        println!(
            "Configuration: {} threads, {} transactions/thread",
            self.num_threads, self.txn_per_thread
        );
        println!("Scale Factor: {} warehouses", self.scale_factor);
        println!("Read-Write Ratio: {}", self.rw_ratio);

        println!();
        println!("Performance:");
        println!("  Total Transactions: {:>10}", self.total_transactions);
        println!("  Successful:         {:>10}", self.successful_transactions);
        println!("  Failed (retried):   {:>10}", self.failed_transactions);
        if self.total_transactions > 0 {
            let rate =
                self.successful_transactions as f64 / self.total_transactions as f64 * 100.0;
            println!("  Success Rate:       {:>9.2}%", rate);
        }
        println!("  Total Duration:     {:>9.2} seconds", self.total_duration);
        println!(
            "  Average Response:   {:>9.2} ms",
            self.avg_response_time * 1000.0
        );
        println!("  Throughput:         {:>9.2} TPS", self.throughput_tps);
        println!("  tpmC:              {:>10.2}", self.tpmc);

        println!();
        println!("Transaction Mix:");
        let txn_names = [
            TransactionType::NewOrder,
            TransactionType::Payment,
            TransactionType::Delivery,
            TransactionType::OrderStatus,
            TransactionType::StockLevel,
        ];
        for txn_type in &txn_names {
            let count = self.transaction_breakdown.get(txn_type).copied().unwrap_or(0);
            if count > 0 {
                let pct = count as f64 / self.total_transactions as f64 * 100.0;
                print!("  {:>12}: {:>6} ({:>5.1}%)", txn_type.name(), count, pct);

                // Print latency percentiles
                if let Some(latencies) = self.transaction_latencies.get(txn_type) {
                    if !latencies.is_empty() {
                        let p50 = percentile(latencies, 50.0);
                        let p99 = percentile(latencies, 99.0);
                        print!("  P50={:.1}ms P99={:.1}ms", p50 * 1000.0, p99 * 1000.0);
                    }
                }
                println!();
            }
        }

        println!("============================================================");
    }
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}
