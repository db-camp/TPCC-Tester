mod check_executor;
mod checker;
mod config;
mod connection;
pub mod consistency;
mod data_gen;
mod diagnostic_executor;
mod error;
mod executor;
mod loader;
pub mod measurement;
mod model;
pub mod phases;
mod profile;
mod ranking;
mod recovery_sample_checker;
mod report;
mod routing;
mod run_state;
mod runtime_schema;
mod sample_evidence;
mod transaction;
mod workload;

use std::future::Future;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tracing::{error, info, warn};

use config::{Config, DiagnosticSegment, LifecycleEvent, ResolvedProfile};
use connection::client::RmdbClient;
use connection::cursor::RmdbCursor;
use error::TpccError;
use run_state::{
    CrashLifecycleEvent, DiagnosticStage, RunConformance, RunContract, SetupClaimOrigin,
};

fn setup_tracing(verbose: u8) {
    use tracing_subscriber::EnvFilter;

    let filter = match verbose {
        0 => "tpcc_tester=info",
        1 => "tpcc_tester=debug",
        _ => "tpcc_tester=trace",
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)),
        )
        .with_target(true)
        .with_ansi(true)
        .init();
}

#[tokio::main]
async fn main() {
    let config = Config::parse();
    setup_tracing(config.verbose);

    if let Err(error) = config.validate() {
        error!("配置无效: {error}");
        process::exit(1);
    }
    let effective = match config.resolved_profile() {
        Ok(effective) => effective,
        Err(error) => {
            error!("配置无效: {error}");
            process::exit(1);
        }
    };
    if config.attest_formal_state {
        let seed = effective
            .seed
            .expect("validated formal-state attestation has a caller seed");
        let root = config
            .state_dir
            .as_deref()
            .expect("validated formal-state attestation has a state directory");
        let contract = run_contract(&config, &effective);
        match run_state::attest_formal_state(root, seed, &contract) {
            Ok(receipt) => {
                println!("{receipt}");
                return;
            }
            Err(error) => {
                error!("正式状态证明失败: {error}");
                process::exit(1);
            }
        }
    }
    print_effective_profile(&config, &effective);

    if let Err(e) = run(config, effective).await {
        error!("执行失败: {e}");
        process::exit(1);
    }
}

async fn run(config: Config, effective: ResolvedProfile) -> Result<(), Box<dyn std::error::Error>> {
    if config.probe_ready {
        info!(
            "执行 Wire v3 readiness probe: {}:{}",
            config.host, config.port
        );
        let shared_budget = Duration::from_millis(
            config
                .probe_budget_millis
                .ok_or("validated readiness probe lost its shared budget")?,
        );
        RmdbClient::probe_readiness(&config.host, config.port, shared_budget).await?;
        info!("Wire v3 readiness probe 通过（完整执行 `show tables;`）");
        return Ok(());
    }

    if let Some(seed) = effective.seed {
        std::env::set_var("RMDB_TPCC_SEED", seed.to_string());
    }

    if let Some(event) = config.lifecycle_event {
        let seed = effective
            .seed
            .ok_or("validated lifecycle event lost its seed")?;
        let contract = run_contract(&config, &effective);
        match event {
            LifecycleEvent::SetupIntent => {
                let run_id = run_id_for_seed(seed);
                let store = run_state::StateStore::open_terminal(
                    config
                        .state_dir
                        .as_deref()
                        .ok_or("validated setup-intent lost its state directory")?,
                )?;
                store.publish_setup_intent(&run_id, seed, &contract)?;
                info!(
                    "已在 RMDB 启动前持久化 setup intent: {}",
                    store.root().join("setup.started").display()
                );
            }
            LifecycleEvent::CrashIntent
            | LifecycleEvent::CrashKilled
            | LifecycleEvent::RestartStarted
            | LifecycleEvent::RestartReady => {
                let (store, dataset, contract) = load_bound_state(&config, &effective)?;
                let crash_event = match event {
                    LifecycleEvent::CrashIntent => CrashLifecycleEvent::Intent,
                    LifecycleEvent::CrashKilled => CrashLifecycleEvent::Killed,
                    LifecycleEvent::RestartStarted => CrashLifecycleEvent::RestartStarted,
                    LifecycleEvent::RestartReady => CrashLifecycleEvent::RestartReady,
                    LifecycleEvent::SetupIntent => unreachable!(),
                };
                store.record_crash_lifecycle_from_terminal_evidence(
                    &dataset,
                    &contract,
                    crash_event,
                )?;
                info!(
                    "已持久化 write-once lifecycle transition: {}",
                    event.as_str()
                );
            }
        }
        return Ok(());
    }

    // Diagnose mode
    if config.diagnose {
        return run_diagnose(&config).await;
    }

    if config.diagnostic_workload_seconds.is_some() {
        let (store, dataset, contract) = load_bound_state(&config, &effective)?;
        let segment = config
            .diagnostic_segment
            .ok_or("validated diagnostic configuration lost its segment")?;
        let stage = match segment {
            DiagnosticSegment::Warmup => DiagnosticStage::Warmup,
            DiagnosticSegment::Observation => DiagnosticStage::Observation,
        };
        let claim = store.begin_diagnostic_from_terminal_evidence(&dataset, &contract, stage)?;
        info!(
            "启动 final2026 非排名诊断 {}；已持久化数据库漂移 claim",
            segment.as_str()
        );
        let exec = diagnostic_executor::DiagnosticExecutor::new(config, effective);
        let result = exec.run().await?;
        store.complete_diagnostic_from_terminal_evidence(&dataset, &contract, claim)?;
        result.print_report();
        return Ok(());
    }

    let needs_connection =
        config.create_schema || config.init || config.check || config.stats || config.benchmark;

    if !needs_connection {
        info!("用法: tpcc-tester --create-schema | --init | --check | --stats | --benchmark | --diagnostic-workload-seconds N --diagnostic-segment warmup|observation | --diagnose");
        info!("  --create-schema 创建 TPC-C 表和索引");
        info!("  --init          加载 TPC-C 初始数据");
        info!("  --check         运行一致性检查");
        info!("  --stats         显示各表行数统计");
        info!("  --benchmark     运行并发基准测试");
        info!("  --diagnostic-workload-seconds N --diagnostic-segment warmup|observation");
        info!("  --diagnose      运行数据库兼容性诊断");
        info!("  -v / -vv        详细日志");
        return Ok(());
    }

    let needs_control_connection =
        config.create_schema || config.init || config.check || config.stats;
    if needs_control_connection {
        let mut setup_run = if config.init {
            let seed = effective
                .seed
                .ok_or("validated init configuration lost its seed")?;
            let run_id = run_id_for_seed(seed);
            let store = run_state::StateStore::open_terminal(
                config
                    .state_dir
                    .as_deref()
                    .ok_or("validated init configuration lost its state directory")?,
            )?;
            let contract = run_contract(&config, &effective);
            let (claim, origin) = store.begin_or_resume_setup(&run_id, seed, &contract)?;
            match origin {
                SetupClaimOrigin::Created => info!(
                    "已在连接/首个 setup SQL 前持久化本地 setup claim: {}",
                    store.root().join("setup.started").display()
                ),
                SetupClaimOrigin::Resumed => info!(
                    "已在连接前消费 RMDB 启动前持久化的 setup claim: {}",
                    store.root().join("setup.started").display()
                ),
            }
            Some((store, contract, claim, run_id, seed))
        } else {
            None
        };
        let mut setup_check_run = None;
        let mut online_check_run = None;
        let mut recovery_check_run = None;
        if config.check && !config.init {
            let (store, dataset, contract) = load_bound_state(&config, &effective)?;
            match config.check_scope {
                config::CheckScope::Setup => {
                    let claim = store.begin_setup_check(&dataset, &contract)?;
                    setup_check_run = Some((store, dataset, contract, claim));
                }
                config::CheckScope::Online => {
                    let (claim, terminal_evidence) =
                        store.begin_online_check_from_terminal_evidence(&dataset, &contract)?;
                    online_check_run = Some((store, dataset, contract, claim, terminal_evidence));
                }
                config::CheckScope::Recovery => {
                    let (claim, terminal_evidence, baseline) =
                        store.begin_recovery_check_from_terminal_evidence(&dataset, &contract)?;
                    recovery_check_run =
                        Some((store, dataset, contract, claim, terminal_evidence, baseline));
                }
            }
            info!(
                "已在 tester 首个 connect/ping 前持久化独立 {} check claim",
                config.check_scope.as_str()
            );
        }

        let setup_schema = if config.create_schema || config.init {
            let seed = effective
                .seed
                .ok_or("validated setup configuration lost its seed")?;
            let schema = if config.canonical_schema {
                runtime_schema::RuntimeSchema::canonical(seed)?
            } else {
                runtime_schema::RuntimeSchema::opaque(seed)?
            };
            Some(Arc::new(schema))
        } else {
            None
        };
        let materialized = if config.init {
            let schema = Arc::clone(
                setup_schema
                    .as_ref()
                    .ok_or("validated init configuration lost its runtime schema")?,
            );
            let scale_factor = config.scale_factor;
            Some(
                tokio::task::spawn_blocking(move || {
                    loader::CsvMaterializer::new(scale_factor, &schema)?.materialize()
                })
                .await??,
            )
        } else {
            None
        };

        info!("连接 RMDB: {}:{} ...", config.host, config.port);
        let response_timeout = control_response_timeout(&config, &effective);
        let client =
            RmdbClient::connect_with_timeout(&config.host, config.port, response_timeout).await?;
        let setup_deadline = (config.create_schema || config.init)
            .then(|| tokio::time::Instant::now() + effective.final2026.load_budget);
        let mut cursor = RmdbCursor::new(client);
        info!("RMDB Wire v3 连接正常");

        if config.create_schema {
            info!("创建 TPC-C 表和索引");
            let mut ldr = loader::Loader::new(
                &mut cursor,
                config.scale_factor,
                setup_schema
                    .as_deref()
                    .ok_or("validated schema setup lost its runtime schema")?,
            );
            setup_step(setup_deadline, "create 9 tables", ldr.create_tables()).await?;
            setup_step(setup_deadline, "create 10 indexes", ldr.create_indexes()).await?;
            info!("TPC-C 表和索引创建完成");
        }

        if config.init {
            info!("加载 TPC-C 初始数据 (scale_factor={})", config.scale_factor);
            let mut ldr = loader::Loader::new(
                &mut cursor,
                config.scale_factor,
                setup_schema
                    .as_deref()
                    .ok_or("validated init configuration lost its runtime schema")?,
            );
            let load = setup_step(
                setup_deadline,
                "load 9 pre-materialized relations and verify exactly 9 counts",
                ldr.load_materialized(
                    materialized.ok_or("validated init configuration lost its CSV assets")?,
                ),
            )
            .await?;
            let (store, contract, claim, run_id, seed) = setup_run
                .take()
                .ok_or("validated init configuration lost its pre-DDL setup claim")?;
            let state = run_state::DatasetState::from_load_with_schema(
                run_id,
                seed,
                config.scale_factor,
                load,
                setup_schema
                    .as_deref()
                    .ok_or("validated init configuration lost its runtime schema")?
                    .clone(),
            )?;
            store.complete_dataset(&state, &contract, claim)?;
            info!(
                "已保存版本化装载状态: {}",
                store.root().join("dataset.state").display()
            );
            info!("TPC-C 初始数据加载完成");
        }

        if config.check {
            info!("运行 {} 阶段一致性检查", config.check_scope.as_str());
            match config.check_scope {
                config::CheckScope::Setup => {
                    let (store, dataset, contract, claim) =
                        if let Some(preflight) = setup_check_run.take() {
                            preflight
                        } else {
                            let (store, dataset, contract) = load_bound_state(&config, &effective)?;
                            let claim = store.begin_setup_check(&dataset, &contract)?;
                            (store, dataset, contract, claim)
                        };
                    setup_step(
                        setup_deadline,
                        "run public setup integrity checks",
                        check_executor::run_setup(cursor.client_mut(), &dataset),
                    )
                    .await?;
                    store.complete_setup_check(&dataset, &contract, claim)?;
                }
                config::CheckScope::Online => {
                    let (store, dataset, contract, claim, terminal_evidence) = online_check_run
                        .take()
                        .ok_or("independent online check lost its pre-connect claim")?;
                    let baseline = check_executor::run_final_online_from_terminal_evidence(
                        cursor.client_mut(),
                        &dataset,
                        &terminal_evidence,
                        dataset.initial_order_line_amounts(),
                    )
                    .await?;
                    store.complete_online_check_from_terminal_evidence(
                        &dataset, &contract, claim, &baseline,
                    )?;
                    info!(
                        "已原子保存 online FLOAT baseline: {}",
                        store.root().join("float_baseline.state").display()
                    );
                }
                config::CheckScope::Recovery => {
                    let (store, dataset, contract, claim, terminal_evidence, baseline) =
                        recovery_check_run
                            .take()
                            .ok_or("independent recovery check lost its pre-connect claim")?;
                    check_executor::run_final_recovery_from_terminal_evidence(
                        cursor.client_mut(),
                        &dataset,
                        &terminal_evidence,
                        dataset.initial_order_line_amounts(),
                        &baseline,
                    )
                    .await?;
                    store.complete_recovery_check_from_terminal_evidence(
                        &dataset, &contract, claim,
                    )?;
                    info!(
                        "已原子保存 recovery pass receipt: {}",
                        store.root().join("recovery_check.passed").display()
                    );
                }
            }
        }

        if config.stats {
            let (_, dataset, _) = load_bound_state(&config, &effective)?;
            let mut chk = checker::ConsistencyChecker::new(
                &mut cursor,
                &dataset.runtime_schema,
                config.scale_factor,
                None,
            );
            chk.show_stats().await?;
        }
    }

    if config.benchmark {
        info!("启动原生 final2026 连续三窗口基准测试...");
        let (store, dataset, contract) = load_bound_state(&config, &effective)?;
        let claim = store.begin_rank(&dataset, &contract)?;
        let setup = dataset.setup_evidence();
        let setup_timestamp = String::from_utf8(setup.load_timestamp.clone())
            .map_err(|_| "validated setup load timestamp is not UTF-8")?;
        let setup_generator = Arc::new(data_gen::TpccDataGen::with_seed_and_timestamp(
            dataset.warehouses,
            setup.load_seed,
            setup_timestamp,
        ));
        let exec = executor::BenchmarkExecutor::new(
            config,
            effective,
            dataset.runtime_schema.clone(),
            setup_generator,
        );
        let result = exec.run().await?;
        store.complete_rank_with_terminal_evidence(
            &dataset,
            &contract,
            claim,
            result.terminal_evidence(),
        )?;
        info!(
            "已原子保存有界 terminal evidence: {}",
            store.root().join("terminal_evidence.state").display()
        );
        result.print_report();
    }

    Ok(())
}

fn run_id_for_seed(seed: u64) -> String {
    std::env::var("RMDB_TPCC_RUN_ID").unwrap_or_else(|_| format!("local-{seed}"))
}

fn load_bound_state(
    config: &Config,
    effective: &ResolvedProfile,
) -> Result<(run_state::StateStore, run_state::DatasetState, RunContract), Box<dyn std::error::Error>>
{
    let store = run_state::StateStore::open_existing_terminal(
        config
            .state_dir
            .as_deref()
            .ok_or("validated stateful phase lost its state directory")?,
    )?;
    let contract = run_contract(config, effective);
    let dataset = store.load_bound_dataset(&contract)?;
    let seed_mismatch = effective.seed.is_some_and(|seed| dataset.seed != seed);
    let run_id_mismatch = std::env::var("RMDB_TPCC_RUN_ID")
        .ok()
        .is_some_and(|run_id| dataset.run_id != run_id);
    let expected_schema_mode = if config.canonical_schema {
        runtime_schema::SchemaMode::Canonical
    } else {
        runtime_schema::SchemaMode::LocalSeedOpaqueV1
    };
    let schema_mismatch = dataset.runtime_schema.mode() != expected_schema_mode;
    if dataset.warehouses != config.scale_factor
        || seed_mismatch
        || run_id_mismatch
        || schema_mismatch
    {
        return Err(format!(
            "run state mismatch: state run/seed/SF/schema={}/{}/{}/{}, CLI run/seed/SF/schema={:?}/{:?}/{}/{}",
            dataset.run_id,
            dataset.seed,
            dataset.warehouses,
            dataset.runtime_schema.mode().as_str(),
            std::env::var("RMDB_TPCC_RUN_ID").ok(),
            effective.seed,
            config.scale_factor,
            expected_schema_mode.as_str()
        )
        .into());
    }
    Ok((store, dataset, contract))
}

fn run_contract(config: &Config, effective: &ResolvedProfile) -> RunContract {
    let profile = &effective.final2026;
    RunContract {
        warehouses: profile.warehouses,
        clients: profile.clients,
        warmup_seconds: profile.warmup.as_secs(),
        measurement_windows: profile.measurement_windows,
        window_seconds: profile.measurement_window.as_secs(),
        load_budget_seconds: profile.load_budget.as_secs(),
        recovery_ready_budget_seconds: profile.recovery_ready_budget.as_secs(),
        response_timeout_seconds: config.response_timeout_seconds,
        phase_tail_grace_seconds: config.phase_tail_grace_seconds,
        conformance: if effective.is_ranked_configuration() {
            RunConformance::PublicSpecAligned
        } else {
            RunConformance::NonRankedDeviation
        },
    }
}

fn control_response_timeout(config: &Config, effective: &ResolvedProfile) -> Duration {
    if config.create_schema || config.init {
        // The public setup contract has one absolute 900-second SQL budget but
        // does not publish a stricter per-request response deadline.  The
        // outer `setup_step` deadline below remains the single authority.
        effective.final2026.load_budget
    } else {
        Duration::from_secs(config.response_timeout_seconds)
    }
}

async fn setup_step<T, F>(
    deadline: Option<tokio::time::Instant>,
    context: &'static str,
    future: F,
) -> Result<T, TpccError>
where
    F: Future<Output = Result<T, TpccError>>,
{
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, future)
            .await
            .map_err(|_| TpccError::Timeout {
                context: format!(
                    "final2026 setup exceeded the public {}s budget while attempting {context}",
                    profile::LOAD_BUDGET_SECONDS
                ),
            })?,
        None => future.await,
    }
}

#[cfg(test)]
mod main_tests {
    use super::*;

    #[test]
    fn setup_uses_the_public_absolute_budget_as_its_response_ceiling() {
        let config = Config::try_parse_from([
            "tpcc-tester",
            "--create-schema",
            "--init",
            "--seed",
            "73",
            "--state-dir",
            "/tmp/tpcc-final2026-setup-timeout",
        ])
        .unwrap();
        let effective = config.resolved_profile().unwrap();

        assert_eq!(
            control_response_timeout(&config, &effective),
            effective.final2026.load_budget
        );
        assert!(
            control_response_timeout(&config, &effective)
                > Duration::from_secs(config.response_timeout_seconds)
        );
    }

    #[test]
    fn non_setup_control_requests_keep_the_local_response_deadline() {
        let config = Config::try_parse_from([
            "tpcc-tester",
            "--check",
            "--state-dir",
            "/tmp/tpcc-final2026-check-timeout",
            "--response-timeout-seconds",
            "17",
        ])
        .unwrap();
        let effective = config.resolved_profile().unwrap();

        assert_eq!(
            control_response_timeout(&config, &effective),
            Duration::from_secs(17)
        );
    }
}

fn print_effective_profile(config: &Config, effective: &ResolvedProfile) {
    let profile = &effective.final2026;
    let mix = profile::TRANSACTION_MIX
        .iter()
        .map(|(_, weight)| weight.to_string())
        .collect::<Vec<_>>()
        .join("/");
    let seed = effective
        .seed
        .map(|value| format!("{value} (caller-supplied)"))
        .unwrap_or_else(|| "not supplied (no hidden seed assumed)".to_owned());

    info!(
        "Effective profile: name={}, warehouses={}, clients={}, warmup={}s, windows={}x{}s, derived_write_ratio=0.92, mix={}, seed={}",
        config.profile.as_str(),
        profile.warehouses,
        profile.clients,
        profile.warmup.as_secs(),
        profile.measurement_windows,
        profile.measurement_window.as_secs(),
        mix,
        seed
    );

    if config.diagnostic_workload_seconds.is_some() {
        info!(
            "Effective execution conformance: explicit non_ranked diagnostic workload \
             (ranking terminal evidence/baseline disabled)"
        );
    } else if effective.is_ranked_configuration() {
        info!("Effective profile conformance: public-spec ranked configuration");
    } else {
        warn!("Effective profile conformance: NON-RANKED (--allow-deviation)");
        for deviation in profile.deviations() {
            warn!(
                "  deviation {}: official={}, effective={}",
                deviation.field, deviation.official, deviation.effective
            );
        }
        for deviation in effective.extra_deviations() {
            warn!(
                "  deviation {}: official={}, effective={}",
                deviation.field, deviation.official, deviation.effective
            );
        }
    }
}

async fn run_diagnose(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    info!("========================================");
    info!("   RMDB 兼容性诊断");
    info!("========================================");

    // 1. Connection test
    info!("[1/6] 连接测试");
    let client = match RmdbClient::connect_with_timeout(
        &config.host,
        config.port,
        Duration::from_secs(config.response_timeout_seconds),
    )
    .await
    {
        Ok(c) => {
            info!("  连接成功: {}:{}", config.host, config.port);
            c
        }
        Err(e) => {
            error!("  连接失败: {e}");
            return Err(e.into());
        }
    };
    let mut cursor = RmdbCursor::new(client);
    let mut failures = 0_usize;

    // 2. Basic SQL
    info!("[2/6] 基础 SQL 能力检测");
    let tests = vec![
        (
            "CREATE TABLE",
            "CREATE TABLE diagtest (id int, name char(10), val float)",
        ),
        ("INSERT", "INSERT INTO diagtest VALUES (1, 'hello', 3.14)"),
        ("SELECT", "SELECT * FROM diagtest WHERE id = 1"),
        ("UPDATE", "UPDATE diagtest SET val = 2.71 WHERE id = 1"),
        ("DELETE", "DELETE FROM diagtest WHERE id = 1"),
    ];

    for (name, sql) in &tests {
        match cursor.execute_update(sql, &[]).await {
            Ok(_) => info!("  {name:>15}: PASS"),
            Err(e) => {
                failures += 1;
                error!("  {name:>15}: FAIL - {e}");
            }
        }
    }

    // 3. Transaction support
    info!("[3/6] 事务支持检测");
    for cmd in &[
        "BEGIN",
        "INSERT INTO diagtest VALUES (2, 'txn', 1.0)",
        "COMMIT",
    ] {
        match cursor.execute_update(cmd, &[]).await {
            Ok(_) => info!("  {:>15}: PASS", *cmd),
            Err(e) => {
                failures += 1;
                error!("  {:>15}: FAIL - {e}", *cmd);
            }
        }
    }

    for cmd in &[
        "BEGIN",
        "INSERT INTO diagtest VALUES (3, 'rb', 1.0)",
        "ROLLBACK",
    ] {
        match cursor.execute_update(cmd, &[]).await {
            Ok(_) => info!("  {:>15}: PASS", *cmd),
            Err(e) => {
                failures += 1;
                error!("  {:>15}: FAIL - {e}", *cmd);
            }
        }
    }

    // 4. Aggregate functions
    info!("[4/6] 聚合函数支持检测");
    // Insert some test data
    for i in 1..=5 {
        let sql = format!("INSERT INTO diagtest VALUES ({i}, 'agg', {}.0)", i * 10);
        let _ = cursor.execute_update(&sql, &[]).await;
    }

    let agg_tests = vec![
        ("COUNT(*)", "SELECT COUNT(*) FROM diagtest"),
        ("COUNT(col)", "SELECT COUNT(name) FROM diagtest"),
        ("SUM", "SELECT SUM(val) FROM diagtest"),
        ("MIN", "SELECT MIN(val) FROM diagtest"),
        ("MAX", "SELECT MAX(val) FROM diagtest"),
    ];

    for (name, sql) in &agg_tests {
        match cursor.execute(sql, &[]).await {
            Ok(r) if !r.is_empty() => info!("  {name:>15}: PASS (result={})", r.rows[0][0]),
            Ok(_) => warn!("  {name:>15}: WARN - 返回空结果 (rmdb 可能不支持此功能)"),
            Err(e) => {
                failures += 1;
                error!("  {name:>15}: FAIL - {e}");
            }
        }
    }

    // 5. ORDER BY / LIMIT
    info!("[5/6] ORDER BY / LIMIT 支持检测");
    let order_tests = vec![
        ("ORDER BY ASC", "SELECT * FROM diagtest ORDER BY id"),
        ("ORDER BY DESC", "SELECT * FROM diagtest ORDER BY id DESC"),
        ("LIMIT", "SELECT * FROM diagtest ORDER BY id DESC LIMIT 1"),
    ];

    for (name, sql) in &order_tests {
        match cursor.execute(sql, &[]).await {
            Ok(r) if !r.is_empty() => info!("  {name:>15}: PASS ({} rows)", r.len()),
            Ok(_) => warn!("  {name:>15}: WARN - 返回空结果 (rmdb 可能不支持此功能)"),
            Err(e) => {
                failures += 1;
                error!("  {name:>15}: FAIL - {e}");
            }
        }
    }

    // 6. Cross-table query
    info!("[6/6] 交叉表查询支持检测");
    let _ = cursor
        .execute_update("CREATE TABLE diagref (id int, refid int)", &[])
        .await;
    let _ = cursor
        .execute_update("INSERT INTO diagref VALUES (1, 1)", &[])
        .await;

    match cursor
        .execute(
            "SELECT * FROM diagtest, diagref WHERE diagtest.id = diagref.refid",
            &[],
        )
        .await
    {
        Ok(r) if !r.is_empty() => info!("  交叉表查询: PASS ({} rows)", r.len()),
        Ok(_) => warn!("  交叉表查询: WARN - 返回空结果"),
        Err(e) => {
            failures += 1;
            error!("  交叉表查询: FAIL - {e}");
        }
    }

    // Cleanup - best effort
    let _ = cursor.execute_update("DROP TABLE diagtest", &[]).await;
    let _ = cursor.execute_update("DROP TABLE diagref", &[]).await;

    info!("========================================");
    info!("   诊断完成");
    info!("========================================");

    if failures == 0 {
        Ok(())
    } else {
        Err(format!("兼容性诊断有 {failures} 项 SQL 操作失败").into())
    }
}
