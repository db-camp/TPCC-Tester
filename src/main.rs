mod checker;
mod config;
mod connection;
mod data_gen;
mod error;
mod executor;
mod loader;
pub mod measurement;
mod model;
mod profile;
mod report;
mod transaction;

use std::process;

use clap::Parser;
use tracing::{error, info, warn};

use config::{Config, ResolvedProfile};
use connection::client::RmdbClient;
use connection::cursor::RmdbCursor;
use connection::wire::StreamResponse;
use error::TpccError;

const SNAPSHOT_ISOLATION_SQL: &str = "SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;";

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
        let mut client = RmdbClient::connect(&config.host, config.port).await?;
        client.ping().await?;
        info!("Wire v3 readiness probe 通过（完整执行 `show tables;`）");
        return Ok(());
    }

    if let Some(seed) = effective.seed {
        std::env::set_var("RMDB_TPCC_SEED", seed.to_string());
    }

    // Diagnose mode
    if config.diagnose {
        return run_diagnose(&config).await;
    }

    let needs_connection =
        config.create_schema || config.init || config.check || config.stats || config.benchmark;

    if !needs_connection {
        info!("用法: tpcc-tester --create-schema | --init | --check | --stats | --benchmark | --diagnose");
        info!("  --create-schema 创建 TPC-C 表和索引");
        info!("  --init          加载 TPC-C 初始数据");
        info!("  --check         运行一致性检查");
        info!("  --stats         显示各表行数统计");
        info!("  --benchmark     运行并发基准测试");
        info!("  --diagnose      运行数据库兼容性诊断");
        info!("  -v / -vv        详细日志");
        return Ok(());
    }

    // Connection pre-check
    info!("连接 RMDB: {}:{} ...", config.host, config.port);
    let client = RmdbClient::connect(&config.host, config.port).await?;
    let mut cursor = RmdbCursor::new(client);
    configure_session(&mut cursor).await?;

    // Ping test
    match cursor.client_mut().ping().await {
        Ok(_) => info!("RMDB 连接正常"),
        Err(e) => {
            warn!("连接预检异常 (不一定致命): {e}");
        }
    }

    // Schema
    if config.create_schema {
        info!("创建 TPC-C 表和索引");
        let mut ldr = loader::Loader::new(&mut cursor, config.scale_factor);
        ldr.create_tables().await?;
        ldr.create_indexes().await?;
        info!("TPC-C 表和索引创建完成");
    }

    // Init data
    if config.init {
        info!("加载 TPC-C 初始数据 (scale_factor={})", config.scale_factor);
        let mut ldr = loader::Loader::new(&mut cursor, config.scale_factor);
        ldr.load_all_data().await?;
        info!("TPC-C 初始数据加载完成");
    }

    // Check
    if config.check {
        info!("运行 {} 阶段一致性检查", config.check_scope.as_str());
        let mut chk = checker::ConsistencyChecker::new(
            &mut cursor,
            config.scale_factor,
            config.expected_new_orders,
        );
        let all_passed = chk.run_all_checks().await?;
        if !all_passed {
            error!("一致性检查未全部通过");
            process::exit(1);
        }
    }

    // Stats
    if config.stats {
        let mut chk = checker::ConsistencyChecker::new(&mut cursor, config.scale_factor, None);
        chk.show_stats().await?;
    }

    // Benchmark
    if config.benchmark {
        info!("启动 TPC-C 基准测试...");
        let exec = executor::BenchmarkExecutor::new(config);
        let result = exec.run().await?;
        result.print_report();
    }

    Ok(())
}

fn print_effective_profile(config: &Config, effective: &ResolvedProfile) {
    let profile = &effective.final2026;
    let mix = config
        .txn_probs
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join("/");
    let seed = effective
        .seed
        .map(|value| format!("{value} (caller-supplied)"))
        .unwrap_or_else(|| "not supplied (no hidden seed assumed)".to_owned());

    info!(
        "Effective profile: name={}, warehouses={}, clients={}, warmup={}s, windows={}x{}s, rw_ratio={}, mix={}, seed={}",
        config.profile.as_str(),
        profile.warehouses,
        profile.clients,
        profile.warmup.as_secs(),
        profile.measurement_windows,
        profile.measurement_window.as_secs(),
        config.rw_ratio,
        mix,
        seed
    );

    if effective.is_ranked_configuration() {
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

async fn configure_session(cursor: &mut RmdbCursor) -> Result<(), TpccError> {
    info!("发送 SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION");
    match cursor
        .client_mut()
        .exec_stream(SNAPSHOT_ISOLATION_SQL)
        .await?
    {
        StreamResponse::CommandOk => Ok(()),
        StreamResponse::TransactionAbort { diagnostic } => Err(TpccError::Abort(diagnostic)),
        StreamResponse::Error { diagnostic } => Err(TpccError::QueryError(format!(
            "设置 SNAPSHOT ISOLATION 失败: {diagnostic}"
        ))),
        StreamResponse::Query { .. } => Err(TpccError::Protocol(
            "SET SNAPSHOT ISOLATION 意外返回查询结果".to_owned(),
        )),
    }
}

async fn run_diagnose(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    info!("========================================");
    info!("   RMDB 兼容性诊断");
    info!("========================================");

    // 1. Connection test
    info!("[1/6] 连接测试");
    let client = match RmdbClient::connect(&config.host, config.port).await {
        Ok(c) => {
            info!("  连接成功: {}:{}", config.host, config.port);
            c
        }
        Err(e) => {
            error!("  连接失败: {e}");
            return Ok(());
        }
    };
    let mut cursor = RmdbCursor::new(client);
    configure_session(&mut cursor).await?;

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
            Err(e) => error!("  {name:>15}: FAIL - {e}"),
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
            Err(e) => error!("  {:>15}: FAIL - {e}", *cmd),
        }
    }

    for cmd in &[
        "BEGIN",
        "INSERT INTO diagtest VALUES (3, 'rb', 1.0)",
        "ROLLBACK",
    ] {
        match cursor.execute_update(cmd, &[]).await {
            Ok(_) => info!("  {:>15}: PASS", *cmd),
            Err(e) => error!("  {:>15}: FAIL - {e}", *cmd),
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
            Err(e) => error!("  {name:>15}: FAIL - {e}"),
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
            Err(e) => error!("  {name:>15}: FAIL - {e}"),
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
        Err(e) => error!("  交叉表查询: FAIL - {e}"),
    }

    // Cleanup - best effort
    let _ = cursor.execute_update("DROP TABLE diagtest", &[]).await;
    let _ = cursor.execute_update("DROP TABLE diagref", &[]).await;

    info!("========================================");
    info!("   诊断完成");
    info!("========================================");

    Ok(())
}
