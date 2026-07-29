use clap::{Parser, ValueEnum};
use std::path::PathBuf;

use crate::profile::{Final2026Profile, ProfileError, SmokeOverrides};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ProfileName {
    #[value(name = "final2026")]
    Final2026,
}

impl ProfileName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Final2026 => "final2026",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CheckScope {
    Setup,
    Online,
    Recovery,
}

impl CheckScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Setup => "setup",
            Self::Online => "online",
            Self::Recovery => "recovery",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LifecycleEvent {
    SetupIntent,
    CrashIntent,
    CrashKilled,
    RestartStarted,
    RestartReady,
}

impl LifecycleEvent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SetupIntent => "setup-intent",
            Self::CrashIntent => "crash-intent",
            Self::CrashKilled => "crash-killed",
            Self::RestartStarted => "restart-started",
            Self::RestartReady => "restart-ready",
        }
    }
}

pub const DIAGNOSTIC_WARMUP_SECONDS: u64 = 10;
pub const DIAGNOSTIC_OBSERVATION_SECONDS: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DiagnosticSegment {
    Warmup,
    Observation,
}

impl DiagnosticSegment {
    pub const fn duration_seconds(self) -> u64 {
        match self {
            Self::Warmup => DIAGNOSTIC_WARMUP_SECONDS,
            Self::Observation => DIAGNOSTIC_OBSERVATION_SECONDS,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warmup => "warmup",
            Self::Observation => "observation",
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(name = "tpcc-tester", about = "TPC-C Benchmark Tool for RMDB")]
pub struct Config {
    /// Public final-round profile
    #[arg(long, value_enum, default_value_t = ProfileName::Final2026)]
    pub profile: ProfileName,

    /// Explicitly allow a non-ranked smoke configuration
    #[arg(long)]
    pub allow_deviation: bool,

    /// Caller-provided deterministic seed (the grader seed is not embedded)
    #[arg(long)]
    pub seed: Option<u64>,

    /// Use the public logical TPC-C identifiers (non-ranked compatibility mode)
    #[arg(long = "canonical-schema")]
    pub canonical_schema: bool,

    /// Scale factor / 仓库数量
    #[arg(short = 's', long = "scale", default_value_t = 50)]
    pub scale_factor: i32,

    /// RMDB 服务地址
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// RMDB 服务端口
    #[arg(long, default_value_t = 8765)]
    pub port: u16,

    /// 创建 TPC-C 表和索引
    #[arg(long = "create-schema")]
    pub create_schema: bool,

    /// 加载 TPC-C 初始数据
    #[arg(long)]
    pub init: bool,

    /// 运行一致性检查
    #[arg(long)]
    pub check: bool,

    /// Select the public consistency-check phase
    #[arg(long = "check-scope", value_enum, default_value_t = CheckScope::Setup)]
    pub check_scope: CheckScope,

    /// 一致性检查期望已提交 NewOrder 数
    #[arg(long = "expected-new-orders")]
    pub expected_new_orders: Option<i64>,

    /// 显示各表行数统计
    #[arg(long)]
    pub stats: bool,

    /// 运行并发基准测试
    #[arg(long)]
    pub benchmark: bool,

    /// Run one explicitly non-ranked final2026 diagnostic workload phase
    #[arg(long = "diagnostic-workload-seconds")]
    pub diagnostic_workload_seconds: Option<u64>,

    /// Select the public 10-second warmup or 60-second observation segment
    #[arg(long = "diagnostic-segment", value_enum)]
    pub diagnostic_segment: Option<DiagnosticSegment>,

    /// Complete a Wire v3 handshake and exact `show tables;` request, then exit
    #[arg(long = "probe-ready")]
    pub probe_ready: bool,

    /// Persist one write-once workflow lifecycle transition, then exit
    #[arg(long = "lifecycle-event", value_enum)]
    pub lifecycle_event: Option<LifecycleEvent>,

    /// 并发客户端数 (`--threads` is retained as a compatibility alias)
    #[arg(long = "clients", visible_alias = "threads", default_value_t = 32)]
    pub threads: usize,

    /// Warmup duration in seconds
    #[arg(long = "warmup-seconds")]
    pub warmup_seconds: Option<u64>,

    /// Duration of each of the three measurement windows in seconds
    #[arg(long = "window-seconds")]
    pub window_seconds: Option<u64>,

    /// Local socket-response safety deadline; the official value is unpublished
    #[arg(long = "response-timeout-seconds", default_value_t = 30)]
    pub response_timeout_seconds: u64,

    /// Local grace for a response already in flight at a phase boundary
    #[arg(long = "phase-tail-grace-seconds", default_value_t = 5)]
    pub phase_tail_grace_seconds: u64,

    /// Public recovery restart/readiness budget; workflow must pass its effective value
    #[arg(
        long = "recovery-ready-budget-seconds",
        visible_alias = "ready-timeout-seconds",
        default_value_t = crate::profile::RECOVERY_READY_BUDGET_SECONDS
    )]
    pub recovery_ready_budget_seconds: u64,

    /// Run-owned directory for dataset, commit-ledger, and crash baselines
    #[arg(long = "state-dir")]
    pub state_dir: Option<PathBuf>,

    /// 运行数据库兼容性诊断
    #[arg(long)]
    pub diagnose: bool,

    /// 详细日志 (-v=DEBUG, -vv=TRACE)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EffectiveDeviation {
    pub field: &'static str,
    pub official: String,
    pub effective: String,
}

#[derive(Debug, Clone)]
pub struct ResolvedProfile {
    pub final2026: Final2026Profile,
    pub seed: Option<u64>,
    extra_deviations: Vec<EffectiveDeviation>,
}

impl ResolvedProfile {
    pub fn is_ranked_configuration(&self) -> bool {
        self.final2026.is_ranked_configuration() && self.extra_deviations.is_empty()
    }

    pub fn extra_deviations(&self) -> &[EffectiveDeviation] {
        &self.extra_deviations
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{field} must be in {range}")]
    OutOfRange {
        field: &'static str,
        range: &'static str,
    },

    #[error("invalid final2026 profile: {0}")]
    Profile(#[from] ProfileError),

    #[error("non-ranked overrides require --allow-deviation; differing fields: {fields}")]
    DeviationRequiresOptIn { fields: String },

    #[error(
        "--seed is required for --create-schema, --init, --benchmark, \
         --diagnostic-workload-seconds, and --lifecycle-event \
         (no grader seed is embedded)"
    )]
    MissingSeed,

    #[error(
        "--state-dir is required for init, benchmark, diagnostic workload, online check, \
         recovery check, and lifecycle events"
    )]
    MissingStateDir,

    #[error("--probe-ready must be used by itself")]
    ProbeReadyMustBeExclusive,

    #[error("--lifecycle-event must be used by itself")]
    LifecycleEventMustBeExclusive,

    #[error("--init --check may only use --check-scope setup")]
    InitCheckScopeMustBeSetup,

    #[error("--diagnose must be used by itself")]
    DiagnoseMustBeExclusive,

    #[error("--diagnostic-workload-seconds must be used by itself")]
    DiagnosticWorkloadMustBeExclusive,

    #[error("--diagnostic-workload-seconds and --diagnostic-segment must be supplied together")]
    DiagnosticSegmentMustMatchWorkload,

    #[error(
        "diagnostic {segment} must run for exactly {expected_seconds}s, got {actual_seconds}s"
    )]
    DiagnosticDurationMismatch {
        segment: &'static str,
        expected_seconds: u64,
        actual_seconds: u64,
    },

    #[error(
        "final2026 diagnostic workload requires exactly 50 warehouses and 32 clients; \
         it is non-ranked but retains the published workload shape"
    )]
    DiagnosticWorkloadRequiresOfficialShape,
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_raw()?;
        let diagnostic_workload = self.diagnostic_workload_seconds.is_some();
        if diagnostic_workload != self.diagnostic_segment.is_some() {
            return Err(ConfigError::DiagnosticSegmentMustMatchWorkload);
        }
        if let (Some(actual_seconds), Some(segment)) =
            (self.diagnostic_workload_seconds, self.diagnostic_segment)
        {
            let expected_seconds = segment.duration_seconds();
            if actual_seconds != expected_seconds {
                return Err(ConfigError::DiagnosticDurationMismatch {
                    segment: segment.as_str(),
                    expected_seconds,
                    actual_seconds,
                });
            }
        }
        if self.diagnose
            && (self.create_schema
                || self.init
                || self.check
                || self.stats
                || self.benchmark
                || diagnostic_workload
                || self.probe_ready
                || self.lifecycle_event.is_some()
                || self.allow_deviation
                || self.seed.is_some()
                || self.canonical_schema
                || self.expected_new_orders.is_some()
                || self.warmup_seconds.is_some()
                || self.window_seconds.is_some()
                || self.diagnostic_segment.is_some()
                || self.state_dir.is_some()
                || self.check_scope != CheckScope::Setup)
        {
            return Err(ConfigError::DiagnoseMustBeExclusive);
        }
        if diagnostic_workload
            && (self.create_schema
                || self.init
                || self.check
                || self.stats
                || self.benchmark
                || self.probe_ready
                || self.lifecycle_event.is_some()
                || self.diagnose
                || self.canonical_schema
                || self.expected_new_orders.is_some()
                || self.warmup_seconds.is_some()
                || self.window_seconds.is_some()
                || self.check_scope != CheckScope::Setup)
        {
            return Err(ConfigError::DiagnosticWorkloadMustBeExclusive);
        }
        if diagnostic_workload
            && (self.scale_factor != i32::from(crate::profile::OFFICIAL_WAREHOUSES)
                || self.threads != usize::from(crate::profile::OFFICIAL_CLIENTS))
        {
            return Err(ConfigError::DiagnosticWorkloadRequiresOfficialShape);
        }

        if self.lifecycle_event.is_some()
            && (self.create_schema
                || self.init
                || self.check
                || self.stats
                || self.benchmark
                || diagnostic_workload
                || self.probe_ready
                || self.diagnose
                || self.expected_new_orders.is_some()
                || self.diagnostic_segment.is_some()
                || self.check_scope != CheckScope::Setup)
        {
            return Err(ConfigError::LifecycleEventMustBeExclusive);
        }
        if self.init && self.check && self.check_scope != CheckScope::Setup {
            return Err(ConfigError::InitCheckScopeMustBeSetup);
        }

        if (self.create_schema
            || self.init
            || self.benchmark
            || diagnostic_workload
            || self.lifecycle_event.is_some())
            && self.seed.is_none()
        {
            return Err(ConfigError::MissingSeed);
        }
        if (self.init
            || self.benchmark
            || self.check
            || diagnostic_workload
            || self.lifecycle_event.is_some())
            && self.state_dir.is_none()
        {
            return Err(ConfigError::MissingStateDir);
        }
        self.resolved_profile()?;

        if self.probe_ready
            && (self.create_schema
                || self.init
                || self.check
                || self.stats
                || self.benchmark
                || diagnostic_workload
                || self.lifecycle_event.is_some()
                || self.diagnose)
        {
            return Err(ConfigError::ProbeReadyMustBeExclusive);
        }

        Ok(())
    }

    pub fn resolved_profile(&self) -> Result<ResolvedProfile, ConfigError> {
        self.validate_raw()?;

        let final2026 = Final2026Profile::smoke(SmokeOverrides {
            warehouses: Some(self.scale_factor as u16),
            clients: Some(self.threads as u16),
            warmup_seconds: self.warmup_seconds,
            measurement_seconds: self.window_seconds,
            recovery_ready_budget_seconds: Some(self.recovery_ready_budget_seconds),
            ..SmokeOverrides::default()
        })?;

        let mut extra_deviations: Vec<EffectiveDeviation> = Vec::new();
        if self.create_schema && !self.init {
            extra_deviations.push(EffectiveDeviation {
                field: "setup_actions",
                official: "--create-schema --init".to_owned(),
                effective: "--create-schema without --init".to_owned(),
            });
        }
        if self.canonical_schema {
            extra_deviations.push(EffectiveDeviation {
                field: "runtime_schema",
                official: "local_seed_opaque_v1".to_owned(),
                effective: "canonical".to_owned(),
            });
        }

        let mut differing_fields = final2026
            .deviations()
            .iter()
            .map(|deviation| deviation.field)
            .chain(extra_deviations.iter().map(|deviation| deviation.field))
            .collect::<Vec<_>>();
        differing_fields.sort_unstable();
        differing_fields.dedup();

        if !differing_fields.is_empty() && !self.allow_deviation {
            return Err(ConfigError::DeviationRequiresOptIn {
                fields: differing_fields.join(", "),
            });
        }

        Ok(ResolvedProfile {
            final2026,
            seed: self.seed,
            extra_deviations,
        })
    }

    fn validate_raw(&self) -> Result<(), ConfigError> {
        if !(1..=i32::from(u16::MAX)).contains(&self.scale_factor) {
            return Err(ConfigError::OutOfRange {
                field: "scale",
                range: "1..=65535",
            });
        }
        if self.threads == 0 || self.threads > usize::from(u16::MAX) {
            return Err(ConfigError::OutOfRange {
                field: "clients",
                range: "1..=65535",
            });
        }
        if self.response_timeout_seconds == 0 {
            return Err(ConfigError::OutOfRange {
                field: "response-timeout-seconds",
                range: "1..",
            });
        }
        if self.phase_tail_grace_seconds == 0 {
            return Err(ConfigError::OutOfRange {
                field: "phase-tail-grace-seconds",
                range: "1..",
            });
        }
        if self.recovery_ready_budget_seconds == 0 {
            return Err(ConfigError::OutOfRange {
                field: "recovery-ready-budget-seconds",
                range: "1..",
            });
        }
        if self.diagnostic_workload_seconds == Some(0) {
            return Err(ConfigError::OutOfRange {
                field: "diagnostic-workload-seconds",
                range: "1..",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::{
        Conformance, MEASUREMENT_SECONDS, OFFICIAL_CLIENTS, OFFICIAL_WAREHOUSES,
        RECOVERY_READY_BUDGET_SECONDS, WARMUP_SECONDS,
    };

    #[test]
    fn final_profile_defaults_resolve_to_ranked_public_values() {
        let config = Config::try_parse_from([
            "tpcc-tester",
            "--profile",
            "final2026",
            "--benchmark",
            "--seed",
            "73",
            "--state-dir",
            "/tmp/tpcc-final2026-test-state",
        ])
        .unwrap();
        config.validate().unwrap();
        let resolved = config.resolved_profile().unwrap();

        assert_eq!(resolved.final2026.warehouses, OFFICIAL_WAREHOUSES);
        assert_eq!(resolved.final2026.clients, OFFICIAL_CLIENTS);
        assert_eq!(resolved.final2026.warmup.as_secs(), WARMUP_SECONDS);
        assert_eq!(
            resolved.final2026.measurement_window.as_secs(),
            MEASUREMENT_SECONDS
        );
        assert_eq!(
            resolved.final2026.recovery_ready_budget.as_secs(),
            RECOVERY_READY_BUDGET_SECONDS
        );
        assert_eq!(resolved.final2026.conformance(), Conformance::Official);
        assert_eq!(resolved.seed, Some(73));
        assert!(resolved.is_ranked_configuration());
    }

    #[test]
    fn deviations_require_opt_in_and_are_resolved_non_ranked() {
        let rejected = Config::try_parse_from([
            "tpcc-tester",
            "--benchmark",
            "--seed",
            "1",
            "--scale",
            "2",
            "--clients",
            "3",
            "--warmup-seconds",
            "0",
            "--window-seconds",
            "5",
            "--state-dir",
            "/tmp/tpcc-final2026-smoke-state",
        ])
        .unwrap();
        assert!(matches!(
            rejected.validate(),
            Err(ConfigError::DeviationRequiresOptIn { .. })
        ));

        let accepted = Config {
            allow_deviation: true,
            ..rejected
        };
        accepted.validate().unwrap();
        let resolved = accepted.resolved_profile().unwrap();
        assert!(!resolved.is_ranked_configuration());
        assert_eq!(resolved.final2026.warehouses, 2);
        assert_eq!(resolved.final2026.clients, 3);
        assert_eq!(resolved.final2026.warmup.as_secs(), 0);
        assert_eq!(resolved.final2026.measurement_window.as_secs(), 5);
    }

    #[test]
    fn recovery_ready_budget_override_is_explicitly_non_ranked() {
        let rejected = Config::try_parse_from([
            "tpcc-tester",
            "--benchmark",
            "--seed",
            "1",
            "--state-dir",
            "/tmp/tpcc-final2026-ready-state",
            "--recovery-ready-budget-seconds",
            "30",
        ])
        .unwrap();
        assert!(matches!(
            rejected.validate(),
            Err(ConfigError::DeviationRequiresOptIn { .. })
        ));

        let accepted = Config {
            allow_deviation: true,
            ..rejected
        };
        accepted.validate().unwrap();
        let resolved = accepted.resolved_profile().unwrap();
        assert_eq!(resolved.final2026.recovery_ready_budget.as_secs(), 30);
        assert!(!resolved.is_ranked_configuration());
    }

    #[test]
    fn threads_alias_and_check_scope_are_supported() {
        let config = Config::try_parse_from([
            "tpcc-tester",
            "--allow-deviation",
            "--threads",
            "2",
            "--check",
            "--check-scope",
            "recovery",
            "--state-dir",
            "/tmp/tpcc-final2026-recovery-state",
        ])
        .unwrap();

        assert_eq!(config.threads, 2);
        assert_eq!(config.check_scope, CheckScope::Recovery);
        config.validate().unwrap();
    }

    #[test]
    fn setup_and_ranked_actions_never_guess_the_hidden_seed() {
        for action in ["--create-schema", "--init", "--benchmark"] {
            let config = Config::try_parse_from(["tpcc-tester", action]).unwrap();
            assert!(matches!(config.validate(), Err(ConfigError::MissingSeed)));
        }
    }

    #[test]
    fn canonical_schema_is_an_explicit_non_ranked_deviation() {
        let rejected = Config::try_parse_from([
            "tpcc-tester",
            "--benchmark",
            "--seed",
            "73",
            "--canonical-schema",
            "--state-dir",
            "/tmp/tpcc-final2026-canonical-state",
        ])
        .unwrap();
        assert!(matches!(
            rejected.validate(),
            Err(ConfigError::DeviationRequiresOptIn { .. })
        ));

        let accepted = Config {
            allow_deviation: true,
            ..rejected
        };
        accepted.validate().unwrap();
        let resolved = accepted.resolved_profile().unwrap();
        assert!(!resolved.is_ranked_configuration());
        assert_eq!(
            resolved.extra_deviations(),
            &[EffectiveDeviation {
                field: "runtime_schema",
                official: "local_seed_opaque_v1".to_owned(),
                effective: "canonical".to_owned(),
            }]
        );
    }

    #[test]
    fn stateful_phases_require_an_explicit_run_owned_state_directory() {
        let benchmark =
            Config::try_parse_from(["tpcc-tester", "--benchmark", "--seed", "7"]).unwrap();
        assert!(matches!(
            benchmark.validate(),
            Err(ConfigError::MissingStateDir)
        ));

        let recovery =
            Config::try_parse_from(["tpcc-tester", "--check", "--check-scope", "recovery"])
                .unwrap();
        assert!(matches!(
            recovery.validate(),
            Err(ConfigError::MissingStateDir)
        ));
    }

    #[test]
    fn probe_ready_is_exclusive() {
        let config = Config::try_parse_from(["tpcc-tester", "--probe-ready", "--stats"]).unwrap();
        assert!(matches!(
            config.validate(),
            Err(ConfigError::ProbeReadyMustBeExclusive)
        ));
    }

    #[test]
    fn lifecycle_events_are_exclusive_seeded_and_state_bound() {
        for event in [
            "setup-intent",
            "crash-intent",
            "crash-killed",
            "restart-started",
            "restart-ready",
        ] {
            let missing_seed = Config::try_parse_from([
                "tpcc-tester",
                "--lifecycle-event",
                event,
                "--state-dir",
                "/tmp/tpcc-final2026-lifecycle-state",
            ])
            .unwrap();
            assert!(matches!(
                missing_seed.validate(),
                Err(ConfigError::MissingSeed)
            ));

            let missing_state =
                Config::try_parse_from(["tpcc-tester", "--lifecycle-event", event, "--seed", "11"])
                    .unwrap();
            assert!(matches!(
                missing_state.validate(),
                Err(ConfigError::MissingStateDir)
            ));

            let valid = Config::try_parse_from([
                "tpcc-tester",
                "--lifecycle-event",
                event,
                "--seed",
                "11",
                "--state-dir",
                "/tmp/tpcc-final2026-lifecycle-state",
            ])
            .unwrap();
            valid.validate().unwrap();
        }

        let combined = Config::try_parse_from([
            "tpcc-tester",
            "--lifecycle-event",
            "crash-intent",
            "--benchmark",
            "--seed",
            "11",
            "--state-dir",
            "/tmp/tpcc-final2026-lifecycle-state",
        ])
        .unwrap();
        assert!(matches!(
            combined.validate(),
            Err(ConfigError::LifecycleEventMustBeExclusive)
        ));
    }

    #[test]
    fn standalone_formal_schema_creation_is_an_explicit_deviation() {
        let formal =
            Config::try_parse_from(["tpcc-tester", "--create-schema"]).expect("valid CLI syntax");
        assert!(matches!(
            formal.validate(),
            Err(ConfigError::DeviationRequiresOptIn { .. })
        ));

        let local = Config {
            allow_deviation: true,
            ..formal
        };
        local.validate().unwrap();
        assert!(!local.resolved_profile().unwrap().is_ranked_configuration());
    }

    #[test]
    fn init_can_only_combine_with_the_setup_check() {
        let config = Config::try_parse_from([
            "tpcc-tester",
            "--init",
            "--check",
            "--check-scope",
            "recovery",
            "--seed",
            "11",
            "--state-dir",
            "/tmp/tpcc-final2026-init-check-state",
        ])
        .unwrap();
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InitCheckScopeMustBeSetup)
        ));
    }

    #[test]
    fn diagnose_is_exclusive() {
        for action in ["--benchmark", "--init", "--stats", "--probe-ready"] {
            let config = Config::try_parse_from(["tpcc-tester", "--diagnose", action]).unwrap();
            assert!(matches!(
                config.validate(),
                Err(ConfigError::DiagnoseMustBeExclusive)
            ));
        }
    }

    #[test]
    fn diagnostic_workload_is_explicit_non_ranked_and_state_bound() {
        let missing_state = Config::try_parse_from([
            "tpcc-tester",
            "--diagnostic-workload-seconds",
            "10",
            "--diagnostic-segment",
            "warmup",
            "--seed",
            "7",
        ])
        .unwrap();
        assert!(matches!(
            missing_state.validate(),
            Err(ConfigError::MissingStateDir)
        ));

        let config = Config::try_parse_from([
            "tpcc-tester",
            "--diagnostic-workload-seconds",
            "60",
            "--diagnostic-segment",
            "observation",
            "--seed",
            "7",
            "--state-dir",
            "/tmp/tpcc-final2026-diagnostic-state",
        ])
        .unwrap();
        config.validate().unwrap();
        assert_eq!(config.diagnostic_workload_seconds, Some(60));
        assert_eq!(
            config.diagnostic_segment,
            Some(DiagnosticSegment::Observation)
        );
    }

    #[test]
    fn diagnostic_workload_rejects_ranked_actions_and_shape_deviations() {
        let combined = Config::try_parse_from([
            "tpcc-tester",
            "--diagnostic-workload-seconds",
            "10",
            "--diagnostic-segment",
            "warmup",
            "--benchmark",
            "--seed",
            "7",
            "--state-dir",
            "/tmp/tpcc-final2026-diagnostic-state",
        ])
        .unwrap();
        assert!(matches!(
            combined.validate(),
            Err(ConfigError::DiagnosticWorkloadMustBeExclusive)
        ));

        let wrong_clients = Config::try_parse_from([
            "tpcc-tester",
            "--diagnostic-workload-seconds",
            "10",
            "--diagnostic-segment",
            "warmup",
            "--clients",
            "1",
            "--allow-deviation",
            "--seed",
            "7",
            "--state-dir",
            "/tmp/tpcc-final2026-diagnostic-state",
        ])
        .unwrap();
        assert!(matches!(
            wrong_clients.validate(),
            Err(ConfigError::DiagnosticWorkloadRequiresOfficialShape)
        ));

        let ignored_ranked_timing = Config::try_parse_from([
            "tpcc-tester",
            "--diagnostic-workload-seconds",
            "10",
            "--diagnostic-segment",
            "warmup",
            "--warmup-seconds",
            "30",
            "--seed",
            "7",
            "--state-dir",
            "/tmp/tpcc-final2026-diagnostic-state",
        ])
        .unwrap();
        assert!(matches!(
            ignored_ranked_timing.validate(),
            Err(ConfigError::DiagnosticWorkloadMustBeExclusive)
        ));
    }

    #[test]
    fn diagnostic_segments_are_explicit_and_have_fixed_public_durations() {
        let state = "/tmp/tpcc-final2026-diagnostic-state";
        for args in [
            vec![
                "tpcc-tester",
                "--diagnostic-workload-seconds",
                "10",
                "--seed",
                "7",
                "--state-dir",
                state,
            ],
            vec![
                "tpcc-tester",
                "--diagnostic-segment",
                "warmup",
                "--seed",
                "7",
                "--state-dir",
                state,
            ],
        ] {
            let config = Config::try_parse_from(args).unwrap();
            assert!(matches!(
                config.validate(),
                Err(ConfigError::DiagnosticSegmentMustMatchWorkload)
            ));
        }

        for (segment, seconds) in [("warmup", "60"), ("observation", "10")] {
            let config = Config::try_parse_from([
                "tpcc-tester",
                "--diagnostic-workload-seconds",
                seconds,
                "--diagnostic-segment",
                segment,
                "--seed",
                "7",
                "--state-dir",
                state,
            ])
            .unwrap();
            assert!(matches!(
                config.validate(),
                Err(ConfigError::DiagnosticDurationMismatch { .. })
            ));
        }
    }
}
