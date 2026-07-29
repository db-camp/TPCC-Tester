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

    /// Complete a Wire v3 handshake and exact `show tables;` request, then exit
    #[arg(long = "probe-ready")]
    pub probe_ready: bool,

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

    #[error("--seed is required for --init and --benchmark (no grader seed is embedded)")]
    MissingSeed,

    #[error("--state-dir is required for init, benchmark, online check, and recovery check")]
    MissingStateDir,

    #[error("--probe-ready must be used by itself")]
    ProbeReadyMustBeExclusive,
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_raw()?;
        self.resolved_profile()?;

        if (self.init || self.benchmark) && self.seed.is_none() {
            return Err(ConfigError::MissingSeed);
        }
        if (self.init || self.benchmark || (self.check && self.check_scope != CheckScope::Setup))
            && self.state_dir.is_none()
        {
            return Err(ConfigError::MissingStateDir);
        }

        if self.probe_ready
            && (self.create_schema
                || self.init
                || self.check
                || self.stats
                || self.benchmark
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
            ..SmokeOverrides::default()
        })?;

        let extra_deviations: Vec<EffectiveDeviation> = Vec::new();

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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::{
        Conformance, MEASUREMENT_SECONDS, OFFICIAL_CLIENTS, OFFICIAL_WAREHOUSES, WARMUP_SECONDS,
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
    fn init_and_benchmark_never_guess_the_hidden_seed() {
        let config = Config::try_parse_from(["tpcc-tester", "--init"]).unwrap();
        assert!(matches!(config.validate(), Err(ConfigError::MissingSeed)));
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
}
