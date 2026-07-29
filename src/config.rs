use clap::{Parser, ValueEnum};

use crate::profile::{Final2026Profile, ProfileError, SmokeOverrides, TRANSACTION_MIX};

const OFFICIAL_RW_RATIO: f64 = 0.92;

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

    /// 每线程事务数（旧执行器兼容字段）
    #[arg(long, default_value_t = 100)]
    pub transactions: usize,

    /// 读写比例 0.0-1.0（正式配置固定为 0.92）
    #[arg(long = "rw-ratio", default_value_t = OFFICIAL_RW_RATIO)]
    pub rw_ratio: f64,

    /// 事务概率 [NewOrder Payment OrderStatus Delivery StockLevel]
    #[arg(
        long = "txn-probs",
        num_args = 5,
        default_values_t = vec![45.0, 43.0, 4.0, 4.0, 4.0]
    )]
    pub txn_probs: Vec<f64>,

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

        let mut extra_deviations = Vec::new();
        if self.rw_ratio.to_bits() != OFFICIAL_RW_RATIO.to_bits() {
            extra_deviations.push(EffectiveDeviation {
                field: "rw_ratio",
                official: OFFICIAL_RW_RATIO.to_string(),
                effective: self.rw_ratio.to_string(),
            });
        }

        let official_mix: Vec<f64> = TRANSACTION_MIX
            .iter()
            .map(|(_, weight)| f64::from(*weight))
            .collect();
        if self.txn_probs != official_mix {
            extra_deviations.push(EffectiveDeviation {
                field: "txn_probs",
                official: format_mix(&official_mix),
                effective: format_mix(&self.txn_probs),
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
        if self.transactions == 0 {
            return Err(ConfigError::OutOfRange {
                field: "transactions",
                range: "1..",
            });
        }
        if !self.rw_ratio.is_finite() || !(0.0..=1.0).contains(&self.rw_ratio) {
            return Err(ConfigError::OutOfRange {
                field: "rw-ratio",
                range: "0.0..=1.0",
            });
        }
        if self.txn_probs.len() != 5
            || self
                .txn_probs
                .iter()
                .any(|value| !value.is_finite() || *value < 0.0)
            || self.txn_probs.iter().sum::<f64>() <= 0.0
        {
            return Err(ConfigError::OutOfRange {
                field: "txn-probs",
                range: "five finite non-negative weights with a positive sum",
            });
        }
        Ok(())
    }
}

fn format_mix(values: &[f64]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join("/")
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
    fn probe_ready_is_exclusive() {
        let config = Config::try_parse_from(["tpcc-tester", "--probe-ready", "--stats"]).unwrap();
        assert!(matches!(
            config.validate(),
            Err(ConfigError::ProbeReadyMustBeExclusive)
        ));
    }
}
