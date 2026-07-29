//! Public configuration contract for the 2026 final TPC-C run.
//!
//! The official workload seed is hidden by the grader.  This module deliberately
//! contains no guessed seed: callers must supply a seed to the routing module.

use std::error::Error;
use std::fmt;
use std::time::Duration;

pub const OFFICIAL_WAREHOUSES: u16 = 50;
pub const OFFICIAL_CLIENTS: u16 = 32;
pub const WARMUP_SECONDS: u64 = 30;
pub const MEASUREMENT_WINDOWS: u8 = 3;
pub const MEASUREMENT_SECONDS: u64 = 150;
pub const LOAD_BUDGET_SECONDS: u64 = 900;
pub const RECOVERY_READY_BUDGET_SECONDS: u64 = 90;

pub const ROUTING_SLOTS: usize = 160;
pub const ROUTING_WAVES: u64 = 5;
pub const HOT_WAREHOUSES: usize = 4;
pub const HOT_SLOTS_PER_WAREHOUSE: usize = 26;
pub const EXTRA_COLD_WAREHOUSES_PER_STAGE: usize = 10;
pub const DISTRICTS_PER_WAREHOUSE: u8 = 10;
pub const HOT_DISTRICT_PERCENT: u8 = 65;
pub const ITEM_COUNT: u32 = 100_000;
pub const HOT_ITEMS: usize = 24;
pub const HOT_ITEM_PERCENT: u8 = 25;
pub const NEW_ORDER_REMOTE_PERCENT: u8 = 8;
pub const PAYMENT_REMOTE_PERCENT: u8 = 30;

pub const COVERAGE_SAMPLE_THRESHOLD: u64 = 400;
pub const WINDOW_WAREHOUSE_COVERAGE: u16 = 45;
pub const COMBINED_WAREHOUSE_COVERAGE: u16 = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionKind {
    NewOrder,
    Payment,
    OrderStatus,
    Delivery,
    StockLevel,
}

pub const TRANSACTION_MIX: [(TransactionKind, u8); 5] = [
    (TransactionKind::NewOrder, 45),
    (TransactionKind::Payment, 43),
    (TransactionKind::OrderStatus, 4),
    (TransactionKind::Delivery, 4),
    (TransactionKind::StockLevel, 4),
];

/// Maps an unbiased bucket in `0..100` onto the published transaction mix.
pub fn transaction_for_bucket(bucket: u8) -> Option<TransactionKind> {
    if bucket >= 100 {
        return None;
    }

    let mut upper = 0_u8;
    for (kind, weight) in TRANSACTION_MIX {
        upper += weight;
        if bucket < upper {
            return Some(kind);
        }
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverageScope {
    MeasurementWindow,
    CombinedWindows,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoverageRequirement {
    pub minimum_distinct_warehouses: u16,
    pub require_all_hot_warehouses: bool,
}

/// Returns the public coverage gate for a completed-sample count.
///
/// Below 400 completed samples, the warehouse minimum is scaled with a
/// mathematical ceiling and the hot-warehouse requirement is disabled.
pub fn coverage_requirement(scope: CoverageScope, completed_samples: u64) -> CoverageRequirement {
    let full_minimum = match scope {
        CoverageScope::MeasurementWindow => WINDOW_WAREHOUSE_COVERAGE,
        CoverageScope::CombinedWindows => COMBINED_WAREHOUSE_COVERAGE,
    };

    if completed_samples >= COVERAGE_SAMPLE_THRESHOLD {
        return CoverageRequirement {
            minimum_distinct_warehouses: full_minimum,
            require_all_hot_warehouses: true,
        };
    }

    let numerator = u128::from(full_minimum) * u128::from(completed_samples);
    let denominator = u128::from(COVERAGE_SAMPLE_THRESHOLD);
    let scaled = (numerator + denominator - 1) / denominator;
    CoverageRequirement {
        minimum_distinct_warehouses: scaled as u16,
        require_all_hot_warehouses: false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Conformance {
    Official,
    DeviatedSmoke,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deviation {
    pub field: &'static str,
    pub official: u64,
    pub effective: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Final2026Profile {
    pub warehouses: u16,
    pub clients: u16,
    pub warmup: Duration,
    pub measurement_windows: u8,
    pub measurement_window: Duration,
    pub load_budget: Duration,
    pub recovery_ready_budget: Duration,
    deviations: Vec<Deviation>,
}

impl Default for Final2026Profile {
    fn default() -> Self {
        Self::official()
    }
}

impl Final2026Profile {
    pub fn official() -> Self {
        Self {
            warehouses: OFFICIAL_WAREHOUSES,
            clients: OFFICIAL_CLIENTS,
            warmup: Duration::from_secs(WARMUP_SECONDS),
            measurement_windows: MEASUREMENT_WINDOWS,
            measurement_window: Duration::from_secs(MEASUREMENT_SECONDS),
            load_budget: Duration::from_secs(LOAD_BUDGET_SECONDS),
            recovery_ready_budget: Duration::from_secs(RECOVERY_READY_BUDGET_SECONDS),
            deviations: Vec::new(),
        }
    }

    /// Applies explicitly unranked smoke-test overrides.
    ///
    /// An override equal to the official value is not a deviation.  Any actual
    /// change makes `conformance()` return `DeviatedSmoke`.
    pub fn smoke(overrides: SmokeOverrides) -> Result<Self, ProfileError> {
        let mut profile = Self::official();

        apply_override(
            &mut profile.warehouses,
            overrides.warehouses,
            "warehouses",
            OFFICIAL_WAREHOUSES,
            &mut profile.deviations,
        )?;
        apply_override(
            &mut profile.clients,
            overrides.clients,
            "clients",
            OFFICIAL_CLIENTS,
            &mut profile.deviations,
        )?;
        apply_duration_override(
            &mut profile.warmup,
            overrides.warmup_seconds,
            "warmup_seconds",
            WARMUP_SECONDS,
            true,
            &mut profile.deviations,
        )?;
        apply_override(
            &mut profile.measurement_windows,
            overrides.measurement_windows,
            "measurement_windows",
            MEASUREMENT_WINDOWS,
            &mut profile.deviations,
        )?;
        apply_duration_override(
            &mut profile.measurement_window,
            overrides.measurement_seconds,
            "measurement_seconds",
            MEASUREMENT_SECONDS,
            false,
            &mut profile.deviations,
        )?;
        apply_duration_override(
            &mut profile.load_budget,
            overrides.load_budget_seconds,
            "load_budget_seconds",
            LOAD_BUDGET_SECONDS,
            false,
            &mut profile.deviations,
        )?;
        apply_duration_override(
            &mut profile.recovery_ready_budget,
            overrides.recovery_ready_budget_seconds,
            "recovery_ready_budget_seconds",
            RECOVERY_READY_BUDGET_SECONDS,
            false,
            &mut profile.deviations,
        )?;

        Ok(profile)
    }

    pub fn conformance(&self) -> Conformance {
        if self.deviations.is_empty() {
            Conformance::Official
        } else {
            Conformance::DeviatedSmoke
        }
    }

    pub fn is_ranked_configuration(&self) -> bool {
        self.conformance() == Conformance::Official
    }

    pub fn deviations(&self) -> &[Deviation] {
        &self.deviations
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SmokeOverrides {
    pub warehouses: Option<u16>,
    pub clients: Option<u16>,
    pub warmup_seconds: Option<u64>,
    pub measurement_windows: Option<u8>,
    pub measurement_seconds: Option<u64>,
    pub load_budget_seconds: Option<u64>,
    pub recovery_ready_budget_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileError {
    field: &'static str,
}

impl ProfileError {
    pub fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for ProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} must be greater than zero", self.field)
    }
}

impl Error for ProfileError {}

trait ProfileValue: Copy + Eq + Into<u64> {
    fn is_zero(self) -> bool;
}

impl ProfileValue for u8 {
    fn is_zero(self) -> bool {
        self == 0
    }
}

impl ProfileValue for u16 {
    fn is_zero(self) -> bool {
        self == 0
    }
}

fn apply_override<T: ProfileValue>(
    target: &mut T,
    value: Option<T>,
    field: &'static str,
    official: T,
    deviations: &mut Vec<Deviation>,
) -> Result<(), ProfileError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_zero() {
        return Err(ProfileError { field });
    }
    if value != official {
        deviations.push(Deviation {
            field,
            official: official.into(),
            effective: value.into(),
        });
    }
    *target = value;
    Ok(())
}

fn apply_duration_override(
    target: &mut Duration,
    value: Option<u64>,
    field: &'static str,
    official: u64,
    allow_zero: bool,
    deviations: &mut Vec<Deviation>,
) -> Result<(), ProfileError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value == 0 && !allow_zero {
        return Err(ProfileError { field });
    }
    if value != official {
        deviations.push(Deviation {
            field,
            official,
            effective: value,
        });
    }
    *target = Duration::from_secs(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn official_profile_is_exact_and_ranked() {
        let profile = Final2026Profile::official();
        assert_eq!(profile.warehouses, 50);
        assert_eq!(profile.clients, 32);
        assert_eq!(profile.warmup, Duration::from_secs(30));
        assert_eq!(profile.measurement_windows, 3);
        assert_eq!(profile.measurement_window, Duration::from_secs(150));
        assert_eq!(profile.load_budget, Duration::from_secs(900));
        assert_eq!(profile.recovery_ready_budget, Duration::from_secs(90));
        assert!(profile.is_ranked_configuration());
        assert!(profile.deviations().is_empty());
    }

    #[test]
    fn transaction_buckets_match_the_published_mix_exactly() {
        let mut counts = HashMap::new();
        for bucket in 0..100 {
            *counts
                .entry(transaction_for_bucket(bucket).unwrap())
                .or_insert(0_u8) += 1;
        }
        for (kind, expected) in TRANSACTION_MIX {
            assert_eq!(counts[&kind], expected);
        }
        assert_eq!(transaction_for_bucket(100), None);
    }

    #[test]
    fn coverage_boundary_uses_ceiling_and_only_full_runs_require_hot() {
        assert_eq!(
            coverage_requirement(CoverageScope::MeasurementWindow, 0),
            CoverageRequirement {
                minimum_distinct_warehouses: 0,
                require_all_hot_warehouses: false,
            }
        );
        assert_eq!(
            coverage_requirement(CoverageScope::MeasurementWindow, 1).minimum_distinct_warehouses,
            1
        );
        assert_eq!(
            coverage_requirement(CoverageScope::MeasurementWindow, 200).minimum_distinct_warehouses,
            23
        );
        assert_eq!(
            coverage_requirement(CoverageScope::CombinedWindows, 200).minimum_distinct_warehouses,
            25
        );
        assert!(
            !coverage_requirement(CoverageScope::CombinedWindows, 399).require_all_hot_warehouses
        );
        assert_eq!(
            coverage_requirement(CoverageScope::MeasurementWindow, 400),
            CoverageRequirement {
                minimum_distinct_warehouses: 45,
                require_all_hot_warehouses: true,
            }
        );
        assert_eq!(
            coverage_requirement(CoverageScope::CombinedWindows, 9_999),
            CoverageRequirement {
                minimum_distinct_warehouses: 50,
                require_all_hot_warehouses: true,
            }
        );
    }

    #[test]
    fn smoke_overrides_are_never_silent() {
        let profile = Final2026Profile::smoke(SmokeOverrides {
            clients: Some(2),
            warmup_seconds: Some(0),
            measurement_seconds: Some(1),
            ..SmokeOverrides::default()
        })
        .unwrap();

        assert_eq!(profile.conformance(), Conformance::DeviatedSmoke);
        assert!(!profile.is_ranked_configuration());
        assert_eq!(
            profile
                .deviations()
                .iter()
                .map(|deviation| deviation.field)
                .collect::<Vec<_>>(),
            ["clients", "warmup_seconds", "measurement_seconds"]
        );
    }

    #[test]
    fn invalid_smoke_override_is_rejected() {
        let error = Final2026Profile::smoke(SmokeOverrides {
            clients: Some(0),
            ..SmokeOverrides::default()
        })
        .unwrap_err();
        assert_eq!(error.field(), "clients");
    }
}
