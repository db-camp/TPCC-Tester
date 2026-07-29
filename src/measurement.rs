//! Final-2026 TPC-C measurement accounting and validity gates.
//!
//! This module deliberately contains no executor or wire-protocol code.  Callers
//! record the terminal outcome of each request and can therefore keep retries,
//! deadline abandonment, and grace-tail responses out of the ranked sample.

use std::collections::BTreeSet;
use std::time::Duration;

use crate::transaction::TransactionType;

pub const FORMAL_WINDOW_COUNT: usize = 3;
pub const FORMAL_WINDOW_DURATION: Duration = Duration::from_secs(150);
pub const OFFICIAL_WAREHOUSE_COUNT: usize = 50;
pub const OFFICIAL_HOT_WAREHOUSE_COUNT: usize = 4;
pub const COVERAGE_FULL_SAMPLE_SIZE: u64 = 400;
pub const WINDOW_FULL_COVERAGE: usize = 45;
pub const COMBINED_FULL_COVERAGE: usize = 50;

const TRANSACTION_TYPE_COUNT: usize = 5;

/// Counters and ranked samples for one formal measurement window.
///
/// `attempted` counts requests sent to the server, including requests that
/// return a retryable abort or finish outside the ranked window.  In contrast,
/// [`Self::completed`] counts only an in-window commit or a business-expected
/// rollback, exactly as the coverage gate requires.
#[derive(Debug, Clone)]
pub struct WindowStats {
    pub attempted: u64,
    pub committed: u64,
    pub retry_aborts: u64,
    pub expected_rollbacks: u64,
    pub abandoned: u64,
    pub grace_tail: u64,
    pub committed_by_type: [u64; TRANSACTION_TYPE_COUNT],
    pub delivery_processed: u64,
    pub warehouse_completions: [u64; OFFICIAL_WAREHOUSE_COUNT],
    pub hot_warehouses: BTreeSet<u16>,
    pub new_order_latencies: Vec<Duration>,
}

impl WindowStats {
    pub fn new(hot_warehouses: impl IntoIterator<Item = u16>) -> Self {
        Self {
            attempted: 0,
            committed: 0,
            retry_aborts: 0,
            expected_rollbacks: 0,
            abandoned: 0,
            grace_tail: 0,
            committed_by_type: [0; TRANSACTION_TYPE_COUNT],
            delivery_processed: 0,
            warehouse_completions: [0; OFFICIAL_WAREHOUSE_COUNT],
            hot_warehouses: hot_warehouses.into_iter().collect(),
            new_order_latencies: Vec::new(),
        }
    }

    /// Records a successful commit that completed inside this window.
    ///
    /// `latency` is retained only for NewOrder. `delivery_processed` is counted
    /// only for Delivery, so work from another transaction family cannot
    /// accidentally satisfy the Delivery gate.
    pub fn record_commit(
        &mut self,
        transaction_type: TransactionType,
        home_warehouse: u16,
        latency: Duration,
        delivery_processed: u64,
    ) {
        self.attempted = self.attempted.saturating_add(1);
        self.committed = self.committed.saturating_add(1);
        let index = transaction_index(transaction_type);
        self.committed_by_type[index] = self.committed_by_type[index].saturating_add(1);
        self.record_completion_warehouse(home_warehouse);

        if transaction_type == TransactionType::NewOrder {
            self.new_order_latencies.push(latency);
        }
        if transaction_type == TransactionType::Delivery {
            self.delivery_processed = self.delivery_processed.saturating_add(delivery_processed);
        }
    }

    /// Records a NewOrder invalid-item (or equivalent) business rollback.
    ///
    /// It is a completed sample for warehouse coverage, but is neither a
    /// successful NewOrder nor a latency/ranking sample.
    pub fn record_expected_rollback(&mut self, home_warehouse: u16) {
        self.attempted = self.attempted.saturating_add(1);
        self.expected_rollbacks = self.expected_rollbacks.saturating_add(1);
        self.record_completion_warehouse(home_warehouse);
    }

    /// Records an attempt that returned a retryable transaction abort.
    pub fn record_retry_abort(&mut self) {
        self.attempted = self.attempted.saturating_add(1);
        self.retry_aborts = self.retry_aborts.saturating_add(1);
    }

    /// Records a selected transaction that was ultimately abandoned.
    pub fn record_abandoned(&mut self) {
        self.attempted = self.attempted.saturating_add(1);
        self.abandoned = self.abandoned.saturating_add(1);
    }

    /// Records a selected transaction abandoned before another request was sent.
    ///
    /// This is used for an expired reservation and for a retry that cannot be
    /// started before the phase deadline. The transaction still counts as
    /// abandoned, but there is no additional physical attempt to report.
    pub fn record_unsent_abandoned(&mut self) {
        self.abandoned = self.abandoned.saturating_add(1);
    }

    /// Records a response completed only after the formal window deadline.
    pub fn record_grace_tail(&mut self) {
        self.attempted = self.attempted.saturating_add(1);
        self.grace_tail = self.grace_tail.saturating_add(1);
    }

    /// Successful commits plus business-expected rollbacks.
    pub fn completed(&self) -> u64 {
        self.committed.saturating_add(self.expected_rollbacks)
    }

    pub fn transaction_commits(&self, transaction_type: TransactionType) -> u64 {
        self.committed_by_type[transaction_index(transaction_type)]
    }

    pub fn covered_warehouses(&self) -> usize {
        self.warehouse_completions
            .iter()
            .filter(|&&count| count > 0)
            .count()
    }

    pub fn covered_hot_warehouses(&self) -> usize {
        self.hot_warehouses
            .iter()
            .filter(|&&warehouse| self.warehouse_was_covered(warehouse))
            .count()
    }

    pub fn all_transaction_families_committed(&self) -> bool {
        TransactionType::all()
            .iter()
            .all(|&kind| self.transaction_commits(kind) > 0)
    }

    pub fn delivery_gate_passed(&self) -> bool {
        self.delivery_processed > 0
    }

    pub fn coverage_gate(&self) -> CoverageGate {
        build_coverage_gate(
            self.completed(),
            self.covered_warehouses(),
            self.covered_hot_warehouses(),
            hot_set_is_valid(&self.hot_warehouses),
            WINDOW_FULL_COVERAGE,
        )
    }

    pub fn gate(&self) -> WindowGate {
        let missing_transaction_families = TransactionType::all()
            .iter()
            .copied()
            .filter(|&kind| self.transaction_commits(kind) == 0)
            .collect();
        WindowGate {
            missing_transaction_families,
            delivery_processed: self.delivery_processed,
            coverage: self.coverage_gate(),
        }
    }

    pub fn new_order_per_minute(&self) -> f64 {
        self.new_order_per_minute_for(FORMAL_WINDOW_DURATION)
            .expect("the official formal-window duration is non-zero")
    }

    pub fn new_order_per_minute_for(&self, duration: Duration) -> Option<f64> {
        let seconds = duration.as_secs_f64();
        (seconds > 0.0)
            .then(|| self.transaction_commits(TransactionType::NewOrder) as f64 * 60.0 / seconds)
    }

    fn record_completion_warehouse(&mut self, warehouse: u16) {
        if let Some(index) = warehouse_index(warehouse) {
            self.warehouse_completions[index] = self.warehouse_completions[index].saturating_add(1);
        }
    }

    fn warehouse_was_covered(&self, warehouse: u16) -> bool {
        warehouse_index(warehouse)
            .map(|index| self.warehouse_completions[index] > 0)
            .unwrap_or(false)
    }
}

/// Detailed result of a single-window or combined warehouse coverage gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoverageGate {
    pub completed_samples: u64,
    pub covered_warehouses: usize,
    pub required_warehouses: usize,
    pub covered_hot_warehouses: usize,
    pub required_hot_warehouses: usize,
    pub hot_requirement_active: bool,
    pub hot_set_valid: bool,
}

impl CoverageGate {
    pub fn passed(&self) -> bool {
        let warehouse_gate = self.covered_warehouses >= self.required_warehouses;
        let hot_gate = !self.hot_requirement_active
            || (self.hot_set_valid && self.covered_hot_warehouses >= self.required_hot_warehouses);
        warehouse_gate && hot_gate
    }
}

/// All mandatory gates for one formal window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowGate {
    pub missing_transaction_families: Vec<TransactionType>,
    pub delivery_processed: u64,
    pub coverage: CoverageGate,
}

impl WindowGate {
    pub fn passed(&self) -> bool {
        self.missing_transaction_families.is_empty()
            && self.delivery_processed > 0
            && self.coverage.passed()
    }
}

/// Computes the three-window combined warehouse coverage gate.
pub fn combined_coverage_gate(windows: &[WindowStats]) -> CoverageGate {
    let completed_samples = windows.iter().fold(0u64, |total, window| {
        total.saturating_add(window.completed())
    });
    let mut combined_histogram = [0u64; OFFICIAL_WAREHOUSE_COUNT];
    for window in windows {
        for (combined, count) in combined_histogram
            .iter_mut()
            .zip(window.warehouse_completions.iter())
        {
            *combined = combined.saturating_add(*count);
        }
    }
    let covered_warehouses = combined_histogram
        .iter()
        .filter(|&&count| count > 0)
        .count();

    let canonical_hot_set = windows
        .first()
        .map(|window| window.hot_warehouses.clone())
        .unwrap_or_default();
    let hot_sets_match = windows
        .iter()
        .all(|window| window.hot_warehouses == canonical_hot_set);
    let covered_hot_warehouses = canonical_hot_set
        .iter()
        .filter(|&&warehouse| {
            warehouse_index(warehouse)
                .map(|index| combined_histogram[index] > 0)
                .unwrap_or(false)
        })
        .count();

    build_coverage_gate(
        completed_samples,
        covered_warehouses,
        covered_hot_warehouses,
        hot_sets_match && hot_set_is_valid(&canonical_hot_set),
        COMBINED_FULL_COVERAGE,
    )
}

/// Ranked and diagnostic aggregates for exactly three formal windows.
#[derive(Debug, Clone)]
pub struct MeasurementSummary {
    pub window_gates: [WindowGate; FORMAL_WINDOW_COUNT],
    pub combined_coverage: CoverageGate,
    pub window_new_order_per_minute: [f64; FORMAL_WINDOW_COUNT],
    pub median_new_order_per_minute: f64,
    pub new_order_latency_p50: Option<Duration>,
    pub new_order_latency_p99: Option<Duration>,
}

impl MeasurementSummary {
    pub fn from_windows(windows: &[WindowStats; FORMAL_WINDOW_COUNT]) -> Self {
        let window_gates = std::array::from_fn(|index| windows[index].gate());
        let window_new_order_per_minute =
            std::array::from_fn(|index| windows[index].new_order_per_minute());
        let median_new_order_per_minute = median_of_three(window_new_order_per_minute);

        let mut new_order_latencies: Vec<Duration> = windows
            .iter()
            .flat_map(|window| window.new_order_latencies.iter().copied())
            .collect();
        new_order_latencies.sort_unstable();

        Self {
            window_gates,
            combined_coverage: combined_coverage_gate(windows),
            window_new_order_per_minute,
            median_new_order_per_minute,
            new_order_latency_p50: nearest_rank(&new_order_latencies, 50),
            new_order_latency_p99: nearest_rank(&new_order_latencies, 99),
        }
    }

    pub fn passed(&self) -> bool {
        self.window_gates.iter().all(WindowGate::passed) && self.combined_coverage.passed()
    }
}

fn transaction_index(transaction_type: TransactionType) -> usize {
    match transaction_type {
        TransactionType::NewOrder => 0,
        TransactionType::Payment => 1,
        TransactionType::Delivery => 2,
        TransactionType::OrderStatus => 3,
        TransactionType::StockLevel => 4,
    }
}

fn warehouse_index(warehouse: u16) -> Option<usize> {
    (1..=OFFICIAL_WAREHOUSE_COUNT as u16)
        .contains(&warehouse)
        .then_some(warehouse as usize - 1)
}

fn hot_set_is_valid(hot_warehouses: &BTreeSet<u16>) -> bool {
    hot_warehouses.len() == OFFICIAL_HOT_WAREHOUSE_COUNT
        && hot_warehouses
            .iter()
            .all(|&warehouse| warehouse_index(warehouse).is_some())
}

fn build_coverage_gate(
    completed_samples: u64,
    covered_warehouses: usize,
    covered_hot_warehouses: usize,
    hot_set_valid: bool,
    full_requirement: usize,
) -> CoverageGate {
    let hot_requirement_active = completed_samples >= COVERAGE_FULL_SAMPLE_SIZE;
    let required_warehouses = if hot_requirement_active {
        full_requirement
    } else {
        scaled_coverage_requirement(full_requirement, completed_samples)
    };
    CoverageGate {
        completed_samples,
        covered_warehouses,
        required_warehouses,
        covered_hot_warehouses,
        required_hot_warehouses: if hot_requirement_active {
            OFFICIAL_HOT_WAREHOUSE_COUNT
        } else {
            0
        },
        hot_requirement_active,
        hot_set_valid,
    }
}

fn scaled_coverage_requirement(full_requirement: usize, completed_samples: u64) -> usize {
    let numerator = full_requirement as u128 * completed_samples as u128;
    numerator
        .div_ceil(COVERAGE_FULL_SAMPLE_SIZE as u128)
        .try_into()
        .unwrap_or(usize::MAX)
}

fn median_of_three(mut values: [f64; FORMAL_WINDOW_COUNT]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[1]
}

fn nearest_rank(sorted_samples: &[Duration], percentile: usize) -> Option<Duration> {
    if sorted_samples.is_empty() || !(1..=100).contains(&percentile) {
        return None;
    }
    let rank = (percentile * sorted_samples.len()).div_ceil(100);
    sorted_samples.get(rank - 1).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_commits_across(
        stats: &mut WindowStats,
        count: usize,
        warehouses: &[u16],
        transaction_type: TransactionType,
    ) {
        for index in 0..count {
            stats.record_commit(
                transaction_type,
                warehouses[index % warehouses.len()],
                Duration::from_millis(1),
                0,
            );
        }
    }

    #[test]
    fn window_coverage_switches_hot_gate_at_400() {
        let hot = [1, 2, 3, 4];
        let cold_45: Vec<u16> = (5..=49).collect();
        let mut below_threshold = WindowStats::new(hot);
        record_commits_across(
            &mut below_threshold,
            399,
            &cold_45,
            TransactionType::Payment,
        );

        let below_gate = below_threshold.coverage_gate();
        assert_eq!(below_gate.required_warehouses, 45);
        assert!(!below_gate.hot_requirement_active);
        assert_eq!(below_gate.covered_hot_warehouses, 0);
        assert!(below_gate.passed());

        let missing_one_hot: Vec<u16> = (1..=3).chain(5..=46).collect();
        let mut at_threshold = WindowStats::new(hot);
        record_commits_across(
            &mut at_threshold,
            400,
            &missing_one_hot,
            TransactionType::Payment,
        );

        let threshold_gate = at_threshold.coverage_gate();
        assert_eq!(threshold_gate.required_warehouses, 45);
        assert!(threshold_gate.hot_requirement_active);
        assert_eq!(threshold_gate.covered_hot_warehouses, 3);
        assert!(!threshold_gate.passed());

        let all_hot_and_45: Vec<u16> = (1..=45).collect();
        let mut passing = WindowStats::new(hot);
        record_commits_across(&mut passing, 400, &all_hot_and_45, TransactionType::Payment);
        assert!(passing.coverage_gate().passed());
    }

    #[test]
    fn combined_coverage_uses_50_warehouse_rule() {
        let hot = [1, 2, 3, 4];
        let warehouses_49: Vec<u16> = (1..=49).collect();
        let mut first = WindowStats::new(hot);
        record_commits_across(&mut first, 399, &warehouses_49, TransactionType::Payment);

        let gate_399 = combined_coverage_gate(&[first]);
        assert_eq!(gate_399.required_warehouses, 50);
        assert!(!gate_399.hot_requirement_active);
        assert!(!gate_399.passed());

        let mut second = WindowStats::new(hot);
        second.record_commit(TransactionType::Payment, 50, Duration::from_millis(1), 0);
        let gate_400 = combined_coverage_gate(&[
            {
                let mut stats = WindowStats::new(hot);
                record_commits_across(&mut stats, 399, &warehouses_49, TransactionType::Payment);
                stats
            },
            second,
        ]);
        assert_eq!(gate_400.required_warehouses, 50);
        assert!(gate_400.hot_requirement_active);
        assert!(gate_400.passed());
    }

    #[test]
    fn only_commit_and_expected_rollback_are_completed_samples() {
        let mut stats = WindowStats::new([1, 2, 3, 4]);
        stats.record_commit(TransactionType::Payment, 1, Duration::from_millis(10), 0);
        stats.record_expected_rollback(2);
        stats.record_retry_abort();
        stats.record_abandoned();
        stats.record_unsent_abandoned();
        stats.record_grace_tail();

        assert_eq!(stats.attempted, 5);
        assert_eq!(stats.abandoned, 2);
        assert_eq!(stats.completed(), 2);
        assert_eq!(stats.covered_warehouses(), 2);
        assert_eq!(stats.warehouse_completions[0], 1);
        assert_eq!(stats.warehouse_completions[1], 1);
    }

    #[test]
    fn expected_rollback_counts_for_coverage_but_not_new_order() {
        let mut stats = WindowStats::new([1, 2, 3, 4]);
        stats.record_expected_rollback(37);

        assert_eq!(stats.completed(), 1);
        assert_eq!(stats.covered_warehouses(), 1);
        assert_eq!(stats.transaction_commits(TransactionType::NewOrder), 0);
        assert_eq!(stats.new_order_per_minute(), 0.0);
        assert!(stats.new_order_latencies.is_empty());
    }

    #[test]
    fn family_and_nonempty_delivery_gates_are_independent() {
        let mut stats = WindowStats::new([1, 2, 3, 4]);
        for &kind in TransactionType::all() {
            stats.record_commit(kind, 1, Duration::from_millis(1), 0);
        }

        let empty_delivery = stats.gate();
        assert!(empty_delivery.missing_transaction_families.is_empty());
        assert_eq!(empty_delivery.delivery_processed, 0);
        assert!(!empty_delivery.passed());

        let mut missing_family = WindowStats::new([1, 2, 3, 4]);
        for &kind in TransactionType::all()
            .iter()
            .filter(|&&kind| kind != TransactionType::StockLevel)
        {
            let processed = u64::from(kind == TransactionType::Delivery);
            missing_family.record_commit(kind, 1, Duration::from_millis(1), processed);
        }
        let missing_family_gate = missing_family.gate();
        assert_eq!(
            missing_family_gate.missing_transaction_families,
            vec![TransactionType::StockLevel]
        );
        assert_eq!(missing_family_gate.delivery_processed, 1);
        assert!(!missing_family_gate.passed());

        stats.record_commit(TransactionType::Delivery, 1, Duration::from_millis(1), 1);
        assert!(stats.gate().passed());
    }

    #[test]
    fn summary_uses_three_window_median_and_merged_latency_percentiles() {
        let hot = [1, 2, 3, 4];
        let mut windows = [
            WindowStats::new(hot),
            WindowStats::new(hot),
            WindowStats::new(hot),
        ];
        let new_order_counts = [10, 30, 20];
        let mut next_latency_ms = 1;

        for (window, count) in windows.iter_mut().zip(new_order_counts) {
            for _ in 0..count {
                window.record_commit(
                    TransactionType::NewOrder,
                    1,
                    Duration::from_millis(next_latency_ms),
                    0,
                );
                next_latency_ms += 1;
            }
        }
        for latency_ms in 61..=100 {
            windows[2].record_commit(
                TransactionType::NewOrder,
                1,
                Duration::from_millis(latency_ms),
                0,
            );
        }

        let summary = MeasurementSummary::from_windows(&windows);
        assert_eq!(summary.window_new_order_per_minute, [4.0, 12.0, 24.0]);
        assert_eq!(summary.median_new_order_per_minute, 12.0);
        assert_eq!(
            summary.new_order_latency_p50,
            Some(Duration::from_millis(50))
        );
        assert_eq!(
            summary.new_order_latency_p99,
            Some(Duration::from_millis(99))
        );
    }
}
