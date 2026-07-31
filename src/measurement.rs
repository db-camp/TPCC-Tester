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
pub const STABILITY_BUCKET_DURATION: Duration = Duration::from_secs(5);
pub const STABILITY_BUCKET_COUNT: usize = 30;

const TRANSACTION_TYPE_COUNT: usize = 5;

#[derive(Debug, Clone, Default)]
pub struct TransactionWindowStats {
    pub attempted: u64,
    pub committed: u64,
    pub expected_rollbacks: u64,
    pub abandoned: u64,
    pub committed_latencies: Vec<Duration>,
}

/// Counters and ranked samples for one formal measurement window.
///
/// `attempted` follows the official transaction-completion table: every
/// physical attempt that reaches an in-window terminal appears exactly once as
/// committed, expected rollback, or abandoned. A retryable
/// `TRANSACTION_ABORT` is therefore one abandoned attempt; its later retry is a
/// separate physical attempt with the same immutable transaction parameters.
/// Unsent cutoff stops and grace-tail terminals are reported separately.
#[derive(Debug, Clone)]
pub struct WindowStats {
    pub attempted: u64,
    pub physical_attempts: u64,
    pub committed: u64,
    pub retry_aborts: u64,
    pub expected_rollbacks: u64,
    pub abandoned: u64,
    pub cutoff_stopped: u64,
    pub grace_tail: u64,
    pub committed_by_type: [u64; TRANSACTION_TYPE_COUNT],
    pub transactions_by_type: [TransactionWindowStats; TRANSACTION_TYPE_COUNT],
    pub delivery_processed: u64,
    pub warehouse_completions: [u64; OFFICIAL_WAREHOUSE_COUNT],
    pub hot_warehouses: BTreeSet<u16>,
    pub new_order_latencies: Vec<Duration>,
    pub new_order_stability_buckets: [u64; STABILITY_BUCKET_COUNT],
}

impl WindowStats {
    pub fn new(hot_warehouses: impl IntoIterator<Item = u16>) -> Self {
        Self {
            attempted: 0,
            physical_attempts: 0,
            committed: 0,
            retry_aborts: 0,
            expected_rollbacks: 0,
            abandoned: 0,
            cutoff_stopped: 0,
            grace_tail: 0,
            committed_by_type: [0; TRANSACTION_TYPE_COUNT],
            transactions_by_type: std::array::from_fn(|_| TransactionWindowStats::default()),
            delivery_processed: 0,
            warehouse_completions: [0; OFFICIAL_WAREHOUSE_COUNT],
            hot_warehouses: hot_warehouses.into_iter().collect(),
            new_order_latencies: Vec::new(),
            new_order_stability_buckets: [0; STABILITY_BUCKET_COUNT],
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
        self.record_commit_at_offset(
            transaction_type,
            home_warehouse,
            latency,
            delivery_processed,
            None,
        );
    }

    pub fn record_commit_at_offset(
        &mut self,
        transaction_type: TransactionType,
        home_warehouse: u16,
        latency: Duration,
        delivery_processed: u64,
        completion_offset: Option<Duration>,
    ) {
        self.attempted = self.attempted.saturating_add(1);
        self.physical_attempts = self.physical_attempts.saturating_add(1);
        self.committed = self.committed.saturating_add(1);
        let index = transaction_index(transaction_type);
        self.committed_by_type[index] = self.committed_by_type[index].saturating_add(1);
        let family = &mut self.transactions_by_type[index];
        family.attempted = family.attempted.saturating_add(1);
        family.committed = family.committed.saturating_add(1);
        family.committed_latencies.push(latency);
        self.record_completion_warehouse(home_warehouse);

        if transaction_type == TransactionType::NewOrder {
            self.new_order_latencies.push(latency);
            if let Some(offset) = completion_offset {
                let bucket = offset.as_nanos() / STABILITY_BUCKET_DURATION.as_nanos();
                if let Ok(bucket) = usize::try_from(bucket) {
                    if let Some(count) = self.new_order_stability_buckets.get_mut(bucket) {
                        *count = count.saturating_add(1);
                    }
                }
            }
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
        self.physical_attempts = self.physical_attempts.saturating_add(1);
        self.expected_rollbacks = self.expected_rollbacks.saturating_add(1);
        let family = &mut self.transactions_by_type[transaction_index(TransactionType::NewOrder)];
        family.attempted = family.attempted.saturating_add(1);
        family.expected_rollbacks = family.expected_rollbacks.saturating_add(1);
        self.record_completion_warehouse(home_warehouse);
    }

    /// Records an in-window attempt that returned a retryable transaction
    /// abort. The next attempt reuses the same logical transaction parameters,
    /// but this terminal remains an abandoned physical attempt in the report.
    pub fn record_retry_abort(&mut self, transaction_type: TransactionType) {
        self.attempted = self.attempted.saturating_add(1);
        self.physical_attempts = self.physical_attempts.saturating_add(1);
        self.retry_aborts = self.retry_aborts.saturating_add(1);
        self.abandoned = self.abandoned.saturating_add(1);
        let family = &mut self.transactions_by_type[transaction_index(transaction_type)];
        family.attempted = family.attempted.saturating_add(1);
        family.abandoned = family.abandoned.saturating_add(1);
    }

    /// Records an in-window physical attempt that was abandoned without a
    /// normal response terminal, for example a read-only response timeout.
    pub fn record_abandoned(&mut self, transaction_type: TransactionType) {
        self.attempted = self.attempted.saturating_add(1);
        self.physical_attempts = self.physical_attempts.saturating_add(1);
        self.abandoned = self.abandoned.saturating_add(1);
        let family = &mut self.transactions_by_type[transaction_index(transaction_type)];
        family.attempted = family.attempted.saturating_add(1);
        family.abandoned = family.abandoned.saturating_add(1);
    }

    /// Records a reservation that reached the phase cutoff before any request
    /// was sent. It is neither attempted nor abandoned in the official table.
    pub fn record_cutoff_stop(&mut self) {
        self.cutoff_stopped = self.cutoff_stopped.saturating_add(1);
    }

    /// Records a response completed only after the formal window deadline.
    pub fn record_grace_tail(&mut self, _transaction_type: TransactionType) {
        self.physical_attempts = self.physical_attempts.saturating_add(1);
        self.grace_tail = self.grace_tail.saturating_add(1);
    }

    /// Successful commits plus business-expected rollbacks.
    pub fn completed(&self) -> u64 {
        self.committed.saturating_add(self.expected_rollbacks)
    }

    pub fn transaction_commits(&self, transaction_type: TransactionType) -> u64 {
        self.committed_by_type[transaction_index(transaction_type)]
    }

    pub fn transaction_stats(&self, transaction_type: TransactionType) -> &TransactionWindowStats {
        &self.transactions_by_type[transaction_index(transaction_type)]
    }

    pub fn accounting_is_consistent(&self) -> bool {
        let mut attempted = 0_u64;
        let mut committed = 0_u64;
        let mut expected_rollbacks = 0_u64;
        let mut abandoned = 0_u64;

        for transaction_type in TransactionType::all() {
            let index = transaction_index(*transaction_type);
            let family = &self.transactions_by_type[index];
            if family.attempted
                != family
                    .committed
                    .saturating_add(family.expected_rollbacks)
                    .saturating_add(family.abandoned)
                || family.committed != self.committed_by_type[index]
                || u64::try_from(family.committed_latencies.len()).ok() != Some(family.committed)
            {
                return false;
            }
            attempted = attempted.saturating_add(family.attempted);
            committed = committed.saturating_add(family.committed);
            expected_rollbacks = expected_rollbacks.saturating_add(family.expected_rollbacks);
            abandoned = abandoned.saturating_add(family.abandoned);
        }

        attempted == self.attempted
            && committed == self.committed
            && expected_rollbacks == self.expected_rollbacks
            && abandoned == self.abandoned
            && self.attempted
                == self
                    .committed
                    .saturating_add(self.expected_rollbacks)
                    .saturating_add(self.abandoned)
            && self.physical_attempts == self.attempted.saturating_add(self.grace_tail)
            && self
                .transaction_stats(TransactionType::NewOrder)
                .committed_latencies
                == self.new_order_latencies
            && self
                .new_order_stability_buckets
                .iter()
                .copied()
                .fold(0_u64, u64::saturating_add)
                <= self.transaction_commits(TransactionType::NewOrder)
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

    pub fn new_order_stability(&self) -> NewOrderStability {
        let rates = self
            .new_order_stability_buckets
            .map(|commits| commits as f64 * 60.0 / STABILITY_BUCKET_DURATION.as_secs_f64());
        let average_per_minute = rates.iter().sum::<f64>() / rates.len() as f64;
        let variance = rates
            .iter()
            .map(|rate| {
                let difference = *rate - average_per_minute;
                difference * difference
            })
            .sum::<f64>()
            / rates.len() as f64;
        let cv_percent = if average_per_minute == 0.0 {
            0.0
        } else {
            variance.sqrt() * 100.0 / average_per_minute
        };
        NewOrderStability {
            average_per_minute,
            cv_percent,
            min_per_minute: rates.iter().copied().fold(f64::INFINITY, f64::min),
            max_per_minute: rates.iter().copied().fold(0.0, f64::max),
            zero_buckets: self
                .new_order_stability_buckets
                .iter()
                .filter(|&&commits| commits == 0)
                .count(),
        }
    }

    pub fn stability_samples_complete(&self) -> bool {
        self.new_order_stability_buckets
            .iter()
            .copied()
            .fold(0_u64, u64::saturating_add)
            == self.transaction_commits(TransactionType::NewOrder)
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NewOrderStability {
    pub average_per_minute: f64,
    pub cv_percent: f64,
    pub min_per_minute: f64,
    pub max_per_minute: f64,
    pub zero_buckets: usize,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionCompletionSummary {
    pub attempted: u64,
    pub committed: u64,
    pub expected_rollbacks: u64,
    pub abandoned: u64,
    pub latency_average: Option<Duration>,
    pub latency_p50: Option<Duration>,
    pub latency_p99: Option<Duration>,
    pub latency_max: Option<Duration>,
}

impl TransactionCompletionSummary {
    pub fn completion_percent(&self) -> f64 {
        if self.attempted == 0 {
            0.0
        } else {
            self.committed.saturating_add(self.expected_rollbacks) as f64 * 100.0
                / self.attempted as f64
        }
    }

    pub fn abort_percent(&self) -> f64 {
        if self.attempted == 0 {
            0.0
        } else {
            self.abandoned as f64 * 100.0 / self.attempted as f64
        }
    }
}

pub fn transaction_completion_summary(
    windows: &[WindowStats],
    transaction_type: TransactionType,
) -> TransactionCompletionSummary {
    let mut attempted = 0_u64;
    let mut committed = 0_u64;
    let mut expected_rollbacks = 0_u64;
    let mut abandoned = 0_u64;
    let mut latencies = Vec::new();
    for window in windows {
        let family = window.transaction_stats(transaction_type);
        attempted = attempted.saturating_add(family.attempted);
        committed = committed.saturating_add(family.committed);
        expected_rollbacks = expected_rollbacks.saturating_add(family.expected_rollbacks);
        abandoned = abandoned.saturating_add(family.abandoned);
        latencies.extend(family.committed_latencies.iter().copied());
    }
    latencies.sort_unstable();
    let latency_average = average_duration(&latencies);
    let latency_max = latencies.last().copied();
    TransactionCompletionSummary {
        attempted,
        committed,
        expected_rollbacks,
        abandoned,
        latency_average,
        latency_p50: nearest_rank(&latencies, 50),
        latency_p99: nearest_rank(&latencies, 99),
        latency_max,
    }
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

fn average_duration(samples: &[Duration]) -> Option<Duration> {
    if samples.is_empty() {
        return None;
    }
    let total_nanos = samples.iter().fold(0_u128, |total, sample| {
        total.saturating_add(sample.as_nanos())
    });
    let average_nanos = total_nanos / samples.len() as u128;
    Some(Duration::from_nanos(
        u64::try_from(average_nanos).unwrap_or(u64::MAX),
    ))
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
        stats.record_retry_abort(TransactionType::Payment);
        stats.record_abandoned(TransactionType::Payment);
        stats.record_cutoff_stop();
        stats.record_grace_tail(TransactionType::Payment);

        assert_eq!(stats.attempted, 4);
        assert_eq!(stats.physical_attempts, 5);
        assert_eq!(stats.abandoned, 2);
        assert_eq!(stats.cutoff_stopped, 1);
        assert_eq!(stats.completed(), 2);
        assert_eq!(stats.covered_warehouses(), 2);
        assert_eq!(stats.warehouse_completions[0], 1);
        assert_eq!(stats.warehouse_completions[1], 1);
        assert_eq!(
            stats.transaction_stats(TransactionType::Payment).attempted,
            3
        );
        assert_eq!(
            stats
                .transaction_stats(TransactionType::NewOrder)
                .expected_rollbacks,
            1
        );
        assert!(stats.accounting_is_consistent());
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

        let new_order = transaction_completion_summary(&windows, TransactionType::NewOrder);
        assert_eq!(new_order.attempted, 100);
        assert_eq!(new_order.committed, 100);
        assert_eq!(new_order.expected_rollbacks, 0);
        assert_eq!(new_order.abandoned, 0);
        assert_eq!(
            new_order.latency_average,
            Some(Duration::from_millis(50) + Duration::from_micros(500))
        );
        assert_eq!(new_order.latency_p50, Some(Duration::from_millis(50)));
        assert_eq!(new_order.latency_p99, Some(Duration::from_millis(99)));
        assert_eq!(new_order.latency_max, Some(Duration::from_millis(100)));
        assert_eq!(new_order.completion_percent(), 100.0);
        assert_eq!(new_order.abort_percent(), 0.0);
    }

    #[test]
    fn retry_terminals_are_abandoned_attempts_but_tail_is_excluded() {
        let mut stats = WindowStats::new([1, 2, 3, 4]);
        stats.record_retry_abort(TransactionType::Payment);
        stats.record_retry_abort(TransactionType::Payment);
        stats.record_commit(TransactionType::Payment, 1, Duration::from_millis(20), 0);
        stats.record_grace_tail(TransactionType::Payment);

        let payment = stats.transaction_stats(TransactionType::Payment);
        assert_eq!(stats.attempted, 3);
        assert_eq!(stats.physical_attempts, 4);
        assert_eq!(stats.retry_aborts, 2);
        assert_eq!(stats.abandoned, 2);
        assert_eq!(stats.grace_tail, 1);
        assert_eq!(payment.attempted, 3);
        assert_eq!(payment.committed, 1);
        assert_eq!(payment.abandoned, 2);
        assert_eq!(
            payment.attempted,
            payment
                .committed
                .saturating_add(payment.expected_rollbacks)
                .saturating_add(payment.abandoned)
        );
        assert!(stats.accounting_is_consistent());
    }

    #[test]
    fn accounting_consistency_rejects_counter_and_latency_drift() {
        let mut stats = WindowStats::new([1, 2, 3, 4]);
        stats.record_commit(TransactionType::Delivery, 1, Duration::from_millis(12), 1);
        assert!(stats.accounting_is_consistent());

        stats.transactions_by_type[transaction_index(TransactionType::Delivery)]
            .committed_latencies
            .clear();
        assert!(!stats.accounting_is_consistent());
    }

    #[test]
    fn five_second_stability_uses_completion_time_and_population_cv() {
        let mut stats = WindowStats::new([1, 2, 3, 4]);
        for bucket in 0..STABILITY_BUCKET_COUNT {
            let offset = STABILITY_BUCKET_DURATION
                .checked_mul(bucket as u32)
                .unwrap()
                .saturating_add(Duration::from_millis(1));
            let commits = if bucket < STABILITY_BUCKET_COUNT / 2 {
                1
            } else {
                3
            };
            for _ in 0..commits {
                stats.record_commit_at_offset(
                    TransactionType::NewOrder,
                    1,
                    Duration::from_millis(20),
                    0,
                    Some(offset),
                );
            }
        }

        let stability = stats.new_order_stability();
        assert_eq!(stats.new_order_per_minute(), 24.0);
        assert_eq!(stability.average_per_minute, 24.0);
        assert_eq!(stability.cv_percent, 50.0);
        assert_eq!(stability.min_per_minute, 12.0);
        assert_eq!(stability.max_per_minute, 36.0);
        assert_eq!(stability.zero_buckets, 0);
        assert!(stats.stability_samples_complete());
        assert!(stats.accounting_is_consistent());
    }

    #[test]
    fn five_second_bucket_boundaries_are_half_open() {
        let mut stats = WindowStats::new([1, 2, 3, 4]);
        stats.record_commit_at_offset(
            TransactionType::NewOrder,
            1,
            Duration::from_millis(10),
            0,
            Some(STABILITY_BUCKET_DURATION - Duration::from_nanos(1)),
        );
        stats.record_commit_at_offset(
            TransactionType::NewOrder,
            1,
            Duration::from_millis(10),
            0,
            Some(STABILITY_BUCKET_DURATION),
        );

        assert_eq!(stats.new_order_stability_buckets[0], 1);
        assert_eq!(stats.new_order_stability_buckets[1], 1);
        assert_eq!(
            stats
                .new_order_stability_buckets
                .iter()
                .copied()
                .sum::<u64>(),
            2
        );
    }

    #[test]
    fn stability_completeness_rejects_unbucketed_new_order_commits() {
        let mut stats = WindowStats::new([1, 2, 3, 4]);
        stats.record_commit(TransactionType::NewOrder, 1, Duration::from_millis(10), 0);

        assert!(stats.accounting_is_consistent());
        assert!(!stats.stability_samples_complete());
    }
}
