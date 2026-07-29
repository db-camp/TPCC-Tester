//! Exact, bounded Payment endpoint chains for the public Warehouse and
//! District rows.
//!
//! Ranked responses may arrive out of commit order.  A bounded terminal
//! reorder buffer keeps each Payment's Warehouse and District edges paired,
//! then advances both fixed-domain chains in one common serial order.  This
//! catches contradictory per-row orders that independent interval collectors
//! would incorrectly accept.

use std::cmp::Ordering;

use thiserror::Error;

pub const MAX_PAYMENT_WAREHOUSES: u16 = 50;
pub const DISTRICTS_PER_WAREHOUSE: u8 = 10;
pub const WAREHOUSE_YTD_ROOT_BITS: u32 = 300_000.0_f32.to_bits();
pub const DISTRICT_YTD_ROOT_BITS: u32 = 30_000.0_f32.to_bits();

/// One raw FLOAT32 relative-update edge returned by a Payment transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaymentFloatEdge {
    pub before_bits: u32,
    pub after_bits: u32,
    pub amount_bits: u32,
}

/// The paired public-row evidence from one successful Payment terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaymentTerminalEvidence {
    pub warehouse_id: u16,
    pub district_id: u8,
    pub warehouse: PaymentFloatEdge,
    pub district: PaymentFloatEdge,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PaymentEndpointError {
    #[error("invalid Payment endpoint configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("invalid Payment endpoint key: warehouse {warehouse_id}, district {district_id:?}")]
    InvalidKey {
        warehouse_id: u16,
        district_id: Option<u8>,
    },
    #[error("{domain} {field} is not a finite binary32 value")]
    NonFinite {
        domain: &'static str,
        field: &'static str,
    },
    #[error("{domain} Payment amount must be positive")]
    NonPositiveAmount { domain: &'static str },
    #[error("paired Warehouse and District Payment amounts differ")]
    PairedAmountMismatch,
    #[error("{domain} relative update is not bit-exact binary32 RNE")]
    FloatMismatch { domain: &'static str },
    #[error("{domain} interval starts behind the sealed reorder frontier")]
    StaleInterval { domain: &'static str },
    #[error("{domain} has more than one forward interval from the same predecessor")]
    Fork { domain: &'static str },
    #[error("Payment pending edge limit exceeded: {actual} > {limit}")]
    PendingLimit { actual: usize, limit: usize },
    #[error("Payment endpoint counter overflow: {0}")]
    Overflow(&'static str),
    #[error(
        "Payment evidence has no common rooted terminal order ({pending_edges} pending edges)"
    )]
    Disconnected { pending_edges: usize },
    #[error("Payment endpoint collector is poisoned")]
    Poisoned,
    #[error("invalid sealed Payment endpoint invariant: {0}")]
    InvalidInvariant(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaymentCollectorStorage {
    pub warehouse_slots: usize,
    pub district_slots: usize,
    pub pending_edges: usize,
    pub pending_capacity: usize,
    pub terminal_count: u64,
}

#[derive(Clone, Copy, Debug)]
struct EndpointChain {
    root_bits: u32,
    endpoint_bits: u32,
    update_count: u64,
}

impl EndpointChain {
    fn new(root_bits: u32) -> Self {
        Self {
            root_bits,
            endpoint_bits: root_bits,
            update_count: 0,
        }
    }

    fn apply(
        &mut self,
        domain: &'static str,
        edge: ValidatedEdge,
    ) -> Result<(), PaymentEndpointError> {
        if self.endpoint_bits != edge.before_bits {
            return Err(PaymentEndpointError::InvalidInvariant(domain));
        }
        self.update_count = self
            .update_count
            .checked_add(1)
            .ok_or(PaymentEndpointError::Overflow("endpoint update count"))?;
        self.endpoint_bits = edge.after_bits;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct ValidatedEdge {
    before_bits: u32,
    after_bits: u32,
}

impl ValidatedEdge {
    fn is_self_loop(self) -> bool {
        self.before_bits == self.after_bits
    }
}

#[derive(Clone, Copy, Debug)]
struct BufferedTerminal {
    warehouse_index: usize,
    district_index: usize,
    warehouse: ValidatedEdge,
    district: ValidatedEdge,
}

#[derive(Debug)]
struct TerminalPlan {
    warehouse_chains: Box<[EndpointChain]>,
    district_chains: Box<[EndpointChain]>,
    pending: Vec<BufferedTerminal>,
    terminal_count: u64,
}

/// Fixed-domain, bounded-memory collector shared by all ranked workers.
///
/// At most one paired terminal per client remains reorderable.  When the
/// buffer would exceed that bound, at least one terminal must be removable
/// from the common Warehouse/District rooted order or the collector poisons.
#[derive(Debug)]
pub struct PaymentEndpointCollector {
    warehouses: u16,
    terminal_limit: usize,
    warehouse_chains: Box<[EndpointChain]>,
    district_chains: Box<[EndpointChain]>,
    pending: Vec<BufferedTerminal>,
    terminal_count: u64,
    poisoned: bool,
}

impl PaymentEndpointCollector {
    pub fn new(warehouses: u16, clients: u16) -> Result<Self, PaymentEndpointError> {
        if warehouses == 0 || warehouses > MAX_PAYMENT_WAREHOUSES {
            return Err(PaymentEndpointError::InvalidConfiguration(
                "warehouse count must be in 1..=50",
            ));
        }
        if clients == 0 {
            return Err(PaymentEndpointError::InvalidConfiguration(
                "client count must be positive",
            ));
        }

        let terminal_limit = usize::from(clients);
        let district_count = usize::from(warehouses)
            .checked_mul(usize::from(DISTRICTS_PER_WAREHOUSE))
            .ok_or(PaymentEndpointError::Overflow("district slot count"))?;
        Ok(Self {
            warehouses,
            terminal_limit,
            warehouse_chains: vec![
                EndpointChain::new(WAREHOUSE_YTD_ROOT_BITS);
                usize::from(warehouses)
            ]
            .into_boxed_slice(),
            district_chains: vec![EndpointChain::new(DISTRICT_YTD_ROOT_BITS); district_count]
                .into_boxed_slice(),
            pending: Vec::with_capacity(terminal_limit),
            terminal_count: 0,
            poisoned: false,
        })
    }

    /// Validates and applies both public Payment edges transactionally.
    pub fn record_terminal(
        &mut self,
        terminal: PaymentTerminalEvidence,
    ) -> Result<(), PaymentEndpointError> {
        if self.poisoned {
            return Err(PaymentEndpointError::Poisoned);
        }

        match self.prepare_terminal(terminal) {
            Ok(plan) => {
                self.warehouse_chains = plan.warehouse_chains;
                self.district_chains = plan.district_chains;
                self.pending = plan.pending;
                self.terminal_count = plan.terminal_count;
                Ok(())
            }
            Err(error) => {
                self.poisoned = true;
                Err(error)
            }
        }
    }

    pub fn storage(&self) -> PaymentCollectorStorage {
        PaymentCollectorStorage {
            warehouse_slots: self.warehouse_chains.len(),
            district_slots: self.district_chains.len(),
            pending_edges: self.pending.len() * 2,
            pending_capacity: self.pending.capacity() * 2,
            terminal_count: self.terminal_count,
        }
    }

    pub fn seal(mut self) -> Result<SealedPaymentEvidence, PaymentEndpointError> {
        if self.poisoned {
            return Err(PaymentEndpointError::Poisoned);
        }
        validate_visible_forks(&self.pending)?;
        while !self.pending.is_empty() {
            if !apply_one_serializable(
                &mut self.warehouse_chains,
                &mut self.district_chains,
                &mut self.pending,
            )? {
                return Err(PaymentEndpointError::Disconnected {
                    pending_edges: self.pending.len() * 2,
                });
            }
        }

        validate_chain_totals(
            self.warehouses,
            self.terminal_count,
            &self.warehouse_chains,
            &self.district_chains,
        )?;
        Ok(SealedPaymentEvidence {
            warehouses: self.warehouses,
            terminal_count: self.terminal_count,
            warehouse_edge_count: self.terminal_count,
            district_edge_count: self.terminal_count,
            warehouse_endpoints: self
                .warehouse_chains
                .into_vec()
                .into_iter()
                .map(SealedEndpoint::from_chain)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            district_endpoints: self
                .district_chains
                .into_vec()
                .into_iter()
                .map(SealedEndpoint::from_chain)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn prepare_terminal(
        &self,
        terminal: PaymentTerminalEvidence,
    ) -> Result<TerminalPlan, PaymentEndpointError> {
        let warehouse_index = self.warehouse_index(terminal.warehouse_id)?;
        let district_index = self.district_index(terminal.warehouse_id, terminal.district_id)?;
        if terminal.warehouse.amount_bits != terminal.district.amount_bits {
            return Err(PaymentEndpointError::PairedAmountMismatch);
        }
        let warehouse = validate_edge("warehouse.w_ytd", terminal.warehouse)?;
        let district = validate_edge("district.d_ytd", terminal.district)?;
        reject_behind_frontier(
            "warehouse.w_ytd",
            warehouse,
            self.warehouse_chains[warehouse_index],
        )?;
        reject_behind_frontier(
            "district.d_ytd",
            district,
            self.district_chains[district_index],
        )?;
        let terminal_count = self
            .terminal_count
            .checked_add(1)
            .ok_or(PaymentEndpointError::Overflow("terminal count"))?;

        // The complete mutable state is scratch.  Nothing becomes observable
        // until both chains and the reorder limit have succeeded.
        let mut warehouse_chains = self.warehouse_chains.clone();
        let mut district_chains = self.district_chains.clone();
        let mut pending = self.pending.clone();
        pending.push(BufferedTerminal {
            warehouse_index,
            district_index,
            warehouse,
            district,
        });
        validate_visible_forks(&pending)?;

        while pending.len() > self.terminal_limit {
            if !apply_one_serializable(&mut warehouse_chains, &mut district_chains, &mut pending)? {
                return Err(PaymentEndpointError::PendingLimit {
                    actual: pending.len() * 2,
                    limit: self.terminal_limit * 2,
                });
            }
        }

        // A scratch Vec may grow to clients+1 before reduction.  Repack into
        // the configured capacity so a successful commit never retains that
        // transient allocation.
        let mut fixed_pending = Vec::with_capacity(self.terminal_limit);
        fixed_pending.extend(pending);
        Ok(TerminalPlan {
            warehouse_chains,
            district_chains,
            pending: fixed_pending,
            terminal_count,
        })
    }

    fn warehouse_index(&self, warehouse_id: u16) -> Result<usize, PaymentEndpointError> {
        if warehouse_id == 0 || warehouse_id > self.warehouses {
            return Err(PaymentEndpointError::InvalidKey {
                warehouse_id,
                district_id: None,
            });
        }
        Ok(usize::from(warehouse_id - 1))
    }

    fn district_index(
        &self,
        warehouse_id: u16,
        district_id: u8,
    ) -> Result<usize, PaymentEndpointError> {
        if warehouse_id == 0
            || warehouse_id > self.warehouses
            || district_id == 0
            || district_id > DISTRICTS_PER_WAREHOUSE
        {
            return Err(PaymentEndpointError::InvalidKey {
                warehouse_id,
                district_id: Some(district_id),
            });
        }
        Ok(
            usize::from(warehouse_id - 1) * usize::from(DISTRICTS_PER_WAREHOUSE)
                + usize::from(district_id - 1),
        )
    }
}

fn validate_edge(
    domain: &'static str,
    edge: PaymentFloatEdge,
) -> Result<ValidatedEdge, PaymentEndpointError> {
    let before = require_finite(domain, "before", edge.before_bits)?;
    require_finite(domain, "after", edge.after_bits)?;
    let amount = require_finite(domain, "amount", edge.amount_bits)?;
    if amount <= 0.0 {
        return Err(PaymentEndpointError::NonPositiveAmount { domain });
    }
    let expected = before + amount;
    if !expected.is_finite() || expected.to_bits() != edge.after_bits {
        return Err(PaymentEndpointError::FloatMismatch { domain });
    }
    Ok(ValidatedEdge {
        before_bits: edge.before_bits,
        after_bits: edge.after_bits,
    })
}

fn require_finite(
    domain: &'static str,
    field: &'static str,
    bits: u32,
) -> Result<f32, PaymentEndpointError> {
    let value = f32::from_bits(bits);
    if !value.is_finite() {
        return Err(PaymentEndpointError::NonFinite { domain, field });
    }
    Ok(value)
}

fn reject_behind_frontier(
    domain: &'static str,
    edge: ValidatedEdge,
    chain: EndpointChain,
) -> Result<(), PaymentEndpointError> {
    if compare_bits(edge.before_bits, chain.endpoint_bits) == Ordering::Less {
        return Err(PaymentEndpointError::StaleInterval { domain });
    }
    Ok(())
}

fn validate_visible_forks(pending: &[BufferedTerminal]) -> Result<(), PaymentEndpointError> {
    for left in 0..pending.len() {
        for right in left + 1..pending.len() {
            let left_terminal = pending[left];
            let right_terminal = pending[right];
            if left_terminal.warehouse_index == right_terminal.warehouse_index
                && left_terminal.warehouse.before_bits == right_terminal.warehouse.before_bits
                && !left_terminal.warehouse.is_self_loop()
                && !right_terminal.warehouse.is_self_loop()
            {
                return Err(PaymentEndpointError::Fork {
                    domain: "warehouse.w_ytd",
                });
            }
            if left_terminal.district_index == right_terminal.district_index
                && left_terminal.district.before_bits == right_terminal.district.before_bits
                && !left_terminal.district.is_self_loop()
                && !right_terminal.district.is_self_loop()
            {
                return Err(PaymentEndpointError::Fork {
                    domain: "district.d_ytd",
                });
            }
        }
    }
    Ok(())
}

/// Applies one minimal terminal in the partial order induced by both rows.
fn apply_one_serializable(
    warehouse_chains: &mut [EndpointChain],
    district_chains: &mut [EndpointChain],
    pending: &mut Vec<BufferedTerminal>,
) -> Result<bool, PaymentEndpointError> {
    validate_visible_forks(pending)?;
    let candidate = pending.iter().position(|terminal| {
        terminal_is_minimal(terminal, warehouse_chains, district_chains, pending)
    });
    let Some(index) = candidate else {
        return Ok(false);
    };

    let terminal = pending.swap_remove(index);
    warehouse_chains[terminal.warehouse_index].apply("warehouse.w_ytd", terminal.warehouse)?;
    district_chains[terminal.district_index].apply("district.d_ytd", terminal.district)?;
    Ok(true)
}

fn terminal_is_minimal(
    terminal: &BufferedTerminal,
    warehouse_chains: &[EndpointChain],
    district_chains: &[EndpointChain],
    pending: &[BufferedTerminal],
) -> bool {
    let warehouse_endpoint = warehouse_chains[terminal.warehouse_index].endpoint_bits;
    let district_endpoint = district_chains[terminal.district_index].endpoint_bits;
    if terminal.warehouse.before_bits != warehouse_endpoint
        || terminal.district.before_bits != district_endpoint
    {
        return false;
    }

    // Every self-loop at a predecessor precedes its single forward edge.
    // This rule makes late but still-buffered self-loops order-independent.
    if !terminal.warehouse.is_self_loop()
        && pending.iter().any(|other| {
            other.warehouse_index == terminal.warehouse_index
                && other.warehouse.before_bits == warehouse_endpoint
                && other.warehouse.is_self_loop()
        })
    {
        return false;
    }
    if !terminal.district.is_self_loop()
        && pending.iter().any(|other| {
            other.district_index == terminal.district_index
                && other.district.before_bits == district_endpoint
                && other.district.is_self_loop()
        })
    {
        return false;
    }
    true
}

fn compare_bits(left: u32, right: u32) -> Ordering {
    f32::from_bits(left).total_cmp(&f32::from_bits(right))
}

fn validate_chain_totals(
    warehouses: u16,
    terminal_count: u64,
    warehouse_chains: &[EndpointChain],
    district_chains: &[EndpointChain],
) -> Result<(), PaymentEndpointError> {
    let expected_districts = usize::from(warehouses)
        .checked_mul(usize::from(DISTRICTS_PER_WAREHOUSE))
        .ok_or(PaymentEndpointError::Overflow("district count"))?;
    if warehouse_chains.len() != usize::from(warehouses)
        || district_chains.len() != expected_districts
    {
        return Err(PaymentEndpointError::InvalidInvariant(
            "endpoint cardinality does not match warehouse count",
        ));
    }

    let mut warehouse_total = 0_u64;
    let mut district_total = 0_u64;
    for (warehouse_index, warehouse) in warehouse_chains.iter().enumerate() {
        validate_sealed_chain(WAREHOUSE_YTD_ROOT_BITS, *warehouse)?;
        warehouse_total = warehouse_total
            .checked_add(warehouse.update_count)
            .ok_or(PaymentEndpointError::Overflow("warehouse update total"))?;

        let start = warehouse_index * usize::from(DISTRICTS_PER_WAREHOUSE);
        let end = start + usize::from(DISTRICTS_PER_WAREHOUSE);
        let per_warehouse_districts =
            district_chains[start..end]
                .iter()
                .try_fold(0_u64, |count, district| {
                    validate_sealed_chain(DISTRICT_YTD_ROOT_BITS, *district)?;
                    count
                        .checked_add(district.update_count)
                        .ok_or(PaymentEndpointError::Overflow("district update total"))
                })?;
        if per_warehouse_districts != warehouse.update_count {
            return Err(PaymentEndpointError::InvalidInvariant(
                "Warehouse count differs from its District counts",
            ));
        }
        district_total = district_total
            .checked_add(per_warehouse_districts)
            .ok_or(PaymentEndpointError::Overflow("district update total"))?;
    }
    if warehouse_total != terminal_count || district_total != terminal_count {
        return Err(PaymentEndpointError::InvalidInvariant(
            "endpoint counts differ from terminal total",
        ));
    }
    Ok(())
}

fn validate_sealed_chain(
    expected_root_bits: u32,
    chain: EndpointChain,
) -> Result<(), PaymentEndpointError> {
    if chain.root_bits != expected_root_bits {
        return Err(PaymentEndpointError::InvalidInvariant(
            "endpoint setup root is not bit-exact",
        ));
    }
    let endpoint = f32::from_bits(chain.endpoint_bits);
    if !endpoint.is_finite() {
        return Err(PaymentEndpointError::InvalidInvariant(
            "endpoint is not finite",
        ));
    }
    if compare_bits(chain.endpoint_bits, expected_root_bits) == Ordering::Less {
        return Err(PaymentEndpointError::InvalidInvariant(
            "endpoint precedes setup root",
        ));
    }
    if compare_bits(chain.endpoint_bits, expected_root_bits) == Ordering::Greater
        && chain.update_count == 0
    {
        return Err(PaymentEndpointError::InvalidInvariant(
            "empty chain endpoint differs from setup root",
        ));
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct SealedEndpoint {
    root_bits: u32,
    endpoint_bits: u32,
    update_count: u64,
}

impl SealedEndpoint {
    fn from_chain(chain: EndpointChain) -> Self {
        Self {
            root_bits: chain.root_bits,
            endpoint_bits: chain.endpoint_bits,
            update_count: chain.update_count,
        }
    }
}

/// Fully validated public Payment endpoint certificate.
///
/// Fields are private, and this type intentionally implements neither
/// `Clone` nor `Default`.  There is deliberately no endpoint-only public
/// decoder: endpoints plus counts cannot prove that Warehouse and District
/// edges used the same amounts in one serial terminal order.
#[derive(Debug, Eq, PartialEq)]
pub struct SealedPaymentEvidence {
    warehouses: u16,
    terminal_count: u64,
    warehouse_edge_count: u64,
    district_edge_count: u64,
    warehouse_endpoints: Box<[SealedEndpoint]>,
    district_endpoints: Box<[SealedEndpoint]>,
}

impl SealedPaymentEvidence {
    pub fn warehouses(&self) -> u16 {
        self.warehouses
    }

    pub fn terminal_count(&self) -> u64 {
        self.terminal_count
    }

    pub fn warehouse_edge_count(&self) -> u64 {
        self.warehouse_edge_count
    }

    pub fn district_edge_count(&self) -> u64 {
        self.district_edge_count
    }

    pub fn warehouse_endpoint_bits(&self, warehouse_id: u16) -> Option<u32> {
        warehouse_id
            .checked_sub(1)
            .and_then(|index| self.warehouse_endpoints.get(usize::from(index)))
            .map(|endpoint| endpoint.endpoint_bits)
    }

    pub fn warehouse_update_count(&self, warehouse_id: u16) -> Option<u64> {
        warehouse_id
            .checked_sub(1)
            .and_then(|index| self.warehouse_endpoints.get(usize::from(index)))
            .map(|endpoint| endpoint.update_count)
    }

    pub fn district_endpoint_bits(&self, warehouse_id: u16, district_id: u8) -> Option<u32> {
        self.district_index(warehouse_id, district_id)
            .and_then(|index| self.district_endpoints.get(index))
            .map(|endpoint| endpoint.endpoint_bits)
    }

    pub fn district_update_count(&self, warehouse_id: u16, district_id: u8) -> Option<u64> {
        self.district_index(warehouse_id, district_id)
            .and_then(|index| self.district_endpoints.get(index))
            .map(|endpoint| endpoint.update_count)
    }

    fn district_index(&self, warehouse_id: u16, district_id: u8) -> Option<usize> {
        if warehouse_id == 0
            || warehouse_id > self.warehouses
            || district_id == 0
            || district_id > DISTRICTS_PER_WAREHOUSE
        {
            return None;
        }
        Some(
            usize::from(warehouse_id - 1) * usize::from(DISTRICTS_PER_WAREHOUSE)
                + usize::from(district_id - 1),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edge(before: f32, amount: f32) -> PaymentFloatEdge {
        PaymentFloatEdge {
            before_bits: before.to_bits(),
            after_bits: (before + amount).to_bits(),
            amount_bits: amount.to_bits(),
        }
    }

    fn terminal(
        warehouse_id: u16,
        district_id: u8,
        warehouse_before: f32,
        district_before: f32,
        amount: f32,
    ) -> PaymentTerminalEvidence {
        PaymentTerminalEvidence {
            warehouse_id,
            district_id,
            warehouse: edge(warehouse_before, amount),
            district: edge(district_before, amount),
        }
    }

    #[test]
    fn out_of_order_edges_join_one_common_rooted_order() {
        let mut collector = PaymentEndpointCollector::new(50, 4).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let w1 = w0 + 2.0;
        let d1 = d0 + 2.0;
        let w2 = w1 + 3.0;
        let d2 = d1 + 3.0;

        collector
            .record_terminal(terminal(1, 1, w2, d2, 4.0))
            .unwrap();
        collector
            .record_terminal(terminal(1, 1, w1, d1, 3.0))
            .unwrap();
        collector
            .record_terminal(terminal(1, 1, w0, d0, 2.0))
            .unwrap();

        let sealed = collector.seal().unwrap();
        assert_eq!(sealed.terminal_count(), 3);
        assert_eq!(sealed.warehouse_edge_count(), 3);
        assert_eq!(sealed.district_edge_count(), 3);
        assert_eq!(sealed.warehouse_update_count(1), Some(3));
        assert_eq!(sealed.district_update_count(1, 1), Some(3));
        assert_eq!(
            sealed.warehouse_endpoint_bits(1),
            Some((w2 + 4.0).to_bits())
        );
        assert_eq!(
            sealed.district_endpoint_bits(1, 1),
            Some((d2 + 4.0).to_bits())
        );
        assert_eq!(sealed.warehouse_update_count(50), Some(0));
        assert_eq!(sealed.district_update_count(50, 10), Some(0));
    }

    #[test]
    fn contradictory_warehouse_and_district_orders_are_rejected() {
        let mut collector = PaymentEndpointCollector::new(1, 2).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        collector
            .record_terminal(terminal(1, 1, w0, d0 + 2.0, 1.0))
            .unwrap();
        collector
            .record_terminal(terminal(1, 1, w0 + 1.0, d0, 2.0))
            .unwrap();
        assert_eq!(
            collector.seal(),
            Err(PaymentEndpointError::Disconnected { pending_edges: 4 })
        );
    }

    #[test]
    fn late_self_loop_inside_reorder_bound_is_counted() {
        let mut collector = PaymentEndpointCollector::new(1, 2).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let large = 33_554_432.0_f32;
        collector
            .record_terminal(terminal(1, 1, w0, d0, large))
            .unwrap();
        let w1 = w0 + large;
        let d1 = d0 + large;
        collector
            .record_terminal(terminal(1, 1, w1, d1, 4.0))
            .unwrap();
        let late_loop = terminal(1, 1, w1, d1, 1.0);
        assert_eq!(
            late_loop.warehouse.before_bits,
            late_loop.warehouse.after_bits
        );
        assert_eq!(
            late_loop.district.before_bits,
            late_loop.district.after_bits
        );
        collector.record_terminal(late_loop).unwrap();

        let sealed = collector.seal().unwrap();
        assert_eq!(sealed.warehouse_update_count(1), Some(3));
        assert_eq!(sealed.district_update_count(1, 1), Some(3));
        assert_eq!(
            sealed.warehouse_endpoint_bits(1),
            Some((w1 + 4.0).to_bits())
        );
        assert_eq!(
            sealed.district_endpoint_bits(1, 1),
            Some((d1 + 4.0).to_bits())
        );
    }

    #[test]
    fn visible_forward_fork_poison_is_sticky() {
        let mut collector = PaymentEndpointCollector::new(1, 3).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        collector
            .record_terminal(terminal(1, 1, w0 + 10.0, d0 + 10.0, 2.0))
            .unwrap();
        assert!(matches!(
            collector.record_terminal(terminal(1, 1, w0 + 10.0, d0 + 10.0, 3.0)),
            Err(PaymentEndpointError::Fork { .. })
        ));
        assert_eq!(
            collector.record_terminal(terminal(1, 1, w0, d0, 10.0)),
            Err(PaymentEndpointError::Poisoned)
        );
        assert_eq!(collector.seal(), Err(PaymentEndpointError::Poisoned));
    }

    #[test]
    fn gap_is_rejected_at_seal() {
        let mut collector = PaymentEndpointCollector::new(1, 1).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        collector
            .record_terminal(terminal(1, 1, w0 + 5.0, d0 + 5.0, 1.0))
            .unwrap();
        assert_eq!(
            collector.seal(),
            Err(PaymentEndpointError::Disconnected { pending_edges: 2 })
        );
    }

    #[test]
    fn wrong_rne_bit_poison_does_not_mutate_state() {
        let mut collector = PaymentEndpointCollector::new(1, 1).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let mut bad = terminal(1, 1, w0, d0, 2.0);
        bad.warehouse.after_bits ^= 1;
        let before = collector.storage();
        assert_eq!(
            collector.record_terminal(bad),
            Err(PaymentEndpointError::FloatMismatch {
                domain: "warehouse.w_ytd"
            })
        );
        assert_eq!(collector.storage(), before);
    }

    #[test]
    fn paired_amount_mismatch_and_signed_zero_root_are_raw() {
        let mut mismatch = PaymentEndpointCollector::new(1, 1).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let mut evidence = terminal(1, 1, w0, d0, 2.0);
        evidence.district.amount_bits = 3.0_f32.to_bits();
        assert_eq!(
            mismatch.record_terminal(evidence),
            Err(PaymentEndpointError::PairedAmountMismatch)
        );

        let mut signed_zero = PaymentEndpointCollector::new(1, 1).unwrap();
        let bad_root = PaymentTerminalEvidence {
            warehouse_id: 1,
            district_id: 1,
            warehouse: edge(-0.0, 1.0),
            district: edge(d0, 1.0),
        };
        assert_eq!(
            signed_zero.record_terminal(bad_root),
            Err(PaymentEndpointError::StaleInterval {
                domain: "warehouse.w_ytd"
            })
        );
    }

    #[test]
    fn pending_overflow_is_atomic_and_sticky() {
        let mut collector = PaymentEndpointCollector::new(1, 1).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        collector
            .record_terminal(terminal(1, 1, w0 + 10.0, d0 + 10.0, 1.0))
            .unwrap();
        let before = collector.storage();

        assert_eq!(
            collector.record_terminal(terminal(1, 2, w0 + 20.0, d0 + 20.0, 1.0)),
            Err(PaymentEndpointError::PendingLimit {
                actual: 4,
                limit: 2
            })
        );
        assert_eq!(collector.storage(), before);
        assert_eq!(
            collector.record_terminal(terminal(1, 1, w0, d0, 10.0)),
            Err(PaymentEndpointError::Poisoned)
        );
    }

    #[test]
    fn district_fork_does_not_commit_valid_warehouse_scratch() {
        let mut collector = PaymentEndpointCollector::new(1, 2).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        collector
            .record_terminal(terminal(1, 1, w0 + 10.0, d0 + 20.0, 1.0))
            .unwrap();
        let before = collector.storage();
        let warehouse_before = collector.warehouse_chains[0];

        assert_eq!(
            collector.record_terminal(terminal(1, 1, w0, d0 + 20.0, 1.0)),
            Err(PaymentEndpointError::Fork {
                domain: "district.d_ytd"
            })
        );
        assert_eq!(collector.storage(), before);
        assert_eq!(
            collector.warehouse_chains[0].endpoint_bits,
            warehouse_before.endpoint_bits
        );
        assert_eq!(
            collector.warehouse_chains[0].update_count,
            warehouse_before.update_count
        );
    }

    #[test]
    fn persistent_reorder_capacity_never_grows() {
        let mut collector = PaymentEndpointCollector::new(1, 2).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let capacity = collector.storage().pending_capacity;
        collector
            .record_terminal(terminal(1, 1, w0, d0, 1.0))
            .unwrap();
        collector
            .record_terminal(terminal(1, 1, w0 + 1.0, d0 + 1.0, 1.0))
            .unwrap();
        collector
            .record_terminal(terminal(1, 1, w0 + 2.0, d0 + 2.0, 1.0))
            .unwrap();
        assert_eq!(collector.storage().pending_edges, 4);
        assert_eq!(collector.storage().pending_capacity, capacity);
    }

    #[test]
    fn one_million_updates_keep_fixed_collector_shape() {
        let mut collector = PaymentEndpointCollector::new(50, 32).unwrap();
        let initial = collector.storage();
        let mut warehouse = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let mut district = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        for _ in 0..1_000_000 {
            collector
                .record_terminal(terminal(1, 1, warehouse, district, 1.0))
                .unwrap();
            warehouse += 1.0;
            district += 1.0;
        }
        let final_storage = collector.storage();
        assert_eq!(final_storage.warehouse_slots, initial.warehouse_slots);
        assert_eq!(final_storage.district_slots, initial.district_slots);
        assert_eq!(final_storage.pending_capacity, initial.pending_capacity);
        assert_eq!(final_storage.pending_edges, initial.pending_capacity);
        assert_eq!(final_storage.terminal_count, 1_000_000);

        let sealed = collector.seal().unwrap();
        assert_eq!(sealed.terminal_count(), 1_000_000);
        assert_eq!(sealed.warehouse_update_count(1), Some(1_000_000));
        assert_eq!(sealed.district_update_count(1, 1), Some(1_000_000));
    }
}
