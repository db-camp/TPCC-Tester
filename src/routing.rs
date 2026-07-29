//! Deterministic warehouse and hotspot routing for the 2026 final profile.
//!
//! `WorkloadSeed` is supplied by the caller.  It is useful for reproducible
//! local runs, but it does not claim to reproduce the grader's hidden seed.

use crate::profile::{
    transaction_for_bucket, TransactionKind, DISTRICTS_PER_WAREHOUSE,
    EXTRA_COLD_WAREHOUSES_PER_STAGE, HOT_DISTRICT_PERCENT, HOT_ITEMS, HOT_ITEM_PERCENT,
    HOT_SLOTS_PER_WAREHOUSE, HOT_WAREHOUSES, ITEM_COUNT, NEW_ORDER_REMOTE_PERCENT,
    OFFICIAL_CLIENTS, OFFICIAL_WAREHOUSES, PAYMENT_REMOTE_PERCENT, ROUTING_SLOTS, ROUTING_WAVES,
};
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkloadSeed(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StageId(u64);

impl StageId {
    pub const WARMUP: Self = Self(0);

    pub const fn measurement(index: u8) -> Self {
        Self(1 + index as u64)
    }

    pub const fn custom(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarehouseWheel {
    stage: StageId,
    slots: [u16; ROUTING_SLOTS],
    extra_cold_warehouses: [u16; EXTRA_COLD_WAREHOUSES_PER_STAGE],
}

impl WarehouseWheel {
    pub fn stage(&self) -> StageId {
        self.stage
    }

    pub fn slots(&self) -> &[u16; ROUTING_SLOTS] {
        &self.slots
    }

    pub fn extra_cold_warehouses(&self) -> &[u16; EXTRA_COLD_WAREHOUSES_PER_STAGE] {
        &self.extra_cold_warehouses
    }

    pub fn slot_index(client_id: u16, txn_no: u64) -> Result<usize, RouteError> {
        if client_id >= OFFICIAL_CLIENTS {
            return Err(RouteError::InvalidClient(client_id));
        }
        let index = u64::from(client_id)
            + u64::from(OFFICIAL_CLIENTS) * (txn_no % ROUTING_WAVES)
            + 13 * (txn_no / ROUTING_WAVES);
        Ok((index % ROUTING_SLOTS as u64) as usize)
    }

    pub fn warehouse_for(&self, client_id: u16, txn_no: u64) -> Result<u16, RouteError> {
        Ok(self.slots[Self::slot_index(client_id, txn_no)?])
    }
}

#[derive(Debug, Clone)]
pub struct OfficialRouter {
    seed: WorkloadSeed,
    hot_warehouses: [u16; HOT_WAREHOUSES],
    hot_districts: [u8; HOT_WAREHOUSES],
    hot_items: [u32; HOT_ITEMS],
}

impl OfficialRouter {
    pub fn new(seed: WorkloadSeed) -> Self {
        let mut warehouses: Vec<u16> = (1..=OFFICIAL_WAREHOUSES).collect();
        shuffle(&mut warehouses, derive_seed(seed.0, "hot-warehouses", &[]));
        let hot_warehouses: [u16; HOT_WAREHOUSES] = warehouses[..HOT_WAREHOUSES]
            .try_into()
            .expect("published hot warehouse count must fit");

        let hot_districts = hot_warehouses.map(|warehouse| {
            1 + bounded(
                derive_seed(seed.0, "hot-district", &[u64::from(warehouse)]),
                u64::from(DISTRICTS_PER_WAREHOUSE),
            ) as u8
        });

        let mut items: Vec<u32> = (1..=ITEM_COUNT).collect();
        shuffle(&mut items, derive_seed(seed.0, "hot-items", &[]));
        let hot_items: [u32; HOT_ITEMS] = items[..HOT_ITEMS]
            .try_into()
            .expect("published hot item count must fit");

        Self {
            seed,
            hot_warehouses,
            hot_districts,
            hot_items,
        }
    }

    pub fn seed(&self) -> WorkloadSeed {
        self.seed
    }

    pub fn hot_warehouses(&self) -> &[u16; HOT_WAREHOUSES] {
        &self.hot_warehouses
    }

    pub fn hot_items(&self) -> &[u32; HOT_ITEMS] {
        &self.hot_items
    }

    pub fn hot_district_for(&self, warehouse: u16) -> Option<u8> {
        self.hot_warehouses
            .iter()
            .position(|candidate| *candidate == warehouse)
            .map(|index| self.hot_districts[index])
    }

    pub fn wheel(&self, stage: StageId) -> WarehouseWheel {
        let mut cold: Vec<u16> = (1..=OFFICIAL_WAREHOUSES)
            .filter(|warehouse| !self.hot_warehouses.contains(warehouse))
            .collect();
        shuffle(
            &mut cold,
            derive_seed(self.seed.0, "stage-extra-cold", &[stage.value()]),
        );
        let extra_cold_warehouses: [u16; EXTRA_COLD_WAREHOUSES_PER_STAGE] = cold
            [..EXTRA_COLD_WAREHOUSES_PER_STAGE]
            .try_into()
            .expect("published extra cold count must fit");

        let mut slots = Vec::with_capacity(ROUTING_SLOTS);
        for warehouse in self.hot_warehouses {
            slots.extend(std::iter::repeat(warehouse).take(HOT_SLOTS_PER_WAREHOUSE));
        }
        slots.extend(cold.iter().copied());
        slots.extend(extra_cold_warehouses);
        assert_eq!(slots.len(), ROUTING_SLOTS);
        shuffle(
            &mut slots,
            derive_seed(self.seed.0, "stage-slot-shuffle", &[stage.value()]),
        );

        WarehouseWheel {
            stage,
            slots: slots
                .try_into()
                .expect("published wheel must contain exactly 160 slots"),
            extra_cold_warehouses,
        }
    }

    /// Freezes all routing choices associated with one newly picked transaction.
    ///
    /// Keep and reuse the returned value for a retry.  Calling this method is
    /// the only operation that consumes `txn_no` from `ClientSequence`.
    pub fn begin_transaction(
        &self,
        wheel: &WarehouseWheel,
        sequence: &mut ClientSequence,
    ) -> Result<RoutedTransaction, RouteError> {
        let txn_no = sequence.consume();
        let client_id = sequence.client_id;
        let home_warehouse = wheel.warehouse_for(client_id, txn_no)?;
        let coordinates = [
            wheel.stage.value(),
            u64::from(client_id),
            txn_no,
            u64::from(home_warehouse),
        ];
        let kind_bucket = bounded(
            derive_seed(self.seed.0, "transaction-kind", &coordinates),
            100,
        ) as u8;
        let kind = transaction_for_bucket(kind_bucket)
            .expect("a bounded transaction bucket is always valid");

        let home_district = match self.hot_district_for(home_warehouse) {
            Some(hot_district)
                if chance(
                    derive_seed(self.seed.0, "hot-district-choice", &coordinates),
                    HOT_DISTRICT_PERCENT,
                ) =>
            {
                hot_district
            }
            Some(hot_district) => {
                let rank = bounded(
                    derive_seed(self.seed.0, "cold-district-choice", &coordinates),
                    u64::from(DISTRICTS_PER_WAREHOUSE - 1),
                ) as u8;
                select_u8_excluding(rank, DISTRICTS_PER_WAREHOUSE, hot_district)
            }
            None => {
                1 + bounded(
                    derive_seed(self.seed.0, "district-choice", &coordinates),
                    u64::from(DISTRICTS_PER_WAREHOUSE),
                ) as u8
            }
        };

        let payment_customer_warehouse = if chance(
            derive_seed(self.seed.0, "payment-remote", &coordinates),
            PAYMENT_REMOTE_PERCENT,
        ) {
            select_u16_excluding(
                bounded(
                    derive_seed(self.seed.0, "payment-remote-warehouse", &coordinates),
                    u64::from(OFFICIAL_WAREHOUSES - 1),
                ) as u16,
                OFFICIAL_WAREHOUSES,
                home_warehouse,
            )
        } else {
            home_warehouse
        };

        Ok(RoutedTransaction {
            seed: self.seed,
            stage: wheel.stage,
            client_id,
            txn_no,
            kind,
            home_warehouse,
            home_district,
            payment_customer_warehouse,
            hot_items: self.hot_items,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSequence {
    client_id: u16,
    next_txn_no: u64,
}

impl ClientSequence {
    /// Creates a stage-local counter.  Construct a new sequence for every stage.
    pub fn new(client_id: u16) -> Result<Self, RouteError> {
        if client_id >= OFFICIAL_CLIENTS {
            return Err(RouteError::InvalidClient(client_id));
        }
        Ok(Self {
            client_id,
            next_txn_no: 0,
        })
    }

    pub fn client_id(&self) -> u16 {
        self.client_id
    }

    pub fn next_txn_no(&self) -> u64 {
        self.next_txn_no
    }

    fn consume(&mut self) -> u64 {
        let current = self.next_txn_no;
        self.next_txn_no = self
            .next_txn_no
            .checked_add(1)
            .expect("transaction sequence overflow");
        current
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedTransaction {
    seed: WorkloadSeed,
    pub stage: StageId,
    pub client_id: u16,
    pub txn_no: u64,
    pub kind: TransactionKind,
    pub home_warehouse: u16,
    pub home_district: u8,
    pub payment_customer_warehouse: u16,
    hot_items: [u32; HOT_ITEMS],
}

impl RoutedTransaction {
    /// A retry reuses this immutable value and therefore consumes no new number.
    pub fn retry(&self) -> Self {
        self.clone()
    }

    pub fn item_id(&self, line_number: u8) -> u32 {
        let coordinates = self.line_coordinates(line_number);
        if chance(
            derive_seed(self.seed.0, "hot-item-choice", &coordinates),
            HOT_ITEM_PERCENT,
        ) {
            let index = bounded(
                derive_seed(self.seed.0, "hot-item-index", &coordinates),
                HOT_ITEMS as u64,
            ) as usize;
            return self.hot_items[index];
        }

        let rank = bounded(
            derive_seed(self.seed.0, "ordinary-item-index", &coordinates),
            u64::from(ITEM_COUNT - HOT_ITEMS as u32),
        ) as u32;
        select_u32_excluding(rank, ITEM_COUNT, &self.hot_items)
    }

    pub fn new_order_supply_warehouse(&self, line_number: u8) -> u16 {
        let coordinates = self.line_coordinates(line_number);
        if !chance(
            derive_seed(self.seed.0, "new-order-remote", &coordinates),
            NEW_ORDER_REMOTE_PERCENT,
        ) {
            return self.home_warehouse;
        }

        select_u16_excluding(
            bounded(
                derive_seed(self.seed.0, "new-order-remote-warehouse", &coordinates),
                u64::from(OFFICIAL_WAREHOUSES - 1),
            ) as u16,
            OFFICIAL_WAREHOUSES,
            self.home_warehouse,
        )
    }

    fn line_coordinates(&self, line_number: u8) -> [u64; 5] {
        [
            self.stage.value(),
            u64::from(self.client_id),
            self.txn_no,
            u64::from(self.home_warehouse),
            u64::from(line_number),
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteError {
    InvalidClient(u16),
}

impl fmt::Display for RouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidClient(client_id) => {
                write!(f, "client_id {client_id} is outside 0..{OFFICIAL_CLIENTS}")
            }
        }
    }
}

impl Error for RouteError {}

fn select_u8_excluding(rank: u8, inclusive_max: u8, excluded: u8) -> u8 {
    let candidate = rank + 1;
    if candidate >= excluded {
        candidate + 1
    } else {
        candidate
    }
    .min(inclusive_max)
}

fn select_u16_excluding(rank: u16, inclusive_max: u16, excluded: u16) -> u16 {
    let candidate = rank + 1;
    if candidate >= excluded {
        candidate + 1
    } else {
        candidate
    }
    .min(inclusive_max)
}

fn select_u32_excluding(rank: u32, inclusive_max: u32, excluded: &[u32; HOT_ITEMS]) -> u32 {
    let mut excluded = *excluded;
    excluded.sort_unstable();
    let mut candidate = rank + 1;
    for value in excluded {
        if candidate >= value {
            candidate += 1;
        } else {
            break;
        }
    }
    debug_assert!(candidate <= inclusive_max);
    candidate
}

fn chance(sample: u64, percent: u8) -> bool {
    bounded(sample, 100) < u64::from(percent)
}

fn shuffle<T>(values: &mut [T], seed: u64) {
    let mut rng = SplitMix64::new(seed);
    for upper in (2..=values.len()).rev() {
        let index = rng.bounded(upper as u64) as usize;
        values.swap(upper - 1, index);
    }
}

fn bounded(seed: u64, upper: u64) -> u64 {
    SplitMix64::new(seed).bounded(upper)
}

fn derive_seed(base: u64, domain: &str, coordinates: &[u64]) -> u64 {
    let mut state = mix64(base ^ 0xa076_1d64_78bd_642f);
    for byte in domain.bytes() {
        state = mix64(state ^ u64::from(byte));
    }
    state = mix64(state ^ domain.len() as u64);
    for (index, coordinate) in coordinates.iter().enumerate() {
        state = mix64(state ^ mix64(*coordinate ^ index as u64));
    }
    state
}

#[derive(Debug, Clone, Copy)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mix64(self.state)
    }

    fn bounded(&mut self, upper: u64) -> u64 {
        assert!(upper > 0);
        let threshold = upper.wrapping_neg() % upper;
        loop {
            let product = u128::from(self.next()) * u128::from(upper);
            if product as u64 >= threshold {
                return (product >> 64) as u64;
            }
        }
    }
}

fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    fn router() -> OfficialRouter {
        OfficialRouter::new(WorkloadSeed(0x2026_cafe_f00d))
    }

    #[test]
    fn wheel_has_exact_published_multiplicities() {
        let router = router();
        for stage in [
            StageId::WARMUP,
            StageId::measurement(0),
            StageId::measurement(1),
            StageId::measurement(2),
        ] {
            let wheel = router.wheel(stage);
            let mut counts = HashMap::new();
            for warehouse in wheel.slots() {
                *counts.entry(*warehouse).or_insert(0_usize) += 1;
            }

            assert_eq!(counts.len(), OFFICIAL_WAREHOUSES as usize);
            for warehouse in router.hot_warehouses() {
                assert_eq!(counts[warehouse], HOT_SLOTS_PER_WAREHOUSE);
            }
            let extras: HashSet<_> = wheel.extra_cold_warehouses().iter().copied().collect();
            assert_eq!(extras.len(), EXTRA_COLD_WAREHOUSES_PER_STAGE);
            for warehouse in 1..=OFFICIAL_WAREHOUSES {
                if router.hot_warehouses().contains(&warehouse) {
                    continue;
                }
                assert_eq!(
                    counts[&warehouse],
                    if extras.contains(&warehouse) { 2 } else { 1 }
                );
            }
        }
    }

    #[test]
    fn synchronized_five_waves_cover_every_slot_once() {
        let indices: HashSet<_> = (0..ROUTING_WAVES)
            .flat_map(|txn_no| {
                (0..OFFICIAL_CLIENTS)
                    .map(move |client_id| WarehouseWheel::slot_index(client_id, txn_no).unwrap())
            })
            .collect();
        assert_eq!(indices, (0..ROUTING_SLOTS).collect());
        assert_eq!(WarehouseWheel::slot_index(0, 5).unwrap(), 13);
        assert_eq!(
            WarehouseWheel::slot_index(OFFICIAL_CLIENTS - 1, 4).unwrap(),
            ROUTING_SLOTS - 1
        );
    }

    #[test]
    fn stage_shuffle_is_independent_but_hot_identity_is_run_scoped() {
        let router = router();
        let warmup = router.wheel(StageId::WARMUP);
        let first = router.wheel(StageId::measurement(0));
        let second = router.wheel(StageId::measurement(1));

        assert_ne!(warmup.slots(), first.slots());
        assert_ne!(first.slots(), second.slots());
        for wheel in [&warmup, &first, &second] {
            for hot in router.hot_warehouses() {
                assert_eq!(
                    wheel.slots().iter().filter(|value| *value == hot).count(),
                    HOT_SLOTS_PER_WAREHOUSE
                );
            }
        }
    }

    #[test]
    fn same_seed_reproduces_every_identity_and_wheel() {
        let left = router();
        let right = router();
        assert_eq!(left.hot_warehouses(), right.hot_warehouses());
        assert_eq!(left.hot_items(), right.hot_items());
        assert_eq!(
            left.wheel(StageId::measurement(2)),
            right.wheel(StageId::measurement(2))
        );
    }

    #[test]
    fn retry_is_bitwise_identical_and_does_not_consume_a_number() {
        let router = router();
        let wheel = router.wheel(StageId::measurement(0));
        let mut sequence = ClientSequence::new(7).unwrap();

        let original = router.begin_transaction(&wheel, &mut sequence).unwrap();
        assert_eq!(sequence.next_txn_no(), 1);
        assert_eq!(original, original.retry());
        assert_eq!(original.item_id(1), original.retry().item_id(1));
        assert_eq!(
            original.new_order_supply_warehouse(1),
            original.retry().new_order_supply_warehouse(1)
        );
        assert_eq!(sequence.next_txn_no(), 1);

        drop(original);
        let after_abandon = router.begin_transaction(&wheel, &mut sequence).unwrap();
        assert_eq!(after_abandon.txn_no, 1);
        assert_eq!(sequence.next_txn_no(), 2);
    }

    #[test]
    fn routed_parameters_are_valid_and_ratios_converge() {
        let router = router();
        let wheel = router.wheel(StageId::measurement(0));
        let hot_items: HashSet<_> = router.hot_items().iter().copied().collect();
        let mut hot_item_count = 0_u64;
        let mut remote_new_order_count = 0_u64;
        let mut remote_payment_count = 0_u64;
        let mut hot_district_opportunities = 0_u64;
        let mut hot_district_count = 0_u64;
        let mut total = 0_u64;

        for client_id in 0..OFFICIAL_CLIENTS {
            let mut sequence = ClientSequence::new(client_id).unwrap();
            for _ in 0..4_000 {
                let txn = router.begin_transaction(&wheel, &mut sequence).unwrap();
                let item = txn.item_id(1);
                let supply = txn.new_order_supply_warehouse(1);
                assert!((1..=ITEM_COUNT).contains(&item));
                assert!((1..=OFFICIAL_WAREHOUSES).contains(&supply));
                assert!((1..=OFFICIAL_WAREHOUSES).contains(&txn.payment_customer_warehouse));
                assert!((1..=DISTRICTS_PER_WAREHOUSE).contains(&txn.home_district));

                hot_item_count += u64::from(hot_items.contains(&item));
                remote_new_order_count += u64::from(supply != txn.home_warehouse);
                remote_payment_count +=
                    u64::from(txn.payment_customer_warehouse != txn.home_warehouse);
                if let Some(hot_district) = router.hot_district_for(txn.home_warehouse) {
                    hot_district_opportunities += 1;
                    hot_district_count += u64::from(txn.home_district == hot_district);
                }
                total += 1;
            }
        }

        let ratio = |count: u64, denominator: u64| count as f64 / denominator as f64;
        assert!((ratio(hot_item_count, total) - 0.25).abs() < 0.01);
        assert!((ratio(remote_new_order_count, total) - 0.08).abs() < 0.01);
        assert!((ratio(remote_payment_count, total) - 0.30).abs() < 0.01);
        assert!((ratio(hot_district_count, hot_district_opportunities) - 0.65).abs() < 0.01);
    }

    #[test]
    fn invalid_client_is_rejected() {
        assert_eq!(
            ClientSequence::new(OFFICIAL_CLIENTS),
            Err(RouteError::InvalidClient(OFFICIAL_CLIENTS))
        );
    }
}
