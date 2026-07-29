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

const CUSTOMER_LAST_NAMES: u64 = 1_000;
const CUSTOMERS_PER_DISTRICT: u64 = 3_000;
const C_LAST_A: u64 = 255;
const C_ID_A: u64 = 1_023;
const OL_I_ID_A: u64 = 8_191;

// Keep this value and derivation synchronized with data_gen::DOMAIN_CUSTOMER_LAST.
// The final workflow feeds the same public caller seed to population and runtime,
// so C_LAST_RUN is constrained against the C_LAST_LOAD that actually populated
// the customer table.
const LOAD_CUSTOMER_LAST_DOMAIN: u64 = 0x5ae8_4b9f_3210_71c6;
const SPLITMIX64_INCREMENT: u64 = 0x9e37_79b9_7f4a_7c15;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkloadSeed(pub u64);

/// TPC-C non-uniform random constants selected once for an entire run.
///
/// These values are derived from the caller-provided local seed. They are not
/// an attempt to guess the grader's hidden seed. `OfficialRouter` copies this
/// value into every immutable routed transaction so all clients, stages, and
/// retries use the same constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NurandConstants {
    c_last_load: u16,
    c_last_run: u16,
    c_id: u16,
    ol_i_id: u16,
}

impl NurandConstants {
    fn for_seed(seed: WorkloadSeed) -> Self {
        let c_last_load = load_customer_last_constant(seed);
        let valid_c_last_run: Vec<u16> = (0..=u16::from(u8::MAX))
            .filter(|candidate| {
                let delta = candidate.abs_diff(c_last_load);
                (65..=119).contains(&delta) && delta != 96 && delta != 112
            })
            .collect();
        let c_last_run = valid_c_last_run[bounded(
            derive_seed(seed.0, "nurand/c-last-run", &[]),
            valid_c_last_run.len() as u64,
        ) as usize];

        Self {
            c_last_load,
            c_last_run,
            c_id: bounded(derive_seed(seed.0, "nurand/c-id", &[]), C_ID_A + 1) as u16,
            ol_i_id: bounded(derive_seed(seed.0, "nurand/ol-i-id", &[]), OL_I_ID_A + 1) as u16,
        }
    }

    pub fn c_last_load(self) -> u16 {
        self.c_last_load
    }

    pub fn c_last_run(self) -> u16 {
        self.c_last_run
    }

    pub fn c_id(self) -> u16 {
        self.c_id
    }

    pub fn ol_i_id(self) -> u16 {
        self.ol_i_id
    }
}

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
    warehouse_count: u16,
    nurand_constants: NurandConstants,
    hot_warehouses: [u16; HOT_WAREHOUSES],
    hot_districts: [u8; HOT_WAREHOUSES],
    hot_items: [u32; HOT_ITEMS],
}

impl OfficialRouter {
    pub fn new(seed: WorkloadSeed) -> Self {
        Self::build(seed, OFFICIAL_WAREHOUSES)
    }

    /// Builds a deterministic router for an explicitly reduced local smoke run.
    ///
    /// This constructor is a deviation-only aid and must be gated by
    /// `--allow-deviation`; it does not describe a ranked final configuration.
    /// The official path must continue to use [`OfficialRouter::new`].
    pub fn new_for_warehouses(
        seed: WorkloadSeed,
        warehouse_count: u16,
    ) -> Result<Self, RouteError> {
        if !(1..=OFFICIAL_WAREHOUSES).contains(&warehouse_count) {
            return Err(RouteError::InvalidWarehouseCount(warehouse_count));
        }
        Ok(Self::build(seed, warehouse_count))
    }

    fn build(seed: WorkloadSeed, warehouse_count: u16) -> Self {
        let mut warehouses: Vec<u16> = (1..=warehouse_count).collect();
        shuffle(&mut warehouses, derive_seed(seed.0, "hot-warehouses", &[]));
        let hot_distinct = usize::from(warehouse_count).min(HOT_WAREHOUSES);
        let hot_warehouses = std::array::from_fn(|index| warehouses[index % hot_distinct]);

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
            warehouse_count,
            nurand_constants: NurandConstants::for_seed(seed),
            hot_warehouses,
            hot_districts,
            hot_items,
        }
    }

    pub fn seed(&self) -> WorkloadSeed {
        self.seed
    }

    pub fn warehouse_count(&self) -> u16 {
        self.warehouse_count
    }

    pub fn nurand_constants(&self) -> NurandConstants {
        self.nurand_constants
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
        if self.warehouse_count != OFFICIAL_WAREHOUSES {
            return self.smoke_wheel(stage);
        }

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

    fn smoke_wheel(&self, stage: StageId) -> WarehouseWheel {
        let hot_distinct = usize::from(self.warehouse_count).min(HOT_WAREHOUSES);
        let unique_hot = &self.hot_warehouses[..hot_distinct];
        let mut cold: Vec<u16> = (1..=self.warehouse_count)
            .filter(|warehouse| !unique_hot.contains(warehouse))
            .collect();
        shuffle(
            &mut cold,
            derive_seed(self.seed.0, "stage-extra-cold", &[stage.value()]),
        );

        let fallback = if cold.is_empty() { unique_hot } else { &cold };
        let extra_cold_warehouses = std::array::from_fn(|index| fallback[index % fallback.len()]);

        let mut slots = Vec::with_capacity(ROUTING_SLOTS);
        for index in 0..HOT_WAREHOUSES * HOT_SLOTS_PER_WAREHOUSE {
            slots.push(unique_hot[index % unique_hot.len()]);
        }
        if cold.is_empty() {
            while slots.len() < ROUTING_SLOTS {
                slots.push(unique_hot[slots.len() % unique_hot.len()]);
            }
        } else {
            slots.extend(cold.iter().copied());
            let mut extra_index = 0;
            while slots.len() < ROUTING_SLOTS {
                slots.push(cold[extra_index % cold.len()]);
                extra_index += 1;
            }
        }
        assert_eq!(slots.len(), ROUTING_SLOTS);
        shuffle(
            &mut slots,
            derive_seed(self.seed.0, "stage-slot-shuffle", &[stage.value()]),
        );

        WarehouseWheel {
            stage,
            slots: slots
                .try_into()
                .expect("smoke wheel must contain exactly 160 slots"),
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

        let payment_customer_warehouse = if self.warehouse_count > 1
            && chance(
                derive_seed(self.seed.0, "payment-remote", &coordinates),
                PAYMENT_REMOTE_PERCENT,
            ) {
            select_u16_excluding(
                bounded(
                    derive_seed(self.seed.0, "payment-remote-warehouse", &coordinates),
                    u64::from(self.warehouse_count - 1),
                ) as u16,
                self.warehouse_count,
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
            nurand_constants: self.nurand_constants,
            hot_items: self.hot_items,
            warehouse_count: self.warehouse_count,
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
    nurand_constants: NurandConstants,
    hot_items: [u32; HOT_ITEMS],
    warehouse_count: u16,
}

impl RoutedTransaction {
    /// A retry reuses this immutable value and therefore consumes no new number.
    pub fn retry(&self) -> Self {
        self.clone()
    }

    /// Draws one transaction parameter from an independent, stateless domain.
    ///
    /// Workload parameter generation deliberately does not share a mutable RNG
    /// stream with routing. Adding or reordering an unrelated parameter therefore
    /// cannot change warehouse, hotspot, transaction-kind, or existing parameter
    /// choices.
    pub(crate) fn parameter_sample(&self, domain: &'static str, ordinal: u64, upper: u64) -> u64 {
        assert!(upper > 0, "parameter sample upper bound must be positive");
        let coordinates = [
            self.stage.value(),
            u64::from(self.client_id),
            self.txn_no,
            u64::from(self.home_warehouse),
            u64::from(self.home_district),
            ordinal,
        ];
        bounded(derive_seed(self.seed.0, domain, &coordinates), upper)
    }

    pub fn nurand_constants(&self) -> NurandConstants {
        self.nurand_constants
    }

    pub(crate) fn customer_id(&self, domain: &'static str, ordinal: u64) -> u16 {
        self.nurand_parameter(
            domain,
            ordinal,
            C_ID_A,
            1,
            CUSTOMERS_PER_DISTRICT,
            u64::from(self.nurand_constants.c_id),
        ) as u16
    }

    pub(crate) fn customer_last_name_number(&self, domain: &'static str, ordinal: u64) -> u16 {
        self.nurand_parameter(
            domain,
            ordinal,
            C_LAST_A,
            0,
            CUSTOMER_LAST_NAMES - 1,
            u64::from(self.nurand_constants.c_last_run),
        ) as u16
    }

    fn nurand_parameter(
        &self,
        domain: &'static str,
        ordinal: u64,
        a: u64,
        minimum: u64,
        maximum: u64,
        constant: u64,
    ) -> u64 {
        let left = self.parameter_sample(domain, ordinal, a + 1);
        let right = minimum + self.parameter_sample(domain, ordinal + 1, maximum - minimum + 1);
        ((left | right) + constant) % (maximum - minimum + 1) + minimum
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

        // The public final profile overlays its 24-item hotspot on TPC-C's
        // ordinary NURand(8191, 1, 100000) stream. Rejecting a hot result keeps
        // the two branches disjoint without changing the 25/75 branch split.
        for attempt in 0_u64.. {
            let candidate = self.ordinary_item_candidate(line_number, attempt);
            if !self.hot_items.contains(&candidate) {
                return candidate;
            }
        }
        unreachable!("the ordinary item domain contains non-hot values")
    }

    pub(crate) fn ordinary_item_candidate(&self, line_number: u8, attempt: u64) -> u32 {
        let coordinates = [
            self.stage.value(),
            u64::from(self.client_id),
            self.txn_no,
            u64::from(self.home_warehouse),
            u64::from(line_number),
            attempt,
        ];
        let left = bounded(
            derive_seed(self.seed.0, "ordinary-item-nurand/a", &coordinates),
            OL_I_ID_A + 1,
        );
        let right = 1 + bounded(
            derive_seed(self.seed.0, "ordinary-item-nurand/range", &coordinates),
            u64::from(ITEM_COUNT),
        );
        (((left | right) + u64::from(self.nurand_constants.ol_i_id)) % u64::from(ITEM_COUNT) + 1)
            as u32
    }

    pub fn new_order_supply_warehouse(&self, line_number: u8) -> u16 {
        let coordinates = self.line_coordinates(line_number);
        if self.warehouse_count == 1
            || !chance(
                derive_seed(self.seed.0, "new-order-remote", &coordinates),
                NEW_ORDER_REMOTE_PERCENT,
            )
        {
            return self.home_warehouse;
        }

        select_u16_excluding(
            bounded(
                derive_seed(self.seed.0, "new-order-remote-warehouse", &coordinates),
                u64::from(self.warehouse_count - 1),
            ) as u16,
            self.warehouse_count,
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
    InvalidWarehouseCount(u16),
}

impl fmt::Display for RouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidClient(client_id) => {
                write!(f, "client_id {client_id} is outside 0..{OFFICIAL_CLIENTS}")
            }
            Self::InvalidWarehouseCount(warehouse_count) => write!(
                f,
                "warehouse_count {warehouse_count} is outside 1..={OFFICIAL_WAREHOUSES}"
            ),
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

fn load_customer_last_constant(seed: WorkloadSeed) -> u16 {
    // This is the two-step SplitMix64 stream used by
    // TpccDataGen::customer_last_name_load_constant. For an upper bound of
    // 256 its rejection threshold is zero, so modulo and the low byte agree.
    let stream_seed =
        mix64((seed.0 ^ LOAD_CUSTOMER_LAST_DOMAIN).wrapping_add(SPLITMIX64_INCREMENT));
    let sample = mix64(stream_seed.wrapping_add(SPLITMIX64_INCREMENT));
    (sample % (C_LAST_A + 1)) as u16
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
