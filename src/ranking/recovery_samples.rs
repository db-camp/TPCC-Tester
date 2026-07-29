//! Seed-bound, mergeable bottom-k reservoirs for recovery evidence.
//!
//! A worker may see an arbitrary subset and ordering of ranked transactions.
//! Keeping the lowest ranks is composable: merging the bottom-k set from every
//! worker produces the same set as offering every distinct key to one serial
//! collector.  Ranking is derived only from the run seed, a fixed recovery
//! domain, and a framed canonical key.  Values and arrival order cannot bias
//! selection.
//!
//! Facts offered to this type are immutable.  Repeated equal observations are
//! idempotent; two different values for one retained key produce a sticky
//! conflict and `seal` fails closed.  Mutable Customer and Stock chains must be
//! validated and reduced to immutable final facts by the exact interval
//! collector before they are offered here.  Likewise, History multiplicities
//! must be finalized by their dedicated aggregation layer first.

use std::collections::BTreeMap;

use thiserror::Error;

pub const RECOVERY_SAMPLE_CAPACITY: usize = 64;
pub const MAX_CANONICAL_SAMPLE_KEY_BYTES: usize = 256;

/// The five independently ranked recovery evidence domains.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RecoverySampleDomain {
    NewOrder,
    Delivery,
    Stock,
    CustomerBadCredit,
    History,
}

impl RecoverySampleDomain {
    pub const fn canonical_tag(self) -> &'static [u8] {
        match self {
            Self::NewOrder => b"recovery/new-order/v1",
            Self::Delivery => b"recovery/delivery/v1",
            Self::Stock => b"recovery/stock/v1",
            Self::CustomerBadCredit => b"recovery/customer-bad-credit/v1",
            Self::History => b"recovery/history/v1",
        }
    }
}

/// An unambiguous, length-framed key used for ranking and tie-breaking.
///
/// Each part is retained byte-for-byte behind a format marker, part count, and
/// length prefix.  Integer fields should be supplied in an explicitly chosen
/// byte order; the convenience constructors use little-endian.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CanonicalSampleKey(Box<[u8]>);

impl CanonicalSampleKey {
    pub fn from_parts(parts: &[&[u8]]) -> Result<Self, RecoverySampleError> {
        let part_count = u16::try_from(parts.len())
            .map_err(|_| RecoverySampleError::TooManyCanonicalKeyParts(parts.len()))?;
        if part_count == 0 {
            return Err(RecoverySampleError::EmptyCanonicalKey);
        }

        let mut encoded_len = 6_usize;
        for part in parts {
            let _ = u32::try_from(part.len())
                .map_err(|_| RecoverySampleError::CanonicalKeyTooLong(usize::MAX))?;
            encoded_len = encoded_len
                .checked_add(4)
                .and_then(|length| length.checked_add(part.len()))
                .ok_or(RecoverySampleError::CanonicalKeyTooLong(usize::MAX))?;
        }
        if encoded_len > MAX_CANONICAL_SAMPLE_KEY_BYTES {
            return Err(RecoverySampleError::CanonicalKeyTooLong(encoded_len));
        }

        let mut encoded = Vec::with_capacity(encoded_len);
        encoded.extend_from_slice(b"RKS1");
        encoded.extend_from_slice(&part_count.to_le_bytes());
        for part in parts {
            encoded.extend_from_slice(&(part.len() as u32).to_le_bytes());
            encoded.extend_from_slice(part);
        }
        Ok(Self(encoded.into_boxed_slice()))
    }

    pub fn from_u64(value: u64) -> Self {
        Self::from_parts(&[&value.to_le_bytes()]).expect("one u64 key is within the fixed limit")
    }

    pub fn from_i32_fields(fields: &[i32]) -> Result<Self, RecoverySampleError> {
        let encoded_fields = fields
            .iter()
            .map(|field| field.to_le_bytes())
            .collect::<Vec<_>>();
        let parts = encoded_fields
            .iter()
            .map(|field| field.as_slice())
            .collect::<Vec<_>>();
        Self::from_parts(&parts)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// A 128-bit deterministic rank.  Lower ranks are retained.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SampleScore {
    pub high: u64,
    pub low: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RankKey {
    score: SampleScore,
    key: CanonicalSampleKey,
}

impl Ord for RankKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .cmp(&other.score)
            .then_with(|| self.key.cmp(&other.key))
    }
}

impl PartialOrd for RankKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RetainedFact<T> {
    Unique(T),
    Conflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfferOutcome {
    Inserted,
    AlreadyPresent,
    ConflictRecorded,
    IgnoredByRank,
}

/// A fixed-entry bottom-k reservoir.
///
/// The number of retained values never exceeds
/// [`RECOVERY_SAMPLE_CAPACITY`]. Payload types must enforce their own
/// field-size limits; the recovery evidence structs do so at their protocol
/// boundaries.
#[derive(Clone, Debug)]
pub struct BottomKRecoverySamples<T> {
    run_seed: u64,
    domain: RecoverySampleDomain,
    entries: BTreeMap<RankKey, RetainedFact<T>>,
}

impl<T> BottomKRecoverySamples<T>
where
    T: Clone + Eq,
{
    pub fn new(run_seed: u64, domain: RecoverySampleDomain) -> Self {
        Self {
            run_seed,
            domain,
            entries: BTreeMap::new(),
        }
    }

    pub fn run_seed(&self) -> u64 {
        self.run_seed
    }

    pub fn domain(&self) -> RecoverySampleDomain {
        self.domain
    }

    pub const fn capacity(&self) -> usize {
        RECOVERY_SAMPLE_CAPACITY
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn score_for(&self, key: &CanonicalSampleKey) -> SampleScore {
        score(self.run_seed, self.domain, key)
    }

    pub fn offer(&mut self, key: CanonicalSampleKey, value: T) -> OfferOutcome {
        if let Some(existing_rank) = self.entries.keys().find(|rank| rank.key == key).cloned() {
            let existing = self
                .entries
                .get_mut(&existing_rank)
                .expect("rank was obtained from this map");
            return match existing {
                RetainedFact::Unique(current) if *current == value => OfferOutcome::AlreadyPresent,
                RetainedFact::Unique(_) => {
                    *existing = RetainedFact::Conflict;
                    OfferOutcome::ConflictRecorded
                }
                RetainedFact::Conflict => OfferOutcome::ConflictRecorded,
            };
        }

        let rank = RankKey {
            score: self.score_for(&key),
            key,
        };
        if self.entries.len() == RECOVERY_SAMPLE_CAPACITY {
            let worst = self
                .entries
                .last_key_value()
                .map(|(worst, _)| worst.clone())
                .expect("non-zero full reservoir has a worst rank");
            if rank >= worst {
                return OfferOutcome::IgnoredByRank;
            }
            self.entries.remove(&worst);
        }
        self.entries.insert(rank, RetainedFact::Unique(value));
        OfferOutcome::Inserted
    }

    /// Transactionally merges another worker reservoir.
    ///
    /// On a binding mismatch, `self` is left unchanged. Conflicting immutable
    /// facts are combined into a sticky state that makes the final seal fail,
    /// independent of worker merge order.
    pub fn merge(&mut self, other: &Self) -> Result<(), RecoverySampleError> {
        if self.run_seed != other.run_seed || self.domain != other.domain {
            return Err(RecoverySampleError::RunBindingMismatch);
        }

        let mut merged = self.clone();
        for (rank, retained) in &other.entries {
            merged.offer_retained(rank.key.clone(), retained.clone());
        }
        *self = merged;
        Ok(())
    }

    /// Produces canonical `(score, key)` order for persistence and checking.
    pub fn seal(self) -> Result<SealedBottomKRecoverySamples<T>, RecoverySampleError> {
        let mut entries = Vec::with_capacity(self.entries.len());
        for (rank, retained) in self.entries {
            let value = match retained {
                RetainedFact::Unique(value) => value,
                RetainedFact::Conflict => {
                    return Err(RecoverySampleError::ConflictingFact { key: rank.key });
                }
            };
            entries.push(SealedRecoverySample {
                score: rank.score,
                key: rank.key,
                value,
            });
        }
        Ok(SealedBottomKRecoverySamples {
            run_seed: self.run_seed,
            domain: self.domain,
            entries,
        })
    }

    fn offer_retained(&mut self, key: CanonicalSampleKey, incoming: RetainedFact<T>) {
        if let Some(existing_rank) = self.entries.keys().find(|rank| rank.key == key).cloned() {
            let existing = self
                .entries
                .get_mut(&existing_rank)
                .expect("rank was obtained from this map");
            *existing = match (&*existing, incoming) {
                (RetainedFact::Unique(left), RetainedFact::Unique(right)) if *left == right => {
                    RetainedFact::Unique(right)
                }
                _ => RetainedFact::Conflict,
            };
            return;
        }

        let rank = RankKey {
            score: self.score_for(&key),
            key,
        };
        if self.entries.len() == RECOVERY_SAMPLE_CAPACITY {
            let worst = self
                .entries
                .last_key_value()
                .map(|(worst, _)| worst.clone())
                .expect("fixed-capacity reservoir has a worst rank");
            if rank >= worst {
                return;
            }
            self.entries.remove(&worst);
        }
        self.entries.insert(rank, incoming);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedRecoverySample<T> {
    pub score: SampleScore,
    pub key: CanonicalSampleKey,
    pub value: T,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedBottomKRecoverySamples<T> {
    pub run_seed: u64,
    pub domain: RecoverySampleDomain,
    pub entries: Vec<SealedRecoverySample<T>>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RecoverySampleError {
    #[error("canonical recovery sample key must contain at least one part")]
    EmptyCanonicalKey,
    #[error("canonical recovery sample key has too many parts: {0}")]
    TooManyCanonicalKeyParts(usize),
    #[error(
        "canonical recovery sample key is {0} bytes, limit is {MAX_CANONICAL_SAMPLE_KEY_BYTES}"
    )]
    CanonicalKeyTooLong(usize),
    #[error("bottom-k recovery samples have different run seed or domain bindings")]
    RunBindingMismatch,
    #[error("recovery sample key has conflicting immutable facts: {key:?}")]
    ConflictingFact { key: CanonicalSampleKey },
}

fn score(run_seed: u64, domain: RecoverySampleDomain, key: &CanonicalSampleKey) -> SampleScore {
    let tag = domain.canonical_tag();
    SampleScore {
        high: hash_lane(
            run_seed ^ 0x243f_6a88_85a3_08d3,
            tag,
            key.as_bytes(),
            0x9e37_79b9_7f4a_7c15,
        ),
        low: hash_lane(
            run_seed ^ 0x1319_8a2e_0370_7344,
            tag,
            key.as_bytes(),
            0xd1b5_4a32_d192_ed03,
        ),
    }
}

fn hash_lane(seed: u64, domain: &[u8], key: &[u8], lane: u64) -> u64 {
    let mut state = mix64(
        seed ^ lane ^ (domain.len() as u64).rotate_left(17) ^ (key.len() as u64).rotate_left(41),
    );
    absorb(&mut state, domain, lane);
    state = mix64(state ^ 0xa409_3822_299f_31d0);
    absorb(&mut state, key, lane.rotate_left(23));
    mix64(state ^ lane ^ 0x082e_fa98_ec4e_6c89)
}

fn absorb(state: &mut u64, bytes: &[u8], lane: u64) {
    for (index, chunk) in bytes.chunks(8).enumerate() {
        let mut block = [0_u8; 8];
        block[..chunk.len()].copy_from_slice(chunk);
        if chunk.len() < block.len() {
            block[chunk.len()] = 0x80;
        }
        let word = u64::from_le_bytes(block);
        *state = mix64(*state ^ word ^ lane ^ (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    }
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::SeedableRng;

    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestValue {
        marker: u64,
    }

    fn key(number: u64) -> CanonicalSampleKey {
        CanonicalSampleKey::from_u64(number)
    }

    fn value(number: u64) -> TestValue {
        TestValue {
            marker: number.wrapping_mul(17),
        }
    }

    #[test]
    fn serial_selection_matches_independent_bottom_k_oracle() {
        const CAPACITY: usize = 64;
        assert_eq!(CAPACITY, RECOVERY_SAMPLE_CAPACITY);
        let mut collector =
            BottomKRecoverySamples::<TestValue>::new(71, RecoverySampleDomain::Stock);
        let mut facts = BTreeMap::new();

        for number in 0..10_000_u64 {
            let sample_value = value(number);
            collector.offer(key(number), sample_value.clone());
            facts.insert(number, sample_value);
        }

        let mut oracle = facts
            .into_iter()
            .map(|(number, sample_value)| {
                let canonical_key = key(number);
                (
                    score(71, RecoverySampleDomain::Stock, &canonical_key),
                    canonical_key,
                    sample_value,
                )
            })
            .collect::<Vec<_>>();
        oracle.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        oracle.truncate(CAPACITY);

        let sealed = collector.seal().unwrap();
        let actual = sealed
            .entries
            .into_iter()
            .map(|entry| (entry.score, entry.key, entry.value))
            .collect::<Vec<_>>();
        assert_eq!(actual, oracle);
    }

    #[test]
    fn shuffled_worker_merge_matches_serial_oracle() {
        const WORKERS: usize = 32;
        let mut offers = (0..20_000_u64).collect::<Vec<_>>();
        offers.shuffle(&mut StdRng::seed_from_u64(91));

        let mut serial =
            BottomKRecoverySamples::<TestValue>::new(808, RecoverySampleDomain::NewOrder);
        let mut workers = (0..WORKERS)
            .map(|_| BottomKRecoverySamples::<TestValue>::new(808, RecoverySampleDomain::NewOrder))
            .collect::<Vec<_>>();
        for (ordinal, number) in offers.into_iter().enumerate() {
            let sample_value = value(number);
            serial.offer(key(number), sample_value.clone());
            workers[ordinal % WORKERS].offer(key(number), sample_value);
        }
        workers.shuffle(&mut StdRng::seed_from_u64(92));

        let mut merged =
            BottomKRecoverySamples::<TestValue>::new(808, RecoverySampleDomain::NewOrder);
        for worker in &workers {
            merged.merge(worker).unwrap();
        }
        assert_eq!(merged.seal().unwrap(), serial.seal().unwrap());
    }

    #[test]
    fn duplicate_key_is_idempotent_or_fails_closed() {
        let mut samples =
            BottomKRecoverySamples::<TestValue>::new(1, RecoverySampleDomain::Delivery);
        let canonical_key = key(9);

        assert_eq!(
            samples.offer(canonical_key.clone(), value(9)),
            OfferOutcome::Inserted
        );
        assert_eq!(
            samples.offer(canonical_key.clone(), value(9)),
            OfferOutcome::AlreadyPresent
        );
        assert_eq!(
            samples.offer(canonical_key.clone(), TestValue { marker: 999 },),
            OfferOutcome::ConflictRecorded
        );
        assert_eq!(
            samples.offer(canonical_key.clone(), value(9)),
            OfferOutcome::ConflictRecorded
        );
        assert_eq!(
            samples.seal(),
            Err(RecoverySampleError::ConflictingFact { key: canonical_key })
        );
    }

    #[test]
    fn conflicting_merge_is_sticky_and_binding_mismatch_is_transactional() {
        let mut left = BottomKRecoverySamples::<TestValue>::new(4, RecoverySampleDomain::History);
        left.offer(key(1), value(1));

        let mut conflict =
            BottomKRecoverySamples::<TestValue>::new(4, RecoverySampleDomain::History);
        conflict.offer(key(1), TestValue { marker: 55 });
        left.merge(&conflict).unwrap();
        assert_eq!(
            left.clone().seal(),
            Err(RecoverySampleError::ConflictingFact { key: key(1) })
        );

        let different_seed =
            BottomKRecoverySamples::<TestValue>::new(5, RecoverySampleDomain::History);
        let conflicted = left.clone();
        assert_eq!(
            left.merge(&different_seed),
            Err(RecoverySampleError::RunBindingMismatch)
        );
        let different_domain =
            BottomKRecoverySamples::<TestValue>::new(4, RecoverySampleDomain::Stock);
        assert_eq!(
            left.merge(&different_domain),
            Err(RecoverySampleError::RunBindingMismatch)
        );
        assert_eq!(left.seal(), conflicted.seal());
    }

    #[test]
    fn immutable_conflict_survives_every_worker_merge_order() {
        let facts = [value(7), TestValue { marker: 999 }, value(7)];
        let workers = facts.map(|fact| {
            let mut worker = BottomKRecoverySamples::new(44, RecoverySampleDomain::NewOrder);
            worker.offer(key(7), fact);
            worker
        });
        let permutations = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];

        for permutation in permutations {
            let mut merged = BottomKRecoverySamples::new(44, RecoverySampleDomain::NewOrder);
            for index in permutation {
                merged.merge(&workers[index]).unwrap();
            }
            assert_eq!(
                merged.seal(),
                Err(RecoverySampleError::ConflictingFact { key: key(7) })
            );
        }

        let mut left_branch = workers[0].clone();
        left_branch.merge(&workers[1]).unwrap();
        let mut right_branch = workers[2].clone();
        right_branch.merge(&workers[0]).unwrap();
        left_branch.merge(&right_branch).unwrap();
        assert_eq!(
            left_branch.seal(),
            Err(RecoverySampleError::ConflictingFact { key: key(7) })
        );
    }

    #[test]
    fn seed_and_domain_are_part_of_every_score() {
        let canonical_key = CanonicalSampleKey::from_i32_fields(&[1, 2, 3]).unwrap();
        let seed_a = BottomKRecoverySamples::<u64>::new(10, RecoverySampleDomain::Stock);
        let seed_b = BottomKRecoverySamples::<u64>::new(11, RecoverySampleDomain::Stock);
        let domain_b = BottomKRecoverySamples::<u64>::new(10, RecoverySampleDomain::History);

        assert_ne!(
            seed_a.score_for(&canonical_key),
            seed_b.score_for(&canonical_key)
        );
        assert_ne!(
            seed_a.score_for(&canonical_key),
            domain_b.score_for(&canonical_key)
        );
        assert_ne!(seed_a.score_for(&canonical_key), seed_a.score_for(&key(99)));
    }

    #[test]
    fn canonical_key_framing_prevents_part_ambiguity_and_enforces_limit() {
        let split = CanonicalSampleKey::from_parts(&[b"a", b"bc"]).unwrap();
        let other_split = CanonicalSampleKey::from_parts(&[b"ab", b"c"]).unwrap();
        assert_ne!(split, other_split);
        assert_eq!(
            CanonicalSampleKey::from_parts(&[]),
            Err(RecoverySampleError::EmptyCanonicalKey)
        );

        let oversized = vec![0_u8; MAX_CANONICAL_SAMPLE_KEY_BYTES];
        assert!(matches!(
            CanonicalSampleKey::from_parts(&[&oversized]),
            Err(RecoverySampleError::CanonicalKeyTooLong(_))
        ));
    }

    #[test]
    fn score_collision_uses_canonical_key_as_stable_tie_break() {
        let score = SampleScore { high: 3, low: 4 };
        let first = RankKey { score, key: key(1) };
        let second = RankKey { score, key: key(2) };
        assert_eq!(first.cmp(&second), first.key.cmp(&second.key));
    }

    #[test]
    fn one_million_offers_retain_constant_entry_count() {
        let mut samples =
            BottomKRecoverySamples::<u64>::new(19, RecoverySampleDomain::CustomerBadCredit);
        for ordinal in 0..1_000_000_u64 {
            samples.offer(key(ordinal), ordinal);
            if ordinal % 8_192 == 0 {
                assert!(samples.len() <= RECOVERY_SAMPLE_CAPACITY);
            }
        }
        assert_eq!(samples.len(), RECOVERY_SAMPLE_CAPACITY);
        assert_eq!(
            samples.seal().unwrap().entries.len(),
            RECOVERY_SAMPLE_CAPACITY
        );
    }
}
