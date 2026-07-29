//! Strict canonical binary building blocks for bounded terminal artifacts.
//!
//! This module intentionally encodes only individual, stable sections. It is
//! not an artifact envelope and must not be published as a complete terminal
//! evidence bundle: the final artifact also binds the rich recovery sections
//! and outer run metadata.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use thiserror::Error;

use crate::consistency::{
    CustomerLogicalVersion, CustomerUpdateEndpoint, FloatError, NonNegativeF32Accumulator,
};
use crate::data_gen::TpccDataGen;
use crate::profile::OFFICIAL_WAREHOUSES;

use super::bounded_stats::{
    BoundedPhysicalStats, BoundedStatsError, ClassTotals, PartitionTotals, LEDGER_CLASS_COUNT,
    PHYSICAL_PARTITION_COUNT,
};
use super::evidence_collector::{
    CanonicalCustomerChain, CanonicalRejectedSample, CanonicalStockChain, CollectorError,
    CustomerKey, SealedIntervalEvidence, StockKey,
};
use super::payment_endpoints::{
    PaymentEndpointError, PaymentEndpointView, PersistedPaymentEndpoints,
    DISTRICTS_PER_WAREHOUSE as PAYMENT_DISTRICTS_PER_WAREHOUSE, MAX_PAYMENT_WAREHOUSES,
};
use super::recovery_samples::SampleScore;
use super::rich_recovery_samples::{
    CanonicalRichNewOrder, CanonicalRichOrderLine, CanonicalRichOrderWitness,
    CanonicalRichRecoveryHeader, OrderKey, RichRecoveryError, SealedRichRecoverySamples,
    MAX_RICH_RECOVERY_RAW_BYTES, RICH_RECOVERY_POLICY_VERSION, RICH_RECOVERY_SAMPLE_CAPACITY,
};
use super::runner::StockVersion;

const SECTION_MAGIC: [u8; 4] = *b"TCS1";
const SECTION_VERSION: u16 = 1;

pub(crate) const MAX_CORE_SECTION_BYTES: usize = 20 * 1024;
pub(crate) const MAX_CORE_SECTION_HEX_CHARS: usize = MAX_CORE_SECTION_BYTES * 2;
pub(crate) const MAX_PHYSICAL_STATS_SECTION_BYTES: usize = MAX_CORE_SECTION_BYTES;
pub(crate) const MAX_CUSTOMER_INTERVAL_SECTION_BYTES: usize = 4 * 1024;
pub(crate) const MAX_STOCK_INTERVAL_SECTION_BYTES: usize = 4 * 1024;
pub(crate) const MAX_PAYMENT_ENDPOINT_SECTION_BYTES: usize = 8 * 1024;
pub(crate) const MAX_RICH_NEW_ORDER_SECTION_BYTES: usize = 68 * 1024;
const MAX_ACCUMULATOR_WORDS: u32 = 6;
const MAX_INTERVAL_SAMPLES: u32 = 64;
const MAX_RICH_ENTRY_TIMESTAMP_BYTES: usize = 19;
const MAX_RICH_DELIVERY_TIMESTAMP_BYTES: usize = 30;
const RICH_DISTRICT_INFO_BYTES: usize = 24;
const MIN_RICH_ORDER_LINES: usize = 5;
const MAX_RICH_ORDER_LINES: usize = 15;
const SECTION_HEADER_BYTES: usize = 7;
const RICH_HEADER_BYTES: usize = 2 + 8 + 4 + 4 + 4 * 8;
const RICH_NEW_ORDER_WITNESS_BYTES: usize = 1 + 16 + 7;
const MAX_ENCODED_RICH_NEW_ORDER_LINE_BYTES: usize =
    1 + 4 + 2 + 1 + MAX_RICH_DELIVERY_TIMESTAMP_BYTES + 1 + 4 + 1 + RICH_DISTRICT_INFO_BYTES;
const MAX_ENCODED_RICH_NEW_ORDER_BYTES: usize = 16
    + 7
    + 2
    + 1
    + MAX_RICH_ENTRY_TIMESTAMP_BYTES
    + 1
    + 1
    + 1
    + 1
    + MAX_RICH_ORDER_LINES * MAX_ENCODED_RICH_NEW_ORDER_LINE_BYTES;
const _: () = assert!(
    SECTION_HEADER_BYTES
        + RICH_HEADER_BYTES
        + RICH_NEW_ORDER_WITNESS_BYTES
        + 4
        + RICH_RECOVERY_SAMPLE_CAPACITY * MAX_ENCODED_RICH_NEW_ORDER_BYTES
        <= MAX_RICH_NEW_ORDER_SECTION_BYTES
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum SectionKind {
    PhysicalStats = 1,
    CustomerIntervals = 2,
    StockIntervals = 3,
    PaymentEndpoints = 4,
    RichNewOrders = 5,
}

impl SectionKind {
    fn name(self) -> &'static str {
        match self {
            Self::PhysicalStats => "physical statistics",
            Self::CustomerIntervals => "customer intervals",
            Self::StockIntervals => "stock intervals",
            Self::PaymentEndpoints => "Payment endpoints",
            Self::RichNewOrders => "rich NewOrder samples",
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum CoreCodecError {
    #[error("{section} section is oversized: {actual} bytes exceeds {maximum}")]
    OversizedSection {
        section: &'static str,
        actual: usize,
        maximum: usize,
    },
    #[error("canonical section writer exceeded its {maximum}-byte limit")]
    WriterLimit { maximum: usize },
    #[error("canonical section is truncated")]
    Truncated,
    #[error("canonical section has invalid magic")]
    InvalidMagic,
    #[error("canonical section kind {actual} is not the expected {expected}")]
    UnexpectedSection { expected: &'static str, actual: u8 },
    #[error("unsupported canonical section version {actual}")]
    UnsupportedSectionVersion { actual: u16 },
    #[error("canonical section has {remaining} trailing bytes")]
    TrailingBytes { remaining: usize },
    #[error("{field} count {actual} exceeds the canonical limit {maximum}")]
    OversizedCount {
        field: &'static str,
        actual: u64,
        maximum: u64,
    },
    #[error("canonical lower-hex input is odd-length or oversized")]
    InvalidHexLength,
    #[error("canonical lower-hex input contains a non-lowercase-hex byte")]
    InvalidHexDigit,
    #[error("invalid canonical bounded physical statistics: {0}")]
    InvalidPhysicalStats(#[source] BoundedStatsError),
    #[error("invalid canonical {field} accumulator: {source}")]
    InvalidAccumulator {
        field: &'static str,
        #[source]
        source: FloatError,
    },
    #[error("canonical {field} accumulator sum is outside its live per-term range")]
    ImpossibleAccumulatorSum { field: &'static str },
    #[error("invalid canonical interval evidence: {0}")]
    InvalidIntervals(#[source] CollectorError),
    #[error("customer and Stock interval sections disagree on {0}")]
    MismatchedIntervalMetadata(&'static str),
    #[error("canonical interval {field} {actual} does not match trusted outer value {expected}")]
    IntervalBindingMismatch {
        field: &'static str,
        expected: u64,
        actual: u64,
    },
    #[error("canonical {field} presence flag has invalid value {actual}")]
    InvalidPresenceFlag { field: &'static str, actual: u8 },
    #[error("invalid canonical Payment endpoint evidence: {0}")]
    InvalidPaymentEndpoints(#[source] PaymentEndpointError),
    #[error(
        "canonical Payment endpoint view is missing {domain} key ({warehouse_id}, {district_id:?})"
    )]
    MissingPaymentEndpoint {
        domain: &'static str,
        warehouse_id: u16,
        district_id: Option<u8>,
    },
    #[error("{field} length {actual} is outside the canonical range {minimum}..={maximum}")]
    InvalidLength {
        field: &'static str,
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    #[error("canonical {field} boolean has invalid value {actual}")]
    InvalidBoolean { field: &'static str, actual: u8 },
    #[error("canonical {domain} samples are not in strict (score, key) order")]
    NonCanonicalRichOrder { domain: &'static str },
    #[error("canonical {domain} section contains duplicate key {key:?}")]
    DuplicateRichOrderKey { domain: &'static str, key: OrderKey },
    #[error("invalid canonical rich recovery evidence: {0}")]
    InvalidRichRecovery(#[source] RichRecoveryError),
}

/// A limit-aware writer for fixed-width little-endian canonical values.
///
/// It deliberately has no `usize`, native-struct, or floating-point methods.
pub(crate) struct CanonicalWriter {
    bytes: Vec<u8>,
    maximum: usize,
}

impl CanonicalWriter {
    pub(crate) fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }

    pub(crate) fn put_u8(&mut self, value: u8) -> Result<(), CoreCodecError> {
        self.put_bytes(&[value])
    }

    pub(crate) fn put_u16(&mut self, value: u16) -> Result<(), CoreCodecError> {
        self.put_bytes(&value.to_le_bytes())
    }

    pub(crate) fn put_u32(&mut self, value: u32) -> Result<(), CoreCodecError> {
        self.put_bytes(&value.to_le_bytes())
    }

    pub(crate) fn put_i32(&mut self, value: i32) -> Result<(), CoreCodecError> {
        self.put_bytes(&value.to_le_bytes())
    }

    pub(crate) fn put_u64(&mut self, value: u64) -> Result<(), CoreCodecError> {
        self.put_bytes(&value.to_le_bytes())
    }

    pub(crate) fn put_bytes(&mut self, value: &[u8]) -> Result<(), CoreCodecError> {
        let next =
            self.bytes
                .len()
                .checked_add(value.len())
                .ok_or(CoreCodecError::WriterLimit {
                    maximum: self.maximum,
                })?;
        if next > self.maximum {
            return Err(CoreCodecError::WriterLimit {
                maximum: self.maximum,
            });
        }
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

/// A non-allocating reader for fixed-width little-endian canonical values.
pub(crate) struct CanonicalReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CanonicalReader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(crate) fn get_u8(&mut self) -> Result<u8, CoreCodecError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn get_u16(&mut self) -> Result<u16, CoreCodecError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    pub(crate) fn get_u32(&mut self) -> Result<u32, CoreCodecError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub(crate) fn get_i32(&mut self) -> Result<i32, CoreCodecError> {
        Ok(i32::from_le_bytes(self.array()?))
    }

    pub(crate) fn get_u64(&mut self) -> Result<u64, CoreCodecError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    pub(crate) fn take(&mut self, length: usize) -> Result<&'a [u8], CoreCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CoreCodecError::Truncated)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(CoreCodecError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    pub(crate) fn bounded_count(
        &mut self,
        field: &'static str,
        maximum: u32,
    ) -> Result<u32, CoreCodecError> {
        let actual = self.get_u32()?;
        if actual > maximum {
            return Err(CoreCodecError::OversizedCount {
                field,
                actual: u64::from(actual),
                maximum: u64::from(maximum),
            });
        }
        Ok(actual)
    }

    pub(crate) fn finish(self) -> Result<(), CoreCodecError> {
        let remaining = self.bytes.len() - self.offset;
        if remaining == 0 {
            Ok(())
        } else {
            Err(CoreCodecError::TrailingBytes { remaining })
        }
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CoreCodecError> {
        self.take(N)?
            .try_into()
            .map_err(|_| CoreCodecError::Truncated)
    }
}

fn section_writer(kind: SectionKind, maximum: usize) -> Result<CanonicalWriter, CoreCodecError> {
    let mut writer = CanonicalWriter::new(maximum);
    writer.put_bytes(&SECTION_MAGIC)?;
    writer.put_u8(kind as u8)?;
    writer.put_u16(SECTION_VERSION)?;
    Ok(writer)
}

fn section_reader<'a>(
    bytes: &'a [u8],
    expected: SectionKind,
    maximum: usize,
) -> Result<CanonicalReader<'a>, CoreCodecError> {
    if bytes.len() > maximum {
        return Err(CoreCodecError::OversizedSection {
            section: expected.name(),
            actual: bytes.len(),
            maximum,
        });
    }
    let mut reader = CanonicalReader::new(bytes);
    if reader.take(SECTION_MAGIC.len())? != SECTION_MAGIC {
        return Err(CoreCodecError::InvalidMagic);
    }
    let actual = reader.get_u8()?;
    if actual != expected as u8 {
        return Err(CoreCodecError::UnexpectedSection {
            expected: expected.name(),
            actual,
        });
    }
    let version = reader.get_u16()?;
    if version != SECTION_VERSION {
        return Err(CoreCodecError::UnsupportedSectionVersion { actual: version });
    }
    Ok(reader)
}

pub(crate) fn encode_lower_hex(
    bytes: &[u8],
    maximum_bytes: usize,
) -> Result<String, CoreCodecError> {
    if bytes.len() > maximum_bytes {
        return Err(CoreCodecError::OversizedSection {
            section: "hex payload",
            actual: bytes.len(),
            maximum: maximum_bytes,
        });
    }
    let capacity = bytes
        .len()
        .checked_mul(2)
        .ok_or(CoreCodecError::InvalidHexLength)?;
    let mut encoded = String::with_capacity(capacity);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(encoded)
}

pub(crate) fn decode_lower_hex(
    encoded: &str,
    maximum_bytes: usize,
) -> Result<Vec<u8>, CoreCodecError> {
    if encoded.len() % 2 != 0 || encoded.len() / 2 > maximum_bytes {
        return Err(CoreCodecError::InvalidHexLength);
    }
    let mut decoded = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let high = lower_hex_nibble(pair[0])?;
        let low = lower_hex_nibble(pair[1])?;
        decoded.push((high << 4) | low);
    }
    Ok(decoded)
}

fn lower_hex_nibble(byte: u8) -> Result<u8, CoreCodecError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(CoreCodecError::InvalidHexDigit),
    }
}

pub(crate) fn encode_physical_stats_section(
    stats: &BoundedPhysicalStats,
) -> Result<Vec<u8>, CoreCodecError> {
    stats
        .validate()
        .map_err(CoreCodecError::InvalidPhysicalStats)?;
    let mut writer = section_writer(SectionKind::PhysicalStats, MAX_PHYSICAL_STATS_SECTION_BYTES)?;
    let (classes, partitions, payment_history, new_order_lines, delivery_customers) =
        stats.canonical_parts();
    validate_accumulator_ranges(payment_history, new_order_lines, delivery_customers)?;
    for totals in classes {
        encode_class_totals(&mut writer, *totals)?;
    }
    for totals in partitions {
        encode_partition_totals(&mut writer, *totals)?;
    }
    for (field, accumulators) in [
        ("Payment/history amount", payment_history),
        ("NewOrder line amount", new_order_lines),
        ("Delivery customer amount", delivery_customers),
    ] {
        for accumulator in accumulators {
            encode_accumulator(&mut writer, field, accumulator)?;
        }
    }
    Ok(writer.finish())
}

pub(crate) fn decode_physical_stats_section(
    bytes: &[u8],
) -> Result<BoundedPhysicalStats, CoreCodecError> {
    let mut reader = section_reader(
        bytes,
        SectionKind::PhysicalStats,
        MAX_PHYSICAL_STATS_SECTION_BYTES,
    )?;
    let mut classes = [ClassTotals::default(); LEDGER_CLASS_COUNT];
    for totals in &mut classes {
        *totals = decode_class_totals(&mut reader)?;
    }
    let mut partitions = [PartitionTotals::default(); PHYSICAL_PARTITION_COUNT];
    for totals in &mut partitions {
        *totals = decode_partition_totals(&mut reader)?;
    }
    let mut payment_history = std::array::from_fn(|_| NonNegativeF32Accumulator::default());
    let mut new_order_lines = std::array::from_fn(|_| NonNegativeF32Accumulator::default());
    let mut delivery_customers = std::array::from_fn(|_| NonNegativeF32Accumulator::default());
    for accumulator in &mut payment_history {
        *accumulator = decode_accumulator(&mut reader, "Payment/history amount")?;
    }
    for accumulator in &mut new_order_lines {
        *accumulator = decode_accumulator(&mut reader, "NewOrder line amount")?;
    }
    for accumulator in &mut delivery_customers {
        *accumulator = decode_accumulator(&mut reader, "Delivery customer amount")?;
    }
    reader.finish()?;
    validate_accumulator_ranges(&payment_history, &new_order_lines, &delivery_customers)?;
    BoundedPhysicalStats::from_canonical_parts(
        classes,
        partitions,
        payment_history,
        new_order_lines,
        delivery_customers,
    )
    .map_err(CoreCodecError::InvalidPhysicalStats)
}

fn encode_class_totals(
    writer: &mut CanonicalWriter,
    totals: ClassTotals,
) -> Result<(), CoreCodecError> {
    for value in [
        totals.new_order_commits,
        totals.payment_commits,
        totals.order_status_commits,
        totals.delivery_commits,
        totals.stock_level_commits,
        totals.expected_rollbacks,
        totals.new_orders,
        totals.new_order_lines,
        totals.remote_new_order_lines,
        totals.stock_quantity_delta,
        totals.delivered_orders,
        totals.delivered_order_lines,
    ] {
        writer.put_u64(value)?;
    }
    Ok(())
}

fn decode_class_totals(reader: &mut CanonicalReader<'_>) -> Result<ClassTotals, CoreCodecError> {
    Ok(ClassTotals {
        new_order_commits: reader.get_u64()?,
        payment_commits: reader.get_u64()?,
        order_status_commits: reader.get_u64()?,
        delivery_commits: reader.get_u64()?,
        stock_level_commits: reader.get_u64()?,
        expected_rollbacks: reader.get_u64()?,
        new_orders: reader.get_u64()?,
        new_order_lines: reader.get_u64()?,
        remote_new_order_lines: reader.get_u64()?,
        stock_quantity_delta: reader.get_u64()?,
        delivered_orders: reader.get_u64()?,
        delivered_order_lines: reader.get_u64()?,
    })
}

fn encode_partition_totals(
    writer: &mut CanonicalWriter,
    totals: PartitionTotals,
) -> Result<(), CoreCodecError> {
    writer.put_u64(totals.new_orders)?;
    writer.put_u64(totals.new_order_lines)?;
    writer.put_u64(totals.delivered_orders)?;
    writer.put_u64(totals.delivered_order_lines)
}

fn decode_partition_totals(
    reader: &mut CanonicalReader<'_>,
) -> Result<PartitionTotals, CoreCodecError> {
    Ok(PartitionTotals {
        new_orders: reader.get_u64()?,
        new_order_lines: reader.get_u64()?,
        delivered_orders: reader.get_u64()?,
        delivered_order_lines: reader.get_u64()?,
    })
}

fn encode_accumulator(
    writer: &mut CanonicalWriter,
    field: &'static str,
    accumulator: &NonNegativeF32Accumulator,
) -> Result<(), CoreCodecError> {
    let (term_count, words) = accumulator.to_words();
    let word_count = u32::try_from(words.len()).map_err(|_| CoreCodecError::OversizedCount {
        field,
        actual: u64::MAX,
        maximum: u64::from(MAX_ACCUMULATOR_WORDS),
    })?;
    if word_count > MAX_ACCUMULATOR_WORDS {
        return Err(CoreCodecError::OversizedCount {
            field,
            actual: u64::from(word_count),
            maximum: u64::from(MAX_ACCUMULATOR_WORDS),
        });
    }
    writer.put_u64(term_count)?;
    writer.put_u32(word_count)?;
    for word in words {
        writer.put_u64(word)?;
    }
    Ok(())
}

fn decode_accumulator(
    reader: &mut CanonicalReader<'_>,
    field: &'static str,
) -> Result<NonNegativeF32Accumulator, CoreCodecError> {
    let term_count = reader.get_u64()?;
    let word_count = reader.bounded_count(field, MAX_ACCUMULATOR_WORDS)?;
    let mut words = Vec::with_capacity(word_count as usize);
    for _ in 0..word_count {
        words.push(reader.get_u64()?);
    }
    NonNegativeF32Accumulator::from_words(term_count, &words)
        .map_err(|source| CoreCodecError::InvalidAccumulator { field, source })
}

fn validate_accumulator_ranges(
    payment_history: &[NonNegativeF32Accumulator; LEDGER_CLASS_COUNT],
    new_order_lines: &[NonNegativeF32Accumulator; LEDGER_CLASS_COUNT],
    delivery_customers: &[NonNegativeF32Accumulator; LEDGER_CLASS_COUNT],
) -> Result<(), CoreCodecError> {
    validate_accumulator_group_range(
        payment_history,
        "Payment/history amount",
        1.0_f32.to_bits(),
        5_000.0_f32.to_bits(),
    )?;
    validate_accumulator_group_range(
        new_order_lines,
        "NewOrder line amount",
        1.0_f32.to_bits(),
        1_000.0_f32.to_bits(),
    )?;
    // Initial undelivered lines may be binary32(0.01). Delivery sums at
    // least five values exactly as binary64, then rounds once to binary32.
    // 0x3d4ccccc is that smallest possible order total.
    validate_accumulator_group_range(
        delivery_customers,
        "Delivery customer amount",
        0x3d4c_cccc,
        150_000.0_f32.to_bits(),
    )
}

fn validate_accumulator_group_range(
    accumulators: &[NonNegativeF32Accumulator; LEDGER_CLASS_COUNT],
    field: &'static str,
    minimum_bits: u32,
    maximum_bits: u32,
) -> Result<(), CoreCodecError> {
    for accumulator in accumulators {
        validate_accumulator_range(accumulator, field, minimum_bits, maximum_bits)?;
    }
    Ok(())
}

fn validate_accumulator_range(
    accumulator: &NonNegativeF32Accumulator,
    field: &'static str,
    minimum_bits: u32,
    maximum_bits: u32,
) -> Result<(), CoreCodecError> {
    let term_count = accumulator.term_count();
    let actual_words = accumulator.to_words().1;
    let mut minimum = NonNegativeF32Accumulator::default();
    minimum
        .add_repeated_bits(minimum_bits, term_count)
        .map_err(|source| CoreCodecError::InvalidAccumulator { field, source })?;
    let mut maximum = NonNegativeF32Accumulator::default();
    maximum
        .add_repeated_bits(maximum_bits, term_count)
        .map_err(|source| CoreCodecError::InvalidAccumulator { field, source })?;
    let minimum_words = minimum.to_words().1;
    let maximum_words = maximum.to_words().1;
    if compare_accumulator_words(&actual_words, &minimum_words) == Ordering::Less
        || compare_accumulator_words(&actual_words, &maximum_words) == Ordering::Greater
    {
        return Err(CoreCodecError::ImpossibleAccumulatorSum { field });
    }
    Ok(())
}

fn compare_accumulator_words(left: &[u64], right: &[u64]) -> Ordering {
    left.len()
        .cmp(&right.len())
        .then_with(|| left.iter().rev().cmp(right.iter().rev()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IntervalMetadata {
    warehouses: u16,
    sample_seed: u64,
    policy_version: u32,
}

/// Trusted outer metadata required to restore persisted interval sections.
///
/// Keeping these values out of the section payload prevents two mutually
/// consistent but foreign sections from silently rebinding a terminal
/// artifact to another run or dataset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IntervalSectionBinding {
    warehouses: u16,
    sample_seed: u64,
    load_seed: u64,
}

impl IntervalSectionBinding {
    pub(crate) const fn new(warehouses: u16, sample_seed: u64, load_seed: u64) -> Self {
        Self {
            warehouses,
            sample_seed,
            load_seed,
        }
    }
}

pub(crate) struct DecodedCustomerIntervals {
    metadata: IntervalMetadata,
    update_count: u64,
    rejected: Option<CanonicalRejectedSample>,
    entries: Vec<CanonicalCustomerChain>,
}

pub(crate) struct DecodedStockIntervals {
    metadata: IntervalMetadata,
    update_count: u64,
    rejected: Option<CanonicalRejectedSample>,
    entries: Vec<CanonicalStockChain>,
}

pub(crate) fn encode_customer_interval_section(
    intervals: &SealedIntervalEvidence,
) -> Result<Vec<u8>, CoreCodecError> {
    let mut writer = section_writer(
        SectionKind::CustomerIntervals,
        MAX_CUSTOMER_INTERVAL_SECTION_BYTES,
    )?;
    encode_interval_metadata(&mut writer, intervals)?;
    writer.put_u64(intervals.customer_update_count())?;
    encode_rejected_sample(&mut writer, intervals.customer_rejected_sample())?;
    let count = encode_sample_count(
        "customer interval samples",
        intervals.customer_sample_count(),
    )?;
    writer.put_u32(count)?;
    for chain in intervals.customers() {
        let key = chain.key();
        let endpoint = chain.endpoint();
        writer.put_i32(key.warehouse_id)?;
        writer.put_i32(key.district_id)?;
        writer.put_i32(key.customer_id)?;
        writer.put_u64(chain.sample_rank())?;
        writer.put_i32(endpoint.version.payment_count)?;
        writer.put_i32(endpoint.version.delivery_count)?;
        writer.put_u32(endpoint.balance_bits)?;
        writer.put_u32(endpoint.ytd_payment_bits)?;
    }
    Ok(writer.finish())
}

pub(crate) fn decode_customer_interval_section(
    bytes: &[u8],
) -> Result<DecodedCustomerIntervals, CoreCodecError> {
    let mut reader = section_reader(
        bytes,
        SectionKind::CustomerIntervals,
        MAX_CUSTOMER_INTERVAL_SECTION_BYTES,
    )?;
    let metadata = decode_interval_metadata(&mut reader)?;
    let update_count = reader.get_u64()?;
    let rejected = decode_rejected_sample(&mut reader, "customer rejected sample")?;
    let count = reader.bounded_count("customer interval samples", MAX_INTERVAL_SAMPLES)?;
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let key = CustomerKey {
            warehouse_id: reader.get_i32()?,
            district_id: reader.get_i32()?,
            customer_id: reader.get_i32()?,
        };
        let sample_rank = reader.get_u64()?;
        let endpoint = CustomerUpdateEndpoint {
            version: CustomerLogicalVersion {
                payment_count: reader.get_i32()?,
                delivery_count: reader.get_i32()?,
            },
            balance_bits: reader.get_u32()?,
            ytd_payment_bits: reader.get_u32()?,
        };
        entries.push(CanonicalCustomerChain::new(key, sample_rank, endpoint));
    }
    reader.finish()?;
    Ok(DecodedCustomerIntervals {
        metadata,
        update_count,
        rejected,
        entries,
    })
}

pub(crate) fn encode_stock_interval_section(
    intervals: &SealedIntervalEvidence,
) -> Result<Vec<u8>, CoreCodecError> {
    let mut writer = section_writer(
        SectionKind::StockIntervals,
        MAX_STOCK_INTERVAL_SECTION_BYTES,
    )?;
    encode_interval_metadata(&mut writer, intervals)?;
    writer.put_u64(intervals.stock_update_count())?;
    encode_rejected_sample(&mut writer, intervals.stock_rejected_sample())?;
    let count = encode_sample_count("Stock interval samples", intervals.stock_sample_count())?;
    writer.put_u32(count)?;
    for chain in intervals.stocks() {
        let key = chain.key();
        writer.put_i32(key.warehouse_id)?;
        writer.put_i32(key.item_id)?;
        writer.put_u64(chain.sample_rank())?;
        encode_stock_version(&mut writer, &chain.initial())?;
        encode_stock_version(&mut writer, &chain.endpoint())?;
    }
    Ok(writer.finish())
}

pub(crate) fn decode_stock_interval_section(
    bytes: &[u8],
) -> Result<DecodedStockIntervals, CoreCodecError> {
    let mut reader = section_reader(
        bytes,
        SectionKind::StockIntervals,
        MAX_STOCK_INTERVAL_SECTION_BYTES,
    )?;
    let metadata = decode_interval_metadata(&mut reader)?;
    let update_count = reader.get_u64()?;
    let rejected = decode_rejected_sample(&mut reader, "Stock rejected sample")?;
    let count = reader.bounded_count("Stock interval samples", MAX_INTERVAL_SAMPLES)?;
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let key = StockKey {
            warehouse_id: reader.get_i32()?,
            item_id: reader.get_i32()?,
        };
        let sample_rank = reader.get_u64()?;
        let initial = decode_stock_version(&mut reader)?;
        let endpoint = decode_stock_version(&mut reader)?;
        entries.push(CanonicalStockChain::new(
            key,
            sample_rank,
            initial,
            endpoint,
        ));
    }
    reader.finish()?;
    Ok(DecodedStockIntervals {
        metadata,
        update_count,
        rejected,
        entries,
    })
}

pub(crate) fn decode_interval_sections(
    customer_bytes: &[u8],
    stock_bytes: &[u8],
    binding: IntervalSectionBinding,
) -> Result<SealedIntervalEvidence, CoreCodecError> {
    let customers = decode_customer_interval_section(customer_bytes)?;
    let stocks = decode_stock_interval_section(stock_bytes)?;
    restore_interval_evidence(customers, stocks, binding)
}

pub(crate) fn restore_interval_evidence(
    customers: DecodedCustomerIntervals,
    stocks: DecodedStockIntervals,
    binding: IntervalSectionBinding,
) -> Result<SealedIntervalEvidence, CoreCodecError> {
    if customers.metadata.warehouses != stocks.metadata.warehouses {
        return Err(CoreCodecError::MismatchedIntervalMetadata("warehouses"));
    }
    if customers.metadata.sample_seed != stocks.metadata.sample_seed {
        return Err(CoreCodecError::MismatchedIntervalMetadata("sample seed"));
    }
    if customers.metadata.policy_version != stocks.metadata.policy_version {
        return Err(CoreCodecError::MismatchedIntervalMetadata(
            "sample policy version",
        ));
    }
    let metadata = customers.metadata;
    if metadata.warehouses != binding.warehouses {
        return Err(CoreCodecError::IntervalBindingMismatch {
            field: "warehouse count",
            expected: u64::from(binding.warehouses),
            actual: u64::from(metadata.warehouses),
        });
    }
    if metadata.sample_seed != binding.sample_seed {
        return Err(CoreCodecError::IntervalBindingMismatch {
            field: "sample seed",
            expected: binding.sample_seed,
            actual: metadata.sample_seed,
        });
    }
    let generator = TpccDataGen::with_seed(i32::from(metadata.warehouses), binding.load_seed);
    let roots = |key: StockKey| {
        Some(StockVersion {
            quantity: generator.initial_stock_quantity(key.warehouse_id, key.item_id),
            ytd_bits: 0.0_f32.to_bits(),
            order_count: 0,
            remote_count: 0,
        })
    };
    SealedIntervalEvidence::from_canonical_entries(
        metadata.warehouses,
        metadata.sample_seed,
        metadata.policy_version,
        customers.update_count,
        stocks.update_count,
        customers.rejected,
        stocks.rejected,
        customers.entries,
        stocks.entries,
        &roots,
    )
    .map_err(CoreCodecError::InvalidIntervals)
}

fn encode_interval_metadata(
    writer: &mut CanonicalWriter,
    intervals: &SealedIntervalEvidence,
) -> Result<(), CoreCodecError> {
    writer.put_u16(intervals.warehouses())?;
    writer.put_u64(intervals.sample_seed())?;
    writer.put_u32(intervals.policy_version())
}

fn decode_interval_metadata(
    reader: &mut CanonicalReader<'_>,
) -> Result<IntervalMetadata, CoreCodecError> {
    Ok(IntervalMetadata {
        warehouses: reader.get_u16()?,
        sample_seed: reader.get_u64()?,
        policy_version: reader.get_u32()?,
    })
}

fn encode_rejected_sample(
    writer: &mut CanonicalWriter,
    rejected: Option<CanonicalRejectedSample>,
) -> Result<(), CoreCodecError> {
    match rejected {
        None => writer.put_u8(0),
        Some(sample) => {
            writer.put_u8(1)?;
            writer.put_u64(sample.rank())?;
            writer.put_u32(sample.key_index())
        }
    }
}

fn decode_rejected_sample(
    reader: &mut CanonicalReader<'_>,
    field: &'static str,
) -> Result<Option<CanonicalRejectedSample>, CoreCodecError> {
    match reader.get_u8()? {
        0 => Ok(None),
        1 => Ok(Some(CanonicalRejectedSample::new(
            reader.get_u64()?,
            reader.get_u32()?,
        ))),
        actual => Err(CoreCodecError::InvalidPresenceFlag { field, actual }),
    }
}

fn encode_sample_count(field: &'static str, count: usize) -> Result<u32, CoreCodecError> {
    let actual = u64::try_from(count).unwrap_or(u64::MAX);
    let count = u32::try_from(count).map_err(|_| CoreCodecError::OversizedCount {
        field,
        actual,
        maximum: u64::from(MAX_INTERVAL_SAMPLES),
    })?;
    if count > MAX_INTERVAL_SAMPLES {
        return Err(CoreCodecError::OversizedCount {
            field,
            actual: u64::from(count),
            maximum: u64::from(MAX_INTERVAL_SAMPLES),
        });
    }
    Ok(count)
}

fn encode_stock_version(
    writer: &mut CanonicalWriter,
    version: &StockVersion,
) -> Result<(), CoreCodecError> {
    writer.put_i32(version.quantity)?;
    writer.put_u32(version.ytd_bits)?;
    writer.put_i32(version.order_count)?;
    writer.put_i32(version.remote_count)
}

fn decode_stock_version(reader: &mut CanonicalReader<'_>) -> Result<StockVersion, CoreCodecError> {
    Ok(StockVersion {
        quantity: reader.get_i32()?,
        ytd_bits: reader.get_u32()?,
        order_count: reader.get_i32()?,
        remote_count: reader.get_i32()?,
    })
}

/// Encode the endpoint-only Payment section in fixed Warehouse/District key
/// order.
///
/// The decoder deliberately returns [`PersistedPaymentEndpoints`], never live
/// sealed Payment evidence: this compact section cannot recreate the paired
/// amounts or their common live serial order.
pub(crate) fn encode_payment_endpoint_section(
    endpoints: &dyn PaymentEndpointView,
) -> Result<Vec<u8>, CoreCodecError> {
    let canonical = canonicalize_payment_view(endpoints)?;
    let mut writer = section_writer(
        SectionKind::PaymentEndpoints,
        MAX_PAYMENT_ENDPOINT_SECTION_BYTES,
    )?;
    writer.put_u16(canonical.warehouses())?;
    writer.put_u64(canonical.terminal_count())?;
    writer.put_u64(canonical.warehouse_edge_count())?;
    writer.put_u64(canonical.district_edge_count())?;
    for warehouse_id in 1..=canonical.warehouses() {
        encode_payment_endpoint(
            &mut writer,
            canonical.warehouse_endpoint_bits(warehouse_id).ok_or(
                CoreCodecError::MissingPaymentEndpoint {
                    domain: "Warehouse",
                    warehouse_id,
                    district_id: None,
                },
            )?,
            canonical.warehouse_update_count(warehouse_id).ok_or(
                CoreCodecError::MissingPaymentEndpoint {
                    domain: "Warehouse",
                    warehouse_id,
                    district_id: None,
                },
            )?,
        )?;
    }
    for warehouse_id in 1..=canonical.warehouses() {
        for district_id in 1..=PAYMENT_DISTRICTS_PER_WAREHOUSE {
            encode_payment_endpoint(
                &mut writer,
                canonical
                    .district_endpoint_bits(warehouse_id, district_id)
                    .ok_or(CoreCodecError::MissingPaymentEndpoint {
                        domain: "District",
                        warehouse_id,
                        district_id: Some(district_id),
                    })?,
                canonical
                    .district_update_count(warehouse_id, district_id)
                    .ok_or(CoreCodecError::MissingPaymentEndpoint {
                        domain: "District",
                        warehouse_id,
                        district_id: Some(district_id),
                    })?,
            )?;
        }
    }
    Ok(writer.finish())
}

pub(crate) fn decode_payment_endpoint_section(
    bytes: &[u8],
) -> Result<PersistedPaymentEndpoints, CoreCodecError> {
    let mut reader = section_reader(
        bytes,
        SectionKind::PaymentEndpoints,
        MAX_PAYMENT_ENDPOINT_SECTION_BYTES,
    )?;
    let warehouses = reader.get_u16()?;
    if warehouses > MAX_PAYMENT_WAREHOUSES {
        return Err(CoreCodecError::OversizedCount {
            field: "Payment warehouses",
            actual: u64::from(warehouses),
            maximum: u64::from(MAX_PAYMENT_WAREHOUSES),
        });
    }
    let terminal_count = reader.get_u64()?;
    let warehouse_edge_count = reader.get_u64()?;
    let district_edge_count = reader.get_u64()?;
    let mut warehouse_endpoints = Vec::with_capacity(usize::from(warehouses));
    for _ in 0..warehouses {
        warehouse_endpoints.push(decode_payment_endpoint(&mut reader)?);
    }
    let district_count = usize::from(warehouses) * usize::from(PAYMENT_DISTRICTS_PER_WAREHOUSE);
    let mut district_endpoints = Vec::with_capacity(district_count);
    for _ in 0..district_count {
        district_endpoints.push(decode_payment_endpoint(&mut reader)?);
    }
    reader.finish()?;
    PersistedPaymentEndpoints::from_canonical_endpoints(
        warehouses,
        terminal_count,
        warehouse_edge_count,
        district_edge_count,
        warehouse_endpoints,
        district_endpoints,
    )
    .map_err(CoreCodecError::InvalidPaymentEndpoints)
}

fn canonicalize_payment_view(
    endpoints: &dyn PaymentEndpointView,
) -> Result<PersistedPaymentEndpoints, CoreCodecError> {
    let warehouses = endpoints.warehouses();
    if warehouses > MAX_PAYMENT_WAREHOUSES {
        return Err(CoreCodecError::OversizedCount {
            field: "Payment warehouses",
            actual: u64::from(warehouses),
            maximum: u64::from(MAX_PAYMENT_WAREHOUSES),
        });
    }
    let mut warehouse_endpoints = Vec::with_capacity(usize::from(warehouses));
    for warehouse_id in 1..=warehouses {
        warehouse_endpoints.push((
            endpoints.warehouse_endpoint_bits(warehouse_id).ok_or(
                CoreCodecError::MissingPaymentEndpoint {
                    domain: "Warehouse",
                    warehouse_id,
                    district_id: None,
                },
            )?,
            endpoints.warehouse_update_count(warehouse_id).ok_or(
                CoreCodecError::MissingPaymentEndpoint {
                    domain: "Warehouse",
                    warehouse_id,
                    district_id: None,
                },
            )?,
        ));
    }
    let district_count = usize::from(warehouses) * usize::from(PAYMENT_DISTRICTS_PER_WAREHOUSE);
    let mut district_endpoints = Vec::with_capacity(district_count);
    for warehouse_id in 1..=warehouses {
        for district_id in 1..=PAYMENT_DISTRICTS_PER_WAREHOUSE {
            district_endpoints.push((
                endpoints
                    .district_endpoint_bits(warehouse_id, district_id)
                    .ok_or(CoreCodecError::MissingPaymentEndpoint {
                        domain: "District",
                        warehouse_id,
                        district_id: Some(district_id),
                    })?,
                endpoints
                    .district_update_count(warehouse_id, district_id)
                    .ok_or(CoreCodecError::MissingPaymentEndpoint {
                        domain: "District",
                        warehouse_id,
                        district_id: Some(district_id),
                    })?,
            ));
        }
    }
    PersistedPaymentEndpoints::from_canonical_endpoints(
        warehouses,
        endpoints.terminal_count(),
        endpoints.warehouse_edge_count(),
        endpoints.district_edge_count(),
        warehouse_endpoints,
        district_endpoints,
    )
    .map_err(CoreCodecError::InvalidPaymentEndpoints)
}

fn encode_payment_endpoint(
    writer: &mut CanonicalWriter,
    endpoint_bits: u32,
    update_count: u64,
) -> Result<(), CoreCodecError> {
    writer.put_u32(endpoint_bits)?;
    writer.put_u64(update_count)
}

fn decode_payment_endpoint(reader: &mut CanonicalReader<'_>) -> Result<(u32, u64), CoreCodecError> {
    Ok((reader.get_u32()?, reader.get_u64()?))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DecodedRichNewOrderSection {
    header: CanonicalRichRecoveryHeader,
    rejected: Option<CanonicalRichOrderWitness>,
    entries: Vec<CanonicalRichNewOrder>,
}

pub(crate) fn encode_rich_new_order_section(
    samples: &SealedRichRecoverySamples,
) -> Result<Vec<u8>, CoreCodecError> {
    let mut writer = section_writer(SectionKind::RichNewOrders, MAX_RICH_NEW_ORDER_SECTION_BYTES)?;
    encode_rich_header(&mut writer, samples)?;
    encode_rich_order_witness(&mut writer, samples.order_rejected_witness())?;
    let count = encode_rich_count("rich NewOrder samples", samples.new_orders().len())?;
    writer.put_u32(count)?;

    let mut previous = None;
    let mut keys = BTreeSet::new();
    for sample in samples.new_orders() {
        validate_rich_order_entry(
            "NewOrder",
            &mut previous,
            &mut keys,
            sample.score(),
            sample.key(),
        )?;
        encode_sample_score(&mut writer, sample.score())?;
        encode_order_key(&mut writer, sample.key())?;
        writer.put_u16(sample.customer_id())?;
        encode_bounded_bytes(
            &mut writer,
            "NewOrder entry timestamp",
            sample.entry_timestamp(),
            1,
            MAX_RICH_ENTRY_TIMESTAMP_BYTES,
        )?;
        writer.put_u8(sample.carrier_id())?;
        let line_count = sample.lines().len();
        if usize::from(sample.line_count()) != line_count {
            return Err(CoreCodecError::InvalidRichRecovery(
                RichRecoveryError::InvalidEvidence(
                    "sealed NewOrder line_count differs from its retained lines",
                ),
            ));
        }
        let line_count = encode_u8_count(
            "NewOrder lines",
            line_count,
            MIN_RICH_ORDER_LINES,
            MAX_RICH_ORDER_LINES,
        )?;
        writer.put_u8(line_count)?;
        encode_boolean(&mut writer, sample.all_local())?;
        encode_boolean(&mut writer, sample.queue_present())?;
        for line in sample.lines() {
            writer.put_u8(line.number())?;
            writer.put_u32(line.item_id())?;
            writer.put_u16(line.supply_warehouse())?;
            encode_bounded_bytes(
                &mut writer,
                "NewOrder line delivery timestamp",
                line.delivery_timestamp(),
                0,
                MAX_RICH_DELIVERY_TIMESTAMP_BYTES,
            )?;
            writer.put_u8(line.quantity())?;
            writer.put_u32(line.amount_bits())?;
            encode_bounded_bytes(
                &mut writer,
                "NewOrder line district information",
                line.district_info(),
                RICH_DISTRICT_INFO_BYTES,
                RICH_DISTRICT_INFO_BYTES,
            )?;
        }
    }
    Ok(writer.finish())
}

fn decode_rich_new_order_section(
    bytes: &[u8],
) -> Result<DecodedRichNewOrderSection, CoreCodecError> {
    let mut reader = section_reader(
        bytes,
        SectionKind::RichNewOrders,
        MAX_RICH_NEW_ORDER_SECTION_BYTES,
    )?;
    let header = decode_rich_header(&mut reader)?;
    let rejected = decode_rich_order_witness(&mut reader, "NewOrder cutoff witness")?;
    let count = reader.bounded_count(
        "rich NewOrder samples",
        u32::try_from(RICH_RECOVERY_SAMPLE_CAPACITY).expect("sample capacity fits u32"),
    )?;
    let mut entries = Vec::with_capacity(count as usize);
    let mut previous = None;
    let mut keys = BTreeSet::new();
    for _ in 0..count {
        let score = decode_sample_score(&mut reader)?;
        let key = decode_order_key(&mut reader)?;
        validate_rich_order_entry("NewOrder", &mut previous, &mut keys, score, key)?;
        let customer_id = reader.get_u16()?;
        let entry_timestamp = decode_bounded_bytes(
            &mut reader,
            "NewOrder entry timestamp",
            1,
            MAX_RICH_ENTRY_TIMESTAMP_BYTES,
        )?;
        let carrier_id = reader.get_u8()?;
        let line_count = decode_u8_count(
            &mut reader,
            "NewOrder lines",
            MIN_RICH_ORDER_LINES,
            MAX_RICH_ORDER_LINES,
        )?;
        let all_local = decode_boolean(&mut reader, "NewOrder all_local")?;
        let queue_present = decode_boolean(&mut reader, "NewOrder queue_present")?;
        let mut lines = Vec::with_capacity(line_count);
        for _ in 0..line_count {
            let number = reader.get_u8()?;
            let item_id = reader.get_u32()?;
            let supply_warehouse = reader.get_u16()?;
            let delivery_timestamp = decode_bounded_bytes(
                &mut reader,
                "NewOrder line delivery timestamp",
                0,
                MAX_RICH_DELIVERY_TIMESTAMP_BYTES,
            )?;
            let quantity = reader.get_u8()?;
            let amount_bits = reader.get_u32()?;
            let district_info = decode_bounded_bytes(
                &mut reader,
                "NewOrder line district information",
                RICH_DISTRICT_INFO_BYTES,
                RICH_DISTRICT_INFO_BYTES,
            )?;
            lines.push(CanonicalRichOrderLine::new(
                number,
                item_id,
                supply_warehouse,
                delivery_timestamp,
                quantity,
                amount_bits,
                district_info,
            ));
        }
        entries.push(
            CanonicalRichNewOrder::new(
                score,
                key,
                customer_id,
                entry_timestamp,
                carrier_id,
                u8::try_from(line_count).expect("bounded line count fits u8"),
                all_local,
                queue_present,
                lines.into_iter(),
            )
            .map_err(CoreCodecError::InvalidRichRecovery)?,
        );
    }
    reader.finish()?;
    Ok(DecodedRichNewOrderSection {
        header,
        rejected,
        entries,
    })
}

fn encode_rich_header(
    writer: &mut CanonicalWriter,
    samples: &SealedRichRecoverySamples,
) -> Result<(), CoreCodecError> {
    let raw_size =
        u32::try_from(samples.raw_size_bytes()).map_err(|_| CoreCodecError::OversizedCount {
            field: "rich recovery raw size",
            actual: u64::try_from(samples.raw_size_bytes()).unwrap_or(u64::MAX),
            maximum: MAX_RICH_RECOVERY_RAW_BYTES as u64,
        })?;
    if samples.raw_size_bytes() > MAX_RICH_RECOVERY_RAW_BYTES {
        return Err(CoreCodecError::OversizedCount {
            field: "rich recovery raw size",
            actual: samples.raw_size_bytes() as u64,
            maximum: MAX_RICH_RECOVERY_RAW_BYTES as u64,
        });
    }
    writer.put_u16(samples.warehouses())?;
    writer.put_u64(samples.run_seed())?;
    writer.put_u32(samples.policy_version())?;
    writer.put_u32(raw_size)?;
    writer.put_u64(samples.new_order_commit_count())?;
    writer.put_u64(samples.delivered_order_count())?;
    writer.put_u64(samples.committed_history_row_count())?;
    writer.put_u64(samples.bad_credit_payment_count())
}

fn decode_rich_header(
    reader: &mut CanonicalReader<'_>,
) -> Result<CanonicalRichRecoveryHeader, CoreCodecError> {
    let warehouses = reader.get_u16()?;
    let run_seed = reader.get_u64()?;
    let policy_version = reader.get_u32()?;
    let raw_size = reader.get_u32()?;
    let new_order_commits = reader.get_u64()?;
    let delivered_orders = reader.get_u64()?;
    let history_rows = reader.get_u64()?;
    let bad_credit_payments = reader.get_u64()?;
    if warehouses == 0 || warehouses > OFFICIAL_WAREHOUSES {
        return Err(CoreCodecError::InvalidRichRecovery(
            RichRecoveryError::InvalidConfiguration("warehouses must be in 1..=50"),
        ));
    }
    if policy_version != RICH_RECOVERY_POLICY_VERSION {
        return Err(CoreCodecError::InvalidRichRecovery(
            RichRecoveryError::UnsupportedPolicy {
                actual: policy_version,
                expected: RICH_RECOVERY_POLICY_VERSION,
            },
        ));
    }
    let raw_size = usize::try_from(raw_size).map_err(|_| CoreCodecError::OversizedCount {
        field: "rich recovery raw size",
        actual: u64::from(raw_size),
        maximum: MAX_RICH_RECOVERY_RAW_BYTES as u64,
    })?;
    if raw_size > MAX_RICH_RECOVERY_RAW_BYTES {
        return Err(CoreCodecError::InvalidRichRecovery(
            RichRecoveryError::RawSizeCeiling {
                actual: raw_size,
                limit: MAX_RICH_RECOVERY_RAW_BYTES,
            },
        ));
    }
    if bad_credit_payments > history_rows {
        return Err(CoreCodecError::InvalidRichRecovery(
            RichRecoveryError::InvalidEvidence(
                "bad-credit Payment count exceeds the committed History row count",
            ),
        ));
    }
    Ok(CanonicalRichRecoveryHeader::new(
        warehouses,
        run_seed,
        policy_version,
        raw_size,
        new_order_commits,
        delivered_orders,
        history_rows,
        bad_credit_payments,
    ))
}

fn encode_sample_score(
    writer: &mut CanonicalWriter,
    score: SampleScore,
) -> Result<(), CoreCodecError> {
    writer.put_u64(score.high)?;
    writer.put_u64(score.low)
}

fn decode_sample_score(reader: &mut CanonicalReader<'_>) -> Result<SampleScore, CoreCodecError> {
    Ok(SampleScore {
        high: reader.get_u64()?,
        low: reader.get_u64()?,
    })
}

fn encode_order_key(writer: &mut CanonicalWriter, key: OrderKey) -> Result<(), CoreCodecError> {
    writer.put_u16(key.warehouse_id())?;
    writer.put_u8(key.district_id())?;
    writer.put_i32(key.order_id())
}

fn decode_order_key(reader: &mut CanonicalReader<'_>) -> Result<OrderKey, CoreCodecError> {
    Ok(OrderKey::from_parts(
        reader.get_u16()?,
        reader.get_u8()?,
        reader.get_i32()?,
    ))
}

fn encode_rich_order_witness(
    writer: &mut CanonicalWriter,
    witness: Option<&super::rich_recovery_samples::OrderCutoffWitness>,
) -> Result<(), CoreCodecError> {
    match witness {
        None => writer.put_u8(0),
        Some(witness) => {
            writer.put_u8(1)?;
            encode_sample_score(writer, witness.score())?;
            encode_order_key(writer, witness.key())
        }
    }
}

fn decode_rich_order_witness(
    reader: &mut CanonicalReader<'_>,
    field: &'static str,
) -> Result<Option<CanonicalRichOrderWitness>, CoreCodecError> {
    match reader.get_u8()? {
        0 => Ok(None),
        1 => Ok(Some(CanonicalRichOrderWitness::new(
            decode_sample_score(reader)?,
            decode_order_key(reader)?,
        ))),
        actual => Err(CoreCodecError::InvalidPresenceFlag { field, actual }),
    }
}

fn encode_rich_count(field: &'static str, count: usize) -> Result<u32, CoreCodecError> {
    let maximum = RICH_RECOVERY_SAMPLE_CAPACITY as u64;
    let actual = u64::try_from(count).unwrap_or(u64::MAX);
    if actual > maximum {
        return Err(CoreCodecError::OversizedCount {
            field,
            actual,
            maximum,
        });
    }
    u32::try_from(count).map_err(|_| CoreCodecError::OversizedCount {
        field,
        actual,
        maximum,
    })
}

fn encode_u8_count(
    field: &'static str,
    count: usize,
    minimum: usize,
    maximum: usize,
) -> Result<u8, CoreCodecError> {
    validate_length(field, count, minimum, maximum)?;
    u8::try_from(count).map_err(|_| CoreCodecError::InvalidLength {
        field,
        actual: count,
        minimum,
        maximum,
    })
}

fn decode_u8_count(
    reader: &mut CanonicalReader<'_>,
    field: &'static str,
    minimum: usize,
    maximum: usize,
) -> Result<usize, CoreCodecError> {
    let count = usize::from(reader.get_u8()?);
    validate_length(field, count, minimum, maximum)?;
    Ok(count)
}

fn encode_bounded_bytes(
    writer: &mut CanonicalWriter,
    field: &'static str,
    bytes: &[u8],
    minimum: usize,
    maximum: usize,
) -> Result<(), CoreCodecError> {
    let length = encode_u8_count(field, bytes.len(), minimum, maximum)?;
    writer.put_u8(length)?;
    writer.put_bytes(bytes)
}

fn decode_bounded_bytes(
    reader: &mut CanonicalReader<'_>,
    field: &'static str,
    minimum: usize,
    maximum: usize,
) -> Result<Vec<u8>, CoreCodecError> {
    let length = decode_u8_count(reader, field, minimum, maximum)?;
    Ok(reader.take(length)?.to_vec())
}

fn validate_length(
    field: &'static str,
    actual: usize,
    minimum: usize,
    maximum: usize,
) -> Result<(), CoreCodecError> {
    if (minimum..=maximum).contains(&actual) {
        Ok(())
    } else {
        Err(CoreCodecError::InvalidLength {
            field,
            actual,
            minimum,
            maximum,
        })
    }
}

fn encode_boolean(writer: &mut CanonicalWriter, value: bool) -> Result<(), CoreCodecError> {
    writer.put_u8(u8::from(value))
}

fn decode_boolean(
    reader: &mut CanonicalReader<'_>,
    field: &'static str,
) -> Result<bool, CoreCodecError> {
    match reader.get_u8()? {
        0 => Ok(false),
        1 => Ok(true),
        actual => Err(CoreCodecError::InvalidBoolean { field, actual }),
    }
}

fn validate_rich_order_entry(
    domain: &'static str,
    previous: &mut Option<(SampleScore, OrderKey)>,
    keys: &mut BTreeSet<OrderKey>,
    score: SampleScore,
    key: OrderKey,
) -> Result<(), CoreCodecError> {
    if !keys.insert(key) {
        return Err(CoreCodecError::DuplicateRichOrderKey { domain, key });
    }
    let current = (score, key);
    if previous.as_ref().is_some_and(|prior| prior >= &current) {
        return Err(CoreCodecError::NonCanonicalRichOrder { domain });
    }
    *previous = Some(current);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consistency::{CustomerUpdateEvidence, CustomerUpdateKind};
    use crate::ranking::evidence_collector::{
        CustomerMutation, IntervalCollector, StockMutation, TerminalEvidence,
    };

    const TEST_LOAD_SEED: u64 = 0x10ad_2026;
    const TEST_SAMPLE_SEED: u64 = 0x5a6d_2026;

    #[test]
    fn primitive_codec_is_little_endian_and_strictly_consumed() {
        let mut writer = CanonicalWriter::new(19);
        writer.put_u8(0x12).unwrap();
        writer.put_u16(0x3456).unwrap();
        writer.put_u32(0x789a_bcde).unwrap();
        writer.put_i32(-2).unwrap();
        writer.put_u64(0x0102_0304_0506_0708).unwrap();
        let encoded = writer.finish();
        assert_eq!(
            encoded,
            [
                0x12, 0x56, 0x34, 0xde, 0xbc, 0x9a, 0x78, 0xfe, 0xff, 0xff, 0xff, 0x08, 0x07, 0x06,
                0x05, 0x04, 0x03, 0x02, 0x01,
            ]
        );

        let mut reader = CanonicalReader::new(&encoded);
        assert_eq!(reader.get_u8().unwrap(), 0x12);
        assert_eq!(reader.get_u16().unwrap(), 0x3456);
        assert_eq!(reader.get_u32().unwrap(), 0x789a_bcde);
        assert_eq!(reader.get_i32().unwrap(), -2);
        assert_eq!(reader.get_u64().unwrap(), 0x0102_0304_0506_0708);
        reader.finish().unwrap();
    }

    #[test]
    fn primitive_codec_rejects_limit_truncation_and_trailing_bytes() {
        let mut writer = CanonicalWriter::new(1);
        assert!(writer.put_u16(1).is_err());

        let mut truncated = CanonicalReader::new(&[1, 2, 3]);
        assert!(matches!(
            truncated.get_u32(),
            Err(CoreCodecError::Truncated)
        ));

        let trailing = CanonicalReader::new(&[1]);
        assert!(matches!(
            trailing.finish(),
            Err(CoreCodecError::TrailingBytes { remaining: 1 })
        ));
    }

    #[test]
    fn lower_hex_is_canonical_and_bounded_before_allocation() {
        let bytes = [0x00, 0x1f, 0xa5, 0xff];
        let encoded = encode_lower_hex(&bytes, bytes.len()).unwrap();
        assert_eq!(encoded, "001fa5ff");
        assert_eq!(decode_lower_hex(&encoded, bytes.len()).unwrap(), bytes);
        assert!(matches!(
            decode_lower_hex("001FA5FF", bytes.len()),
            Err(CoreCodecError::InvalidHexDigit)
        ));
        assert!(matches!(
            decode_lower_hex("0", bytes.len()),
            Err(CoreCodecError::InvalidHexLength)
        ));
        assert!(matches!(
            decode_lower_hex("0000", 1),
            Err(CoreCodecError::InvalidHexLength)
        ));
    }

    #[test]
    fn section_header_rejects_unknown_kind_version_and_magic() {
        let valid = section_writer(SectionKind::PhysicalStats, 32)
            .unwrap()
            .finish();
        section_reader(&valid, SectionKind::PhysicalStats, 32).unwrap();

        let mut wrong_kind = valid.clone();
        wrong_kind[4] = 0xff;
        assert!(matches!(
            section_reader(&wrong_kind, SectionKind::PhysicalStats, 32),
            Err(CoreCodecError::UnexpectedSection { actual: 0xff, .. })
        ));

        let mut wrong_version = valid.clone();
        wrong_version[5..7].copy_from_slice(&2_u16.to_le_bytes());
        assert!(matches!(
            section_reader(&wrong_version, SectionKind::PhysicalStats, 32),
            Err(CoreCodecError::UnsupportedSectionVersion { actual: 2 })
        ));

        let mut wrong_magic = valid;
        wrong_magic[0] ^= 1;
        assert!(matches!(
            section_reader(&wrong_magic, SectionKind::PhysicalStats, 32),
            Err(CoreCodecError::InvalidMagic)
        ));
    }

    #[test]
    fn physical_stats_section_round_trips_and_reencodes_identically() {
        let default = BoundedPhysicalStats::default();
        let (classes, partitions, payment, new_order, delivery) = default.canonical_parts();
        let mut classes = *classes;
        let mut payment = payment.clone();
        classes[0].payment_commits = 1;
        payment[0].add_bits(1.25_f32.to_bits()).unwrap();
        let stats = BoundedPhysicalStats::from_canonical_parts(
            classes,
            *partitions,
            payment,
            new_order.clone(),
            delivery.clone(),
        )
        .unwrap();

        let encoded = encode_physical_stats_section(&stats).unwrap();
        let restored = decode_physical_stats_section(&encoded).unwrap();
        assert_eq!(restored, stats);
        assert_eq!(encode_physical_stats_section(&restored).unwrap(), encoded);
    }

    #[test]
    fn physical_stats_decoder_rejects_truncation_trailing_and_oversize() {
        let encoded = encode_physical_stats_section(&BoundedPhysicalStats::default()).unwrap();
        for end in [0, 1, 6, encoded.len() / 2, encoded.len() - 1] {
            assert!(
                decode_physical_stats_section(&encoded[..end]).is_err(),
                "accepted truncation at {end}"
            );
        }

        let mut trailing = encoded;
        trailing.push(0);
        assert!(matches!(
            decode_physical_stats_section(&trailing),
            Err(CoreCodecError::TrailingBytes { remaining: 1 })
        ));
        assert!(matches!(
            decode_physical_stats_section(&vec![0; MAX_PHYSICAL_STATS_SECTION_BYTES + 1]),
            Err(CoreCodecError::OversizedSection { .. })
        ));
    }

    #[test]
    fn physical_stats_decoder_rejects_impossible_nonempty_accumulator_sum() {
        let default = BoundedPhysicalStats::default();
        let (classes, partitions, payment, new_order, delivery) = default.canonical_parts();
        let mut classes = *classes;
        let mut payment = payment.clone();
        classes[0].payment_commits = 1;
        payment[0].add_bits(1.0_f32.to_bits()).unwrap();
        let stats = BoundedPhysicalStats::from_canonical_parts(
            classes,
            *partitions,
            payment,
            new_order.clone(),
            delivery.clone(),
        )
        .unwrap();
        let mut encoded = encode_physical_stats_section(&stats).unwrap();

        const FIRST_ACCUMULATOR: usize =
            7 + LEDGER_CLASS_COUNT * 12 * 8 + PHYSICAL_PARTITION_COUNT * 4 * 8;
        let word_count = u32::from_le_bytes(
            encoded[FIRST_ACCUMULATOR + 8..FIRST_ACCUMULATOR + 12]
                .try_into()
                .unwrap(),
        ) as usize;
        assert!(word_count > 0);
        encoded[FIRST_ACCUMULATOR + 8..FIRST_ACCUMULATOR + 12]
            .copy_from_slice(&0_u32.to_le_bytes());
        encoded.drain(FIRST_ACCUMULATOR + 12..FIRST_ACCUMULATOR + 12 + word_count * 8);
        assert!(matches!(
            decode_physical_stats_section(&encoded),
            Err(CoreCodecError::ImpossibleAccumulatorSum {
                field: "Payment/history amount"
            })
        ));
    }

    #[test]
    fn interval_sections_round_trip_and_reencode_identically() {
        let intervals = sample_intervals();
        let customers = encode_customer_interval_section(&intervals).unwrap();
        let stocks = encode_stock_interval_section(&intervals).unwrap();
        let restored = decode_interval_sections(&customers, &stocks, interval_binding()).unwrap();
        assert_eq!(
            encode_customer_interval_section(&restored).unwrap(),
            customers
        );
        assert_eq!(encode_stock_interval_section(&restored).unwrap(), stocks);
    }

    #[test]
    fn interval_decoders_reject_unknown_truncated_trailing_and_oversized_sections() {
        let intervals = sample_intervals();
        let customers = encode_customer_interval_section(&intervals).unwrap();
        let stocks = encode_stock_interval_section(&intervals).unwrap();

        for end in [0, 1, 6, customers.len() - 1] {
            assert!(decode_customer_interval_section(&customers[..end]).is_err());
        }
        for end in [0, 1, 6, stocks.len() - 1] {
            assert!(decode_stock_interval_section(&stocks[..end]).is_err());
        }

        let mut customer_trailing = customers.clone();
        customer_trailing.push(0);
        assert!(matches!(
            decode_customer_interval_section(&customer_trailing),
            Err(CoreCodecError::TrailingBytes { remaining: 1 })
        ));
        let mut stock_trailing = stocks;
        stock_trailing.push(0);
        assert!(matches!(
            decode_stock_interval_section(&stock_trailing),
            Err(CoreCodecError::TrailingBytes { remaining: 1 })
        ));

        let mut unknown = customers;
        unknown[4] = 0xff;
        assert!(matches!(
            decode_customer_interval_section(&unknown),
            Err(CoreCodecError::UnexpectedSection { actual: 0xff, .. })
        ));
        assert!(matches!(
            decode_stock_interval_section(&vec![0; MAX_STOCK_INTERVAL_SECTION_BYTES + 1]),
            Err(CoreCodecError::OversizedSection { .. })
        ));
    }

    #[test]
    fn interval_decoders_cap_counts_before_allocating_and_reject_bad_flags() {
        let intervals = sample_intervals();
        let mut customers = encode_customer_interval_section(&intervals).unwrap();
        // Header (7), metadata (14), update count (8), absent witness (1).
        customers[30..34].copy_from_slice(&(MAX_INTERVAL_SAMPLES + 1).to_le_bytes());
        assert!(matches!(
            decode_customer_interval_section(&customers),
            Err(CoreCodecError::OversizedCount {
                field: "customer interval samples",
                ..
            })
        ));

        let mut stocks = encode_stock_interval_section(&intervals).unwrap();
        stocks[29] = 2;
        assert!(matches!(
            decode_stock_interval_section(&stocks),
            Err(CoreCodecError::InvalidPresenceFlag {
                field: "Stock rejected sample",
                actual: 2,
            })
        ));
    }

    #[test]
    fn interval_restore_binds_common_metadata_and_generated_stock_roots() {
        let intervals = sample_intervals();
        let customers = encode_customer_interval_section(&intervals).unwrap();
        let mut stocks = encode_stock_interval_section(&intervals).unwrap();
        // Header (7), warehouses (2), then sample_seed.
        stocks[9] ^= 1;
        assert!(matches!(
            decode_interval_sections(&customers, &stocks, interval_binding()),
            Err(CoreCodecError::MismatchedIntervalMetadata("sample seed"))
        ));

        let stocks = encode_stock_interval_section(&intervals).unwrap();
        let expected_quantity =
            TpccDataGen::with_seed(1, TEST_LOAD_SEED).initial_stock_quantity(1, 1);
        let wrong_seed = (TEST_LOAD_SEED + 1..)
            .find(|seed| {
                TpccDataGen::with_seed(1, *seed).initial_stock_quantity(1, 1) != expected_quantity
            })
            .unwrap();
        assert!(matches!(
            decode_interval_sections(
                &customers,
                &stocks,
                IntervalSectionBinding::new(1, TEST_SAMPLE_SEED, wrong_seed)
            ),
            Err(CoreCodecError::InvalidIntervals(
                CollectorError::StockRootMismatch { .. }
            ))
        ));
    }

    #[test]
    fn interval_restore_rejects_consistently_rebound_outer_metadata() {
        let intervals = sample_intervals();
        let mut customers = encode_customer_interval_section(&intervals).unwrap();
        let mut stocks = encode_stock_interval_section(&intervals).unwrap();
        // Header (7), warehouses (2), then sample_seed.
        customers[9] ^= 1;
        stocks[9] ^= 1;
        assert!(matches!(
            decode_interval_sections(&customers, &stocks, interval_binding()),
            Err(CoreCodecError::IntervalBindingMismatch {
                field: "sample seed",
                expected: TEST_SAMPLE_SEED,
                ..
            })
        ));

        let mut customers = encode_customer_interval_section(&intervals).unwrap();
        let mut stocks = encode_stock_interval_section(&intervals).unwrap();
        customers[7..9].copy_from_slice(&2_u16.to_le_bytes());
        stocks[7..9].copy_from_slice(&2_u16.to_le_bytes());
        assert!(matches!(
            decode_interval_sections(&customers, &stocks, interval_binding()),
            Err(CoreCodecError::IntervalBindingMismatch {
                field: "warehouse count",
                expected: 1,
                actual: 2,
            })
        ));
    }

    #[test]
    fn interval_restore_rejects_reordered_and_duplicate_canonical_entries() {
        let intervals = sample_intervals();
        let customers = encode_customer_interval_section(&intervals).unwrap();
        let stocks = encode_stock_interval_section(&intervals).unwrap();
        // With no rejected witness the two fixed-size Customer entries begin
        // after 34 bytes and occupy 36 bytes each.
        let mut reordered = customers.clone();
        let first = reordered[34..70].to_vec();
        let second = reordered[70..106].to_vec();
        reordered[34..70].copy_from_slice(&second);
        reordered[70..106].copy_from_slice(&first);
        assert!(matches!(
            decode_interval_sections(&reordered, &stocks, interval_binding()),
            Err(CoreCodecError::InvalidIntervals(
                CollectorError::NonCanonicalSampleOrder { domain: "customer" }
            ))
        ));

        let mut duplicate = customers;
        let first = duplicate[34..70].to_vec();
        duplicate[70..106].copy_from_slice(&first);
        assert!(matches!(
            decode_interval_sections(&duplicate, &stocks, interval_binding()),
            Err(CoreCodecError::InvalidIntervals(
                CollectorError::NonCanonicalSampleOrder { domain: "customer" }
            ))
        ));
    }

    #[test]
    fn payment_endpoint_section_round_trips_as_distinct_persisted_view() {
        let endpoints = sample_payment_endpoints();
        let encoded = encode_payment_endpoint_section(&endpoints).unwrap();
        let restored = decode_payment_endpoint_section(&encoded).unwrap();
        assert_eq!(restored, endpoints);
        assert_eq!(encode_payment_endpoint_section(&restored).unwrap(), encoded);
    }

    #[test]
    fn payment_decoder_rejects_unknown_truncated_trailing_and_oversized_sections() {
        let encoded = encode_payment_endpoint_section(&sample_payment_endpoints()).unwrap();
        for end in [0, 1, 6, encoded.len() / 2, encoded.len() - 1] {
            assert!(decode_payment_endpoint_section(&encoded[..end]).is_err());
        }

        let mut unknown = encoded.clone();
        unknown[4] = 0xff;
        assert!(matches!(
            decode_payment_endpoint_section(&unknown),
            Err(CoreCodecError::UnexpectedSection { actual: 0xff, .. })
        ));

        let mut trailing = encoded;
        trailing.push(0);
        assert!(matches!(
            decode_payment_endpoint_section(&trailing),
            Err(CoreCodecError::TrailingBytes { remaining: 1 })
        ));
        assert!(matches!(
            decode_payment_endpoint_section(&vec![0; MAX_PAYMENT_ENDPOINT_SECTION_BYTES + 1]),
            Err(CoreCodecError::OversizedSection { .. })
        ));
    }

    #[test]
    fn payment_decoder_caps_warehouse_allocation_and_revalidates_totals() {
        let encoded = encode_payment_endpoint_section(&sample_payment_endpoints()).unwrap();
        let mut oversized_count = encoded.clone();
        oversized_count[7..9].copy_from_slice(&(MAX_PAYMENT_WAREHOUSES + 1).to_le_bytes());
        assert!(matches!(
            decode_payment_endpoint_section(&oversized_count),
            Err(CoreCodecError::OversizedCount {
                field: "Payment warehouses",
                ..
            })
        ));

        let mut wrong_total = encoded;
        // Header (7), warehouses (2), terminal total (8), then Warehouse
        // edge total.
        wrong_total[17..25].copy_from_slice(&0_u64.to_le_bytes());
        assert!(matches!(
            decode_payment_endpoint_section(&wrong_total),
            Err(CoreCodecError::InvalidPaymentEndpoints(
                PaymentEndpointError::InvalidInvariant(
                    "persisted edge totals differ from terminal total"
                )
            ))
        ));
    }

    #[test]
    fn rich_new_order_section_round_trips_empty_canonical_state() {
        let intervals = empty_rich_intervals();
        let samples = empty_rich_samples(&intervals);
        let encoded = encode_rich_new_order_section(&samples).unwrap();
        assert_eq!(&encoded[..4], b"TCS1");
        assert_eq!(encoded[4], SectionKind::RichNewOrders as u8);
        assert_eq!(
            &encoded[SECTION_HEADER_BYTES..SECTION_HEADER_BYTES + 2],
            &1_u16.to_le_bytes()
        );
        assert_eq!(
            &encoded[SECTION_HEADER_BYTES + 2..SECTION_HEADER_BYTES + 10],
            &TEST_SAMPLE_SEED.to_le_bytes()
        );

        let decoded = decode_rich_new_order_section(&encoded).unwrap();
        assert!(decoded.entries.is_empty());
        assert!(decoded.rejected.is_none());
        let restored = restore_new_order_only(decoded, &intervals);
        assert_eq!(encode_rich_new_order_section(&restored).unwrap(), encoded);
    }

    #[test]
    fn rich_new_order_decoder_prechecks_counts_lengths_and_booleans() {
        let encoded = test_new_order_section(&[(
            SampleScore { high: 1, low: 2 },
            OrderKey::from_parts(1, 1, 3_001),
        )]);
        let count_offset = SECTION_HEADER_BYTES + RICH_HEADER_BYTES + 1;
        let entry_offset = count_offset + 4;

        let mut oversized_count = encoded.clone();
        oversized_count[count_offset..count_offset + 4].copy_from_slice(
            &(u32::try_from(RICH_RECOVERY_SAMPLE_CAPACITY).unwrap() + 1).to_le_bytes(),
        );
        assert!(matches!(
            decode_rich_new_order_section(&oversized_count),
            Err(CoreCodecError::OversizedCount {
                field: "rich NewOrder samples",
                ..
            })
        ));

        let mut oversized_timestamp = encoded.clone();
        oversized_timestamp[entry_offset + 25] =
            u8::try_from(MAX_RICH_ENTRY_TIMESTAMP_BYTES + 1).unwrap();
        assert!(matches!(
            decode_rich_new_order_section(&oversized_timestamp),
            Err(CoreCodecError::InvalidLength {
                field: "NewOrder entry timestamp",
                ..
            })
        ));

        let mut oversized_lines = encoded.clone();
        oversized_lines[entry_offset + 46] = u8::try_from(MAX_RICH_ORDER_LINES + 1).unwrap();
        assert!(matches!(
            decode_rich_new_order_section(&oversized_lines),
            Err(CoreCodecError::InvalidLength {
                field: "NewOrder lines",
                ..
            })
        ));

        let mut invalid_boolean = encoded;
        invalid_boolean[entry_offset + 47] = 2;
        assert!(matches!(
            decode_rich_new_order_section(&invalid_boolean),
            Err(CoreCodecError::InvalidBoolean {
                field: "NewOrder all_local",
                actual: 2,
            })
        ));
    }

    #[test]
    fn rich_new_order_decoder_rejects_reordered_duplicate_and_noncanonical_frames() {
        let first = (
            SampleScore { high: 1, low: 2 },
            OrderKey::from_parts(1, 1, 3_001),
        );
        let second = (
            SampleScore { high: 3, low: 4 },
            OrderKey::from_parts(1, 1, 3_002),
        );
        let canonical = test_new_order_section(&[first, second]);
        let decoded = decode_rich_new_order_section(&canonical).unwrap();
        assert_eq!(decoded.entries.len(), 2);

        let entry_offset = SECTION_HEADER_BYTES + RICH_HEADER_BYTES + 1 + 4;
        assert_eq!(
            &canonical[entry_offset..entry_offset + 8],
            &first.0.high.to_le_bytes()
        );
        assert_eq!(
            &canonical[entry_offset + 16..entry_offset + 18],
            &first.1.warehouse_id().to_le_bytes()
        );

        let reordered = test_new_order_section(&[second, first]);
        assert!(matches!(
            decode_rich_new_order_section(&reordered),
            Err(CoreCodecError::NonCanonicalRichOrder { domain: "NewOrder" })
        ));

        let duplicate = test_new_order_section(&[
            first,
            (
                SampleScore { high: 3, low: 4 },
                OrderKey::from_parts(1, 1, 3_001),
            ),
        ]);
        assert!(matches!(
            decode_rich_new_order_section(&duplicate),
            Err(CoreCodecError::DuplicateRichOrderKey {
                domain: "NewOrder",
                ..
            })
        ));

        for end in [0, 1, 6, canonical.len() / 2, canonical.len() - 1] {
            assert!(decode_rich_new_order_section(&canonical[..end]).is_err());
        }
        let mut unknown = canonical.clone();
        unknown[4] = 0xff;
        assert!(matches!(
            decode_rich_new_order_section(&unknown),
            Err(CoreCodecError::UnexpectedSection { actual: 0xff, .. })
        ));
        let mut trailing = canonical;
        trailing.push(0);
        assert!(matches!(
            decode_rich_new_order_section(&trailing),
            Err(CoreCodecError::TrailingBytes { remaining: 1 })
        ));
        assert!(matches!(
            decode_rich_new_order_section(&vec![0; MAX_RICH_NEW_ORDER_SECTION_BYTES + 1]),
            Err(CoreCodecError::OversizedSection { .. })
        ));
    }

    fn empty_rich_intervals() -> SealedIntervalEvidence {
        IntervalCollector::new(1, 1, TEST_SAMPLE_SEED, |_key: StockKey| None)
            .unwrap()
            .seal()
            .unwrap()
    }

    fn empty_rich_samples(intervals: &SealedIntervalEvidence) -> SealedRichRecoverySamples {
        use crate::ranking::rich_recovery_samples::{
            CanonicalRichBadCreditCustomer, CanonicalRichDelivery, CanonicalRichHistoryTuple,
            InitialCustomerData, InitialHistoryRow,
        };

        let no_history = |_key: CustomerKey| None::<InitialHistoryRow>;
        let no_customer = |_key: CustomerKey| None::<InitialCustomerData>;
        SealedRichRecoverySamples::from_canonical_parts(
            CanonicalRichRecoveryHeader::new(
                1,
                TEST_SAMPLE_SEED,
                RICH_RECOVERY_POLICY_VERSION,
                64,
                0,
                0,
                0,
                0,
            ),
            std::iter::empty::<CanonicalRichNewOrder>(),
            std::iter::empty::<CanonicalRichDelivery>(),
            std::iter::empty::<CanonicalRichBadCreditCustomer>(),
            std::iter::empty::<CanonicalRichHistoryTuple>(),
            None,
            None,
            None,
            None,
            intervals,
            &no_history,
            &no_customer,
        )
        .unwrap()
    }

    fn restore_new_order_only(
        decoded: DecodedRichNewOrderSection,
        intervals: &SealedIntervalEvidence,
    ) -> SealedRichRecoverySamples {
        use crate::ranking::rich_recovery_samples::{
            CanonicalRichBadCreditCustomer, CanonicalRichDelivery, CanonicalRichHistoryTuple,
            InitialCustomerData, InitialHistoryRow,
        };

        let no_history = |_key: CustomerKey| None::<InitialHistoryRow>;
        let no_customer = |_key: CustomerKey| None::<InitialCustomerData>;
        SealedRichRecoverySamples::from_canonical_parts(
            decoded.header,
            decoded.entries.into_iter(),
            std::iter::empty::<CanonicalRichDelivery>(),
            std::iter::empty::<CanonicalRichBadCreditCustomer>(),
            std::iter::empty::<CanonicalRichHistoryTuple>(),
            decoded.rejected,
            None,
            None,
            None,
            intervals,
            &no_history,
            &no_customer,
        )
        .unwrap()
    }

    fn test_new_order_section(entries: &[(SampleScore, OrderKey)]) -> Vec<u8> {
        let empty = encode_rich_new_order_section(&empty_rich_samples(&empty_rich_intervals()))
            .expect("empty rich section encodes");
        let count_offset = SECTION_HEADER_BYTES + RICH_HEADER_BYTES + 1;
        let mut writer = CanonicalWriter::new(MAX_RICH_NEW_ORDER_SECTION_BYTES);
        writer.put_bytes(&empty[..count_offset]).unwrap();
        writer
            .put_u32(u32::try_from(entries.len()).unwrap())
            .unwrap();
        for (score, key) in entries {
            encode_sample_score(&mut writer, *score).unwrap();
            encode_order_key(&mut writer, *key).unwrap();
            writer.put_u16(1).unwrap();
            encode_bounded_bytes(
                &mut writer,
                "NewOrder entry timestamp",
                b"2026-07-29 12:34:56",
                1,
                MAX_RICH_ENTRY_TIMESTAMP_BYTES,
            )
            .unwrap();
            writer.put_u8(0).unwrap();
            writer
                .put_u8(u8::try_from(MIN_RICH_ORDER_LINES).unwrap())
                .unwrap();
            encode_boolean(&mut writer, true).unwrap();
            encode_boolean(&mut writer, true).unwrap();
            for number in 1..=u8::try_from(MIN_RICH_ORDER_LINES).unwrap() {
                writer.put_u8(number).unwrap();
                writer.put_u32(u32::from(number)).unwrap();
                writer.put_u16(1).unwrap();
                encode_bounded_bytes(
                    &mut writer,
                    "NewOrder line delivery timestamp",
                    b"",
                    0,
                    MAX_RICH_DELIVERY_TIMESTAMP_BYTES,
                )
                .unwrap();
                writer.put_u8(1).unwrap();
                writer.put_u32(1.0_f32.to_bits()).unwrap();
                encode_bounded_bytes(
                    &mut writer,
                    "NewOrder line district information",
                    &[b'D'; RICH_DISTRICT_INFO_BYTES],
                    RICH_DISTRICT_INFO_BYTES,
                    RICH_DISTRICT_INFO_BYTES,
                )
                .unwrap();
            }
        }
        writer.finish()
    }

    fn interval_binding() -> IntervalSectionBinding {
        IntervalSectionBinding::new(1, TEST_SAMPLE_SEED, TEST_LOAD_SEED)
    }

    fn sample_intervals() -> SealedIntervalEvidence {
        let roots = move |key: StockKey| {
            let generator = TpccDataGen::with_seed(1, TEST_LOAD_SEED);
            Some(StockVersion {
                quantity: generator.initial_stock_quantity(key.warehouse_id, key.item_id),
                ytd_bits: 0.0_f32.to_bits(),
                order_count: 0,
                remote_count: 0,
            })
        };
        let mut collector = IntervalCollector::new(1, 1, TEST_SAMPLE_SEED, roots).unwrap();

        let customers = [1, 2].map(|customer_id| {
            CustomerMutation::new(
                CustomerKey {
                    warehouse_id: 1,
                    district_id: 1,
                    customer_id,
                },
                CustomerUpdateEvidence {
                    kind: CustomerUpdateKind::Payment,
                    before_version: CustomerLogicalVersion {
                        payment_count: 1,
                        delivery_count: 0,
                    },
                    after_version: CustomerLogicalVersion {
                        payment_count: 2,
                        delivery_count: 0,
                    },
                    amount_bits: 1.0_f32.to_bits(),
                    balance_before_bits: (-10.0_f32).to_bits(),
                    balance_after_bits: (-11.0_f32).to_bits(),
                    ytd_payment_before_bits: Some(10.0_f32.to_bits()),
                    ytd_payment_after_bits: Some(11.0_f32.to_bits()),
                },
            )
        });
        collector
            .record_terminal(TerminalEvidence::customers(&customers))
            .unwrap();

        let stocks = [1, 2].map(|item_id| {
            let generator = TpccDataGen::with_seed(1, TEST_LOAD_SEED);
            let initial_quantity = generator.initial_stock_quantity(1, item_id);
            let initial = StockVersion {
                quantity: initial_quantity,
                ytd_bits: 0.0_f32.to_bits(),
                order_count: 0,
                remote_count: 0,
            };
            let endpoint = StockVersion {
                quantity: if initial_quantity >= 11 {
                    initial_quantity - 1
                } else {
                    initial_quantity + 90
                },
                ytd_bits: 1.0_f32.to_bits(),
                order_count: 1,
                remote_count: 0,
            };
            StockMutation::new(
                StockKey {
                    warehouse_id: 1,
                    item_id,
                },
                1,
                0,
                initial,
                endpoint,
            )
        });
        collector
            .record_terminal(TerminalEvidence::stocks(&stocks))
            .unwrap();
        collector.seal().unwrap()
    }

    fn sample_payment_endpoints() -> PersistedPaymentEndpoints {
        use crate::ranking::payment_endpoints::{DISTRICT_YTD_ROOT_BITS, WAREHOUSE_YTD_ROOT_BITS};

        let warehouse_endpoints = vec![
            ((f32::from_bits(WAREHOUSE_YTD_ROOT_BITS) + 1.0).to_bits(), 1),
            (WAREHOUSE_YTD_ROOT_BITS, 0),
        ];
        let mut district_endpoints =
            vec![(DISTRICT_YTD_ROOT_BITS, 0); 2 * usize::from(PAYMENT_DISTRICTS_PER_WAREHOUSE)];
        district_endpoints[0] = ((f32::from_bits(DISTRICT_YTD_ROOT_BITS) + 1.0).to_bits(), 1);
        PersistedPaymentEndpoints::from_canonical_endpoints(
            2,
            1,
            1,
            1,
            warehouse_endpoints,
            district_endpoints,
        )
        .unwrap()
    }
}
