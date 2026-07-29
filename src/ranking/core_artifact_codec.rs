//! Strict canonical binary building blocks for bounded terminal artifacts.
//!
//! This module intentionally encodes only individual, stable sections. It is
//! not an artifact envelope and must not be published as a complete terminal
//! evidence bundle: the final artifact also binds the rich recovery sections
//! and outer run metadata.

use thiserror::Error;

use crate::consistency::{FloatError, NonNegativeF32Accumulator};

use super::bounded_stats::{
    BoundedPhysicalStats, BoundedStatsError, ClassTotals, PartitionTotals, LEDGER_CLASS_COUNT,
    PHYSICAL_PARTITION_COUNT,
};

const SECTION_MAGIC: [u8; 4] = *b"TCS1";
const SECTION_VERSION: u16 = 1;

pub(crate) const MAX_CORE_SECTION_BYTES: usize = 20 * 1024;
pub(crate) const MAX_CORE_SECTION_HEX_CHARS: usize = MAX_CORE_SECTION_BYTES * 2;
pub(crate) const MAX_PHYSICAL_STATS_SECTION_BYTES: usize = MAX_CORE_SECTION_BYTES;
const MAX_ACCUMULATOR_WORDS: u32 = 6;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum SectionKind {
    PhysicalStats = 1,
    CustomerIntervals = 2,
    StockIntervals = 3,
    PaymentEndpoints = 4,
}

impl SectionKind {
    fn name(self) -> &'static str {
        match self {
            Self::PhysicalStats => "physical statistics",
            Self::CustomerIntervals => "customer intervals",
            Self::StockIntervals => "stock intervals",
            Self::PaymentEndpoints => "Payment endpoints",
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
