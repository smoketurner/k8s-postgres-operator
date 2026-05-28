//! PostgreSQL [LSN] (Log Sequence Number) as a strong type.
//!
//! PostgreSQL writes LSNs as two hex segments separated by a slash, e.g.
//! `"16/B374D848"`. The numeric value is the concatenation: the segment
//! before the slash is the upper 32 bits, the segment after is the lower
//! 32 bits, both in hexadecimal.
//!
//! Internally we store the combined 64-bit position so comparisons and
//! subtractions are O(1) and obviously correct. The wire format
//! (Serde / Display / `FromStr`) round-trips the human-readable string
//! exactly as PostgreSQL emits it (uppercase hex, no leading zeros).
//!
//! ## Why this exists
//!
//! Until this type was introduced, `source_lsn` and `target_lsn` were
//! `String` everywhere — including the cutover gate that compared them
//! for equality. String comparison silently returns the wrong answer for
//! unnormalised forms (lowercase vs uppercase, leading zeros,
//! whitespace), which is the worst possible failure mode for a
//! data-loss-prevention gate.
//!
//! [LSN]: https://www.postgresql.org/docs/current/datatype-pg-lsn.html

use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt;
use std::ops::Sub;
use std::str::FromStr;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::de::{self, Deserializer};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A PostgreSQL Log Sequence Number — a 64-bit position in the WAL.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(u64);

/// Errors raised while parsing the hex/slash text form.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LsnParseError {
    #[error("LSN is missing the '/' separator")]
    MissingSeparator,
    #[error("LSN has empty {segment} segment")]
    EmptySegment { segment: &'static str },
    #[error("LSN {segment} segment {value:?} is not valid hexadecimal")]
    InvalidHex {
        segment: &'static str,
        value: String,
    },
    #[error("LSN {segment} segment {value:?} does not fit in 32 bits")]
    SegmentOverflow {
        segment: &'static str,
        value: String,
    },
}

impl Lsn {
    /// Construct an LSN from its packed 64-bit position.
    pub const fn from_u64(v: u64) -> Self {
        Self(v)
    }

    /// The packed 64-bit position.
    pub const fn as_u64(&self) -> u64 {
        self.0
    }

    /// LSN zero — the start of the WAL.
    pub const ZERO: Lsn = Lsn(0);

    /// Parse PostgreSQL's `"<hex>/<hex>"` text form.
    ///
    /// Whitespace is trimmed. Hex digits are case-insensitive. The
    /// upper segment is the most significant 32 bits; either segment
    /// may have any number of hex digits up to 8 (longer segments
    /// overflow a u32 and are rejected).
    pub fn parse(s: &str) -> Result<Self, LsnParseError> {
        let s = s.trim();
        let Some((upper, lower)) = s.split_once('/') else {
            return Err(LsnParseError::MissingSeparator);
        };
        let upper = upper.trim();
        let lower = lower.trim();
        if upper.is_empty() {
            return Err(LsnParseError::EmptySegment { segment: "upper" });
        }
        if lower.is_empty() {
            return Err(LsnParseError::EmptySegment { segment: "lower" });
        }
        let hi = parse_segment("upper", upper)?;
        let lo = parse_segment("lower", lower)?;
        Ok(Lsn((u64::from(hi) << 32) | u64::from(lo)))
    }

    /// Distance from `older` to `self` in bytes, saturating at zero if
    /// `self < older`. PostgreSQL's WAL is monotonic, so the saturation
    /// is defensive — under normal operation `older` is always
    /// `confirmed_flush_lsn` and `self` is `pg_current_wal_lsn()`.
    pub fn bytes_ahead_of(self, older: Lsn) -> u64 {
        self.0.saturating_sub(older.0)
    }
}

fn parse_segment(name: &'static str, raw: &str) -> Result<u32, LsnParseError> {
    if raw.len() > 8 {
        return Err(LsnParseError::SegmentOverflow {
            segment: name,
            value: raw.to_string(),
        });
    }
    u32::from_str_radix(raw, 16).map_err(|_| LsnParseError::InvalidHex {
        segment: name,
        value: raw.to_string(),
    })
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Matches PostgreSQL's `text` output for `pg_lsn`: uppercase hex,
        // no leading zeros, separator '/'. Example: `16/B374D848`.
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl FromStr for Lsn {
    type Err = LsnParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Lsn::parse(s)
    }
}

impl Sub for Lsn {
    type Output = u64;
    fn sub(self, rhs: Lsn) -> u64 {
        self.bytes_ahead_of(rhs)
    }
}

impl PartialEq<u64> for Lsn {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u64> for Lsn {
    fn partial_cmp(&self, other: &u64) -> Option<Ordering> {
        Some(self.0.cmp(other))
    }
}

impl Serialize for Lsn {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Lsn {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Lsn::parse(&s).map_err(de::Error::custom)
    }
}

impl JsonSchema for Lsn {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("Lsn")
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "pattern": r"^[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}$",
            "description": "PostgreSQL Log Sequence Number in the format \"<hex>/<hex>\", where the upper 32 bits are before the slash and the lower 32 bits are after."
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_zero() {
        assert_eq!(Lsn::parse("0/0").unwrap(), Lsn::ZERO);
        assert_eq!(Lsn::ZERO.as_u64(), 0);
    }

    #[test]
    fn parse_mid_range() {
        // 16/B374D848 → (0x16 << 32) | 0xB374D848 = 0x16_B374_D848
        let lsn = Lsn::parse("16/B374D848").unwrap();
        assert_eq!(lsn.as_u64(), 0x16_B374_D848);
    }

    #[test]
    fn parse_case_insensitive() {
        let upper = Lsn::parse("16/B374D848").unwrap();
        let lower = Lsn::parse("16/b374d848").unwrap();
        let mixed = Lsn::parse("16/b374D848").unwrap();
        assert_eq!(upper, lower);
        assert_eq!(upper, mixed);
    }

    #[test]
    fn parse_tolerates_whitespace() {
        assert_eq!(Lsn::parse("  16/B374D848  ").unwrap(), Lsn(0x16_B374_D848));
        assert_eq!(Lsn::parse("16 / B374D848").unwrap(), Lsn(0x16_B374_D848));
    }

    #[test]
    fn parse_leading_zeros_allowed_but_normalised_on_display() {
        // Leading zeros within each segment are tolerated; both segments
        // must still fit in 32 bits (max 8 hex digits per segment).
        let with_zeros = Lsn::parse("0016/B374D848").unwrap();
        assert_eq!(with_zeros, Lsn(0x16_B374_D848));
        // Display drops the leading zeros to match PG's pg_lsn text output.
        assert_eq!(with_zeros.to_string(), "16/B374D848");
    }

    #[test]
    fn parse_rejects_missing_separator() {
        let err = Lsn::parse("16B374D848").unwrap_err();
        assert!(matches!(err, LsnParseError::MissingSeparator));
    }

    #[test]
    fn parse_rejects_empty_segments() {
        assert!(matches!(
            Lsn::parse("/B374D848"),
            Err(LsnParseError::EmptySegment { segment: "upper" })
        ));
        assert!(matches!(
            Lsn::parse("16/"),
            Err(LsnParseError::EmptySegment { segment: "lower" })
        ));
        assert!(matches!(
            Lsn::parse("/"),
            Err(LsnParseError::EmptySegment { .. })
        ));
    }

    #[test]
    fn parse_rejects_non_hex() {
        assert!(matches!(
            Lsn::parse("XY/B374D848"),
            Err(LsnParseError::InvalidHex {
                segment: "upper",
                ..
            })
        ));
        assert!(matches!(
            Lsn::parse("16/QQQQ"),
            Err(LsnParseError::InvalidHex {
                segment: "lower",
                ..
            })
        ));
    }

    #[test]
    fn parse_rejects_segments_over_32_bits() {
        // 9 hex digits = doesn't fit in u32.
        assert!(matches!(
            Lsn::parse("123456789/0"),
            Err(LsnParseError::SegmentOverflow {
                segment: "upper",
                ..
            })
        ));
        assert!(matches!(
            Lsn::parse("0/123456789"),
            Err(LsnParseError::SegmentOverflow {
                segment: "lower",
                ..
            })
        ));
    }

    #[test]
    fn display_matches_pg_text_output() {
        assert_eq!(Lsn::ZERO.to_string(), "0/0");
        assert_eq!(Lsn(0x16_B374_D848).to_string(), "16/B374D848");
        assert_eq!(Lsn(u64::MAX).to_string(), "FFFFFFFF/FFFFFFFF");
    }

    #[test]
    fn roundtrip_via_display_parse() {
        for v in [0u64, 1, 0xDEAD_BEEF, 0x1_0000_0000, 0xFFFF_FFFF_FFFF_FFFF] {
            let lsn = Lsn(v);
            let s = lsn.to_string();
            let parsed = Lsn::parse(&s).unwrap();
            assert_eq!(lsn, parsed, "round-trip failed for {v:#x} via {s}");
        }
    }

    #[test]
    fn ordering_is_total_and_monotonic() {
        let zero = Lsn::ZERO;
        let small = Lsn(0xFFFF);
        let mid = Lsn(0x1_0000_0000);
        let big = Lsn(0xDEAD_BEEF_DEAD_BEEF);
        assert!(zero < small);
        assert!(small < mid);
        assert!(mid < big);
    }

    #[test]
    fn subtraction_is_byte_distance() {
        let later = Lsn::parse("0/00010000").unwrap();
        let earlier = Lsn::parse("0/00000000").unwrap();
        assert_eq!(later - earlier, 0x1_0000);
    }

    #[test]
    fn subtraction_saturates_when_lhs_lt_rhs() {
        let earlier = Lsn::parse("0/00000000").unwrap();
        let later = Lsn::parse("0/00010000").unwrap();
        // earlier - later should saturate to 0, not panic or wrap.
        assert_eq!(earlier - later, 0);
    }

    #[test]
    fn bytes_ahead_of_zero_when_equal() {
        let lsn = Lsn(0xDEAD_BEEF);
        assert_eq!(lsn.bytes_ahead_of(lsn), 0);
    }

    #[test]
    fn comparison_to_u64_uses_packed_value() {
        // Using the PartialEq<u64> / PartialOrd<u64> impls.
        assert!(Lsn(42) == 42u64);
        assert!(Lsn(0) <= 0u64);
        assert!(Lsn(5) > 4u64);
    }

    #[test]
    fn serde_serializes_as_string() {
        let lsn = Lsn::parse("16/B374D848").unwrap();
        let json = serde_json::to_string(&lsn).unwrap();
        assert_eq!(json, "\"16/B374D848\"");
    }

    #[test]
    fn serde_deserializes_from_string() {
        let lsn: Lsn = serde_json::from_str("\"16/B374D848\"").unwrap();
        assert_eq!(lsn, Lsn(0x16_B374_D848));
    }

    #[test]
    fn serde_rejects_invalid_string() {
        let err = serde_json::from_str::<Lsn>("\"not an lsn\"").unwrap_err();
        assert!(err.to_string().contains("LSN"));
    }
}
