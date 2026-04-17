//! Utilities for representing and resolving HTTP Byte Ranges.

use std::ops::{Range, RangeFrom};

/// Represents an abstract requested slice of a remote object.
#[derive(Debug, Clone, Copy)]
pub enum ByteRange {
    /// An exact bounded range `[start, end)`.
    Exact(u64, u64),
    /// An open-ended range from a specific start offset.
    From(u64),
    /// A range resolving to the final `N` bytes of the file.
    Suffix(u64),
}

impl core::fmt::Display for ByteRange {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Exact(start, end) => write!(f, "bytes={start}-{}", end.saturating_sub(1)),
            Self::From(start) => write!(f, "bytes={start}-"),
            Self::Suffix(suffix_length) => write!(f, "bytes=-{suffix_length}"),
        }
    }
}

/// Parses the authoritative total object size from a standard HTTP `Content-Range` header.
///
/// Looks for the pattern `bytes <start>-<end>/<total_size>` and extracts the `total_size`.
#[must_use]
pub fn parse_object_size_from_content_range(content_range: Option<&str>) -> Option<u64> {
    content_range
        .and_then(|s| s.rsplit_once('/'))
        .and_then(|(_, size)| size.parse::<u64>().ok())
}

impl From<Range<u64>> for ByteRange {
    fn from(r: Range<u64>) -> Self {
        Self::Exact(r.start, r.end)
    }
}

impl From<RangeFrom<u64>> for ByteRange {
    fn from(r: RangeFrom<u64>) -> Self {
        Self::From(r.start)
    }
}

impl ByteRange {
    /// Computes the exact length of the byte range if it is definitively bounded.
    #[must_use]
    pub const fn length(&self) -> Option<u64> {
        match self {
            Self::Exact(start, end) => Some(end.saturating_sub(*start)),
            Self::From(_) => None,
            Self::Suffix(suffix_length) => Some(*suffix_length),
        }
    }

    /// Resolves the absolute (start, end) byte offsets for a given total file size.
    /// The returned bounds are inclusive.
    #[must_use]
    pub fn resolve_bounds(&self, total_size: u64) -> (u64, u64) {
        let max_end = total_size.saturating_sub(1);
        match self {
            Self::Exact(start, end) => (*start, std::cmp::min(end.saturating_sub(1), max_end)),
            Self::From(start) => (*start, max_end),
            Self::Suffix(suffix_len) => {
                let start = total_size.saturating_sub(*suffix_len);
                (start, max_end)
            }
        }
    }
}
