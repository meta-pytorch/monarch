/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module implements a "short" (64-bit) UUID, used to assign
//! names to various objects in meshes.

use std::str::FromStr;
use std::sync::LazyLock;

use rand::RngCore;
use serde::Deserialize;
use serde::Serialize;

/// So-called ["Flickr base 58"](https://www.flickr.com/groups/api/discuss/72157616713786392/)
/// as this alphabet was used in Flickr URLs. It has nice properties: 1) characters are all
/// URL safe, and characters which are easily confused (e.g., I and l) are removed.
const FLICKR_BASE_58: &str = "123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ";

/// Precomputed character ordinals for the alphabet.
static FLICKR_BASE_58_ORD: LazyLock<[Option<usize>; 256]> = LazyLock::new(|| {
    let mut table = [None; 256];
    for (i, c) in FLICKR_BASE_58.chars().enumerate() {
        table[c as usize] = Some(i);
    }
    table
});

/// A short (64-bit) UUID. UUIDs can be generated with [`ShortUuid::generate`],
/// displayed as short, URL-friendly, 12-character gobbleygook, and parsed accordingly.
///
/// ```
/// # use hyperactor_mesh::shortuuid::ShortUuid;
/// let uuid = ShortUuid::generate();
/// println!("nice, short, URL friendly: {}", uuid);
/// assert_eq!(uuid.to_string().parse::<ShortUuid>().unwrap(), uuid);
/// ```
///
/// ShortUuids have a Base-58, alphanumeric URI-friendly representation.
/// The characters "_" and "-" are ignored when decoding, and may be
/// safely interspersed. By default, rendered UUIDs that begin with a
/// numeric character is prefixed with "_".
#[derive(
    PartialEq,
    Eq,
    Hash,
    Debug,
    Clone,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord
)]
pub struct ShortUuid(u64);

impl ShortUuid {
    /// Generate a new UUID.
    pub fn generate() -> ShortUuid {
        ShortUuid(rand::rng().next_u64())
    }

    pub(crate) fn format(&self, f: &mut std::fmt::Formatter<'_>, raw: bool) -> std::fmt::Result {
        let mut num = self.0;
        let base = FLICKR_BASE_58.len() as u64;
        let mut result = String::with_capacity(12);

        for pos in 0..12 {
            let remainder = (num % base) as usize;
            num /= base;
            let c = FLICKR_BASE_58.chars().nth(remainder).unwrap();
            result.push(c);
            // Make sure the first position is never a digit.
            if !raw && pos == 11 && c.is_ascii_digit() {
                result.push('_');
            }
        }
        assert_eq!(num, 0);

        let result = result.chars().rev().collect::<String>();
        write!(f, "{}", result)
    }
}
impl std::fmt::Display for ShortUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.format(f, false /*raw*/)
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ParseShortUuidError {
    #[error("invalid character '{0}' in ShortUuid")]
    InvalidCharacter(char),
}

impl FromStr for ShortUuid {
    type Err = ParseShortUuidError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let base = FLICKR_BASE_58.len() as u64;
        let mut num = 0u64;

        for c in s.chars() {
            if c == '_' || c == '-' {
                continue;
            }
            num *= base;
            if let Some(pos) = FLICKR_BASE_58_ORD[c as usize] {
                num += pos as u64;
            } else {
                return Err(ParseShortUuidError::InvalidCharacter(c));
            }
        }

        Ok(ShortUuid(num))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let cases = vec![
            (0, "_111111111111"),
            (1, "_111111111112"),
            (2, "_111111111113"),
            (3, "_111111111114"),
            (1234657890119888222, "_13Se1Maqzryj"),
            (58 * 58 - 1, "_1111111111ZZ"),
            (u64::MAX, "_1JPwcyDCgEup"),
            (58 * 58, "_111111111211"),
        ];

        for (num, display) in cases {
            let uuid = ShortUuid(num);
            assert_eq!(uuid.to_string(), display);
            // Round-trip test:
            assert_eq!(uuid.to_string().parse::<ShortUuid>().unwrap(), uuid);
        }
    }

    #[test]
    fn test_decode() {
        let cases = vec![
            ("__-_1111_11_111111", 0),
            ("_111111111112", 1),
            ("_111111111113", 2),
            ("_111111111114-", 3),
            ("13Se1-Maqzr-yj", 1234657890119888222),
            ("1111111111ZZ", 58 * 58 - 1),
            ("1JPwcy-----DCgEup", u64::MAX),
            ("_111111111211", 58 * 58),
        ];

        for (display, num) in cases {
            assert_eq!(display.parse::<ShortUuid>().unwrap(), ShortUuid(num));
        }
    }

    #[test]
    fn test_parse_error() {
        let invalid_cases = vec![
            ("11111111111O", 'O'),
            ("11111111111I", 'I'),
            ("11111111111l", 'l'),
            ("11111111111@", '@'),
        ];

        for (input, invalid_char) in invalid_cases {
            assert_eq!(
                input.parse::<ShortUuid>().unwrap_err(),
                ParseShortUuidError::InvalidCharacter(invalid_char),
            )
        }
    }
}
