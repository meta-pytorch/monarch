/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Names in hyperactor.

use rand::RngCore as _;
use serde::Deserialize;
use serde::Serialize;

use crate::reference::lex::Lexer;
use crate::reference::lex::ParseError;
use crate::reference::lex::Token;

/// So-called ["Flickr base 58"](https://www.flickr.com/groups/api/discuss/72157616713786392/)
/// as this alphabet was used in Flickr URLs. It has nice properties: 1) characters are all
/// URL safe, and characters which are easily confused (e.g., I and l) are removed.
pub(crate) const FLICKR_BASE_58: &str =
    "123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ";

/// An id that uniquely identifies an object.
/// UIDs are are displayed in, and parsed from the
/// ["Flickr base 58"](https://www.flickr.com/groups/api/discuss/72157616713786392/)
/// representation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Uid(u64);

impl Uid {
    /// Generate a new uid.
    pub fn generate() -> Self {
        Self(rand::thread_rng().next_u64())
    }

    pub(crate) fn zero() -> Self {
        Self(0)
    }
}

impl From<u64> for Uid {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for Uid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut num = self.0;
        let base = FLICKR_BASE_58.len() as u64;
        let mut result = String::with_capacity(12);

        for _pos in 0..12 {
            let remainder = (num % base) as usize;
            num /= base;
            let c = FLICKR_BASE_58.chars().nth(remainder).unwrap();
            result.push(c);
        }
        debug_assert_eq!(num, 0);

        let result = result.chars().rev().collect::<String>();
        write!(f, "{}", result)
    }
}

impl std::str::FromStr for Uid {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut lexer = Lexer::new(s);
        let uid = lexer.next_or_eof().into_uid()?;
        lexer.expect(Token::Eof)?;
        Ok(uid)
    }
}

/// A Unicode XID identifier.
/// See [Unicode Standard Annex #31](https://unicode.org/reports/tr31/).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ident(String);

impl Ident {
    /// Create a new identifier, returning the original string if it
    /// is not a valid unicode identifier.
    pub fn new(value: String) -> Result<Self, String> {
        let mut chars = value.chars();
        match chars.next() {
            None => return Err(value),
            Some(ch) if !unicode_ident::is_xid_start(ch) && ch != '_' => return Err(value),
            Some(_) => (),
        }
        if chars.all(unicode_ident::is_xid_continue) {
            Ok(Self(value))
        } else {
            Err(value)
        }
    }
}

impl std::fmt::Display for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for Ident {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ident::new(s.to_string())
    }
}

/// A namespaced object name, comprising a namespace, base, and uid.
/// Each namespace contains a set of related objects; the base name is
/// a user-defined name used to indicate the purpose of the named object,
/// and the UID uniquely identifies it. Taken together, each name is
/// global and unique. This facilitates identifiers that are "self-contained",
/// not requiring additional context when they appear in situ.
///
/// Both the namespace and the base must be valid Idents.
///
/// Names have a concrete syntax: `namespace:base-uid`, shown and parsed
/// by `Display` and `FromStr` respectively.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Name {
    namespace: Ident,
    base: Ident,
    uid: Uid,
}

impl Name {
    /// Create a new name. Users should typically generate
    /// a new name with [`Name::generate`].
    pub fn new(namespace: Ident, base: Ident, uid: Uid) -> Self {
        Self {
            namespace,
            base,
            uid,
        }
    }

    /// Generate a new name with the provided namespace and base.
    pub fn generate(namespace: Ident, base: Ident) -> Self {
        Self {
            namespace,
            base,
            uid: Uid::generate(),
        }
    }
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}-{}", self.namespace, self.base, self.uid)
    }
}

impl std::str::FromStr for Name {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut lexer = Lexer::new(s);

        let namespace = lexer.next_or_eof().into_ident()?;
        lexer.expect(Token::Colon)?;
        let base = lexer.next_or_eof().into_ident()?;
        let uid = lexer.next_or_eof().into_uid()?;
        lexer.expect(Token::Eof)?;

        Ok(Name {
            namespace,
            base,
            uid,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate() {
        assert_ne!(
            Name::generate("namespace".parse().unwrap(), "base".parse().unwrap(),),
            Name::generate("namespace".parse().unwrap(), "base".parse().unwrap(),),
        );
    }

    #[test]
    fn test_roundtrip() {
        let name = Name::new(
            "namespace".parse().unwrap(),
            "base".parse().unwrap(),
            Uid::zero(),
        );
        assert_eq!(name.to_string(), "namespace:base-111111111111".to_string());
        assert_eq!(name.to_string().parse::<Name>().unwrap(), name);
    }

    #[test]
    fn test_invalid() {
        assert_eq!(
            "namespace:base-lllll".parse::<Name>().unwrap_err(),
            ParseError::Expected(Token::Uid(Uid::zero()), Token::Error("lllll".to_string()))
        );
    }
}
