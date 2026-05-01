/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Shared parse errors for the new id and ref parsers.

use std::fmt;

use crate::parse::lex::Span;
use crate::parse::lex::Token;

/// A parse error with byte-span context.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParseError {
    /// The byte span where the error occurred.
    pub(crate) span: Span,
    /// The error kind.
    pub(crate) kind: ParseErrorKind,
}

impl ParseError {
    pub(crate) fn expected(token: Token<'_>, expected: impl Into<String>) -> Self {
        Self {
            span: token.span,
            kind: ParseErrorKind::Expected {
                expected: expected.into(),
                found: token.describe(),
            },
        }
    }

    pub(crate) fn trailing_input(token: Token<'_>) -> Self {
        Self {
            span: token.span,
            kind: ParseErrorKind::TrailingInput {
                found: token.describe(),
            },
        }
    }

    pub(crate) fn invalid_port(token: Token<'_>) -> Self {
        Self {
            span: token.span,
            kind: ParseErrorKind::InvalidPort(token.text.to_string()),
        }
    }

    pub(crate) fn missing_location(at: Token<'_>) -> Self {
        Self {
            span: at.span,
            kind: ParseErrorKind::Expected {
                expected: "location".to_string(),
                found: "end of input".to_string(),
            },
        }
    }
}

/// Shared parse error kinds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ParseErrorKind {
    /// A specific token or grammar fragment was expected.
    Expected { expected: String, found: String },
    /// Input remained after a full parse.
    TrailingInput { found: String },
    /// A non-decimal port was encountered.
    InvalidPort(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt(f)
    }
}

impl fmt::Display for ParseErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expected { expected, found } => {
                write!(f, "expected {expected}, found {found}")
            }
            Self::TrailingInput { found } => write!(f, "expected end of input, found {found}"),
            Self::InvalidPort(port) => write!(f, "invalid port {port:?}"),
        }
    }
}

impl std::error::Error for ParseError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::lex::TokenKind;

    #[test]
    fn test_expected_error_message() {
        let err = ParseError::expected(
            Token {
                kind: TokenKind::At,
                text: "@",
                span: Span::new(3, 4),
            },
            "\"<\"",
        );
        assert_eq!(err.to_string(), "expected \"<\", found \"@\"");
        assert_eq!(err.span, Span::new(3, 4));
    }

    #[test]
    fn test_trailing_input_error_message() {
        let err = ParseError::trailing_input(Token {
            kind: TokenKind::Text,
            text: "junk",
            span: Span::new(5, 9),
        });
        assert_eq!(err.to_string(), "expected end of input, found \"junk\"");
        assert_eq!(err.span, Span::new(5, 9));
    }
}
