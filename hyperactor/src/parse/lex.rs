/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Shared lexer for Hyperactor id and ref grammars.

use std::fmt;

/// A byte span into the original input string.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Span {
    /// The inclusive start byte offset.
    pub(crate) start: usize,
    /// The exclusive end byte offset.
    pub(crate) end: usize,
}

impl Span {
    /// Create a new span.
    pub(crate) const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

/// Token kinds in Hyperactor id and ref syntax.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TokenKind {
    /// A text fragment between delimiters.
    Text,
    /// `.`
    Dot,
    /// `:`
    Colon,
    /// `@`
    At,
    /// `!`
    Bang,
    /// `<`
    LessThan,
    /// `>`
    GreaterThan,
    /// End of input.
    Eof,
}

impl TokenKind {
    pub(crate) fn symbol(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Dot => ".",
            Self::Colon => ":",
            Self::At => "@",
            Self::Bang => "!",
            Self::LessThan => "<",
            Self::GreaterThan => ">",
            Self::Eof => "end of input",
        }
    }

    pub(crate) fn expected(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Dot => "\".\"",
            Self::Colon => "\":\"",
            Self::At => "\"@\"",
            Self::Bang => "\"!\"",
            Self::LessThan => "\"<\"",
            Self::GreaterThan => "\">\"",
            Self::Eof => "end of input",
        }
    }
}

/// A token paired with its original source span.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Token<'a> {
    /// The token kind.
    pub(crate) kind: TokenKind,
    /// The original source text for this token.
    pub(crate) text: &'a str,
    /// The byte span of the token.
    pub(crate) span: Span,
}

impl Token<'_> {
    pub(crate) fn eof(pos: usize) -> Self {
        Self {
            kind: TokenKind::Eof,
            text: "",
            span: Span::new(pos, pos),
        }
    }

    pub(crate) fn describe(self) -> String {
        match self.kind {
            TokenKind::Text => format!("{:?}", self.text),
            TokenKind::Eof => "end of input".to_string(),
            _ => format!("{:?}", self.text),
        }
    }
}

impl fmt::Display for TokenKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eof => f.write_str("end of input"),
            _ => write!(f, "{:?}", self.symbol()),
        }
    }
}

/// Lexer over the shared Hyperactor syntax delimiters.
pub(crate) struct Lexer<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer.
    pub(crate) fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    /// Create a lexer positioned at `offset` within `input`. To bound
    /// the scan, pass an `input` already truncated to the desired end;
    /// the lexer emits no tokens past `input.len()`.
    pub(crate) fn new_at(input: &'a str, offset: usize) -> Self {
        Self { input, offset }
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.input.len() {
            return None;
        }

        let start = self.offset;
        let rest = &self.input[start..];
        let first = rest.as_bytes()[0];
        let token = match first {
            b'.' => {
                self.offset += 1;
                Token {
                    kind: TokenKind::Dot,
                    text: &self.input[start..self.offset],
                    span: Span::new(start, self.offset),
                }
            }
            b':' => {
                self.offset += 1;
                Token {
                    kind: TokenKind::Colon,
                    text: &self.input[start..self.offset],
                    span: Span::new(start, self.offset),
                }
            }
            b'@' => {
                self.offset += 1;
                Token {
                    kind: TokenKind::At,
                    text: &self.input[start..self.offset],
                    span: Span::new(start, self.offset),
                }
            }
            b'!' => {
                self.offset += 1;
                Token {
                    kind: TokenKind::Bang,
                    text: &self.input[start..self.offset],
                    span: Span::new(start, self.offset),
                }
            }
            b'<' => {
                self.offset += 1;
                Token {
                    kind: TokenKind::LessThan,
                    text: &self.input[start..self.offset],
                    span: Span::new(start, self.offset),
                }
            }
            b'>' => {
                self.offset += 1;
                Token {
                    kind: TokenKind::GreaterThan,
                    text: &self.input[start..self.offset],
                    span: Span::new(start, self.offset),
                }
            }
            _ => {
                let len = rest
                    .find(['.', ':', '@', '!', '<', '>'])
                    .unwrap_or(rest.len());
                self.offset += len;
                Token {
                    kind: TokenKind::Text,
                    text: &self.input[start..self.offset],
                    span: Span::new(start, self.offset),
                }
            }
        };
        Some(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lexer_tokenizes_id_and_ref_syntax() {
        let tokens = Lexer::new("controller<abc>.local!introspect@inproc://0").collect::<Vec<_>>();
        assert_eq!(
            tokens,
            vec![
                Token {
                    kind: TokenKind::Text,
                    text: "controller",
                    span: Span::new(0, 10),
                },
                Token {
                    kind: TokenKind::LessThan,
                    text: "<",
                    span: Span::new(10, 11),
                },
                Token {
                    kind: TokenKind::Text,
                    text: "abc",
                    span: Span::new(11, 14),
                },
                Token {
                    kind: TokenKind::GreaterThan,
                    text: ">",
                    span: Span::new(14, 15),
                },
                Token {
                    kind: TokenKind::Dot,
                    text: ".",
                    span: Span::new(15, 16),
                },
                Token {
                    kind: TokenKind::Text,
                    text: "local",
                    span: Span::new(16, 21),
                },
                Token {
                    kind: TokenKind::Bang,
                    text: "!",
                    span: Span::new(21, 22),
                },
                Token {
                    kind: TokenKind::Text,
                    text: "introspect",
                    span: Span::new(22, 32),
                },
                Token {
                    kind: TokenKind::At,
                    text: "@",
                    span: Span::new(32, 33),
                },
                Token {
                    kind: TokenKind::Text,
                    text: "inproc",
                    span: Span::new(33, 39),
                },
                Token {
                    kind: TokenKind::Colon,
                    text: ":",
                    span: Span::new(39, 40),
                },
                Token {
                    kind: TokenKind::Text,
                    text: "//0",
                    span: Span::new(40, 43),
                },
            ]
        );
    }

    #[test]
    fn test_token_describe() {
        assert_eq!(Token::eof(3).describe(), "end of input");
        assert_eq!(
            Token {
                kind: TokenKind::At,
                text: "@",
                span: Span::new(0, 1),
            }
            .describe(),
            "\"@\""
        );
    }
}
