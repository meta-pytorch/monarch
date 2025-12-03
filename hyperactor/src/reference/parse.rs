/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A simple parser combinator library intended to parse (syntactically
//! simple) Hyperactor identifiers.

use std::collections::VecDeque;

/// ParseError represents an error that occured during parsing.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum ParseError {
    #[error("expected '{0}', got '{1}'")]
    Expected(String, String),

    #[error("invalid token '{0}'")]
    InvalidToken(String),
}

/// Valid tokens in Hyperactor identifiers.
#[derive(Debug, PartialEq, Eq)]
pub enum Token<'a> {
    /// "["
    LeftBracket,
    /// "]"
    RightBracket,
    /// "<"
    LessThan,
    /// ">"
    GreaterThan,
    /// A decimal unsigned integer.
    Uint(usize),
    /// An element is any valid name.
    Elem(&'a str),
    /// "@"
    At,
    /// "."
    Dot,
    /// ","
    Comma,

    // Special token to denote an invalid element. It is used to poison
    // the parser.
    InvalidElem(&'a str),
}

/// Lexer that produces a stream of [`Token`]s, represented by
/// the lexer's iterator implementation.
pub struct Lexer<'a> {
    tokens: VecDeque<&'a str>,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer over the provided input.
    pub fn new(input: &'a str) -> Self {
        Self {
            // TODO: compose iterators directly; would be simpler with
            // existential type support.
            tokens: chop(input, &["[", "]", "<", ">", ".", "@", ","]).collect(),
        }
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Token<'a>> {
        match self.tokens.pop_front() {
            None => None,
            Some("[") => Some(Token::LeftBracket),
            Some("]") => Some(Token::RightBracket),
            Some("<") => Some(Token::LessThan),
            Some(">") => Some(Token::GreaterThan),
            Some("@") => Some(Token::At),
            Some(".") => Some(Token::Dot),
            Some(",") => Some(Token::Comma),
            Some(elem) => Some({
                if let Ok(uint) = elem.parse::<usize>() {
                    Token::Uint(uint)
                } else if is_valid_elem(elem) {
                    Token::Elem(elem)
                } else {
                    Token::InvalidElem(elem)
                }
            }),
        }
    }
}

/// A macro to parse tokens by pattern matching. The macro takes as input
/// a set of productions in the form of one or more space-separated,
/// token-typed patterns, which are matched against the token stream. Each
/// rule must have a production clause. For example:
///
/// ```ignore
/// parse! {
///     token_stream;
///     Token::Dot Token::Dot Token::Elem(ellipsis) => ellipsis
/// }
/// ```
///
/// Parses streams of the form "..blah", returning the string "blah".
// TODO: improve error reporting here
macro_rules! parse {
    (
        $token_stream:expr;
        $(
            $($pattern:pat_param)* => $constructor:expr
        ),* $(,)?
    ) => {
        {
            let tokens = $token_stream.collect::<Vec<_>>();
            let result = match tokens[..] {
                $([$( $pattern ),+] => Some($constructor),)*
                _ => None,
            };
            result.ok_or_else(|| {
                let all_patterns = vec![$(stringify!($($pattern)*),)*];
                ParseError::Expected(
                    all_patterns.join(" or "),
                    tokens.iter().map(|tok| format!("{:?}", tok)).collect::<Vec<_>>().join(" "))
            })
        }
    };
}
pub(crate) use parse;

/// Chop implements a simple lexer on a fixed set of delimiters.
fn chop<'a>(mut s: &'a str, delims: &'a [&'a str]) -> impl Iterator<Item = &'a str> + 'a {
    std::iter::from_fn(move || {
        if s.is_empty() {
            return None;
        }

        match delims
            .iter()
            .enumerate()
            .flat_map(|(index, d)| s.find(d).map(|pos| (index, pos)))
            .min_by_key(|&(_, v)| v)
        {
            Some((index, 0)) => {
                let delim = delims[index];
                s = &s[delim.len()..];
                Some(delim)
            }
            Some((_, pos)) => {
                let token = &s[..pos];
                s = &s[pos..];
                Some(token.trim())
            }
            None => {
                let token = s;
                s = "";
                Some(token.trim())
            }
        }
    })
}

pub fn is_valid(token: &str, is_continue: fn(char) -> bool) -> bool {
    // Disallow raw identifiers;
    if token.starts_with("r#") || token.is_empty() {
        return false;
    }
    let mut chars = token.chars();
    let mut first = true;
    while let Some(ch) = chars.next() {
        let valid = if ch == ':' {
            chars.next() == Some(':')
        } else if first {
            ch == '_' || unicode_ident::is_xid_start(ch)
        } else {
            is_continue(ch)
        };
        if !valid {
            return false;
        }
        first = false;
    }
    true
}

/// Determines whether the provided token is a valid hyperactor identifier.
///
/// Valid hyperactor identifiers are
/// [Rust identifier](https://doc.rust-lang.org/reference/identifiers.html),
/// excluding raw identifiers. Additionally, we allow double colon ("::")
/// to appear anywhere.
pub fn is_valid_ident(token: &str) -> bool {
    is_valid(token, unicode_ident::is_xid_continue)
}

/// Determines whether the provided token is a valid hyperactor element.
/// Like [`is_valid_ident`], but additionally allows '-' as a continuation
/// character.
fn is_valid_elem(token: &str) -> bool {
    fn is_continue(ch: char) -> bool {
        ch == '-' || unicode_ident::is_xid_continue(ch)
    }
    is_valid(token, is_continue)
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    #[test]
    fn test_lexer() {
        let tokens = Lexer::new("foo.bar[123],baz");
        assert_eq!(
            tokens.collect::<Vec<Token>>(),
            vec![
                Token::Elem("foo"),
                Token::Dot,
                Token::Elem("bar"),
                Token::LeftBracket,
                Token::Uint(123),
                Token::RightBracket,
                Token::Comma,
                Token::Elem("baz"),
            ]
        )
    }

    #[test]
    fn test_valid_idents() {
        let idents = vec![
            "foo", "foo_bar", "東京", "_foo", "foo-bar", "::foo", "foo::bar",
        ];

        for ident in idents {
            let tokens = Lexer::new(ident);

            assert_matches!(tokens.collect::<Vec<Token>>()[..], [Token::Elem(ident_)] if ident_ == ident,);
        }
    }

    #[test]
    fn test_invalid_idents() {
        let idents = vec!["-bar", "foo/bar", "r#true"];

        for ident in idents {
            let tokens = Lexer::new(ident);
            assert_matches!(
                &tokens.collect::<Vec<Token>>()[..],
                [Token::InvalidElem(ident_)] if *ident_ == ident,
            );
        }
    }

    #[test]
    fn test_parse() {
        let tokens = Lexer::new("foo.bar[123]");
        let parsed = parse!(
            tokens;
            Token::Elem(first) Token::Dot Token::Elem(second)
              Token::LeftBracket Token::Uint(num) Token::RightBracket => (first, second, num)
        );
        assert_eq!(parsed.unwrap(), ("foo", "bar", 123usize));
    }

    #[test]
    fn test_parse_failure() {
        let tokens = Lexer::new("foo.bar[123]");
        let parsed = parse!(
            tokens;
            Token::Elem(first) Token::Elem(second)
              Token::LeftBracket Token::Uint(num) Token::RightBracket => (first, second, num)
        );
        assert!(parsed.is_err())
    }
}
