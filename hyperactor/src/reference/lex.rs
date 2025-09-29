/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module contains a lexer for Hyperactor identifiers.

use std::iter::Peekable;
use std::str::Chars;
use std::sync::LazyLock;

use crate::reference::name::FLICKR_BASE_58;
use crate::reference::name::Ident;
use crate::reference::name::Uid;

/// Precomputed character ordinals for the alphabet.
static FLICKR_BASE_58_ORD: LazyLock<[Option<usize>; 256]> = LazyLock::new(|| {
    let mut table = [None; 256];
    for (i, c) in FLICKR_BASE_58.chars().enumerate() {
        table[c as usize] = Some(i);
    }
    table
});

/// The tyep of error that occurs while parsing a hyperactor identifier.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParseError {
    /// Todo
    #[error("expected '{0}', got '{1}'")]
    Expected(Token, Token),
}

/// An identifier token.
#[derive(Debug, PartialEq, Eq)]
pub enum Token {
    /// "["
    LeftBracket,
    /// "]"
    RightBracket,
    /// A decimal unsigned integer, can appear in brackets
    Uint(usize),
    /// "."
    Dot,
    /// "-uid" suffixes, FLICKR58 format
    Uid(Uid),
    /// ":", can appear in brackets
    Colon,
    /// "//"
    DoubleSlash,
    /// Typename, within a bracket. These are rust identifiers, plus
    /// ':' characters.
    BracketTypename(String),
    /// An identifier, following rust rules.
    Ident(Ident),

    /// Special token to denote a lexer error. It contains the unlexed
    /// remainder of the input. The error occured on the first character.
    Error(String),

    /// No more tokens.
    Eof,
}

impl Token {
    /// Return the token as a left bracket, otherwise a parse error.
    pub fn into_left_bracket(self) -> Result<(), ParseError> {
        match self {
            Token::LeftBracket => Ok(()),
            other => Err(ParseError::Expected(Token::LeftBracket, other)),
        }
    }

    /// Return the token as a right bracket, otherwise a parse error.
    pub fn into_right_bracket(self) -> Result<(), ParseError> {
        match self {
            Token::RightBracket => Ok(()),
            other => Err(ParseError::Expected(Token::RightBracket, other)),
        }
    }

    /// Return the token as a uint, otherwise a parse error.
    pub fn into_uint(self) -> Result<usize, ParseError> {
        match self {
            Token::Uint(value) => Ok(value),
            other => Err(ParseError::Expected(Token::Uint(0), other)),
        }
    }

    /// Return the token as a dot, otherwise a parse error.
    pub fn into_dot(self) -> Result<(), ParseError> {
        match self {
            Token::Dot => Ok(()),
            other => Err(ParseError::Expected(Token::Dot, other)),
        }
    }

    /// Return the token as a Uid, otherwise a parse error.
    pub fn into_uid(self) -> Result<Uid, ParseError> {
        match self {
            Token::Uid(uid) => Ok(uid),
            other => Err(ParseError::Expected(Token::Uid(Uid::zero()), other)),
        }
    }

    /// Return the token as a colon, otherwise a parse error.
    pub fn into_colon(self) -> Result<(), ParseError> {
        match self {
            Token::Colon => Ok(()),
            other => Err(ParseError::Expected(Token::Colon, other)),
        }
    }

    /// Return the token as a double slash, otherwise a parse error.
    pub fn into_double_slash(self) -> Result<(), ParseError> {
        match self {
            Token::DoubleSlash => Ok(()),
            other => Err(ParseError::Expected(Token::DoubleSlash, other)),
        }
    }

    /// Return the token as a bracket typename, otherwise a parse error.
    pub fn into_bracket_typename(self) -> Result<String, ParseError> {
        match self {
            Token::BracketTypename(value) => Ok(value),
            other => Err(ParseError::Expected(
                Token::BracketTypename(String::new()),
                other,
            )),
        }
    }

    /// Return the token as an ident, otherwise a parse error.
    pub fn into_ident(self) -> Result<Ident, ParseError> {
        match self {
            Token::Ident(value) => Ok(value),
            other => Err(ParseError::Expected(
                Token::Ident("ident".parse().unwrap()),
                other,
            )),
        }
    }

    /// Return the token as an error, otherwise a parse error.
    pub fn into_error(self) -> Result<String, ParseError> {
        match self {
            Token::Error(value) => Ok(value),
            other => Err(ParseError::Expected(Token::Error(String::new()), other)),
        }
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::LeftBracket => write!(f, "["),
            Token::RightBracket => write!(f, "]"),
            Token::Uint(value) => write!(f, "{}", value),
            Token::Dot => write!(f, "."),
            Token::Uid(uid) => write!(f, "-{}", uid),
            Token::Colon => write!(f, ":"),
            Token::DoubleSlash => write!(f, "//"),
            Token::BracketTypename(value) => write!(f, "{}", value),
            Token::Ident(value) => write!(f, "{}", value),
            Token::Error(value) => write!(f, "<error {}>", value),
            Token::Eof => write!(f, "<eof>"),
        }
    }
}

/// A lexer is an iterator over [`Token`].
#[derive(Default, Debug)]
pub(crate) enum Lexer<'a> {
    Next(Peekable<Chars<'a>>),
    Inbracket(Peekable<Chars<'a>>),
    #[default]
    Invalid,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer over the provided input.
    pub(crate) fn new(input: &'a str) -> Self {
        Lexer::Next(input.chars().peekable())
    }

    /// Consume and return the next token, returning [`Token::Eof`]
    /// ("fused") when there are no more tokens available.
    pub(crate) fn next_or_eof(&mut self) -> Token {
        self.next().unwrap_or(Token::Eof)
    }

    /// Expect the provided token, or return a parse error.
    pub(crate) fn expect(&mut self, token: Token) -> Result<(), ParseError> {
        let next = self.next_or_eof();
        if next != token {
            return Err(ParseError::Expected(token, next));
        }
        Ok(())
    }

    fn parse_uint(iter: &mut Peekable<Chars<'a>>) -> usize {
        let mut value = iter.next().unwrap().to_digit(10).unwrap() as usize;
        while let Some(&ch) = iter.peek() {
            if let Some(d) = ch.to_digit(10) {
                value = value * 10 + d as usize;
                iter.next();
            } else {
                break;
            }
        }
        value
    }

    fn parse_ident(iter: &mut Peekable<Chars<'a>>) -> Ident {
        let mut ident = String::new();

        let ch = iter.next().unwrap();
        assert!(unicode_ident::is_xid_start(ch) || ch == '_');
        ident.push(ch);

        while let Some(&ch) = iter.peek()
            && unicode_ident::is_xid_continue(ch)
        {
            ident.push(iter.next().unwrap());
        }
        Ident::new(ident).unwrap()
    }

    fn parse_bracket_typename(iter: &mut Peekable<Chars<'a>>) -> String {
        let mut ident = String::new();

        let ch = iter.next().unwrap();
        assert!(unicode_ident::is_xid_start(ch) || ch == '_');
        ident.push(ch);

        while let Some(&ch) = iter.peek()
            && (unicode_ident::is_xid_continue(ch) || ch == ':')
        {
            ident.push(iter.next().unwrap());
        }
        ident
    }

    fn parse_uid(iter: &mut Peekable<Chars<'a>>) -> Option<Uid> {
        let base = FLICKR_BASE_58.len() as u64;
        let mut num = 0u64;

        for _i in 0..12 {
            let &ch = iter.peek()?;
            let pos = FLICKR_BASE_58_ORD[ch as usize]?;
            let _ = iter.next();
            num *= base;
            num += pos as u64;
        }

        Some(num.into())
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Self::Item> {
        let (new_state, token) = match std::mem::take(self) {
            Lexer::Next(mut iter) => match iter.peek()? {
                '[' => {
                    let _ = iter.next();
                    (Lexer::Inbracket(iter), Some(Token::LeftBracket))
                }
                '.' => {
                    let _ = iter.next();
                    (Lexer::Next(iter), Some(Token::Dot))
                }
                '/' => {
                    let _ = iter.next();
                    match iter.next() {
                        Some('/') => (Lexer::Next(iter), Some(Token::DoubleSlash)),
                        _ => (Lexer::Invalid, Some(Token::Error(iter.collect()))),
                    }
                }
                ':' => {
                    let _ = iter.next();
                    (Lexer::Next(iter), Some(Token::Colon))
                }
                '-' => {
                    let _ = iter.next();
                    match Lexer::parse_uid(&mut iter) {
                        Some(uid) => (Lexer::Next(iter), Some(Token::Uid(uid))),
                        None => (Lexer::Invalid, Some(Token::Error(iter.collect()))),
                    }
                }
                // TODO: support hexadecimal
                '0'..='9' => {
                    let uint = Lexer::parse_uint(&mut iter);
                    (Lexer::Next(iter), Some(Token::Uint(uint)))
                }
                ch if unicode_ident::is_xid_start(*ch) || *ch == '_' => {
                    let ident = Lexer::parse_ident(&mut iter);
                    (Lexer::Next(iter), Some(Token::Ident(ident)))
                }
                _ => (Lexer::Invalid, Some(Token::Error(iter.collect()))),
            },
            Lexer::Inbracket(mut iter) => match iter.peek()? {
                '0'..='9' => {
                    let uint = Lexer::parse_uint(&mut iter);
                    (Lexer::Inbracket(iter), Some(Token::Uint(uint)))
                }
                ch if unicode_ident::is_xid_start(*ch) => {
                    let typename = Lexer::parse_bracket_typename(&mut iter);
                    (
                        Lexer::Inbracket(iter),
                        Some(Token::BracketTypename(typename)),
                    )
                }
                ':' => {
                    let _ = iter.next();
                    (Lexer::Inbracket(iter), Some(Token::Colon))
                }
                ']' => {
                    let _ = iter.next();
                    (Lexer::Next(iter), Some(Token::RightBracket))
                }
                _ => (Lexer::Invalid, Some(Token::Error(iter.collect()))),
            },
            Lexer::Invalid => (Lexer::Invalid, None),
        };

        *self = new_state;
        token
    }
}

#[cfg(test)]
mod tests {
    use Token::*;

    use super::*;

    #[test]
    fn test_basic() {
        assert_eq!(
            Lexer::new("foo.bar[123:foo]//blah").collect::<Vec<_>>(),
            vec![
                Ident("foo".parse().unwrap()),
                Dot,
                Ident("bar".parse().unwrap()),
                LeftBracket,
                Uint(123),
                Colon,
                BracketTypename("foo".to_string()),
                RightBracket,
                DoubleSlash,
                Ident("blah".parse().unwrap())
            ]
        );
        assert_eq!(
            Lexer::new("foo.ba)r[123:foo]//blah").collect::<Vec<_>>(),
            vec![
                Ident("foo".parse().unwrap()),
                Dot,
                Ident("ba".parse().unwrap()),
                Error(")r[123:foo]//blah".to_string())
            ]
        );
    }
}
