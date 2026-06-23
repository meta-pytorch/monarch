/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Recursive-descent parsing for Hyperactor ids.
//!
//! This module parses the id grammar and performs semantic validation for
//! labels, base58 uids, and decimal ports while token spans are still
//! available.

use crate::id::ActorId;
use crate::id::Id;
use crate::id::Label;
use crate::id::PortId;
use crate::id::ProcId;
use crate::id::Uid;
use crate::parse::error::ParseError;
use crate::parse::lex::Lexer;
use crate::parse::lex::Span;
use crate::parse::lex::Token;
use crate::parse::lex::TokenKind;
use crate::port::ControlPort;
use crate::port::Port;

/// Flickr base58 alphabet.
const BASE58_FLICKR: &[u8; 58] = b"123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ";

pub(crate) fn parse_uid_str(input: &str) -> Result<Uid, ParseError> {
    let mut parser = Parser::new(input);
    let uid = parse_uid(&mut parser)?;
    parser.finish()?;
    Ok(uid)
}

pub(crate) fn parse_id(input: &str) -> Result<Id, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_id_with_parser(&mut parser)?;
    parser.finish()?;
    Ok(id)
}

pub(crate) fn parse_proc_id(input: &str) -> Result<ProcId, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_proc_id_with_parser(&mut parser)?;
    parser.finish()?;
    Ok(id)
}

pub(crate) fn parse_actor_id(input: &str) -> Result<ActorId, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_actor_id_with_parser(&mut parser)?;
    parser.finish()?;
    Ok(id)
}

pub(crate) fn parse_port_id(input: &str) -> Result<PortId, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_port_id_with_parser(&mut parser)?;
    parser.finish()?;
    Ok(id)
}

pub(crate) fn parse_uid(parser: &mut Parser<'_>) -> Result<Uid, ParseError> {
    parse_id_component(parser)
}

pub(crate) fn parse_id_with_parser(parser: &mut Parser<'_>) -> Result<Id, ParseError> {
    let first = parse_uid(parser)?;
    match parser.peek().kind {
        TokenKind::Dot => {
            parser.bump();
            let proc_id = parse_proc_id_with_parser(parser)?;
            let actor_id = ActorId::new(first, proc_id, None);
            match parser.peek().kind {
                TokenKind::Colon => {
                    parser.bump();
                    let port = parse_port(parser)?;
                    Ok(Id::Port(PortId::new(actor_id, port)))
                }
                TokenKind::Bang => {
                    parser.bump();
                    let port = parse_control_port(parser)?;
                    Ok(Id::Port(PortId::new(actor_id, port)))
                }
                _ => Ok(Id::Actor(actor_id)),
            }
        }
        _ => Ok(Id::Proc(ProcId::new(first, None))),
    }
}

pub(crate) fn parse_proc_id_with_parser(parser: &mut Parser<'_>) -> Result<ProcId, ParseError> {
    Ok(ProcId::new(parse_uid(parser)?, None))
}

pub(crate) fn parse_actor_id_with_parser(parser: &mut Parser<'_>) -> Result<ActorId, ParseError> {
    let actor = parse_uid(parser)?;
    parser.expect_kind(TokenKind::Dot)?;
    let proc_id = parse_proc_id_with_parser(parser)?;
    Ok(ActorId::new(actor, proc_id, None))
}

pub(crate) fn parse_port_id_with_parser(parser: &mut Parser<'_>) -> Result<PortId, ParseError> {
    let actor_id = parse_actor_id_with_parser(parser)?;
    let port = match parser.peek().kind {
        TokenKind::Colon => {
            parser.bump();
            parse_port(parser)?
        }
        TokenKind::Bang => {
            parser.bump();
            parse_control_port(parser)?
        }
        _ => return Err(ParseError::expected(parser.peek(), "\":\" or \"!\"")),
    };
    Ok(PortId::new(actor_id, port))
}

fn parse_port(parser: &mut Parser<'_>) -> Result<Port, ParseError> {
    match parser.peek().kind {
        TokenKind::Text => {
            let port = parser.bump();
            if port.text.bytes().all(|ch| ch.is_ascii_digit()) {
                let port: u64 = port
                    .text
                    .parse()
                    .map_err(|_| ParseError::invalid_port(port))?;
                return Ok(Port::ephemeral(port));
            }

            let uid = parse_id_component_from_first_text(port, parser)?;
            parse_handler_port(uid, port)
        }
        TokenKind::LessThan => {
            let port = parser.peek();
            parse_handler_port(parse_id_component(parser)?, port)
        }
        _ => Err(ParseError::expected(parser.peek(), "port")),
    }
}

fn parse_handler_port(uid: Uid, token: Token<'_>) -> Result<Port, ParseError> {
    match uid {
        Uid::Instance(_, _) => Ok(Port::Handler(uid)),
        Uid::Singleton(_) => Err(ParseError::invalid_port(token)),
    }
}

fn parse_control_port(parser: &mut Parser<'_>) -> Result<Port, ParseError> {
    let port = parser.expect_text("control port")?;
    port.text
        .parse::<ControlPort>()
        .map(Port::control)
        .map_err(|_| ParseError::invalid_port(port))
}

pub(crate) fn parse_id_component(parser: &mut Parser<'_>) -> Result<Uid, ParseError> {
    match parser.peek().kind {
        TokenKind::Text => {
            let label = parser.bump();
            parse_id_component_from_first_text(label, parser)
        }
        TokenKind::LessThan => {
            parser.bump();
            let uid = parser.expect_text("uid text")?;
            parser.expect_kind(TokenKind::GreaterThan)?;
            Ok(Uid::Instance(parse_base58_uid(uid)?, None))
        }
        _ => Err(ParseError::expected(parser.bump(), "\"label\" or \"<\"")),
    }
}

fn parse_id_component_from_first_text(
    label: Token<'_>,
    parser: &mut Parser<'_>,
) -> Result<Uid, ParseError> {
    if parser.peek().kind == TokenKind::LessThan {
        parser.bump();
        let uid = parser.expect_text("uid text")?;
        parser.expect_kind(TokenKind::GreaterThan)?;
        let label = parse_label(label)?;
        let uid = parse_base58_uid(uid)?;
        Ok(Uid::Instance(uid, Some(label)))
    } else {
        Ok(Uid::Singleton(parse_label(label)?))
    }
}

fn parse_label(token: Token<'_>) -> Result<Label, ParseError> {
    Label::new(token.text).map_err(|err| ParseError::invalid_label(token.span, err.to_string()))
}

fn parse_base58_uid(token: Token<'_>) -> Result<u64, ParseError> {
    decode_base58_uid(token.text).map_err(|_| ParseError::invalid_base58_uid(token))
}

pub(crate) fn encode_base58_uid(mut uid: u64) -> String {
    if uid == 0 {
        return "1".to_string();
    }

    let mut digits = Vec::new();
    while uid > 0 {
        digits.push(BASE58_FLICKR[(uid % 58) as usize] as char);
        uid /= 58;
    }
    digits.iter().rev().collect()
}

pub(crate) fn decode_base58_uid(s: &str) -> Result<u64, ()> {
    if s.is_empty() {
        return Err(());
    }

    let mut uid = 0u64;
    for ch in s.bytes() {
        let digit = BASE58_FLICKR
            .iter()
            .position(|candidate| *candidate == ch)
            .ok_or(())? as u64;
        uid = uid
            .checked_mul(58)
            .and_then(|value| value.checked_add(digit))
            .ok_or(())?;
    }
    Ok(uid)
}

pub(crate) struct Parser<'a> {
    input: &'a str,
    lexer: Lexer<'a>,
    lookahead: Token<'a>,
}

impl<'a> Parser<'a> {
    pub(crate) fn new(input: &'a str) -> Self {
        let mut lexer = Lexer::new(input);
        let lookahead = lexer.next().unwrap_or_else(|| Token::eof(input.len()));
        Self {
            input,
            lexer,
            lookahead,
        }
    }

    pub(crate) fn peek(&self) -> Token<'a> {
        self.lookahead
    }

    pub(crate) fn bump(&mut self) -> Token<'a> {
        let current = self.lookahead;
        self.lookahead = self
            .lexer
            .next()
            .unwrap_or_else(|| Token::eof(self.input.len()));
        current
    }

    pub(crate) fn expect_kind(&mut self, kind: TokenKind) -> Result<Token<'a>, ParseError> {
        if self.peek().kind == kind {
            Ok(self.bump())
        } else {
            Err(ParseError::expected(self.peek(), kind.expected()))
        }
    }

    pub(crate) fn expect_text(&mut self, expected: &'static str) -> Result<Token<'a>, ParseError> {
        if self.peek().kind == TokenKind::Text {
            Ok(self.bump())
        } else {
            Err(ParseError::expected(self.peek(), expected))
        }
    }

    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        if self.peek().kind == TokenKind::Eof {
            Ok(())
        } else {
            Err(ParseError::trailing_input(self.peek()))
        }
    }

    pub(crate) fn rest(&self) -> &'a str {
        &self.input[self.peek().span.start..]
    }

    pub(crate) fn rest_span(&self) -> Span {
        Span::new(self.peek().span.start, self.input.len())
    }

    /// Restrict the remaining input to `[current, end)`, synthesizing
    /// an EOF at `end`. The current position is preserved. Used to
    /// parse a prefix with the combinator when its tail (e.g. a sniffed
    /// ZMQ URL) is handled outside the token grammar.
    pub(crate) fn truncate(&mut self, end: usize) {
        let start = self.peek().span.start;
        self.input = &self.input[..end];
        self.lexer = Lexer::new_at(self.input, start);
        self.lookahead = self.lexer.next().unwrap_or_else(|| Token::eof(end));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_uid_forms() {
        assert_eq!(parse_uid_str("local").unwrap().to_string(), "local");
        assert_eq!(
            parse_uid_str("controller<2>").unwrap().to_string(),
            "controller<2>"
        );
        assert_eq!(parse_uid_str("<2>").unwrap().to_string(), "<2>");
    }

    #[test]
    fn test_parse_proc_id_forms() {
        assert_eq!(parse_proc_id("local").unwrap().to_string(), "local");
        assert_eq!(
            parse_proc_id("controller<2>").unwrap().to_string(),
            "controller<2>"
        );
        assert_eq!(parse_proc_id("<2>").unwrap().to_string(), "<2>");
    }

    #[test]
    fn test_parse_actor_id_forms() {
        assert_eq!(
            parse_actor_id("controller.local").unwrap().to_string(),
            "controller.local"
        );
        assert_eq!(
            parse_actor_id("controller<2>.local").unwrap().to_string(),
            "controller<2>.local"
        );
    }

    #[test]
    fn test_parse_port_id_form() {
        assert_eq!(
            parse_port_id("controller.local:7").unwrap().to_string(),
            "controller.local:7"
        );
        assert_eq!(
            parse_port_id("controller.local:handler<2>")
                .unwrap()
                .to_string(),
            "controller.local:handler<2>"
        );
        assert_eq!(
            parse_port_id("controller.local!introspect")
                .unwrap()
                .to_string(),
            "controller.local!introspect"
        );
    }

    #[test]
    fn test_parse_id_specificity() {
        assert!(parse_id("local").unwrap().is_proc());
        assert!(parse_id("controller.local").unwrap().is_actor());
        assert!(parse_id("controller.local:7").unwrap().is_port());
    }

    #[test]
    fn test_parse_uid_reports_invalid_base58_span() {
        let err = parse_uid_str("controller<0>").unwrap_err();
        assert_eq!(err.to_string(), "invalid base58 uid: 0");
        assert_eq!(err.span, Span::new(11, 12));
    }

    #[test]
    fn test_parse_uid_reports_invalid_label_span() {
        let err = parse_uid_str("Controller<2>").unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid label: label must start with a lowercase letter"
        );
        assert_eq!(err.span, Span::new(0, 10));
    }

    #[test]
    fn test_parse_actor_id_reports_missing_proc_component() {
        let err = parse_actor_id("controller.").unwrap_err();
        assert_eq!(
            err.to_string(),
            "expected \"label\" or \"<\", found end of input"
        );
        assert_eq!(err.span, Span::new(11, 11));
    }

    #[test]
    fn test_parse_proc_id_reports_trailing_input() {
        let err = parse_proc_id("local@inproc://0").unwrap_err();
        assert_eq!(err.to_string(), "expected end of input, found \"@\"");
        assert_eq!(err.span, Span::new(5, 6));
    }

    #[test]
    fn test_parse_port_id_rejects_non_decimal_port() {
        let err = parse_port_id("controller.local:not-a-port").unwrap_err();
        assert_eq!(err.to_string(), "invalid port \"not-a-port\"");
        assert_eq!(err.span, Span::new(17, 27));
    }

    #[test]
    fn test_parse_port_id_reports_missing_port_token() {
        let err = parse_port_id("controller.local:@inproc://0").unwrap_err();
        assert_eq!(err.to_string(), "expected port, found \"@\"");
        assert_eq!(err.span, Span::new(17, 18));
    }

    #[test]
    fn test_parser_rest_tracks_location_boundary() {
        let mut parser = Parser::new("controller.local@inproc://0");
        let actor = parse_actor_id_with_parser(&mut parser).unwrap();
        assert_eq!(actor.to_string(), "controller.local");
        assert_eq!(parser.rest(), "@inproc://0");
    }
}
