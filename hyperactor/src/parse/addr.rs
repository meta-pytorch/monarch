/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Recursive-descent parsing for Hyperactor addresses.
//!
//! These parsers compose on top of the shared id parser and then treat the
//! location tail after `@` as opaque input for `Location::from_str`. That
//! keeps URL punctuation out of the Hyperactor lexer while still giving
//! token-aware errors at the id/address boundary.

use crate::addr::ActorAddr;
use crate::addr::Addr;
use crate::addr::Location;
use crate::addr::PortAddr;
use crate::addr::ProcAddr;
use crate::channel::ChannelAddr;
use crate::parse::error::ParseError;
use crate::parse::id::Parser;
use crate::parse::id::parse_actor_id_with_parser;
use crate::parse::id::parse_id_with_parser;
use crate::parse::id::parse_port_id_with_parser;
use crate::parse::id::parse_proc_id_with_parser;
use crate::parse::id::parse_uid;
use crate::parse::lex::Span;
use crate::parse::lex::Token;
use crate::parse::lex::TokenKind;

pub(crate) fn parse_proc_addr(input: &str) -> Result<ProcAddr, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_proc_id_with_parser(&mut parser)?;
    let location = parse_location(&mut parser)?;
    Ok(ProcAddr::new(id, location))
}

pub(crate) fn parse_actor_addr(input: &str) -> Result<ActorAddr, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_actor_id_with_parser(&mut parser)?;
    let location = parse_location(&mut parser)?;
    Ok(ActorAddr::new(id, location))
}

pub(crate) fn parse_port_addr(input: &str) -> Result<PortAddr, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_port_id_with_parser(&mut parser)?;
    let location = parse_location(&mut parser)?;
    Ok(PortAddr::new(id, location))
}

pub(crate) fn parse_addr(input: &str) -> Result<Addr, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_id_with_parser(&mut parser)?;
    let location = parse_location(&mut parser)?;
    Ok(id.addr(location))
}

/// Standalone location parser: `via* zmq-url`, with no `@` prefix.
/// Used by `Location::from_str`.
pub(crate) fn parse_location_str(input: &str) -> Result<Location, ParseError> {
    let mut parser = Parser::new(input);
    // Synthesize an anchor token at offset 0 for `missing_location`
    // errors on empty input.
    let anchor = Token::eof(0);
    parse_location_body(&mut parser, anchor)
}

fn parse_location(parser: &mut Parser<'_>) -> Result<Location, ParseError> {
    let at = parser.expect_kind(TokenKind::At)?;
    parse_location_body(parser, at)
}

fn parse_location_body(parser: &mut Parser<'_>, anchor: Token<'_>) -> Result<Location, ParseError> {
    let rest = parser.rest();
    if rest.is_empty() {
        return Err(ParseError::missing_location(anchor));
    }

    // The location grammar is `via* zmq-url`, where each via is the
    // full [`Uid`] display syntax (`label`, `<base58>`, or
    // `label<base58>`) followed by `.`. Vias and ZMQ URLs share token
    // shapes (a singleton via `tcp` lexes the same as a scheme `tcp`),
    // so we sniff the URL scheme directly in the raw input and parse
    // it with `ChannelAddr`.
    let rest_start = parser.rest_span().start;
    let url_offset = find_url_scheme_start(rest).ok_or_else(|| {
        ParseError::invalid_location(
            parser.rest_span(),
            "expected ZMQ URL with `://`".to_string(),
        )
    })?;
    let url_start = rest_start + url_offset;

    let url_text = &rest[url_offset..];
    let url_span = Span::new(url_start, rest_start + rest.len());
    let addr = ChannelAddr::from_zmq_url(url_text)
        .map_err(|err: anyhow::Error| ParseError::invalid_location(url_span, err.to_string()))?;

    // Parse the via prefix `(uid ".")*` with the shared combinator,
    // bounding the lexer at the sniffed URL (synthesizing an EOF
    // there) so via uids reuse the id grammar and report token-aware
    // errors.
    parser.truncate(url_start);
    let mut uids = Vec::new();
    while parser.peek().kind != TokenKind::Eof {
        uids.push(parse_uid(parser)?);
        parser.expect_kind(TokenKind::Dot)?;
    }

    let mut location = Location::Addr(addr);
    for uid in uids.into_iter().rev() {
        location = Location::Via(uid, Box::new(location));
    }
    Ok(location)
}

/// Find the byte offset where a ZMQ URL scheme begins in `s`, by
/// locating the first `://` and walking back over alphabetic scheme
/// characters. Returns `None` if no `://` is present, or `Some(0)` if
/// the URL begins immediately at offset 0.
fn find_url_scheme_start(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let sep_offset = s.find("://")?;
    let mut start = sep_offset;
    while start > 0 && bytes[start - 1].is_ascii_alphabetic() {
        start -= 1;
    }
    Some(start)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::lex::Span;

    #[test]
    fn test_parse_proc_addr() {
        let parsed = parse_proc_addr("local@inproc://0").unwrap();
        assert_eq!(parsed.id().to_string(), "local");
        assert_eq!(parsed.location().to_string(), "inproc://0");
    }

    #[test]
    fn test_parse_actor_addr() {
        let parsed = parse_actor_addr("controller.local@tcp://[::1]:2345").unwrap();
        assert_eq!(parsed.id().to_string(), "controller.local");
        assert_eq!(parsed.location().to_string(), "tcp://[::1]:2345");
    }

    #[test]
    fn test_parse_port_addr() {
        let parsed = parse_port_addr("controller.local:7@inproc://0").unwrap();
        assert_eq!(parsed.id().to_string(), "controller.local:7");
        assert_eq!(parsed.location().to_string(), "inproc://0");
    }

    #[test]
    fn test_parse_addr_specificity() {
        assert!(parse_addr("local@inproc://0").unwrap().is_proc());
        assert!(parse_addr("local.local@inproc://0").unwrap().is_actor());
        assert!(parse_addr("local.local:7@inproc://0").unwrap().is_port());
    }

    #[test]
    fn test_parse_addr_reports_invalid_location() {
        let err = parse_addr("local@tcp://").unwrap_err();
        assert!(err.to_string().starts_with("invalid location: "));
        assert_eq!(err.span, Span::new(6, 12));
    }

    #[test]
    fn test_parse_addr_reports_missing_separator() {
        let err = parse_addr("local").unwrap_err();
        assert_eq!(err.to_string(), "expected \"@\", found end of input");
        assert_eq!(err.span, Span::new(5, 5));
    }

    #[test]
    fn test_parse_addr_reports_missing_location() {
        let err = parse_addr("local@").unwrap_err();
        assert_eq!(err.to_string(), "expected location, found end of input");
        assert_eq!(err.span, Span::new(5, 6));
    }

    #[test]
    fn test_parse_location_with_via_prefix() {
        // Two stacked via hops in front of a tcp URL.
        let loc = parse_location_str("<2>.<3>.tcp://[::1]:9000").unwrap();
        // Outer hop is uid 2, then uid 3, then the terminal addr.
        let (uid_outer, inner) = loc.as_via().expect("outermost is via");
        assert_eq!(uid_outer.to_string(), "<2>");
        let (uid_inner, terminal) = inner.as_via().expect("second is via");
        assert_eq!(uid_inner.to_string(), "<3>");
        assert!(terminal.as_addr().is_some(), "innermost must be terminal");
        // Display round-trips.
        assert_eq!(loc.to_string(), "<2>.<3>.tcp://[::1]:9000");
    }

    #[test]
    fn test_parse_addr_with_via_prefix_on_proc_addr() {
        let parsed = parse_proc_addr("local@<2>.tcp://[::1]:9000").unwrap();
        assert_eq!(parsed.id().to_string(), "local");
        assert_eq!(parsed.location().to_string(), "<2>.tcp://[::1]:9000");
    }

    #[test]
    fn test_parse_location_rejects_malformed_via() {
        // Missing `.` separator after `<uid>`.
        assert!(parse_location_str("<2>tcp://[::1]:9000").is_err());
        // Non-base58 uid token.
        assert!(parse_location_str("<!>.tcp://[::1]:9000").is_err());
    }

    #[test]
    fn test_parse_location_with_labeled_via_prefix() {
        // Labeled instance uid (`label<base58>`) and a singleton uid
        // (`label`) as via hops, mixed with an unlabeled instance.
        // Exercises the full Uid display grammar in via position.
        let loc = parse_location_str("host<7PDmJtQJB5S>.local.<2>.tcp://[::1]:1234").unwrap();
        let (uid_outer, after_outer) = loc.as_via().expect("outermost is via");
        assert_eq!(uid_outer.to_string(), "host<7PDmJtQJB5S>");
        let (uid_mid, after_mid) = after_outer.as_via().expect("second hop is via");
        assert_eq!(uid_mid.to_string(), "local");
        let (uid_inner, terminal) = after_mid.as_via().expect("third hop is via");
        assert_eq!(uid_inner.to_string(), "<2>");
        assert!(terminal.as_addr().is_some(), "innermost must be terminal");
        // Display round-trips.
        assert_eq!(
            loc.to_string(),
            "host<7PDmJtQJB5S>.local.<2>.tcp://[::1]:1234"
        );
    }

    #[test]
    fn test_parse_addr_does_not_downcast_malformed_port_addr() {
        let err = parse_addr("local.local:not-a-port@inproc://0").unwrap_err();
        assert_eq!(err.to_string(), "invalid port \"not-a-port\"");
        assert_eq!(err.span, Span::new(12, 22));
    }

    #[test]
    fn test_parse_addr_does_not_downcast_malformed_actor_addr() {
        let err = parse_addr("local.@inproc://0").unwrap_err();
        assert_eq!(err.to_string(), "expected \"label\" or \"<\", found \"@\"");
        assert_eq!(err.span, Span::new(6, 7));
    }
}
