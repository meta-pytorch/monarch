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
use crate::parse::error::ParseError;
use crate::parse::id::Parser;
use crate::parse::id::parse_actor_id_with_parser;
use crate::parse::id::parse_id_with_parser;
use crate::parse::id::parse_port_id_with_parser;
use crate::parse::id::parse_proc_id_with_parser;
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

fn parse_location(parser: &mut Parser<'_>) -> Result<Location, ParseError> {
    let at = parser.expect_kind(TokenKind::At)?;
    let location = parser.rest();
    if location.is_empty() {
        return Err(ParseError::missing_location(at));
    }
    let span = parser.rest_span();
    let parsed = location
        .parse()
        .map_err(|err: anyhow::Error| ParseError::invalid_location(span, err.to_string()))?;
    parser.take_rest();
    parser.finish()?;
    Ok(parsed)
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
