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

use crate::parse::error::ParseError;
use crate::parse::id::ActorIdParts;
use crate::parse::id::Parser;
use crate::parse::id::PortIdParts;
use crate::parse::id::ProcIdParts;
use crate::parse::id::parse_actor_id_parts;
use crate::parse::id::parse_id_component;
use crate::parse::id::parse_port_id_parts;
use crate::parse::id::parse_proc_id_parts;
use crate::parse::lex::TokenKind;

/// A parsed proc address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProcAddrParts<'a> {
    /// The proc id.
    pub(crate) id: ProcIdParts<'a>,
    /// The raw location tail.
    pub(crate) location: &'a str,
}

/// A parsed actor address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ActorAddrParts<'a> {
    /// The actor id.
    pub(crate) id: ActorIdParts<'a>,
    /// The raw location tail.
    pub(crate) location: &'a str,
}

/// A parsed port address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PortAddrParts<'a> {
    /// The port id.
    pub(crate) id: PortIdParts<'a>,
    /// The raw location tail.
    pub(crate) location: &'a str,
}

/// A parsed polymorphic address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AddressParts<'a> {
    /// A proc address.
    Proc(ProcAddrParts<'a>),
    /// An actor address.
    Actor(ActorAddrParts<'a>),
    /// A port address.
    Port(PortAddrParts<'a>),
}

pub(crate) fn parse_proc_addr(input: &str) -> Result<ProcAddrParts<'_>, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_proc_id_parts(&mut parser)?;
    let location = parse_location(&mut parser)?;
    Ok(ProcAddrParts { id, location })
}

pub(crate) fn parse_actor_addr(input: &str) -> Result<ActorAddrParts<'_>, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_actor_id_parts(&mut parser)?;
    let location = parse_location(&mut parser)?;
    Ok(ActorAddrParts { id, location })
}

pub(crate) fn parse_port_addr(input: &str) -> Result<PortAddrParts<'_>, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_port_id_parts(&mut parser)?;
    let location = parse_location(&mut parser)?;
    Ok(PortAddrParts { id, location })
}

pub(crate) fn parse_address(input: &str) -> Result<AddressParts<'_>, ParseError> {
    let mut parser = Parser::new(input);
    let first = parse_id_component(&mut parser)?;
    match parser.peek().kind {
        TokenKind::At => Ok(AddressParts::Proc(ProcAddrParts {
            id: ProcIdParts { component: first },
            location: parse_location(&mut parser)?,
        })),
        TokenKind::Dot => {
            parser.bump();
            let proc_ = parse_id_component(&mut parser)?;
            let actor = ActorIdParts {
                actor: first,
                proc_,
            };
            match parser.peek().kind {
                TokenKind::At => Ok(AddressParts::Actor(ActorAddrParts {
                    id: actor,
                    location: parse_location(&mut parser)?,
                })),
                TokenKind::Colon => {
                    parser.bump();
                    let port = parser.expect_text("decimal port")?;
                    if !port.text.bytes().all(|ch| ch.is_ascii_digit()) {
                        return Err(ParseError::invalid_port(port));
                    }
                    Ok(AddressParts::Port(PortAddrParts {
                        id: PortIdParts {
                            actor,
                            port: port.text,
                        },
                        location: parse_location(&mut parser)?,
                    }))
                }
                _ => Err(ParseError::expected(parser.peek(), "\"@\" or \":\"")),
            }
        }
        _ => Err(ParseError::expected(parser.peek(), "\"@\" or \".\"")),
    }
}

fn parse_location<'a>(parser: &mut Parser<'a>) -> Result<&'a str, ParseError> {
    let at = parser.expect_kind(TokenKind::At)?;
    let location = parser.rest();
    if location.is_empty() {
        return Err(ParseError::missing_location(at));
    }
    let location = parser.take_rest();
    parser.finish()?;
    Ok(location)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::id::IdComponent;
    use crate::parse::lex::Span;

    #[test]
    fn test_parse_proc_addr() {
        assert_eq!(
            parse_proc_addr("local@inproc://0").unwrap(),
            ProcAddrParts {
                id: ProcIdParts {
                    component: IdComponent::Singleton {
                        label: "local",
                        span: Span::new(0, 5),
                    },
                },
                location: "inproc://0",
            }
        );
    }

    #[test]
    fn test_parse_actor_addr() {
        assert_eq!(
            parse_actor_addr("controller.local@tcp://[::1]:2345").unwrap(),
            ActorAddrParts {
                id: ActorIdParts {
                    actor: IdComponent::Singleton {
                        label: "controller",
                        span: Span::new(0, 10),
                    },
                    proc_: IdComponent::Singleton {
                        label: "local",
                        span: Span::new(11, 16),
                    },
                },
                location: "tcp://[::1]:2345",
            }
        );
    }

    #[test]
    fn test_parse_port_addr() {
        assert_eq!(
            parse_port_addr("controller.local:7@inproc://0").unwrap(),
            PortAddrParts {
                id: PortIdParts {
                    actor: ActorIdParts {
                        actor: IdComponent::Singleton {
                            label: "controller",
                            span: Span::new(0, 10),
                        },
                        proc_: IdComponent::Singleton {
                            label: "local",
                            span: Span::new(11, 16),
                        },
                    },
                    port: "7",
                },
                location: "inproc://0",
            }
        );
    }

    #[test]
    fn test_parse_address_specificity() {
        assert!(matches!(
            parse_address("local@inproc://0").unwrap(),
            AddressParts::Proc(_)
        ));
        assert!(matches!(
            parse_address("local.local@inproc://0").unwrap(),
            AddressParts::Actor(_)
        ));
        assert!(matches!(
            parse_address("local.local:7@inproc://0").unwrap(),
            AddressParts::Port(_)
        ));
    }

    #[test]
    fn test_parse_address_reports_missing_separator() {
        let err = parse_address("local").unwrap_err();
        assert_eq!(
            err.to_string(),
            "expected \"@\" or \".\", found end of input"
        );
        assert_eq!(err.span, Span::new(5, 5));
    }

    #[test]
    fn test_parse_address_reports_missing_location() {
        let err = parse_address("local@").unwrap_err();
        assert_eq!(err.to_string(), "expected location, found end of input");
        assert_eq!(err.span, Span::new(5, 6));
    }

    #[test]
    fn test_parse_address_does_not_downcast_malformed_port_addr() {
        let err = parse_address("local.local:not-a-port@inproc://0").unwrap_err();
        assert_eq!(err.to_string(), "invalid port \"not-a-port\"");
        assert_eq!(err.span, Span::new(12, 22));
    }

    #[test]
    fn test_parse_address_does_not_downcast_malformed_actor_addr() {
        let err = parse_address("local.@inproc://0").unwrap_err();
        assert_eq!(err.to_string(), "expected \"label\" or \"<\", found \"@\"");
        assert_eq!(err.span, Span::new(6, 7));
    }
}
