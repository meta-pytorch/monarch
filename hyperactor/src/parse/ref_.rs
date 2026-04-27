/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Recursive-descent parsing for Hyperactor refs.
//!
//! These parsers compose on top of the shared id parser and then treat the
//! location tail after `@` as opaque input for `Location::from_str`. That
//! keeps URL punctuation out of the Hyperactor lexer while still giving
//! token-aware errors at the id/ref boundary.

use crate::parse::error::ParseError;
use crate::parse::id::ActorIdParts;
use crate::parse::id::Parser;
use crate::parse::id::PortIdParts;
use crate::parse::id::ProcIdParts;
use crate::parse::id::parse_actor_id_parts;
use crate::parse::id::parse_id_component;
use crate::parse::id::parse_port_id_parts;
use crate::parse::id::parse_proc_id_parts;
use crate::parse::lex::Span;
use crate::parse::lex::TokenKind;

/// A parsed proc ref.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProcRefParts<'a> {
    /// The proc id.
    pub(crate) id: ProcIdParts<'a>,
    /// The raw location tail.
    pub(crate) location: &'a str,
}

/// A parsed actor ref.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ActorRefParts<'a> {
    /// The actor id.
    pub(crate) id: ActorIdParts<'a>,
    /// The raw location tail.
    pub(crate) location: &'a str,
}

/// A parsed port ref.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PortRefParts<'a> {
    /// The port id.
    pub(crate) id: PortIdParts<'a>,
    /// The raw location tail.
    pub(crate) location: &'a str,
}

/// A parsed polymorphic reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReferenceParts<'a> {
    /// A proc ref.
    Proc(ProcRefParts<'a>),
    /// An actor ref.
    Actor(ActorRefParts<'a>),
    /// A port ref.
    Port(PortRefParts<'a>),
}

pub(crate) fn parse_proc_ref(input: &str) -> Result<ProcRefParts<'_>, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_proc_id_parts(&mut parser)?;
    let location = parse_location(&mut parser)?;
    Ok(ProcRefParts { id, location })
}

pub(crate) fn parse_actor_ref(input: &str) -> Result<ActorRefParts<'_>, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_actor_id_parts(&mut parser)?;
    let location = parse_location(&mut parser)?;
    Ok(ActorRefParts { id, location })
}

pub(crate) fn parse_port_ref(input: &str) -> Result<PortRefParts<'_>, ParseError> {
    let mut parser = Parser::new(input);
    let id = parse_port_id_parts(&mut parser)?;
    let location = parse_location(&mut parser)?;
    Ok(PortRefParts { id, location })
}

pub(crate) fn parse_reference(input: &str) -> Result<ReferenceParts<'_>, ParseError> {
    let mut parser = Parser::new(input);
    let first = parse_id_component(&mut parser)?;
    match parser.peek().kind {
        TokenKind::At => Ok(ReferenceParts::Proc(ProcRefParts {
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
                TokenKind::At => Ok(ReferenceParts::Actor(ActorRefParts {
                    id: actor,
                    location: parse_location(&mut parser)?,
                })),
                TokenKind::Colon => {
                    parser.bump();
                    let port = parser.expect_text("decimal port")?;
                    if !port.text.bytes().all(|ch| ch.is_ascii_digit()) {
                        return Err(ParseError::invalid_port(port));
                    }
                    Ok(ReferenceParts::Port(PortRefParts {
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

    #[test]
    fn test_parse_proc_ref() {
        assert_eq!(
            parse_proc_ref("local@inproc://0").unwrap(),
            ProcRefParts {
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
    fn test_parse_actor_ref() {
        assert_eq!(
            parse_actor_ref("controller.local@tcp://[::1]:2345").unwrap(),
            ActorRefParts {
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
    fn test_parse_port_ref() {
        assert_eq!(
            parse_port_ref("controller.local:7@inproc://0").unwrap(),
            PortRefParts {
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
    fn test_parse_reference_specificity() {
        assert!(matches!(
            parse_reference("local@inproc://0").unwrap(),
            ReferenceParts::Proc(_)
        ));
        assert!(matches!(
            parse_reference("local.local@inproc://0").unwrap(),
            ReferenceParts::Actor(_)
        ));
        assert!(matches!(
            parse_reference("local.local:7@inproc://0").unwrap(),
            ReferenceParts::Port(_)
        ));
    }

    #[test]
    fn test_parse_reference_reports_missing_separator() {
        let err = parse_reference("local").unwrap_err();
        assert_eq!(
            err.to_string(),
            "expected \"@\" or \".\", found end of input"
        );
        assert_eq!(err.span, Span::new(5, 5));
    }

    #[test]
    fn test_parse_reference_reports_missing_location() {
        let err = parse_reference("local@").unwrap_err();
        assert_eq!(err.to_string(), "expected location, found end of input");
        assert_eq!(err.span, Span::new(5, 6));
    }

    #[test]
    fn test_parse_reference_does_not_downcast_malformed_port_ref() {
        let err = parse_reference("local.local:not-a-port@inproc://0").unwrap_err();
        assert_eq!(err.to_string(), "invalid port \"not-a-port\"");
        assert_eq!(err.span, Span::new(12, 22));
    }

    #[test]
    fn test_parse_reference_does_not_downcast_malformed_actor_ref() {
        let err = parse_reference("local.@inproc://0").unwrap_err();
        assert_eq!(err.to_string(), "expected \"label\" or \"<\", found \"@\"");
        assert_eq!(err.span, Span::new(6, 7));
    }
}
