/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Recursive-descent parsing for Hyperactor ids.
//!
//! This module parses only the id grammar. It does not validate labels,
//! base58 uid text, or decimal ports beyond the structural checks that are
//! needed to distinguish grammar productions. Semantic validation happens when
//! the parsed parts are converted into concrete `id` types.

use crate::parse::error::ParseError;
use crate::parse::lex::Lexer;
use crate::parse::lex::Span;
use crate::parse::lex::Token;
use crate::parse::lex::TokenKind;

/// A parsed id component.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IdComponent<'a> {
    /// A singleton label.
    Singleton { label: &'a str, span: Span },
    /// An instance uid, optionally labeled.
    Instance {
        label: Option<&'a str>,
        uid: &'a str,
        span: Span,
    },
}

/// A parsed proc id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProcIdParts<'a> {
    /// The proc id component.
    pub(crate) component: IdComponent<'a>,
}

/// A parsed actor id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ActorIdParts<'a> {
    /// The actor id component.
    pub(crate) actor: IdComponent<'a>,
    /// The process id component.
    pub(crate) proc_: IdComponent<'a>,
}

/// A parsed port id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PortIdParts<'a> {
    /// The parsed actor id.
    pub(crate) actor: ActorIdParts<'a>,
    /// The raw decimal port text.
    pub(crate) port: &'a str,
}

pub(crate) fn parse_proc_id(input: &str) -> Result<ProcIdParts<'_>, ParseError> {
    let mut parser = Parser::new(input);
    let parts = parse_proc_id_parts(&mut parser)?;
    parser.finish()?;
    Ok(parts)
}

pub(crate) fn parse_actor_id(input: &str) -> Result<ActorIdParts<'_>, ParseError> {
    let mut parser = Parser::new(input);
    let parts = parse_actor_id_parts(&mut parser)?;
    parser.finish()?;
    Ok(parts)
}

pub(crate) fn parse_port_id(input: &str) -> Result<PortIdParts<'_>, ParseError> {
    let mut parser = Parser::new(input);
    let parts = parse_port_id_parts(&mut parser)?;
    parser.finish()?;
    Ok(parts)
}

pub(crate) fn parse_proc_id_parts<'a>(
    parser: &mut Parser<'a>,
) -> Result<ProcIdParts<'a>, ParseError> {
    Ok(ProcIdParts {
        component: parse_id_component(parser)?,
    })
}

pub(crate) fn parse_actor_id_parts<'a>(
    parser: &mut Parser<'a>,
) -> Result<ActorIdParts<'a>, ParseError> {
    let actor = parse_id_component(parser)?;
    parser.expect_kind(TokenKind::Dot)?;
    let proc_ = parse_id_component(parser)?;
    Ok(ActorIdParts { actor, proc_ })
}

pub(crate) fn parse_port_id_parts<'a>(
    parser: &mut Parser<'a>,
) -> Result<PortIdParts<'a>, ParseError> {
    let actor = parse_actor_id_parts(parser)?;
    parser.expect_kind(TokenKind::Colon)?;
    let port = parser.expect_text("decimal port")?;
    if !port.text.bytes().all(|ch| ch.is_ascii_digit()) {
        return Err(ParseError::invalid_port(port));
    }
    Ok(PortIdParts {
        actor,
        port: port.text,
    })
}

pub(crate) fn parse_id_component<'a>(
    parser: &mut Parser<'a>,
) -> Result<IdComponent<'a>, ParseError> {
    match parser.peek().kind {
        TokenKind::Text => {
            let label = parser.bump();
            if parser.peek().kind == TokenKind::LessThan {
                parser.bump();
                let uid = parser.expect_text("uid text")?;
                let end = parser.expect_kind(TokenKind::GreaterThan)?;
                Ok(IdComponent::Instance {
                    label: Some(label.text),
                    uid: uid.text,
                    span: Span::new(label.span.start, end.span.end),
                })
            } else {
                Ok(IdComponent::Singleton {
                    label: label.text,
                    span: label.span,
                })
            }
        }
        TokenKind::LessThan => {
            let start = parser.bump();
            let uid = parser.expect_text("uid text")?;
            let end = parser.expect_kind(TokenKind::GreaterThan)?;
            Ok(IdComponent::Instance {
                label: None,
                uid: uid.text,
                span: Span::new(start.span.start, end.span.end),
            })
        }
        _ => Err(ParseError::expected(parser.bump(), "\"label\" or \"<\"")),
    }
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

    pub(crate) fn take_rest(&mut self) -> &'a str {
        let rest = self.rest();
        self.lookahead = Token::eof(self.input.len());
        rest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_proc_id_forms() {
        assert_eq!(
            parse_proc_id("local").unwrap(),
            ProcIdParts {
                component: IdComponent::Singleton {
                    label: "local",
                    span: Span::new(0, 5),
                },
            }
        );
        assert_eq!(
            parse_proc_id("controller<abc>").unwrap(),
            ProcIdParts {
                component: IdComponent::Instance {
                    label: Some("controller"),
                    uid: "abc",
                    span: Span::new(0, 15),
                },
            }
        );
        assert_eq!(
            parse_proc_id("<abc>").unwrap(),
            ProcIdParts {
                component: IdComponent::Instance {
                    label: None,
                    uid: "abc",
                    span: Span::new(0, 5),
                },
            }
        );
    }

    #[test]
    fn test_parse_actor_id_forms() {
        assert_eq!(
            parse_actor_id("controller.local").unwrap(),
            ActorIdParts {
                actor: IdComponent::Singleton {
                    label: "controller",
                    span: Span::new(0, 10),
                },
                proc_: IdComponent::Singleton {
                    label: "local",
                    span: Span::new(11, 16),
                },
            }
        );
        assert_eq!(
            parse_actor_id("controller<abc>.local").unwrap(),
            ActorIdParts {
                actor: IdComponent::Instance {
                    label: Some("controller"),
                    uid: "abc",
                    span: Span::new(0, 15),
                },
                proc_: IdComponent::Singleton {
                    label: "local",
                    span: Span::new(16, 21),
                },
            }
        );
    }

    #[test]
    fn test_parse_port_id_form() {
        assert_eq!(
            parse_port_id("controller.local:7").unwrap(),
            PortIdParts {
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
            }
        );
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
        assert_eq!(err.to_string(), "expected decimal port, found \"@\"");
        assert_eq!(err.span, Span::new(17, 18));
    }

    #[test]
    fn test_parser_rest_tracks_location_boundary() {
        let mut parser = Parser::new("controller.local@inproc://0");
        let actor = parse_actor_id_parts(&mut parser).unwrap();
        assert_eq!(
            actor,
            ActorIdParts {
                actor: IdComponent::Singleton {
                    label: "controller",
                    span: Span::new(0, 10),
                },
                proc_: IdComponent::Singleton {
                    label: "local",
                    span: Span::new(11, 16),
                },
            }
        );
        assert_eq!(parser.rest(), "@inproc://0");
    }
}
