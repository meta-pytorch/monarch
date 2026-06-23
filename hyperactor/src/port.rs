/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Port identifiers.

use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::id::Label;
use crate::id::Uid;

/// A port identifier within an actor.
#[derive(
    Clone,
    EnumAsInner,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
pub enum Port {
    /// Ephemeral ports, indexed starting at 0.
    Ephemeral(u64),
    /// Handler ports, indexed by uid. The label usually carries the type name.
    Handler(Uid),
    /// Control ports.
    Control(ControlPort),
}

/// Runtime-owned control ports.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
pub enum ControlPort {
    /// Actor introspection.
    Introspect,
    /// Actor lifecycle signals.
    Signal,
    /// Actor lifecycle status.
    Status,
}

/// Errors that can occur when parsing a [`ControlPort`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ControlPortParseError {
    /// The control port name is not known.
    #[error("unknown control port {0:?}")]
    Unknown(String),
}

/// Errors that can occur when parsing a [`Port`].
#[derive(Debug, thiserror::Error)]
pub enum PortParseError {
    /// The ephemeral port index is invalid.
    #[error("invalid ephemeral port: {0}")]
    InvalidEphemeral(#[from] ParseIntError),
    /// The handler port uid is invalid.
    #[error("invalid handler port: {0}")]
    InvalidHandler(#[from] crate::id::UidParseError),
}

impl Port {
    /// Create an ephemeral port.
    pub fn ephemeral(index: u64) -> Self {
        Self::Ephemeral(index)
    }

    /// Create a port for handler message type `M`.
    pub fn handler<M: Named>() -> Self {
        Self::handler_id(M::typehash(), Some(Label::strip(handler_label::<M>())))
    }

    /// Create a handler port from a type hash and optional display label.
    pub fn handler_id(typehash: u64, label: Option<Label>) -> Self {
        Self::Handler(Uid::Instance(typehash, label))
    }

    /// Create a control port.
    pub fn control(port: ControlPort) -> Self {
        Self::Control(port)
    }

    /// The ephemeral port index, if this is an ephemeral port.
    pub fn ephemeral_index(&self) -> Option<u64> {
        match self {
            Self::Ephemeral(index) => Some(*index),
            _ => None,
        }
    }

    /// A numeric representation for telemetry and raw protocol fields.
    pub fn as_u64(&self) -> u64 {
        match self {
            Self::Ephemeral(index) => *index,
            Self::Handler(uid) => uid.instance_value().unwrap_or_else(|| {
                panic!("handler port should be identified by an instance uid: {uid}")
            }),
            Self::Control(ControlPort::Introspect) => 0,
            Self::Control(ControlPort::Signal) => 1,
            Self::Control(ControlPort::Status) => 2,
        }
    }
}

impl From<u64> for Port {
    fn from(v: u64) -> Self {
        Port::Ephemeral(v)
    }
}

impl fmt::Display for Port {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ephemeral(index) => write!(f, "{index}"),
            Self::Handler(uid) => write!(f, "{uid}"),
            Self::Control(port) => write!(f, "{port}"),
        }
    }
}

impl fmt::Debug for Port {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ephemeral(index) => write!(f, "Port::Ephemeral({index})"),
            Self::Handler(uid) => write!(f, "Port::Handler({uid})"),
            Self::Control(port) => write!(f, "Port::Control({port:?})"),
        }
    }
}

impl FromStr for Port {
    type Err = PortParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.bytes().all(|ch| ch.is_ascii_digit()) {
            return Ok(Self::Ephemeral(s.parse()?));
        }
        let uid = s.parse()?;
        match uid {
            Uid::Instance(_, _) => Ok(Self::Handler(uid)),
            Uid::Singleton(_) => Err(PortParseError::InvalidHandler(
                crate::id::UidParseError::InvalidSyntax(s.to_string()),
            )),
        }
    }
}

impl fmt::Display for ControlPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Introspect => f.write_str("introspect"),
            Self::Signal => f.write_str("signal"),
            Self::Status => f.write_str("status"),
        }
    }
}

impl FromStr for ControlPort {
    type Err = ControlPortParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "introspect" => Ok(Self::Introspect),
            "signal" => Ok(Self::Signal),
            "status" => Ok(Self::Status),
            _ => Err(ControlPortParseError::Unknown(s.to_string())),
        }
    }
}

fn handler_label<M: Named>() -> &'static str {
    M::typename()
        .rsplit("::")
        .next()
        .unwrap_or_else(M::typename)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestMsg;

    impl Named for TestMsg {
        fn typename() -> &'static str {
            "test::TestMsg"
        }
    }

    #[test]
    fn test_handler_port() {
        let port = Port::handler::<TestMsg>();
        assert!(port.is_handler(), "handler ports should be explicit");
        assert_eq!(port.as_u64(), TestMsg::typehash());
    }

    #[test]
    fn test_ephemeral_port() {
        let port = Port::from(42);
        assert!(port.is_ephemeral(), "decimal ports should be ephemeral");
        assert!(!port.is_handler(), "ephemeral ports should not be handlers");
        assert_eq!(port.as_u64(), 42);
    }

    #[test]
    fn test_display_fromstr_roundtrip_ephemeral() {
        let port = Port::from(12345);
        assert_eq!(port.to_string(), "12345");
        let parsed: Port = port.to_string().parse().expect("parse port");
        assert_eq!(port, parsed);
    }

    #[test]
    fn test_display_fromstr_roundtrip_handler() {
        let port = Port::handler::<TestMsg>();
        let s = port.to_string();
        let parsed: Port = s.parse().expect("parse port");
        assert_eq!(port, parsed);
    }

    #[test]
    fn test_control_port_status() {
        assert_eq!(ControlPort::Status.to_string(), "status");
        assert_eq!(
            "status".parse::<ControlPort>().expect("parse status"),
            ControlPort::Status
        );
        assert_eq!(Port::control(ControlPort::Status).as_u64(), 2);
    }

    #[test]
    fn test_debug() {
        let port = Port::from(42);
        assert_eq!(format!("{:?}", port), "Port::Ephemeral(42)");
    }
}
