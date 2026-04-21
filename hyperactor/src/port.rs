/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Port identifier newtype.

use std::fmt;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;
use typeuri::ACTOR_PORT_BIT;
use typeuri::Named;

/// A port identifier within an actor.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
pub struct Port(u64);

impl Port {
    /// Create a port for handler message type `M`.
    pub fn handler<M: Named>() -> Self {
        Port(M::port())
    }

    /// Whether this is a handler port (actor port bit set).
    pub fn is_handler(&self) -> bool {
        self.0 & ACTOR_PORT_BIT != 0
    }

    /// The raw port index.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Port {
    fn from(v: u64) -> Self {
        Port(v)
    }
}

impl fmt::Display for Port {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for Port {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Port({})", self.0)
    }
}

impl FromStr for Port {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v: u64 = s.parse()?;
        Ok(Port(v))
    }
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
        assert!(
            port.is_handler(),
            "handler ports should have ACTOR_PORT_BIT set"
        );
        assert_eq!(port.as_u64(), TestMsg::port());
    }

    #[test]
    fn test_non_handler_port() {
        let port = Port::from(42);
        assert!(
            !port.is_handler(),
            "plain ports should not have ACTOR_PORT_BIT set"
        );
        assert_eq!(port.as_u64(), 42);
    }

    #[test]
    fn test_display_fromstr_roundtrip() {
        let port = Port::from(12345);
        assert_eq!(port.to_string(), "12345");
        let parsed: Port = port.to_string().parse().unwrap();
        assert_eq!(port, parsed);
    }

    #[test]
    fn test_display_fromstr_roundtrip_handler() {
        let port = Port::handler::<TestMsg>();
        let s = port.to_string();
        let parsed: Port = s.parse().unwrap();
        assert_eq!(port, parsed);
    }

    #[test]
    fn test_debug() {
        let port = Port::from(42);
        assert_eq!(format!("{:?}", port), "Port(42)");
    }
}
