/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Application-local identity for an actor inside one Chrysalis
// process. Chrysalis routes to the process PID; our protocol routes
// locally by ActorId.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActorId {
    Echo,
    Counter,
}

impl ActorId {
    // Encode actor IDs as one byte in our envelope: 1 selects Echo
    // and 2 selects Counter. These values are conventions of our
    // protocol.
    const ECHO_WIRE_VALUE: u8 = 1;
    const COUNTER_WIRE_VALUE: u8 = 2;

    // Convert explicitly between ActorId and its wire byte, so adding
    // an actor requires updating both the encoder and decoder.

    const fn as_byte(self) -> u8 {
        match self {
            Self::Echo => Self::ECHO_WIRE_VALUE,
            Self::Counter => Self::COUNTER_WIRE_VALUE,
        }
    }

    const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            Self::ECHO_WIRE_VALUE => Some(Self::Echo),
            Self::COUNTER_WIRE_VALUE => Some(Self::Counter),
            _ => None,
        }
    }
}

// A decoded application message: actor selects the local recipient,
// while payload contains bytes meaningful only to that actor.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Envelope {
    actor: ActorId,
    payload: Vec<u8>,
}

impl Envelope {
    pub(crate) fn new(actor: ActorId, payload: &[u8]) -> Self {
        Self {
            actor,
            payload: payload.to_vec(),
        }
    }

    // Parse the first byte as an ActorId and retain all remaining
    // bytes as its payload; reject an empty envelope or an unknown
    // actor byte.
    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        let (&actor, payload) = bytes.split_first()?;
        Some(Self {
            actor: ActorId::from_byte(actor)?,
            payload: payload.to_vec(),
        })
    }

    pub(crate) fn actor(&self) -> ActorId {
        self.actor
    }

    pub(crate) fn payload(&self) -> &[u8] {
        &self.payload
    }

    // Serialize the envelope as one actor-ID byte followed by the
    // payload bytes unchanged.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(1 + self.payload.len());
        bytes.push(self.actor.as_byte());
        bytes.extend_from_slice(&self.payload);
        bytes
    }
}
