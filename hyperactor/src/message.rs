/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module provides a framework for mutating serialized multipart messages
//! without deserializing the full message. This capability is useful when
//! sending messages to a remote destination through intermediate nodes, where
//! the intermediate nodes do not contain the message's type information.
//!
//! Briefly, it works by following these steps:
//!
//! 1. On the sender side, the typed message is serialized with multipart
//!    encoding and bundled in a [`MultipartMessage`] object.
//! 2. On intermediate nodes, the [`MultipartMessage`] object is relayed and
//!    selected typed parts are mutated in place.
//! 3. On the receiver side, the serialized message is delivered to the ordinary
//!    typed handler port and deserialized as the final message type.
//!
//! One main use case of this framework is to mutate the reply ports of a
//! multicast message, so the replies can be relayed through intermediate nodes,
//! rather than directly sent to the original sender. Therefore, a [Castable]
//! trait is defined, which collects requirements for message types using
//! multicast.

use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use typeuri::Named;

use crate::RemoteMessage;

/// This trait collects the necessary requirements for messages that are can be
/// cast.
pub trait Castable: RemoteMessage {}
impl<T: RemoteMessage> Castable for T {}

/// Multipart-serialized message with its message type erased.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, typeuri::Named)]
pub struct MultipartMessage {
    message: wirevalue::Any<wirevalue::encoding::Multipart>,
}
wirevalue::register_type!(MultipartMessage);

impl MultipartMessage {
    /// Create an object directly from a multipart [`wirevalue::Any`].
    pub fn new(message: wirevalue::Any<wirevalue::encoding::Multipart>) -> Self {
        Self { message }
    }

    /// Access the inner serialized message.
    pub fn message(&self) -> &wirevalue::Any<wirevalue::encoding::Multipart> {
        &self.message
    }

    /// Convert this wrapper into its inner serialized message.
    pub fn into_message(self) -> wirevalue::Any<wirevalue::encoding::Multipart> {
        self.message
    }

    /// Create an object from a typed message.
    // Note: cannot implement TryFrom<T> due to conflict with core crate's blanket impl.
    // More can be found in this issue: https://github.com/rust-lang/rust/issues/50133
    pub fn try_from_message<T: Serialize + Named>(msg: T) -> Result<Self, anyhow::Error> {
        let message = wirevalue::Any::<wirevalue::encoding::Multipart>::serialize(&msg)?;
        Ok(Self { message })
    }

    /// Use the provided function to update matching typed multipart parts.
    pub fn visit_mut<T: Serialize + DeserializeOwned + Named>(
        &mut self,
        f: impl FnMut(&mut T) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        Ok(self.message.visit_multipart_parts_mut(f)?)
    }

    /// Deserialize the contained message.
    pub fn deserialize<M: DeserializeOwned>(self) -> anyhow::Result<M> {
        self.downcast()
    }

    fn downcast<M: DeserializeOwned>(self) -> anyhow::Result<M> {
        Ok(self.message.deserialized_unchecked()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PortRef;
    use crate::PortRefRepr;
    use crate::accum::ReducerSpec;
    use crate::accum::StreamingReducerOpts;
    use crate::testing::ids::test_port_id;

    // Used to demonstrate a user defined reply type.
    #[derive(Debug, PartialEq, Serialize, Deserialize, typeuri::Named)]
    struct MyReply(String);

    // Used to demonstrate a two-way message type.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, typeuri::Named)]
    struct MyMessage {
        arg0: bool,
        arg1: u32,
        reply0: PortRef<String>,
        reply1: PortRef<MyReply>,
    }

    #[test]
    fn test_castable() {
        let original_port0 = PortRef::attest(test_port_id("world_0", "actor", 123));
        let original_port1 = PortRef::attest_reducible(
            test_port_id("world_1", "actor1", 456),
            Some(ReducerSpec {
                typehash: 123,
                builder_params: None,
            }),
            StreamingReducerOpts::default(),
        );
        let my_message = MyMessage {
            arg0: true,
            arg1: 42,
            reply0: original_port0.clone(),
            reply1: original_port1.clone(),
        };

        let serialized_multipart_my_message =
            wirevalue::Any::<wirevalue::encoding::Multipart>::serialize(&my_message).unwrap();

        let mut message = MultipartMessage::try_from_message(my_message.clone()).unwrap();
        assert_eq!(
            message,
            MultipartMessage {
                message: serialized_multipart_my_message,
            }
        );

        let new_port_id0 = test_port_id("world_0", "comm", 680);
        assert_ne!(&new_port_id0, original_port0.port_addr());
        let new_port_id1 = test_port_id("world_1", "comm", 257);
        assert_ne!(&new_port_id1, original_port1.port_addr());

        let mut new_ports = vec![&new_port_id0, &new_port_id1].into_iter();
        message
            .visit_mut::<PortRefRepr>(|b| {
                let port = new_ports.next().unwrap();
                b.update_port_addr(port.clone());
                Ok(())
            })
            .unwrap();

        let new_port0 = PortRef::<String>::attest(new_port_id0);
        let new_port1 = PortRef::<MyReply>::attest_reducible(
            new_port_id1,
            Some(ReducerSpec {
                typehash: 123,
                builder_params: None,
            }),
            StreamingReducerOpts::default(),
        );
        let new_my_message = message.downcast::<MyMessage>().unwrap();
        assert_eq!(
            new_my_message,
            MyMessage {
                arg0: true,
                arg1: 42,
                reply0: new_port0,
                reply1: new_port1,
            }
        );
    }
}
