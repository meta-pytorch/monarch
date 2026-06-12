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
//!    encoding and bundled in a `wirevalue::Any<wirevalue::encoding::Multipart>`.
//! 2. On intermediate nodes, the serialized message is relayed and selected
//!    typed parts are mutated in place.
//! 3. On the receiver side, the serialized message is delivered to the ordinary
//!    typed handler port and deserialized as the final message type.
//!
//! One main use case of this framework is to mutate the reply ports of a
//! multicast message, so the replies can be relayed through intermediate nodes,
//! rather than directly sent to the original sender. Therefore, a [Castable]
//! trait is defined, which collects requirements for message types using
//! multicast.

use crate::RemoteMessage;

/// This trait collects the necessary requirements for messages that are can be
/// cast.
pub trait Castable: RemoteMessage {}
impl<T: RemoteMessage> Castable for T {}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde::Serialize;

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

        let mut message =
            wirevalue::Any::<wirevalue::encoding::Multipart>::serialize(&my_message).unwrap();
        assert_eq!(message, serialized_multipart_my_message);

        let new_port_id0 = test_port_id("world_0", "comm", 680);
        assert_ne!(&new_port_id0, original_port0.port_addr());
        let new_port_id1 = test_port_id("world_1", "comm", 257);
        assert_ne!(&new_port_id1, original_port1.port_addr());

        let mut new_ports = vec![&new_port_id0, &new_port_id1].into_iter();
        message
            .visit_multipart_parts_mut::<PortRefRepr, anyhow::Error>(|b| {
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
        let new_my_message = message.deserialized_unchecked::<MyMessage>().unwrap();
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
