/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Runtime-neutral quiche transport foundations for Chrysalis.
//!
//! Endpoint-issued source CIDs use Chrysalis's 20-byte `[PID | connection key]` layout. quiche's
//! current public client constructor chooses the first Initial DCID internally, so that first
//! datagram still requires a directly resolved packet address. Once the server responds with its
//! routed source CID, both directions are PID-routable. Supplying the Initial DCID is the remaining
//! quiche API extension needed for stateless forwarding from the first datagram.

mod admission;
mod buffer;
mod command;
mod error;
mod identity;
mod io;

pub use command::EndpointCommand;
pub use command::EndpointCommands;
pub use error::Error;
pub use identity::EndpointIdentity;
pub use identity::certificate_pid;
pub use io::PacketIo;
pub use io::PacketSendSlot;
