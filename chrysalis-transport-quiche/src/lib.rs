/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Runtime-neutral quiche state-machine driver for Chrysalis.
//!
//! [`Endpoint`] multiplexes quiche connections over one packet engine. It consumes bounded stream
//! submissions from `chrysalis-transport-core`, generates packets directly into stable GSO slots,
//! and publishes operation completions without depending on Tokio or Rust async.
//!
//! The endpoint drains accepted operation ownership during shutdown. Runtime adapters can
//! therefore separate cancellation from joining the driver without losing caller buffers.
//!
//! Endpoint-issued source CIDs use Chrysalis's 20-byte `[PID | connection key]` layout. quiche's
//! Chrysalis overlay accepts an application-selected Initial DCID, so stateless forwarding can
//! route the first datagram by its destination PID.

mod admission;
mod buffer;
mod command;
mod driver;
mod error;
mod identity;
mod io;

pub use command::EndpointCommand;
pub use command::EndpointCommands;
pub use driver::ConnectionStats;
pub use driver::ConnectionStatsHandle;
pub use driver::Endpoint;
pub use driver::EndpointHandle;
pub use driver::EndpointLimits;
pub use driver::EndpointStats;
pub use driver::EndpointStatsHandle;
pub use driver::ShutdownState;
pub use error::Error;
pub use identity::EndpointIdentity;
pub use identity::certificate_pid;
pub use io::PacketIo;
pub use io::PacketSendSlot;
