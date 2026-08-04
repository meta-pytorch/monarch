/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The seam between the command loop and a transport implementation.
//!
//! A transport's whole job is to bring up a connection's pipe and produce a
//! [`ConnectionTransport`](crate::connection::ConnectionTransport) for it, then
//! hand that to the command loop via `Command::TransportConnected`. Everything
//! about *establishment* — announcing our identity, learning the peer's,
//! delivering the hello, detecting death — lives in the command loop and is
//! identical across transports. A transport never reads actor state and never
//! constructs an `Establish`.

use crate::connection::ConnectionRef;
use crate::shm::ShmClientSlot;

pub(crate) trait Transport {
    /// Begin serving `connection` on `url` (this actor is the parent/server).
    /// `shm_client` is the owning actor's shared-memory client slot; only the unix
    /// transport uses it (to move large parts through the slab), others ignore it.
    fn serve(&mut self, url: String, connection: ConnectionRef, shm_client: ShmClientSlot);
    /// Begin joining `connection` on `url` (this actor is the child/client).
    fn join(&mut self, url: String, connection: ConnectionRef, shm_client: ShmClientSlot);
}
