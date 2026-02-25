/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use serde::Deserialize;
use serde::Serialize;

pub use crate as hyperactor;
use crate::HandleClient;
use crate::Handler;
use crate::ProcId;
use crate::RefClient;
use crate::mailbox::ChannelAddr;

/// Messages relating to mailbox administration.
#[derive(
    Handler,
    HandleClient,
    RefClient,
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    typeuri::Named
)]
pub enum MailboxAdminMessage {
    /// An address update.
    UpdateAddress {
        /// The ID of the proc.
        proc_id: ProcId,

        /// The address at which it listens.
        addr: ChannelAddr,
    },
}
