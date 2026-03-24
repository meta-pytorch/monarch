/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Config inspection messages for remote per-proc configuration dumps.
//!
//! See CFG-* invariants in `admin_tui/main.rs`.

use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::RefClient;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

/// Result of a config dump request — the effective configuration entries
/// from the target process.
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub struct ConfigDumpResult {
    pub entries: Vec<hyperactor_config::global::ConfigEntry>,
}
wirevalue::register_type!(ConfigDumpResult);

/// Request a config dump from a proc's process-global config state.
///
/// Sent to ProcAgent (worker procs) or HostAgent (service proc) by the
/// admin HTTP bridge. The handler calls `hyperactor_config::global::config_entries()`
/// and replies with the snapshot.
#[derive(Debug, Serialize, Deserialize, Named, Handler, HandleClient, RefClient)]
pub struct ConfigDump {
    #[reply]
    pub result: hyperactor::reference::OncePortRef<ConfigDumpResult>,
}
wirevalue::register_type!(ConfigDump);
