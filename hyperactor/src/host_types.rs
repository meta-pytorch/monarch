/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Types shared across host-related modules.
//!
//! This module centralizes supporting types and errors used by
//! [`Host`](crate::host::Host) and its
//! [`ProcManager`](crate::proc_manager::ProcManager) implementations.
//!
//! Currently includes:
//! - [`HostError`] — the error type for host and proc manager
//!   operations.
//!
//! Over time, additional host-scoped types (proc status, handles,
//! etc.) may live here as the process manager lifecycle evolves.

use crate::ProcId;
use crate::channel::ChannelError;

/// The type of error produced by host operations.
#[derive(Debug, thiserror::Error)]
pub enum HostError {
    /// A channel error occurred during a host operation.
    #[error(transparent)]
    ChannelError(#[from] ChannelError),

    /// The named proc already exists and cannot be spawned.
    #[error("proc '{0}' already exists")]
    ProcExists(String),

    /// Failures occuring while spawning a subprocess.
    #[error("proc '{0}' failed to spawn process: {1}")]
    ProcessSpawnFailure(ProcId, #[source] std::io::Error),

    /// Failures occuring while spawning a management actor in a proc.
    #[error("failed to spawn agent on proc '{0}': {1}")]
    AgentSpawnFailure(ProcId, #[source] anyhow::Error),

    /// An input parameter was missing.
    #[error("parameter '{0}' missing: {1}")]
    MissingParameter(String, std::env::VarError),

    /// An input parameter was invalid.
    #[error("parameter '{0}' invalid: {1}")]
    InvalidParameter(String, anyhow::Error),
}
