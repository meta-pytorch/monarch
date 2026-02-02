/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Proc launching abstraction.
//!
//! This module defines a small strategy interface, [`ProcLauncher`],
//! used by
//! [`BootstrapProcManager`](crate::bootstrap::BootstrapProcManager)
//! to start and stop procs while keeping lifecycle tracking
//! centralized in the manager.
//!
//! A launcher is responsible for the *mechanics* of running a proc
//! (native OS process, container, VM, etc.) and for reporting
//! *terminal status* back to the manager.
//!
//! ## Key properties
//!
//! - **Readiness is callback-driven.** A proc is considered *ready*
//!   only when it signals readiness via the existing bootstrap
//!   callback (`callback_addr`) in the [`Bootstrap`] payload.
//!   Launchers do not determine readiness.
//! - **Terminal status is channel-driven.** `launch` returns a
//!   [`LaunchResult`] whose `exit_rx` resolves exactly once with a
//!   [`ProcExitResult`]. This channel is the single source of truth
//!   for how a proc ended.
//! - **Termination is initiation-only.** [`ProcLauncher::terminate`]
//!   and [`ProcLauncher::kill`] initiate shutdown and return without
//!   waiting for the proc to exit; callers observe completion by
//!   awaiting `exit_rx` (or a higher level handle built on it).
//!
//! ## Stdio handling
//!
//! [`StdioHandling`] describes whether stdout/stderr are made
//! available to the manager for forwarding and tail collection
//! (`Captured`), inherited (`Inherited`), or handled entirely by the
//! launcher (`ManagedByLauncher`).
#![allow(dead_code, unused_imports)] // Temporary

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::ProcId;
use hyperactor::channel::ChannelAddr;
use tokio::process::ChildStderr;
use tokio::process::ChildStdout;
use tokio::sync::oneshot;

use crate::bootstrap::BootstrapCommand;

mod native;
pub(crate) use native::NativeProcLauncher;

mod systemd;
pub(crate) use systemd::SystemdProcLauncher;

/// Result of launching a proc.
///
/// The launcher arranges for terminal status to be delivered on
/// `exit_rx`. `exit_rx` is the single source of truth for terminal
/// status.
#[derive(Debug)]
pub(crate) struct LaunchResult {
    /// OS process ID if known (`None` for containers/VMs without
    /// visible PID).
    pub pid: Option<u32>,
    /// Captured immediately after spawn succeeds.
    pub started_at: std::time::SystemTime,
    /// How stdio is handled.
    pub stdio: StdioHandling,
    /// Fires exactly once with the proc terminal result.
    pub exit_rx: oneshot::Receiver<ProcExitResult>,
}

/// How proc stdio is handled.
pub(crate) enum StdioHandling {
    /// Pipes provided; manager can attach StreamFwder / tail
    /// collection.
    Captured {
        stdout: ChildStdout,
        stderr: ChildStderr,
    },
    /// Inherited from parent process (no capture).
    Inherited,
    /// Launcher manages logs internally (e.g. Docker log streaming).
    /// Spawner provides stderr_tail in ProcExitResult. Manager should
    /// not attach StreamFwder.
    ManagedByLauncher,
}

impl fmt::Debug for StdioHandling {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StdioHandling::Captured { .. } => f.write_str("Captured"),
            StdioHandling::Inherited => f.write_str("Inherited"),
            StdioHandling::ManagedByLauncher => f.write_str("ManagedByLauncher"),
        }
    }
}

/// How a proc terminated.
#[derive(Debug, Clone)]
pub(crate) enum ProcExitKind {
    /// Normal exit with code.
    Exited { code: i32 },
    /// Killed by signal.
    Signaled { signal: i32, core_dumped: bool },
    /// Launcher level failure (spawn/wait/plumbing failed).
    Failed { reason: String },
}

/// Terminal status of a proc.
#[derive(Debug, Clone)]
pub(crate) struct ProcExitResult {
    /// How the proc terminated.
    pub kind: ProcExitKind,
    /// Tail of stderr output if available.
    pub stderr_tail: Option<Vec<String>>,
}

/// Errors produced by proc launching / termination backends.
///
/// For now, this is intentionally lightweight:
/// - `Launch` preserves an `io::Error` source chain (useful
///   immediately).
/// - `Terminate` / `Kill` carry a string for now, since we haven't
///   committed to a concrete error taxonomy (e.g. nix vs actor RPC vs
///   container APIs).
/// - `Other` is a catch-all for plumbing / unexpected failures.
///
/// As additional launchers are introduced (e.g. actor-based
/// spawning), we can refine this into more structured variants
/// without changing the trait shape.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProcLauncherError {
    /// Failure while launching a proc (e.g. command spawn failure).
    #[error("launch failed: {0}")]
    Launch(#[source] std::io::Error),
    /// Failure while initiating graceful termination.
    #[error("terminate failed: {0}")]
    Terminate(String),
    /// Failure while initiating an immediate kill.
    #[error("kill failed: {0}")]
    Kill(String),
    /// Miscellaneous launcher failure not captured by the other
    /// variants.
    ///
    /// Useful for "shouldn't happen" plumbing errors or
    /// backend-specific cases we haven't modeled yet.
    #[error("launcher error: {0}")]
    Other(String),
}

/// Per-launch policy computed by the manager and handed to the
/// launcher.
///
/// Motivation: different launch backends (native OS process, docker,
/// python/actor protocol) need different *mechanics*, but they should
/// all receive the same *intent* from the manager:
/// - do we need a stdio stream we can forward / tail?
/// - if so, how many tail lines should be retained for diagnostics?
/// - if forwarding over the mesh is enabled, what channel address
///   should be used (if applicable)?

#[derive(Debug, Clone)]
pub(crate) struct LaunchOptions {
    /// Serialized Bootstrap payload for
    /// HYPERACTOR_MESH_BOOTSTRAP_MODE env var.
    pub bootstrap_payload: String,

    /// Human-readable process name for HYPERACTOR_PROCESS_NAME env
    /// var.
    pub process_name: String,

    /// The bootstrap command describing what should be executed for a
    /// proc.
    ///
    /// This is the *payload* of what to run; the launcher decides
    /// *how* to run it:
    /// - Native launcher: execs it directly (spawns the command).
    /// - systemd/container/VM/actor launchers: may treat it as the
    ///   command to run *inside* the launched environment.
    ///
    /// Used by backends to construct the actual invocation.
    pub command: BootstrapCommand,

    /// If true, the manager wants access to a stream (pipes or an
    /// equivalent).
    ///
    /// Native: pipe stdout/stderr (`Stdio::piped()`), return
    /// `StdioHandling::Captured`. Docker/Python: may implement as log
    /// streaming, RPC, etc., or ignore and return
    /// `StdioHandling::ManagedByLauncher` / `Inherited` depending on
    /// capability.
    pub want_stdio: bool,

    /// Max number of stderr (and/or stdout) lines retained for
    /// diagnostics/tailing.
    ///
    /// Manager uses this when attaching `StreamFwder` for native
    /// `Captured` pipes. Backends that manage logs internally may use
    /// it to decide how much to retain before reporting `stderr_tail`
    /// in `ProcExitResult`.
    ///
    /// `0` means "no tail buffering requested".
    pub tail_lines: usize,

    /// Optional "forward logs over mesh" address.
    ///
    /// This is **provisioned by the manager** when log-forwarding is
    /// enabled.
    /// - `None` means "no mesh log forwarding requested".
    /// - `Some(addr)` means "arrange for child logs to be forwarded
    ///   to `addr`" (if the backend supports it).
    ///
    /// Native backend: sets `BOOTSTRAP_LOG_CHANNEL=addr` in the child
    /// env when `Some`. Other backends may ignore it or use it to
    /// wire their own forwarding mechanism.
    pub log_channel: Option<ChannelAddr>,
}

/// Format a human-readable process name for diagnostics and logs.
///
/// Used by launchers to set `HYPERACTOR_PROCESS_NAME` environment
/// variable.
///
/// This string is intended for operators (not for stable identity).
/// We populate [`PROCESS_NAME_ENV`] with it so the launched proc can
/// include a friendly identifier in logs, crash reports, etc.
///
/// Format:
/// - `ProcId::Direct(_, name)` → `proc <name> @ <hostname>`
/// - `ProcId::Ranked(world, rank)` → `proc <world>[<rank>] @ <hostname>`
///
/// Notes:
/// - We best-effort resolve the local hostname; on failure or
///   non-UTF8 we fall back to `"unknown_host"`.
/// - This is **not** guaranteed to be unique and should not be parsed
///   for program logic.
pub(crate) fn format_process_name(proc_id: &ProcId) -> String {
    let who = match proc_id {
        ProcId::Direct(_, name) => name.clone(),
        ProcId::Ranked(world_id, rank) => format!("{world_id}[{rank}]"),
    };

    let host = hostname::get()
        .unwrap_or_else(|_| "unknown_host".into())
        .into_string()
        .unwrap_or("unknown_host".to_string());

    format!("proc {} @ {}", who, host)
}

/// Strategy interface for launching and stopping a proc.
///
/// This trait is internal to `hyperactor_mesh`:
/// `BootstrapProcManager` uses it to delegate the mechanics of
/// starting and stopping a proc while keeping lifecycle tracking
/// (readiness and terminal status) centralized in the manager.
///
/// Contract:
/// - **Readiness is determined by the bootstrap callback mechanism
///   (`callback_addr`)**, not by this trait. A proc is considered
///   ready only when it invokes the callback, regardless of its
///   underlying execution state.
/// - **Terminal status is sourced from `LaunchResult::exit_rx`**:
///   `launch` must return an `exit_rx` that resolves exactly once
///   with the proc's terminal outcome. Implementations must ensure
///   `exit_rx` resolves even if setup fails after partial work (for
///   example, if spawning succeeds but exit monitoring cannot be
///   established).
/// - `terminate` and `kill` initiate shutdown and return without
///   waiting for exit. Callers observe completion by awaiting
///   `exit_rx` (or a higher-level handle built on it).
///
/// **Process tree semantics:** A launched proc may involve wrapper
/// processes (shell, runtime shims, sanitizers) and/or spawn
/// descendants. Launchers should treat termination as applying to the
/// entire launched process tree (e.g., by placing the child in its
/// own process group and signaling the group). Callers must not
/// assume the returned PID is the only process that needs
/// terminating.
#[async_trait]
pub(crate) trait ProcLauncher: Send + Sync + 'static {
    /// Launch a proc using the provided bootstrap payload and config.
    ///
    /// Implementations must:
    /// - Start the underlying proc/container/VM.
    /// - Arrange for `LaunchResult::exit_rx` to resolve exactly once
    ///   with the terminal outcome.
    /// - Return quickly after starting the proc (readiness is handled
    ///   elsewhere).
    ///
    /// `opts` communicates *policy/intent* computed by the manager.
    /// The launcher uses it to choose backend-specific mechanics
    /// (pipes vs inherit, log streaming, etc.).
    async fn launch(
        &self,
        proc_id: &ProcId,
        opts: LaunchOptions,
    ) -> Result<LaunchResult, ProcLauncherError>;

    /// Initiate graceful termination.
    ///
    /// Semantics:
    /// - Send a graceful termination request (SIGTERM / RPC / API
    ///   call).
    /// - Schedule escalation to `kill` after `timeout` if
    ///   appropriate.
    /// - Return immediately; final status is delivered through
    ///   `exit_rx`.
    ///
    /// This is a fallback mechanism used when higher-level
    /// (agent-first) termination cannot be applied or fails.
    async fn terminate(&self, proc_id: &ProcId, timeout: Duration)
    -> Result<(), ProcLauncherError>;

    /// Initiate a force-kill.
    ///
    /// Semantics:
    /// - Request termination as forcefully as the backend allows.
    /// - Return immediately; final status is delivered through
    ///   `exit_rx`.
    ///
    /// The exact mechanism is backend-specific:
    /// - **Native**: sends SIGKILL directly to the PID.
    /// - **Systemd**: calls `StopUnit` (same as `terminate`), which
    ///   sends SIGTERM and escalates to SIGKILL after the unit's
    ///   configured timeout. There is no separate "immediate SIGKILL"
    ///   API in the systemd D-Bus interface without adding `KillUnit`.
    ///
    /// **Note**: For backends like systemd, `kill()` currently behaves
    /// identically to `terminate()`. Callers who need a stronger
    /// guarantee should await `exit_rx` with a timeout rather than
    /// assuming immediate termination.
    ///
    /// Idempotent behavior is preferred: killing an already-dead proc
    /// should not be treated as an error unless the backend cannot
    /// determine state.
    async fn kill(&self, proc_id: &ProcId) -> Result<(), ProcLauncherError>;
}
