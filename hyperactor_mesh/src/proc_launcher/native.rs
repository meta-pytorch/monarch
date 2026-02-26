/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Native OS process launcher.
//!
//! This module provides [`NativeProcLauncher`], a [`ProcLauncher`]
//! backend that runs bootstrap procs as ordinary local OS processes
//! using [`tokio::process::Command`].
//!
//! ## Role in the proc-launching architecture
//!
//! [`BootstrapProcManager`](crate::bootstrap::BootstrapProcManager)
//! owns the *lifecycle* model (readiness tracking, supervision,
//! bookkeeping) and delegates only the *mechanics* of
//! starting/stopping a proc to a launcher. `NativeProcLauncher` is
//! the simplest such backend and serves as the reference
//! implementation for the [`ProcLauncher`] contract.
//!
//! In particular:
//! - **Readiness is callback-driven.** A proc is considered ready
//!   only when it invokes the bootstrap callback address carried in
//!   [`Bootstrap`]. This module does not determine readiness.
//! - **Terminal status is channel-driven.** `launch` returns a
//!   [`LaunchResult`] whose `exit_rx` resolves exactly once with a
//!   [`ProcExitResult`]. The native backend implements this by
//!   awaiting `child.wait()` in a background task and mapping the OS
//!   [`ExitStatus`] into a [`ProcExitKind`].
//! - **Termination is initiation-only.** [`ProcLauncher::terminate`]
//!   and [`ProcLauncher::kill`] send signals and return without
//!   waiting for exit; completion is observed by awaiting `exit_rx`
//!   (or a higher-level handle built on it).
//!
//! ## Bootstrap + diagnostics plumbing
//!
//! The bootstrap payload and a human-readable process name are passed
//! to the child via environment variables:
//! - [`BOOTSTRAP_MODE_ENV`] carries an env-safe encoding of [`Bootstrap`].
//! - [`PROCESS_NAME_ENV`] carries a diagnostic name derived from the
//!   [`ProcId`] and hostname (useful for logs/`ps`/systemd).
//! - [`BOOTSTRAP_LOG_CHANNEL`] is optionally set when the manager
//!   provisions a mesh log-forwarding channel in [`LaunchOptions`].
//!
//! ## Stdio behavior
//!
//! Stdio configuration is driven by [`LaunchOptions::want_stdio`]:
//! - When true, stdout/stderr are piped and returned as
//!   [`StdioHandling::Captured`] so the manager can attach forwarding
//!   / tailing infrastructure.
//! - When false, stdio is inherited from the parent process.
//!
//! The native launcher itself does not compute stderr tails; the
//! manager can do that when it attaches log forwarding for captured
//! pipes.
//!
//! ## Termination semantics
//!
//! Termination is implemented with POSIX signals:
//! - `terminate` sends `SIGTERM` and schedules escalation to
//!   `SIGKILL` after a timeout if the proc still appears present.
//! - `kill` sends `SIGKILL` immediately.
//!
//! A small best-effort PID registry is maintained to support
//! signaling and to avoid leaking long-lived children if the launcher
//! is dropped during teardown.

#![allow(dead_code, unused_imports)] // Temporary.

use std::collections::HashMap;
use std::fmt;
use std::os::unix::process::CommandExt;
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;
use hyperactor::ProcId;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::bootstrap::BOOTSTRAP_LOG_CHANNEL;
use crate::bootstrap::BOOTSTRAP_MODE_ENV;
use crate::bootstrap::Bootstrap;
use crate::bootstrap::BootstrapCommand;
use crate::bootstrap::BootstrapProcConfig;
use crate::bootstrap::PROCESS_NAME_ENV;
use crate::proc_launcher::LaunchOptions;
use crate::proc_launcher::LaunchResult;
use crate::proc_launcher::ProcExitKind;
use crate::proc_launcher::ProcExitResult;
use crate::proc_launcher::ProcLauncher;
use crate::proc_launcher::ProcLauncherError;
use crate::proc_launcher::StdioHandling;
use crate::proc_launcher::format_process_name;

/// Native OS process launcher.
///
/// This launcher runs bootstrap procs as ordinary local OS processes
/// via `tokio::process::Command`.
///
/// It is the "baseline" backend for [`ProcLauncher`]:
/// - it spawns a child process directly,
/// - optionally captures stdout/stderr pipes for the manager,
/// - and reports terminal status by awaiting `child.wait()` and
///   sending a [`ProcExitResult`] through the `exit_rx` channel.
///
/// Termination is implemented by sending POSIX signals (`SIGTERM`
/// with optional escalation to `SIGKILL`, or immediate `SIGKILL`).
pub(crate) struct NativeProcLauncher {
    /// Best-effort PID registry for processes launched by this
    /// launcher.
    ///
    /// The map is used to:
    /// - look up the OS pid when `terminate`/`kill` are requested,
    ///   and
    /// - perform best-effort cleanup on `Drop`.
    ///
    /// Entries are inserted immediately after a successful spawn and
    /// removed by the exit-monitor task after `wait()` completes.
    /// Missing entries are treated as idempotent success (already
    /// exited / unknown).
    pid_table: Arc<Mutex<HashMap<ProcId, u32>>>,
}

impl NativeProcLauncher {
    /// Create a new native launcher.
    /// `command` as the base "bootstrap command".
    ///
    /// A fresh `tokio::process::Command` is constructed per launch
    /// configured with:
    /// - the bootstrap payload / diagnostics env vars, and
    /// - stdio behavior as directed by [`LaunchOptions`].
    ///
    /// The internal PID table starts empty and is populated as procs
    /// are spawned.
    pub fn new() -> Self {
        Self {
            pid_table: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Send a POSIX signal to `pid`.
    ///
    /// Semantics:
    /// - `Ok(())` if delivered successfully.
    /// - `Ok(())` if the process is already gone (`ESRCH`).
    /// - Otherwise returns a launcher error in the provided `kind`.
    ///
    /// Note: We signal the process *group* (negative PID) because the
    /// child is started in its own process group via `setpgid(0, 0)`.
    /// This ensures we kill the entire process tree, including any
    /// sub-processes spawned by wrappers (e.g., shell scripts, python
    /// launchers).
    fn send_signal(
        pid: u32,
        sig: i32,
        kind: fn(String) -> ProcLauncherError,
    ) -> Result<(), ProcLauncherError> {
        // We use negative PID to signal the process group (PGID = pid)
        // rather than just the leader.
        let pgid = -(pid as i32);
        // SAFETY: `libc::kill` is a simple syscall wrapper with no
        // memory safety concerns. Passing a negative PID signals the
        // process group, which is valid POSIX semantics.
        let rc = unsafe { libc::kill(pgid, sig) };
        if rc == 0 {
            return Ok(());
        }

        let e = std::io::Error::last_os_error();
        if e.raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(kind(format!("signal(pgid={pgid}, {sig}) failed: {e}")))
        }
    }
}

/// Convert a platform `std::process::ExitStatus` into our
/// launcher-neutral [`ProcExitKind`] representation.
///
/// This interprets an exit status in the usual POSIX order:
/// - If the process terminated due to a signal, returns
///   [`ProcExitKind::Signaled`] with the signal number and whether a
///   core was produced.
/// - Otherwise, if the process exited normally, returns
///   [`ProcExitKind::Exited`] with the integer exit code.
/// - If neither signal nor code is available (should be rare /
///   platform-specific), returns [`ProcExitKind::Failed`] with an
///   "unknown exit status" reason.
///
/// Note: on Unix, `signal()` / `core_dumped()` come from
/// `std::os::unix::process::ExitStatusExt`.
fn exit_kind_from_status(status: &ExitStatus) -> ProcExitKind {
    if let Some(sig) = status.signal() {
        ProcExitKind::Signaled {
            signal: sig,
            core_dumped: status.core_dumped(),
        }
    } else if let Some(code) = status.code() {
        ProcExitKind::Exited { code }
    } else {
        ProcExitKind::Failed {
            reason: "unknown exit status".to_string(),
        }
    }
}

#[async_trait]
impl ProcLauncher for NativeProcLauncher {
    /// Launch a bootstrap proc as a native OS child process.
    ///
    /// This implementation uses [`tokio::process::Command`] to spawn
    /// the configured [`BootstrapCommand`], passes the bootstrap
    /// payload and diagnostic metadata via environment variables, and
    /// returns a [`LaunchResult`] whose `exit_rx` resolves exactly
    /// once with the proc's terminal status.
    ///
    /// ## What this method does
    ///
    /// - **Constructs a Tokio command**.
    /// - **Injects bootstrap state** via [`BOOTSTRAP_MODE_ENV`] using
    ///   [`Bootstrap::to_env_safe_string`].
    /// - **Injects a human-readable name** via [`PROCESS_NAME_ENV`]
    ///   (see [`super::format_process_name`]).
    /// - **Optionally injects log forwarding address** via
    ///   [`BOOTSTRAP_LOG_CHANNEL`] when `opts.log_channel` is `Some`.
    /// - **Configures stdio** based on `opts.want_stdio`:
    ///   - `true` → pipe stdout/stderr and return
    ///     [`StdioHandling::Captured`]
    ///   - `false` → inherit stdout/stderr and return
    ///     [`StdioHandling::Inherited`]
    /// - **Spawns an exit monitor task** that awaits `child.wait()`,
    ///   maps the OS status into [`ProcExitKind`], removes the PID
    ///   from `pid_table`, and sends the final [`ProcExitResult`]
    ///   through `exit_rx`.
    ///
    /// ## Notes / invariants
    ///
    /// - **Readiness is not handled here.** The proc is considered
    ///   *ready* only when it invokes the bootstrap callback address
    ///   carried inside `bootstrap`. This method only starts the
    ///   process.
    /// - **Termination is initiation-only.** `terminate`/`kill` only
    ///   send signals; completion is observed by awaiting `exit_rx`.
    #[hyperactor::instrument(
        level = "debug",
        fields(
            proc_id = proc_id.to_string(),
            want_stdio = opts.want_stdio,
            log_channel_present = opts.log_channel.is_some(),
            pid = tracing::field::Empty,
        )
    )]
    async fn launch(
        &self,
        proc_id: &ProcId,
        opts: LaunchOptions,
    ) -> Result<LaunchResult, ProcLauncherError> {
        // New Tokio Command from BootstrapCommand (template)
        let mut cmd = opts.command.new();

        // Bootstrap payload
        cmd.env(BOOTSTRAP_MODE_ENV, opts.bootstrap_payload);

        // Diagnostics name
        cmd.env(PROCESS_NAME_ENV, opts.process_name);

        // Manager may decide to create a log-forwarding channel; if
        // so, native passes it through via BOOTSTRAP_LOG_CHANNEL.
        if let Some(addr) = &opts.log_channel {
            cmd.env(BOOTSTRAP_LOG_CHANNEL, addr.to_string());
        }

        // Stdio behavior gated on 'want_stdio'.
        if opts.want_stdio {
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        } else {
            cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
        }

        // Put child in its own process group so we can kill the entire
        // tree with kill(-pgid, ...).
        //
        // SAFETY: runs in the child between fork and exec. We must not
        // allocate or do anything complex here.
        unsafe {
            cmd.pre_exec(|| {
                // setpgid(0, 0) => make this process the leader of a new process group.
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        let started_at = hyperactor::clock::RealClock.system_time_now();

        let mut child = cmd.spawn().map_err(ProcLauncherError::Launch)?;
        let pid = child.id().expect("spawned child pid unavailable");

        // Record PID in current span and log spawn event.
        tracing::Span::current().record("pid", pid);
        tracing::debug!("spawned");

        // Record PID for signaling and Drop cleanup.
        {
            let mut table = self.pid_table.lock().expect("pid_table mutex poisoned");
            table.insert(proc_id.clone(), pid);
        }

        // Extract stdio handles only if requested (only present if
        // piped).
        let stdio = if opts.want_stdio {
            let stdout = child.stdout.take().expect("stdout piped but missing");
            let stderr = child.stderr.take().expect("stderr piped but missing");
            StdioHandling::Captured { stdout, stderr }
        } else {
            StdioHandling::Inherited
        };

        // Terminal status channel.
        let (exit_tx, exit_rx) = oneshot::channel();
        let pid_table = Arc::clone(&self.pid_table);

        let proc_id = proc_id.clone();
        // Propagate the launch span into the exit monitor so that
        // exit_observed and any subsequent events are not "floating".
        let launch_span = tracing::Span::current();
        tokio::spawn(
            async move {
                // Exit monitoring runs out-of-band:
                // `launch()` returns immediately with an `exit_rx`, and
                // this task is responsible for eventually resolving that
                // channel exactly once.
                //
                // We intentionally keep this task simple and robust: wait
                // for the child to terminate and translate the OS
                // `ExitStatus` into the trait-level `ProcExitKind` used
                // by the manager.
                let kind = match child.wait().await {
                    Ok(status) => exit_kind_from_status(&status),
                    Err(e) => ProcExitKind::Failed {
                        reason: format!("wait failed: {e}"),
                    },
                };

                tracing::debug!(?kind, "exit_observed");

                // Once the process has reached a terminal state (or we
                // failed to observe it), remove it from the PID table so:
                // - `terminate`/`kill` become idempotent no-ops for this
                //   proc_id, and
                // - Drop-time cleanup doesn't try to SIGKILL something
                //   that's already done.
                //
                {
                    let mut table = pid_table.lock().expect("pid_table mutex poisoned");
                    table.remove(&proc_id);
                }

                // Publish the terminal status. It's okay if the receiver
                // was dropped (e.g., manager shutdown); we just stop
                // trying.
                //
                // Native launcher doesn't provide stderr tail here: the
                // manager attaches StreamFwder for `Captured` stdio and
                // can derive any tailing diagnostics itself.
                let _ = exit_tx.send(ProcExitResult {
                    kind,
                    stderr_tail: None,
                });
            }
            .instrument(launch_span),
        );

        Ok(LaunchResult {
            pid: Some(pid),
            started_at,
            stdio,
            exit_rx,
        })
    }

    /// Initiate graceful termination of a running proc.
    ///
    /// This is a *fire-and-forget* termination request:
    /// - We send **SIGTERM** to the recorded PID (if any).
    /// - We then schedule an **escalation to SIGKILL** after
    ///   `timeout` *iff* the proc still appears present in
    ///   `pid_table`.
    /// - We return immediately; callers observe completion by
    ///   awaiting the `exit_rx` returned from [`Self::launch`].
    ///
    /// ## Semantics
    ///
    /// - If `proc_id` is not present in `pid_table`, this returns
    ///   `Ok(())` (idempotent success: already exited or never
    ///   tracked).
    /// - If SIGTERM fails because the process is already gone
    ///  (`ESRCH`), this returns `Ok(())` (idempotent success).
    /// - Other signal delivery failures are returned as
    ///   [`ProcLauncherError::Terminate`].
    ///
    /// ## Implementation notes
    ///
    /// - Escalation is performed by a background task that sleeps for
    ///   `timeout` using [`RealClock`], re-checks `pid_table`, and
    ///   sends SIGKILL if the proc is still present.
    /// - We rely on the exit-monitor task spawned in `launch` to
    ///   remove the proc from `pid_table` and resolve the `exit_rx`
    ///   channel when the proc actually terminates.
    async fn terminate(
        &self,
        proc_id: &ProcId,
        timeout: Duration,
    ) -> Result<(), ProcLauncherError> {
        let pid = {
            let table = self.pid_table.lock().expect("pid_table mutex poisoned");
            table.get(proc_id).copied()
        };

        let Some(pid) = pid else {
            // Idempotent success: already exited or unknown.
            return Ok(());
        };

        let timeout_ms = timeout.as_millis() as u64;

        // Create a termination span so escalation logs are not
        // floating.
        let term_span = tracing::debug_span!(
            "terminate",
            %proc_id,
            pid,
            timeout_ms
        );
        // Emit the request event *inside* the terminate span.
        {
            let _g = term_span.enter();
            tracing::debug!("terminate_requested");
        }

        // Send SIGTERM.
        Self::send_signal(pid, libc::SIGTERM, ProcLauncherError::Terminate)?;

        //  Escalate to SIGKILL after timeout if still present.
        let pid_table = Arc::clone(&self.pid_table);
        let proc_id = proc_id.clone();

        tokio::spawn(
            async move {
                RealClock.sleep(timeout).await;

                let pid = {
                    let table = pid_table.lock().expect("pid_table mutex poisoned");
                    table.get(&proc_id).copied()
                };

                if let Some(pid) = pid {
                    tracing::info!("terminate timeout; escalating to SIGKILL");
                    if let Err(e) = Self::send_signal(pid, libc::SIGKILL, ProcLauncherError::Kill) {
                        tracing::warn!(error = %e, "SIGKILL escalation failed");
                    }
                }
            }
            .instrument(term_span),
        );

        Ok(())
    }

    /// Initiate an immediate force-kill of a running proc.
    ///
    /// This is the "hard stop" counterpart to [`Self::terminate`]:
    /// - We send **SIGKILL** to the recorded PID (if any).
    /// - We return immediately; callers observe completion by
    ///   awaiting the `exit_rx` returned from [`Self::launch`].
    ///
    /// ## Semantics
    ///
    /// - If `proc_id` is not present in `pid_table`, this returns
    ///   `Ok(())` (idempotent success: already exited or never
    ///   tracked).
    /// - If SIGKILL fails because the process is already gone
    ///   (`ESRCH`), this returns `Ok(())` (idempotent success).
    /// - Other signal delivery failures are returned as
    ///   [`ProcLauncherError::Kill`].
    ///
    /// ## Implementation notes
    ///
    /// - We do not remove `proc_id` from `pid_table` here; the
    ///   exit-monitor task spawned in `launch` removes it once the
    ///   child actually exits.
    async fn kill(&self, proc_id: &ProcId) -> Result<(), ProcLauncherError> {
        let pid = {
            let table = self.pid_table.lock().expect("pid_table mutex poisoned");
            table.get(proc_id).copied()
        };

        let Some(pid) = pid else {
            // Idempotent success.
            return Ok(());
        };

        let kill_span = tracing::debug_span!("kill", %proc_id, pid);
        {
            let _g = kill_span.enter();
            tracing::debug!("kill_requested");

            // Immediate SIGKILL.
            Self::send_signal(pid, libc::SIGKILL, ProcLauncherError::Kill)?;
        }

        Ok(())
    }
}

impl fmt::Debug for NativeProcLauncher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeProcLauncher").finish_non_exhaustive()
    }
}

impl Drop for NativeProcLauncher {
    // Best-effort safety net: if the manager is being torn down while
    // some children are still running, we don't want to leak
    // long-lived procs.
    //
    // This is intentionally *coarse*: at Drop time we may not have an
    // async runtime available to do graceful termination, wait for
    // exit, or run a timeout/escalation ladder. So we prefer a hard
    // stop (SIGKILL) to avoid orphaned processes and test flakiness.
    //
    // Note that this does **not** remove entries from pid_table; the
    // whole structure is being dropped anyway. Also, failure is
    // ignored because Drop must not panic; the most common failure is
    // ESRCH (already exited).
    fn drop(&mut self) {
        // Collect PIDs while holding the lock, then release before killing.
        // This avoids holding the lock during syscalls.
        let pids: Vec<(ProcId, u32)> = {
            let table = self.pid_table.lock().expect("pid_table mutex poisoned");
            table.iter().map(|(k, v)| (k.clone(), *v)).collect()
        };

        for (proc_id, pid) in pids {
            match Self::send_signal(pid, libc::SIGKILL, ProcLauncherError::Kill) {
                Ok(()) => {
                    tracing::info!(%proc_id, pid, "drop cleanup: sent SIGKILL");
                }
                Err(_e) => {
                    tracing::warn!(%proc_id, pid, "drop cleanup: SIGKILL failed");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use hyperactor::channel::ChannelAddr;
    use hyperactor::channel::ChannelTransport;
    use tokio::io::AsyncBufReadExt;
    use tokio::io::AsyncReadExt;
    use tokio::io::BufReader;

    use super::*;

    // Helpers

    /// Construct a fresh ephemeral Unix-domain channel address.
    fn any_unix_addr() -> ChannelAddr {
        ChannelAddr::any(ChannelTransport::Unix)
    }

    /// Build a `NativeProcLauncher` whose "bootstrap command" is
    /// `/bin/sh -c <script>`.
    fn with_sh(script: impl Into<String>) -> BootstrapCommand {
        BootstrapCommand {
            program: PathBuf::from("/bin/sh"),
            args: vec!["-c".into(), script.into()],
            ..Default::default()
        }
    }

    /// Consume captured stdout/stderr pipes from
    /// `StdioHandling::Captured` and return their contents.
    async fn read_captured_lines(stdio: StdioHandling) -> (Vec<String>, Vec<u8>) {
        match stdio {
            StdioHandling::Captured { stdout, mut stderr } => {
                let mut out_lines = vec![];
                let mut out = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = out.next_line().await {
                    out_lines.push(line);
                }

                // Drain stderr just so pipes close cleanly; return
                // raw bytes if useful.
                let mut err_bytes = vec![];
                let _ = stderr.read_to_end(&mut err_bytes).await;

                (out_lines, err_bytes)
            }
            other => panic!("expected Captured stdio, got {other:?}"),
        }
    }

    // Tests

    /// Launch propagates bootstrap/diagnostic environment variables
    /// into the child.
    ///
    /// This is an end-to-end sanity test for the native backend’s
    /// "env plumbing":
    /// - The child prints the three env vars that the launcher is
    ///   responsible for setting: [`BOOTSTRAP_MODE_ENV`],
    ///   [`PROCESS_NAME_ENV`], and (optionally)
    ///   [`BOOTSTRAP_LOG_CHANNEL`].
    /// - The parent verifies:
    ///   - the bootstrap payload round-trips through the env-safe
    ///     encoding,
    ///   - the process name is plausibly formatted (we only check
    ///     stable pieces),
    ///   - and the log-forwarding channel address is passed through
    ///   when requested.
    ///
    /// Finally, the test awaits `exit_rx` to ensure the launch
    /// returns a working terminal-status channel and the monitor task
    /// resolves it.
    #[tokio::test]
    async fn launch_sets_env_and_optional_log_channel() {
        let log_channel = any_unix_addr();

        // Print env vars back to the parent, one per line. Use %s to
        // avoid extra fluff.
        let script = format!(
            r#"
            set -e
            printf "%s\n" "${{{}}}"
            printf "%s\n" "${{{}}}"
            printf "%s\n" "${{{}}}"
            exit 0
            "#,
            BOOTSTRAP_MODE_ENV, PROCESS_NAME_ENV, BOOTSTRAP_LOG_CHANNEL,
        );

        let launcher = NativeProcLauncher::new();

        // v0 bootstrap by default but it doesn't matter here.
        let bootstrap = Bootstrap::default();
        let proc_id = ProcId(
            ChannelAddr::any(ChannelTransport::Local),
            "test_7".to_string(),
        );
        let opts = LaunchOptions {
            command: with_sh(script),
            bootstrap_payload: bootstrap.to_env_safe_string().unwrap(),
            process_name: format_process_name(&proc_id),
            want_stdio: true,
            tail_lines: 0,
            log_channel: Some(log_channel.clone()),
        };

        let lr = launcher.launch(&proc_id, opts).await.expect("launch");

        let (lines, _stderr) = read_captured_lines(lr.stdio).await;

        assert!(
            lines.len() >= 3,
            "expected at least 3 lines of env output, got {lines:?}"
        );

        let bootstrap_env = &lines[0];
        let proc_name_env = &lines[1];
        let log_env = &lines[2];

        // Parent validates bootstrap env is parseable and round-trips
        // stably.
        let decoded =
            Bootstrap::from_env_safe_string(bootstrap_env).expect("child printed bootstrap env");
        let reencoded = decoded.to_env_safe_string().expect("re-encode");
        assert_eq!(
            bootstrap_env, &reencoded,
            "env-safe encoding should be deterministic/stable"
        );

        // Process name: don't overfit; assert it includes the "proc "
        // prefix and proc name.
        assert!(
            proc_name_env.starts_with("proc "),
            "PROCESS_NAME_ENV looks wrong: {proc_name_env:?}"
        );
        assert!(
            proc_name_env.contains("test_7"),
            "expected proc name in process name: {proc_name_env:?}"
        );

        // Log channel propagated
        assert_eq!(log_env.as_str(), log_channel.to_string().as_str());

        // Ensure the exit channel resolves cleanly.
        let exit = RealClock
            .timeout(Duration::from_secs(2), lr.exit_rx)
            .await
            .expect("timed out waiting for exit_rx")
            .expect("exit_rx dropped");
        match exit.kind {
            ProcExitKind::Exited { code } => assert_eq!(code, 0),
            other => panic!("expected Exited(0), got {other:?}"),
        }
    }

    /// Stdio handling respects [`LaunchOptions::want_stdio`].
    ///
    /// This test exercises both branches of the native launcher's
    /// stdio policy:
    ///
    /// - When `want_stdio=true`, the launcher must pipe stdout/stderr
    ///   and return [`StdioHandling::Captured`]. The test verifies we
    ///   can read a known line from stdout and known bytes from
    ///   stderr, and that the proc exits cleanly.
    ///
    /// - When `want_stdio=false`, the launcher must *not* create
    ///   pipes and instead return [`StdioHandling::Inherited`]. The
    ///   test verifies the variant and still awaits `exit_rx` to
    ///   confirm the exit monitor resolves normally.
    #[tokio::test]
    async fn stdio_is_captured_only_when_requested() {
        // Script emits to both stdout/stderr.
        let script = r#"
            printf "out-1\n"
            printf "err-1\n" 1>&2
            exit 0
        "#;

        // want_stdio=true => Captured
        {
            let launcher = NativeProcLauncher::new();
            // v0 bootstrap by default but it doesn't matter here.
            let bootstrap = Bootstrap::default();
            let proc_id = ProcId(any_unix_addr(), "stdio-captured".into());
            let opts = LaunchOptions {
                command: with_sh(script),
                bootstrap_payload: bootstrap.to_env_safe_string().unwrap(),
                process_name: proc_id.to_string(),
                want_stdio: true,
                tail_lines: 0,
                log_channel: None,
            };
            let lr = launcher.launch(&proc_id, opts).await.expect("launch");
            let (lines, stderr_bytes) = read_captured_lines(lr.stdio).await;
            assert!(
                lines.iter().any(|l| l == "out-1"),
                "missing stdout line: {lines:?}"
            );
            assert!(
                String::from_utf8_lossy(&stderr_bytes).contains("err-1"),
                "missing stderr bytes: {:?}",
                String::from_utf8_lossy(&stderr_bytes)
            );

            let exit = RealClock
                .timeout(Duration::from_secs(2), lr.exit_rx)
                .await
                .expect("timed out waiting for exit_rx")
                .expect("exit_rx dropped");
            assert!(matches!(exit.kind, ProcExitKind::Exited { code: 0 }));
        }

        // want_stdio=false => Inherited
        {
            let launcher = NativeProcLauncher::new();
            // v0 bootstrap by default but it doesn't matter here.
            let bootstrap = Bootstrap::default();
            let proc_id = ProcId(any_unix_addr(), "stdio-inherited".into());
            let opts = LaunchOptions {
                command: with_sh(script),
                bootstrap_payload: bootstrap.to_env_safe_string().unwrap(),
                process_name: proc_id.to_string(),
                want_stdio: false,
                tail_lines: 0,
                log_channel: None,
            };
            let lr = launcher.launch(&proc_id, opts).await.expect("launch");

            assert!(matches!(lr.stdio, StdioHandling::Inherited));

            let exit = RealClock
                .timeout(Duration::from_secs(2), lr.exit_rx)
                .await
                .expect("timed out waiting for exit_rx")
                .expect("exit_rx dropped");

            assert!(matches!(exit.kind, ProcExitKind::Exited { code: 0 }));
        }
    }

    /// Exit status mapping preserves the child's numeric exit code.
    ///
    /// This test launches a trivial process that terminates with a
    /// known non-zero exit code and asserts that the native launcher
    /// translates the OS [`ExitStatus`] into [`ProcExitKind::Exited`]
    /// with the same `code`.
    ///
    /// This is the "happy path" for exit propagation (no signals, no
    /// launcher- level failures) and underpins correct mapping into
    /// higher-level exit semantics in the manager.
    #[tokio::test]
    async fn exit_kind_maps_exit_code() {
        let launcher = NativeProcLauncher::new();
        // v0 bootstrap by default but it doesn't matter here.
        let bootstrap = Bootstrap::default();
        let proc_id = ProcId(any_unix_addr(), "exit-7".into());
        let opts = LaunchOptions {
            command: with_sh("exit 7"),
            bootstrap_payload: bootstrap.to_env_safe_string().unwrap(),
            process_name: proc_id.to_string(),
            want_stdio: false,
            tail_lines: 0,
            log_channel: None,
        };

        let lr = launcher.launch(&proc_id, opts).await.expect("launch");
        let exit = RealClock
            .timeout(Duration::from_secs(2), lr.exit_rx)
            .await
            .expect("timed out waiting for exit_rx")
            .expect("exit_rx dropped");
        match exit.kind {
            ProcExitKind::Exited { code } => assert_eq!(code, 7),
            other => panic!("expected Exited(7), got {other:?}"),
        }
    }

    /// Killing a running proc reports `Signaled(SIGKILL)` and clears
    /// PID tracking.
    ///
    /// This test launches a long-lived child (`sleep 30`), then calls
    /// [`ProcLauncher::kill`] and asserts two things:
    ///
    /// 1. The terminal status delivered on `exit_rx` is
    ///    [`ProcExitKind::Signaled`] with `signal == SIGKILL`.
    /// 2. The exit-monitor task removes the proc from the launcher's
    ///    internal `pid_table`, so subsequent termination requests
    ///    become idempotent no-ops and Drop-time cleanup won’t try to
    ///    re-kill an already-exited process.
    #[tokio::test]
    async fn kill_results_in_signaled_and_pid_table_is_removed() {
        let launcher = NativeProcLauncher::new();
        // v0 bootstrap by default but it doesn't matter here.
        let bootstrap = Bootstrap::default();
        let proc_id = ProcId(any_unix_addr(), "killed".into());
        let opts = LaunchOptions {
            command: with_sh("sleep 30"),
            bootstrap_payload: bootstrap.to_env_safe_string().unwrap(),
            process_name: proc_id.to_string(),
            want_stdio: false,
            tail_lines: 0,
            log_channel: None,
        };

        let lr = launcher.launch(&proc_id, opts).await.expect("launch");

        let pid = lr.pid.expect("native launcher should expose pid");
        {
            let table = launcher.pid_table.lock().expect("pid_table lock");
            assert_eq!(table.get(&proc_id).copied(), Some(pid));
        }

        launcher.kill(&proc_id).await.expect("kill");

        let exit = RealClock
            .timeout(Duration::from_secs(2), lr.exit_rx)
            .await
            .expect("timed out waiting for exit_rx")
            .expect("exit_rx dropped");
        match exit.kind {
            ProcExitKind::Signaled { signal, .. } => {
                assert_eq!(signal, libc::SIGKILL, "expected SIGKILL, got {signal}");
            }
            other => panic!("expected Signaled(SIGKILL), got {other:?}"),
        }

        // Exit monitor should have removed it.
        {
            let table = launcher.pid_table.lock().expect("pid_table lock");
            assert!(
                !table.contains_key(&proc_id),
                "pid_table should not retain proc_id after exit"
            );
        }
    }

    /// `terminate()` escalates to SIGKILL when the child ignores
    /// SIGTERM.
    ///
    /// This test exercises the "graceful then forceful" shutdown
    /// ladder:
    ///
    /// - The child process installs a SIGTERM handler that ignores
    ///   termination (`SIGTERM=IGN`) and then blocks (sleeps).
    /// - The parent waits for a small READY handshake on captured
    ///   stdout to ensure the signal disposition has been installed
    ///   *before* sending SIGTERM (avoids a race where SIGTERM
    ///   arrives too early).
    /// - We call [`ProcLauncher::terminate`] with a short timeout.
    /// - After the timeout elapses, the launcher should observe that
    ///   the proc still appears present and escalate by sending
    ///   SIGKILL.
    ///
    /// The assertion is made on the terminal status delivered via
    /// `exit_rx`: it must be [`ProcExitKind::Signaled`] with `signal
    /// == SIGKILL`.
    #[tokio::test]
    async fn terminate_escalates_to_sigkill_if_child_ignores_sigterm() {
        // Use an explicit READY handshake so the child installs
        // SIGTERM=IGN before the parent sends SIGTERM. `exec` ensures
        // we signal the Python process, not the shell.
        let launcher = NativeProcLauncher::new();
        let script = r#"exec python3 -c 'import signal,sys,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print("READY"); sys.stdout.flush(); time.sleep(30)'"#;

        // v0 bootstrap by default but it doesn't matter here.
        let bootstrap = Bootstrap::default();
        let proc_id = ProcId(any_unix_addr(), "term-escalate".into());
        let opts = LaunchOptions {
            command: with_sh(script),
            bootstrap_payload: bootstrap.to_env_safe_string().unwrap(),
            process_name: proc_id.to_string(),
            // Need captured stdout for the READY handshake.
            want_stdio: true,
            tail_lines: 0,
            log_channel: None,
        };

        let lr = launcher.launch(&proc_id, opts).await.expect("launch");

        // Wait until the child has definitely installed SIGTERM=IGN.
        // We only need stdout for this; drain stderr best-effort.
        let (stdout, mut stderr) = match lr.stdio {
            StdioHandling::Captured { stdout, stderr } => (stdout, stderr),
            other => panic!("expected Captured stdio, got {other:?}"),
        };

        let mut out = BufReader::new(stdout);
        let mut line = String::new();
        RealClock
            .timeout(Duration::from_secs(2), out.read_line(&mut line))
            .await
            .expect("timed out waiting for READY")
            .expect("read_line failed");
        assert_eq!(line.trim(), "READY", "child did not signal readiness");

        // Drain stderr in the background so pipes close cleanly.
        tokio::spawn(async move {
            let mut _buf = Vec::new();
            let _ = stderr.read_to_end(&mut _buf).await;
        });

        // Now request graceful termination + escalation after a short
        // timeout.
        launcher
            .terminate(&proc_id, Duration::from_millis(100))
            .await
            .expect("terminate");

        // Give enough time for: sleep(timeout) + SIGKILL delivery +
        // wait() + oneshot send.
        let exit = RealClock
            .timeout(Duration::from_secs(5), lr.exit_rx)
            .await
            .expect("timed out waiting for exit_rx")
            .expect("exit_rx dropped");

        match exit.kind {
            ProcExitKind::Signaled { signal, .. } => {
                assert_eq!(
                    signal,
                    libc::SIGKILL,
                    "expected SIGKILL escalation, got {signal}"
                );
            }
            other => panic!("expected Signaled(SIGKILL), got {other:?}"),
        }
    }

    /// Verify that `NativeProcLauncher::Drop` kills child processes.
    ///
    /// This test exercises the critical cleanup path: when a launcher
    /// is dropped without explicit termination, its `Drop` impl
    /// should SIGKILL any tracked children to prevent orphans.
    ///
    /// We test the observable effect: when the child is killed, its
    /// stdout closes and we get EOF.
    #[tokio::test]
    async fn drop_closes_child_stdio() {
        // Child prints "READY", then writes dots until killed.
        let python_script = r#"
import sys, time
print("READY"); sys.stdout.flush()
while True:
    sys.stdout.write("."); sys.stdout.flush()
    time.sleep(0.05)
"#;

        let command = BootstrapCommand {
            program: PathBuf::from("python3"),
            args: vec!["-u".into(), "-c".into(), python_script.trim().into()],
            ..Default::default()
        };

        let bootstrap = Bootstrap::default();
        let proc_id = ProcId(any_unix_addr(), "drop-cleanup-test".into());
        let opts = LaunchOptions {
            command,
            bootstrap_payload: bootstrap.to_env_safe_string().unwrap(),
            process_name: proc_id.to_string(),
            want_stdio: true,
            tail_lines: 0,
            log_channel: None,
        };

        // Keep stdout in outer scope so we can read after launcher
        // drop.
        let mut stdout_reader: BufReader<tokio::process::ChildStdout>;

        {
            let launcher = NativeProcLauncher::new();
            let lr = launcher.launch(&proc_id, opts).await.expect("launch");

            let (stdout, _stderr) = match lr.stdio {
                StdioHandling::Captured { stdout, stderr } => (stdout, stderr),
                other => panic!("expected Captured stdio, got {other:?}"),
            };

            stdout_reader = BufReader::new(stdout);

            // Wait for "READY".
            let mut line = String::new();
            RealClock
                .timeout(Duration::from_secs(2), stdout_reader.read_line(&mut line))
                .await
                .expect("timeout waiting for READY")
                .expect("read_line failed");
            assert!(line.trim() == "READY", "expected 'READY', got: {line}");

            // Prove child is alive by reading one dot.
            let mut b = [0u8; 1];
            RealClock
                .timeout(Duration::from_secs(2), stdout_reader.read_exact(&mut b))
                .await
                .expect("timeout waiting for dot")
                .expect("read_exact failed");
            assert_eq!(b[0], b'.');

            // launcher drops here; its Drop should SIGKILL all tracked
            // children.
        }

        // After drop: stdout should hit EOF once the child is killed.
        // Drain any remaining output and wait for EOF with a timeout.
        let drain_eof = async {
            let mut buf = [0u8; 64];
            loop {
                match stdout_reader.read(&mut buf).await {
                    Ok(0) => break,     // EOF - child is dead
                    Ok(_n) => continue, // still getting dots, keep draining
                    Err(e) => panic!("read failed: {e}"),
                }
            }
        };

        RealClock
            .timeout(Duration::from_secs(2), drain_eof)
            .await
            .expect("never observed EOF after launcher drop - child likely still alive");
    }
}
