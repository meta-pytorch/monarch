/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! systemd-backed `ProcLauncher`.
//!
//! This module implements [`ProcLauncher`] by delegating process
//! lifetime management to **systemd transient `.service` units** on
//! the *user* bus, rather than spawning and supervising OS children
//! directly.
//!
//! # Why this exists
//!
//! The primary goal is to serve as a second, real implementation of
//! the `ProcLauncher` abstraction (alongside the native
//! `tokio::process` launcher). In other words, this file is an
//! executable "proof" that the proc lifecycle contract is independent
//! of *how* a proc is launched/supervised.
//!
//! Concretely, we map the `ProcLauncher` contract onto systemd like
//! this:
//!
//! - **launch** → `StartTransientUnit` with an `ExecStart` tuple and
//!   a small set of properties (notably `Type=oneshot` +
//!   `RemainAfterExit=true`) so we can reliably read
//!   `ExecMainCode`/`ExecMainStatus` after the process exits.
//! - **exit observation** → subscribe to D-Bus `PropertiesChanged`
//!   signals for `ActiveState`/`SubState` (signal-driven), with
//!   low-frequency safety polling as a fallback. Once terminal,
//!   read `ExecMain*` properties and translate them into
//!   [`ProcExitKind`].
//! - **terminate** → `StopUnit` (SIGTERM). The `timeout` parameter is
//!   informational; systemd enforces its own stop/kill escalation per
//!   unit config (`TimeoutStopSec`).
//! - **kill** → `StopUnit` as well (i.e., "stop via systemd", which
//!   may escalate to SIGKILL per unit config). There is no separate
//!   "send SIGKILL immediately" path.
//!
//! # Behavior and assumptions
//!
//! - Exit status reporting is inherently asynchronous in systemd: the
//!   unit can reach a terminal state before `ExecMain*` is fully
//!   populated, so the monitor task uses a bounded settle loop after
//!   detecting terminal state.
//! - We rely on `RemainAfterExit=true` to keep the unit around long
//!   enough to query execution outcome. Cleanup is best-effort and
//!   may race systemd GC.
//! - Stdio is managed by systemd; this launcher reports
//!   [`StdioHandling::ManagedByLauncher`] and does not attempt to
//!   stream stdout/stderr. Tests verify behavior via files in
//!   `XDG_RUNTIME_DIR` not stdout capture.
//! - Drop cleanup is best-effort: on drop we attempt to stop all
//!   tracked units, but it runs on a detached thread and may not
//!   complete if the process is already exiting.
//!
//! # Diagnostics
//!
//! We set a human-readable process name in [`PROCESS_NAME_ENV`] and
//! propagate the serialized bootstrap payload via
//! [`BOOTSTRAP_MODE_ENV`] (plus [`BOOTSTRAP_LOG_CHANNEL`] when
//! configured).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::MutexGuard;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use hyperactor::ProcId;
use hyperactor::channel::ChannelAddr;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use tokio::sync::oneshot;
use tracing::Instrument;
use zbus::Connection;
use zbus::zvariant::Value;

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
use crate::systemd::SystemdManagerProxy;
use crate::systemd::SystemdUnitHandle;
use crate::systemd::start_transient_service_clean;

/// Child exited normally; status is the exit code (si_code from
/// waitid(2)).
const CLD_EXITED: i32 = 1;
/// Child was killed by a signal; status is the signal number.
const CLD_KILLED: i32 = 2;
/// Child was killed by a signal and dumped core; status is the signal
/// number.
const CLD_DUMPED: i32 = 3;

/// Check if a zbus error indicates the unit has disappeared.
///
/// This detects various "unit gone" conditions that can occur when:
/// - The unit was stopped/GC'd by systemd
/// - The unit object path was invalidated
/// - External `systemctl stop` or manager restart
///
/// We match on D-Bus error names (structural) rather than message
/// strings to be robust across systemd/zbus versions.
fn is_unit_gone(e: &zbus::Error) -> bool {
    match e {
        zbus::Error::MethodError(err_name, ..) => {
            let gone = matches!(
                err_name.as_str(),
                "org.freedesktop.systemd1.NoSuchUnit" | "org.freedesktop.DBus.Error.UnknownObject"
            );

            if !gone {
                tracing::debug!(err_name = %err_name, error = %e, "non-unit-gone MethodError");
            }

            gone
        }
        _ => {
            let s = e.to_string();
            let gone = s.contains("NoSuchUnit")
                || s.contains("Unknown object")
                || s.contains("UnknownObject");
            if gone {
                tracing::debug!(error = %e, "unit-gone detected via string fallback");
            }
            gone
        }
    }
}

/// Acquire lock on units map with poison recovery.
///
/// If the mutex was poisoned (another thread panicked while holding
/// it), we log a warning and recover the guard anyway. This ensures
/// cleanup can proceed even after a panic.
fn units_lock_recover(
    units: &std::sync::Mutex<HashMap<ProcId, String>>,
) -> MutexGuard<'_, HashMap<ProcId, String>> {
    units.lock().unwrap_or_else(|p| {
        tracing::warn!("mutex was poisoned, recovering");
        p.into_inner()
    })
}

/// Result of querying a systemd property with unit-gone detection.
enum PropertyResult<T> {
    /// Successfully retrieved the value.
    Ok(T),
    /// Unit has disappeared - caller should exit.
    UnitGone,
    /// Transient error - caller should retry.
    Retry(zbus::Error),
}

/// Query a systemd property with error handling for unit-gone and
/// transient failures.
///
/// - On success: resets consecutive_errors and returns `Ok(value)`.
/// - On unit-gone: resets consecutive_errors and returns `UnitGone`.
/// - On other error: increments consecutive_errors, logs if >= 5,
///   returns `Retry(error)`. If consecutive_errors exceeds 50,
///   treats as `UnitGone` to prevent infinite loops.
async fn query_property<T, Fut>(
    property: &'static str,
    f: impl FnOnce() -> Fut,
    consecutive_errors: &mut u32,
) -> PropertyResult<T>
where
    Fut: std::future::Future<Output = Result<T, zbus::Error>>,
{
    match f().await {
        Ok(v) => {
            *consecutive_errors = 0;
            PropertyResult::Ok(v)
        }
        Err(e) if is_unit_gone(&e) => {
            *consecutive_errors = 0;
            PropertyResult::UnitGone
        }
        Err(e) => {
            *consecutive_errors += 1;
            if *consecutive_errors >= 50 {
                // Too many consecutive errors without success. The
                // unit is probably gone in a way we didn't recognize.
                // Treat as UnitGone to prevent infinite retry loops.
                tracing::warn!(
                    property,
                    consecutive_errors = *consecutive_errors,
                    error = %e,
                    "too many consecutive errors; treating as unit-gone"
                );
                *consecutive_errors = 0;
                return PropertyResult::UnitGone;
            }
            if *consecutive_errors >= 5 {
                tracing::warn!(
                    property,
                    consecutive_errors = *consecutive_errors,
                    error = %e,
                    "systemd property query failing repeatedly"
                );
            }
            PropertyResult::Retry(e)
        }
    }
}

/// Determine exit kind when a unit has disappeared.
///
/// This is inherently a best-effort heuristic since the unit is gone
/// before we could read its exit properties. The `observed_any_state`
/// flag indicates whether we ever successfully read the unit's state.
///
/// **Limitations:**
/// - If we observed state and the unit disappears, we assume it was
///   stopped (via terminate/kill/Drop) and report `Signaled(SIGTERM)`.
///   This may be incorrect if the unit was killed externally or GC'd
///   due to `CollectMode` races.
/// - If we never observed state, the unit likely never started or
///   disappeared immediately, so we report `Failed`.
///
/// A more accurate approach would track whether *we* initiated a stop
/// request, but that adds complexity for marginal diagnostic benefit.
fn exit_kind_for_unit_gone(observed_any_state: bool) -> ProcExitKind {
    if observed_any_state {
        // We saw the unit running (or at least existing). It's gone
        // now, most likely because we (or Drop) called StopUnit.
        // Report as SIGTERM since that's what StopUnit sends
        // initially.
        ProcExitKind::Signaled {
            signal: libc::SIGTERM,
            core_dumped: false,
        }
    } else {
        // Never observed any state — unit vanished before we could
        // read anything. This is a genuine failure (spawn failed,
        // immediate crash, or D-Bus/systemd weirdness).
        ProcExitKind::Failed {
            reason: "unit disappeared before state could be observed".into(),
        }
    }
}

/// Check if a unit is in a terminal state.
fn is_terminal(active: &str, sub: &str) -> bool {
    active == "failed" || active == "inactive" || (active == "active" && sub == "exited")
}

/// Log exit, remove proc from units map, and send result on channel.
fn send_and_cleanup(
    kind: ProcExitKind,
    proc_id: &ProcId,
    units: &std::sync::Mutex<HashMap<ProcId, String>>,
    exit_tx: oneshot::Sender<ProcExitResult>,
) {
    tracing::debug!(?kind, "exit_observed");
    let mut g = units_lock_recover(units);
    g.remove(proc_id);
    let _ = exit_tx.send(ProcExitResult {
        kind,
        stderr_tail: None,
    });
}

/// Map systemd ExecMainCode/Status/Result to [`ProcExitKind`].
///
/// Primary path: if `code` is a valid CLD_* constant, we use it
/// directly.
/// Fallback path: if `code` is 0 (not yet populated), we infer from
/// the `result` property string.
fn map_exit_kind(code: i32, status: i32, result: &str, active: &str, sub: &str) -> ProcExitKind {
    match code {
        CLD_EXITED => {
            // Consistency check: non-zero exit should have
            // result="exit-code"
            if status != 0 && result != "exit-code" && !result.is_empty() {
                tracing::warn!(
                    status,
                    result,
                    "result string inconsistent with non-zero exit"
                );
            }
            ProcExitKind::Exited { code: status }
        }
        CLD_KILLED => {
            if result != "signal" && !result.is_empty() {
                tracing::warn!(
                    status,
                    result,
                    "result string inconsistent with signal kill"
                );
            }
            ProcExitKind::Signaled {
                signal: status,
                core_dumped: false,
            }
        }
        CLD_DUMPED => {
            if result != "core-dump" && !result.is_empty() {
                tracing::warn!(status, result, "result string inconsistent with core dump");
            }
            ProcExitKind::Signaled {
                signal: status,
                core_dumped: true,
            }
        }
        _ => {
            // ExecMainCode not set (0) — fall back to Result
            // property.
            map_exit_from_result(result, status, active, sub)
        }
    }
}

/// Fallback exit mapping when ExecMainCode is not populated.
///
/// This can happen when systemd reaches terminal state before the
/// ExecMain* properties are fully written.
fn map_exit_from_result(result: &str, status: i32, active: &str, sub: &str) -> ProcExitKind {
    match result {
        "success" => ProcExitKind::Exited { code: 0 },
        "exit-code" => {
            if status < 0 {
                ProcExitKind::Failed {
                    reason: format!(
                        "result=exit-code but status unavailable (active={active} sub={sub})"
                    ),
                }
            } else {
                ProcExitKind::Exited { code: status }
            }
        }
        "signal" => {
            if status < 0 {
                ProcExitKind::Failed {
                    reason: format!(
                        "result=signal but status unavailable (active={active} sub={sub})"
                    ),
                }
            } else {
                ProcExitKind::Signaled {
                    signal: status,
                    core_dumped: false,
                }
            }
        }
        "core-dump" => {
            if status < 0 {
                ProcExitKind::Failed {
                    reason: format!(
                        "result=core-dump but status unavailable (active={active} sub={sub})"
                    ),
                }
            } else {
                ProcExitKind::Signaled {
                    signal: status,
                    core_dumped: true,
                }
            }
        }
        _ => ProcExitKind::Failed {
            reason: format!(
                "unknown systemd result={result} (code={code} status={status} active={active} sub={sub})",
                code = 0, // We're in the fallback path, so code was 0
            ),
        },
    }
}

/// Exit monitor task for a systemd-launched proc.
///
/// This async function waits for the systemd unit to reach a terminal
/// state using D-Bus property change signals, reads the exit properties,
/// and sends the result on `exit_tx`.
///
/// The function:
/// 1. Creates proxies for the unit and service
/// 2. Subscribes to ActiveState/SubState change signals
/// 3. Waits for terminal state (signal-driven with safety polling)
/// 4. Reads ExecMainCode/ExecMainStatus/Result properties
/// 5. Maps to [`ProcExitKind`]
/// 6. Removes the proc from the units map
/// 7. Sends the result on `exit_tx`
async fn monitor_exit(
    conn: Connection,
    units: Arc<std::sync::Mutex<HashMap<ProcId, String>>>,
    proc_id: ProcId,
    handle: SystemdUnitHandle,
    exit_tx: oneshot::Sender<ProcExitResult>,
) {
    // Create proxies ONCE.
    let (unit_proxy, svc_proxy) = match (handle.unit(&conn).await, handle.service(&conn).await) {
        (Ok(u), Ok(s)) => (u, s),
        (Err(e), _) | (_, Err(e)) => {
            let kind = if is_unit_gone(&e) {
                exit_kind_for_unit_gone(false)
            } else {
                ProcExitKind::Failed {
                    reason: format!("failed to get unit/service proxy: {e}"),
                }
            };
            send_and_cleanup(kind, &proc_id, &units, exit_tx);
            return;
        }
    };

    // Helper: query a string property, handling UnitGone by sending
    // cleanup and returning None. Returns Some(value) on success,
    // None on UnitGone (already cleaned up), or retries on transient error.
    macro_rules! refresh {
        ($target:ident, $prop:literal, $getter:expr, $errs:expr, $observed:expr) => {
            match query_property($prop, $getter, $errs).await {
                PropertyResult::Ok(s) => {
                    $target = s;
                    *$observed = true;
                }
                PropertyResult::UnitGone => {
                    send_and_cleanup(
                        exit_kind_for_unit_gone(*$observed),
                        &proc_id,
                        &units,
                        exit_tx,
                    );
                    return;
                }
                PropertyResult::Retry(_) => {}
            }
        };
    }

    let mut active = String::new();
    let mut sub = String::new();
    let mut consecutive_errors = 0u32;
    let mut observed_any_state = false;

    // Subscribe to property change signals FIRST to avoid missing
    // fast transitions. zbus's receive_*_changed() returns a
    // PropertyStream that yields when the D-Bus PropertiesChanged
    // signal fires.
    //
    // Wrapped in Option so we can disable ended streams and avoid
    // hot-looping on None.
    let mut active_stream = Some(unit_proxy.receive_active_state_changed().await);
    let mut sub_stream = Some(unit_proxy.receive_sub_state_changed().await);

    // Initial state read (after subscribing, so we don't miss
    // changes).
    refresh!(
        active,
        "active_state",
        || unit_proxy.active_state(),
        &mut consecutive_errors,
        &mut observed_any_state
    );
    refresh!(
        sub,
        "sub_state",
        || unit_proxy.sub_state(),
        &mut consecutive_errors,
        &mut observed_any_state
    );

    // Loop 1: wait for terminal state using D-Bus signals. Procs can
    // run for arbitrarily long (hours, days), so we wait indefinitely
    // until the unit reaches a terminal state or disappears. Using
    // signals instead of polling reduces D-Bus traffic and CPU usage.
    if !is_terminal(&active, &sub) {
        // Low-frequency safety tick to re-check even if signals are
        // missed (e.g., due to D-Bus congestion or proxy issues).
        let mut safety_tick = tokio::time::interval(Duration::from_secs(10));
        safety_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut logged_fallback = false;

        loop {
            tokio::select! {
                _ = safety_tick.tick() => {
                    // Refresh both states (cheap at low frequency).
                    refresh!(active, "active_state", || unit_proxy.active_state(), &mut consecutive_errors, &mut observed_any_state);
                    refresh!(sub, "sub_state", || unit_proxy.sub_state(), &mut consecutive_errors, &mut observed_any_state);
                }

                // ActiveState changed signal.
                maybe = async {
                    match &mut active_stream {
                        Some(s) => s.next().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if maybe.is_none() {
                        tracing::debug!(%proc_id, "active_state signal stream ended");
                        active_stream = None;
                        if sub_stream.is_none() && !logged_fallback {
                            tracing::debug!(%proc_id, "both signal streams ended; falling back to safety-tick polling");
                            logged_fallback = true;
                        }
                        // Don't refresh here; let safety tick handle polling.
                    } else {
                        refresh!(active, "active_state", || unit_proxy.active_state(), &mut consecutive_errors, &mut observed_any_state);
                    }
                }

                // SubState changed signal.
                maybe = async {
                    match &mut sub_stream {
                        Some(s) => s.next().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if maybe.is_none() {
                        tracing::debug!(%proc_id, "sub_state signal stream ended");
                        sub_stream = None;
                        if active_stream.is_none() && !logged_fallback {
                            tracing::debug!(%proc_id, "both signal streams ended; falling back to safety-tick polling");
                            logged_fallback = true;
                        }
                        // Don't refresh here; let safety tick handle polling.
                    } else {
                        refresh!(sub, "sub_state", || unit_proxy.sub_state(), &mut consecutive_errors, &mut observed_any_state);
                    }
                }
            }

            if is_terminal(&active, &sub) {
                break;
            }
        }
    }

    // Loop 2: wait for ExecMain* properties (bounded). This is a
    // post-exit settle window: the unit has reached terminal state,
    // but ExecMain* properties may not be populated yet due to D-Bus
    // property update lag. This should complete quickly (~seconds).
    //
    // Note: At this point we've definitely observed state, so
    // UnitGone maps to Signaled(SIGTERM).
    let mut status: i32 = -1;
    let mut code: i32 = 0;
    let mut result = String::new();
    consecutive_errors = 0;

    // Simpler refresh for Loop 2 - observed is always true here.
    macro_rules! refresh2 {
        ($target:ident, $prop:literal, $getter:expr) => {
            match query_property($prop, $getter, &mut consecutive_errors).await {
                PropertyResult::Ok(v) => $target = v,
                PropertyResult::UnitGone => {
                    send_and_cleanup(exit_kind_for_unit_gone(true), &proc_id, &units, exit_tx);
                    return;
                }
                PropertyResult::Retry(_) => {}
            }
        };
    }

    for _ in 0..200 {
        refresh2!(status, "exec_main_status", || svc_proxy.exec_main_status());
        refresh2!(code, "exec_main_code", || svc_proxy.exec_main_code());
        refresh2!(result, "result", || svc_proxy.result());

        if matches!(code, CLD_EXITED | CLD_KILLED | CLD_DUMPED) {
            break;
        }

        RealClock.sleep(Duration::from_millis(25)).await;
    }

    // Map exit to ProcExitKind using the helper function.
    let kind = map_exit_kind(code, status, &result, &active, &sub);
    send_and_cleanup(kind, &proc_id, &units, exit_tx);
}

/// Operation type for stopping a systemd unit.
///
/// This enum represents the two termination modes exposed by the
/// [`ProcLauncher`] trait:
///
/// - [`StopOp::Terminate`]: a *graceful* shutdown request that asks
///   systemd to stop the unit and allows it to exit on its own, with
///   an optional timeout after which systemd may escalate.
/// - [`StopOp::Kill`]: an *immediate* force-kill request.
///
/// In both cases this type only models **initiation** of termination.
/// Completion is observed asynchronously via the unit’s exit monitor
/// (`exit_rx`), not by the stop operation itself.
enum StopOp {
    /// Request graceful termination of the unit.
    ///
    /// This maps to `systemd StopUnit`, allowing the unit’s
    /// configured shutdown behavior (e.g. `ExecStop`,
    /// `TimeoutStopSec`, and signal escalation) to run. The provided
    /// `timeout` controls how long the launcher waits before
    /// considering the stop request to have failed at the launcher
    /// level; it does **not** bound the lifetime of the process
    /// itself.
    Terminate { timeout: Duration },

    /// Request immediate force-kill of the unit.
    ///
    /// **Note:** This currently uses `StopUnit`, the same as
    /// `Terminate`. systemd will send SIGTERM and escalate to SIGKILL
    /// per the unit's `TimeoutStopSec` configuration. This is "stop
    /// via systemd" rather than "deliver SIGKILL immediately".
    ///
    /// As with `Terminate`, this only initiates termination; actual
    /// completion is reported through the unit's exit channel.
    Kill,
}

impl StopOp {
    /// Human-readable name for this stop operation, used for logging
    /// and diagnostics.
    fn name(&self) -> &'static str {
        match self {
            StopOp::Terminate { .. } => "terminate",
            StopOp::Kill => "kill",
        }
    }

    /// Map a lower-level error into the appropriate
    /// [`ProcLauncherError`] variant for this operation.
    ///
    /// This preserves the semantic distinction between graceful
    /// termination and force-kill when reporting launcher-level
    /// failures.
    fn map_err(&self, e: impl std::fmt::Display) -> ProcLauncherError {
        match self {
            StopOp::Terminate { .. } => ProcLauncherError::Terminate(format!("{e}")),
            StopOp::Kill => ProcLauncherError::Kill(format!("{e}")),
        }
    }
}

/// systemd-backed ProcLauncher.
///
/// Each proc runs as a transient `.service` unit on the user bus.
pub(crate) struct SystemdProcLauncher {
    /// D-Bus connection to the session bus (user systemd instance).
    ///
    /// Lazily initialized on first use via `connection()`. This
    /// allows `new()` to be synchronous, deferring the async D-Bus
    /// handshake until the first launch/terminate/kill operation.
    conn: tokio::sync::OnceCell<Connection>,

    /// ProcId → unit name mapping for management operations.
    ///
    /// - `terminate`/`kill` use this to find the unit name.
    /// - The exit monitor removes the entry once it observes
    ///   completion.
    /// - Drop clears and stops all tracked units.
    ///
    /// Uses `std::sync::Mutex` (not tokio) so Drop can synchronously
    /// acquire the lock.
    units: Arc<std::sync::Mutex<HashMap<ProcId, String>>>,
}

impl SystemdProcLauncher {
    /// Construct a new systemd-backed launcher.
    ///
    /// The D-Bus connection is established lazily on first use (via
    /// `connection()`), so this is synchronous and infallible.
    pub fn new() -> Self {
        Self {
            conn: tokio::sync::OnceCell::new(),
            units: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Get or establish the D-Bus session connection.
    ///
    /// Uses `OnceCell::get_or_try_init` for single-flight
    /// initialization: multiple concurrent callers will share the
    /// same connection attempt.
    async fn connection(&self) -> Result<&Connection, ProcLauncherError> {
        self.conn
            .get_or_try_init(|| async {
                Connection::session()
                    .await
                    .map_err(|e| ProcLauncherError::Other(format!("connect to systemd: {e}")))
            })
            .await
    }

    /// Compute a stable, collision-free systemd unit name for
    /// `proc_id`.
    ///
    /// systemd unit names are picky about allowed characters.
    /// `ProcId::to_string()` can contain characters that are
    /// awkward/invalid in a unit name, so we **hex-encode** the UTF-8
    /// bytes of that string and embed it in a `monarch-<hex>.service`
    /// name.
    ///
    /// Properties:
    /// - **Stable**: the same `ProcId` always yields the same unit
    ///   name.
    /// - **Bijective**: different `ProcId` strings yield different
    ///   unit names (hex encoding is collision-free on bytes).
    /// - **Systemd-safe**: output contains only ASCII hex digits plus
    ///   `-` and `.service`.
    pub(crate) fn unit_name(proc_id: &ProcId) -> String {
        // Hex-encode the ProcId string to ensure only valid systemd
        // unit name characters. This is bijective (collision-free)
        // and stable.
        let s = proc_id.to_string();
        let mut hex = String::with_capacity(s.len() * 2);
        for b in s.as_bytes() {
            use std::fmt::Write;
            write!(&mut hex, "{:02x}", b).unwrap();
        }
        format!("monarch-{}.service", hex)
    }

    /// Build the `ExecStart` tuple for `StartTransientUnit`.
    ///
    /// Returns `(program, argv, ignore_failure)` where:
    /// - `program` is the absolute path to the executable
    /// - `argv` includes argv[0] (required by systemd's D-Bus API)
    /// - `ignore_failure` is always `false`
    fn build_exec_start(opts: &LaunchOptions) -> Vec<(String, Vec<String>, bool)> {
        let program = opts.command.program.to_string_lossy().to_string();

        // IMPORTANT: systemd's ExecStart D-Bus interface requires
        // argv[0] to be present. Without it, the first actual argument
        // becomes argv[0], causing misinterpretation.
        let mut argv = Vec::new();
        argv.push(opts.command.arg0.clone().unwrap_or_else(|| program.clone()));
        argv.extend(opts.command.args.iter().cloned());

        vec![(program, argv, false)]
    }

    /// Build environment variables for the child process.
    ///
    /// Layers bootstrap-specific vars on top of the command's base
    /// env:
    /// - `HYPERACTOR_MESH_BOOTSTRAP_MODE`: serialized bootstrap
    ///   payload
    /// - `HYPERACTOR_PROCESS_NAME`: human-readable process name
    /// - `BOOTSTRAP_LOG_CHANNEL`: log forwarding address (if
    ///   provided)
    fn build_env(opts: &LaunchOptions) -> Result<Vec<String>, ProcLauncherError> {
        // Start with command's base environment
        let mut env: std::collections::BTreeMap<String, String> = opts
            .command
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Add bootstrap payload
        env.insert(
            BOOTSTRAP_MODE_ENV.to_string(),
            opts.bootstrap_payload.clone(),
        );

        // Add diagnostics name
        env.insert(PROCESS_NAME_ENV.to_string(), opts.process_name.clone());

        // Add log channel if provided
        if let Some(addr) = opts.log_channel.as_ref() {
            env.insert(BOOTSTRAP_LOG_CHANNEL.to_string(), addr.to_string());
        }

        // Convert to systemd Environment format: "KEY=VALUE"
        Ok(env.into_iter().map(|(k, v)| format!("{k}={v}")).collect())
    }

    /// Build the properties for `StartTransientUnit`.
    fn build_unit_props<'a>(
        proc_id: &ProcId,
        exec_start: Vec<(String, Vec<String>, bool)>,
        env_kv: Vec<String>,
    ) -> Vec<(&'a str, Value<'a>)> {
        vec![
            (
                "Description",
                Value::from(format!("monarch proc {}", proc_id)),
            ),
            // Type=oneshot: systemd waits for process completion and
            // properly records ExecMainCode/ExecMainStatus.
            ("Type", Value::from("oneshot")),
            // RemainAfterExit=true: keeps unit around after exit so
            // we can query ExecMainCode/ExecMainStatus.
            ("RemainAfterExit", Value::from(true)),
            // TimeoutStopSec: escalate to SIGKILL quickly if process
            // doesn't respond to SIGTERM. Prevents Drop cleanup from
            // hanging.
            ("TimeoutStopUSec", Value::from(5_000_000u64)),
            ("ExecStart", Value::from(exec_start)),
            ("Environment", Value::from(env_kv)),
        ]
    }

    /// Shared implementation for terminate/kill via systemd StopUnit.
    async fn stop_unit_impl(&self, proc_id: &ProcId, op: StopOp) -> Result<(), ProcLauncherError> {
        let unit = match self.units.lock().unwrap().get(proc_id).cloned() {
            Some(u) => u,
            None => {
                // Idempotent: already gone / never launched here.
                tracing::debug!(%proc_id, op = op.name(), "unknown proc (treating as already exited)");
                return Ok(());
            }
        };

        // Create span with timeout_ms only for Terminate (preserves
        // observability).
        let span = match &op {
            StopOp::Terminate { timeout } => tracing::debug_span!(
                "terminate",
                %proc_id,
                %unit,
                timeout_ms = timeout.as_millis() as u64
            ),
            StopOp::Kill => tracing::debug_span!("kill", %proc_id, %unit),
        };

        async {
            // Keep exact log message strings for stable
            // observability.
            match &op {
                StopOp::Terminate { .. } => tracing::debug!("terminate_requested"),
                StopOp::Kill => tracing::debug!("kill_requested"),
            }

            let conn = self.connection().await.map_err(|e| op.map_err(e))?;
            let manager = SystemdManagerProxy::new(conn)
                .await
                .map_err(|e| op.map_err(e))?;

            manager
                .stop_unit(&unit, "replace")
                .await
                .map_err(|e| op.map_err(e))?;

            Ok(())
        }
        .instrument(span)
        .await
    }
}

#[async_trait]
impl ProcLauncher for SystemdProcLauncher {
    /// Launch a proc under systemd as a transient user `.service`.
    ///
    /// This implementation is the "systemd backend" for the
    /// [`ProcLauncher`] abstraction: instead of forking a child with
    /// `tokio::process`, we create a transient unit on the *user*
    /// systemd instance (session bus) and let systemd manage
    /// lifecycle and bookkeeping.
    ///
    /// ## What this does
    /// - Derives a deterministic, systemd-safe unit name from
    ///   `proc_id`.
    /// - Starts a transient `.service` with:
    ///   - `ExecStart` built from the configured [`BootstrapCommand`]
    ///     (**including argv[0]**, which systemd’s D-Bus API
    ///     requires),
    ///   - environment variables carrying the bootstrap payload and
    ///     diagnostics,
    ///   - `Type=oneshot` + `RemainAfterExit=true` so we can reliably
    ///     read `ExecMainCode` / `ExecMainStatus` after exit (even
    ///     for fast-exiting processes).
    /// - Records `proc_id → unit_name` in `self.units` so
    ///   `terminate`/`kill` can find the unit later.
    /// - Spawns an exit-monitor task that polls unit/service
    ///   properties and sends a [`ProcExitResult`] on the returned
    ///   `exit_rx`, cleaning up `self.units` when the exit is observed.
    ///
    /// ## What this does *not* (yet)
    /// - PID reporting: systemd hides the child PID here, so
    ///   `LaunchResult.pid` is always `None`.
    /// - Stdio capture: stdio is managed by systemd; we currently
    ///   return [`StdioHandling::ManagedByLauncher`] and do not tail
    ///   output here.
    ///
    /// ## Error and exit semantics
    /// - If the transient unit cannot be started, this returns
    ///   [`ProcLauncherError::Launch`].
    /// - Exit reporting comes from systemd's
    ///   `ExecMainCode`/`ExecMainStatus` (CLD_EXITED / CLD_KILLED /
    ///   CLD_DUMPED mapping), delivered asynchronously via `exit_rx`.
    #[hyperactor::instrument(
        level = "debug",
        fields(
            proc_id = proc_id.to_string(),
            want_stdio = opts.want_stdio,
            log_channel_present = opts.log_channel.is_some(),
            unit = tracing::field::Empty,
        )
    )]
    async fn launch(
        &self,
        proc_id: &ProcId,
        opts: LaunchOptions,
    ) -> Result<LaunchResult, ProcLauncherError> {
        let unit = Self::unit_name(proc_id);

        // Record unit in span
        tracing::Span::current().record("unit", &unit);

        // Build command, environment, and unit properties using
        // helper methods
        let exec_start = Self::build_exec_start(&opts);
        let env_kv = Self::build_env(&opts)?;
        let props = Self::build_unit_props(proc_id, exec_start, env_kv);

        let aux = Vec::new();

        // Get or establish D-Bus connection (lazy initialization).
        let conn = self.connection().await?;

        // Start unit and resolve object path
        let handle = start_transient_service_clean(conn, &unit, "replace", props, aux)
            .await
            .map_err(|e| {
                ProcLauncherError::Launch(std::io::Error::other(format!(
                    "systemd start failed: {e}"
                )))
            })?;

        let started_at = hyperactor::clock::RealClock.system_time_now();

        tracing::debug!("spawned");

        // Track mapping
        self.units
            .lock()
            .unwrap()
            .insert(proc_id.clone(), unit.clone());

        let (exit_tx, exit_rx) = oneshot::channel();

        // Spawn monitor task
        let conn = conn.clone();
        let units = Arc::clone(&self.units);
        let proc_id_for_monitor = proc_id.clone();

        // Spawn exit monitor task (propagate launch span for tracing).
        let launch_span = tracing::Span::current();
        tokio::spawn(
            monitor_exit(conn, units, proc_id_for_monitor, handle, exit_tx).instrument(launch_span),
        );

        Ok(LaunchResult {
            pid: None, // systemd hides PID
            started_at,
            stdio: StdioHandling::ManagedByLauncher,
            exit_rx,
        })
    }

    /// Request graceful termination of a running proc.
    ///
    /// This is the "polite" shutdown path: we ask systemd to stop the
    /// unit via `StopUnit`, which corresponds to sending SIGTERM to
    /// the service's main process (and letting systemd handle any
    /// configured escalation).
    ///
    /// `timeout` is currently used for diagnostics/tracing only (we
    /// record it in the span). systemd's actual stop/kill escalation
    /// timing is governed by the unit's configuration (e.g.
    /// `TimeoutStopSec`, `KillSignal`, `SendSIGKILL`), not by this
    /// argument.
    ///
    /// Semantics / caveats:
    /// - If `proc_id` is unknown (not present in our `units` map), this is
    ///   treated as idempotent success (the proc is assumed to have already
    ///   exited or never been launched here).
    /// - This call does not wait for the proc to fully exit; the exit
    ///   monitor reports completion on `exit_rx` and removes the proc
    ///   from the `units` map when observed.
    async fn terminate(
        &self,
        proc_id: &ProcId,
        timeout: Duration,
    ) -> Result<(), ProcLauncherError> {
        self.stop_unit_impl(proc_id, StopOp::Terminate { timeout })
            .await
    }

    /// Forcefully stop a running proc.
    ///
    /// Today this is implemented using systemd `StopUnit`, which
    /// sends SIGTERM and then (depending on unit/service settings)
    /// may escalate to SIGKILL. In other words: "kill" here means
    /// "stop via systemd", not "send a specific signal immediately".
    ///
    /// Semantics / caveats:
    /// - If `proc_id` is unknown (not present in our `units` map), this is
    ///   treated as idempotent success (the proc is assumed to have already
    ///   exited or never been launched here).
    /// - The exit monitor is responsible for observing the final exit
    ///   status (Exited vs Signaled) and for removing the proc from
    ///   the `units` map.
    async fn kill(&self, proc_id: &ProcId) -> Result<(), ProcLauncherError> {
        self.stop_unit_impl(proc_id, StopOp::Kill).await
    }
}

impl Drop for SystemdProcLauncher {
    fn drop(&mut self) {
        // Drain all units atomically. Clearing makes Drop idempotent
        // and prevents double cleanup attempts if something keeps
        // launcher alive.
        //
        // Note: This cleanup is best-effort. If the process is
        // terminating, the spawned cleanup thread may not complete
        // before the process exits.
        let units: Vec<(ProcId, String)> = {
            let mut guard = units_lock_recover(&self.units);
            guard.drain().collect()
        };

        if units.is_empty() {
            return;
        }

        // Spawn a thread with its own tokio runtime to perform async
        // D-Bus cleanup. We can't use the blocking zbus API (requires
        // `blocking-api` feature which has downstream compatibility
        // issues), and we can't block on async in Drop, so we spawn a
        // dedicated thread that creates its own runtime.
        std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::warn!(?e, "drop cleanup: could not create tokio runtime");
                    return;
                }
            };

            rt.block_on(async move {
                // Create a fresh async D-Bus connection. Session bus
                // is correct since we use `systemd --user`.
                let conn = match Connection::session().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(?e, "drop cleanup: could not connect to D-Bus session bus");
                        return;
                    }
                };

                let manager = match SystemdManagerProxy::new(&conn).await {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::warn!(
                            ?e,
                            "drop cleanup: could not create systemd manager proxy"
                        );
                        return;
                    }
                };

                // Stop all tracked units. Unlike
                // NativeProcLauncher::Drop which sends SIGKILL
                // directly, we request StopUnit and rely on systemd's
                // escalation mechanism (TimeoutStopSec) to SIGKILL if
                // the process doesn't respond.
                for (proc_id, unit_name) in units {
                    match manager.stop_unit(&unit_name, "replace").await {
                        Ok(_) => {
                            tracing::info!(%proc_id, %unit_name, "drop cleanup: stopped unit")
                        }
                        Err(e) => {
                            tracing::warn!(%proc_id, %unit_name, ?e, "drop cleanup: StopUnit failed")
                        }
                    }
                }
            });
        });
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use hyperactor::channel::ChannelAddr;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;

    use super::*;

    // Helpers

    /// Construct a fresh ephemeral Unix-domain channel address.
    fn any_unix_addr() -> ChannelAddr {
        ChannelAddr::any(ChannelTransport::Unix)
    }

    /// Build a `SystemdProcLauncher` whose "bootstrap command" is
    /// `/bin/sh -c <script>`.
    fn with_sh(script: impl Into<String>) -> BootstrapCommand {
        BootstrapCommand {
            program: PathBuf::from("/bin/sh"),
            args: vec!["-c".into(), script.into()],
            ..Default::default()
        }
    }

    // Tests

    /// Launch propagates bootstrap/diagnostic environment variables
    /// into the child via systemd Environment property.
    ///
    /// This test verifies that the systemd launcher correctly sets
    /// the environment variables in the transient service unit:
    /// - [`BOOTSTRAP_MODE_ENV`] with the serialized bootstrap payload
    /// - [`PROCESS_NAME_ENV`] with a human-readable process name
    /// - [`BOOTSTRAP_LOG_CHANNEL`] when `opts.log_channel` is `Some`
    ///
    /// Unlike the native launcher, systemd manages stdio internally
    /// (we get `ManagedByLauncher`), so we verify env propagation by
    /// having the child write to a file in XDG_RUNTIME_DIR that we
    /// read back.
    #[tokio::test]
    async fn launch_sets_env_vars() {
        // Skip if no session bus available (GitHub CI runners).
        let Ok(_conn) = Connection::session().await else {
            return;
        };

        // Use XDG_RUNTIME_DIR for output file - guaranteed visible to
        // user units.
        // SAFETY: `libc::getuid()` is a simple syscall that returns
        // the real user ID. It has no memory safety concerns and
        // cannot fail.
        let uid = unsafe { libc::getuid() };
        let runtime_dir =
            std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| format!("/run/user/{uid}"));
        let out_path = format!(
            "{}/monarch-env-vars-{}-{}.txt",
            runtime_dir,
            std::process::id(),
            RealClock
                .system_time_now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // RAII guard ensures cleanup even if test panics.
        struct CleanupGuard(String);
        impl Drop for CleanupGuard {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let _cleanup = CleanupGuard(out_path.clone());

        let log_channel = any_unix_addr();

        // Write env vars to a file since we can't capture stdout.
        let script = format!(
            r#"
            set -e
            printf "%s\n" "${{{}}}" >> "{}"
            printf "%s\n" "${{{}}}" >> "{}"
            printf "%s\n" "${{{}}}" >> "{}"
            exit 0
            "#,
            BOOTSTRAP_MODE_ENV,
            out_path,
            PROCESS_NAME_ENV,
            out_path,
            BOOTSTRAP_LOG_CHANNEL,
            out_path,
        );

        let launcher = SystemdProcLauncher::new();

        let proc_id = ProcId::Direct(any_unix_addr(), "env-vars".into());
        // v0 bootstrap by default but it doesn't matter here.
        let bootstrap = Bootstrap::default();
        let opts = LaunchOptions {
            command: with_sh(script),
            bootstrap_payload: bootstrap.to_env_safe_string().unwrap(),
            process_name: format_process_name(&proc_id),
            want_stdio: true,
            tail_lines: 0,
            log_channel: Some(log_channel.clone()),
        };

        let lr = launcher.launch(&proc_id, opts).await.expect("launch");

        // Systemd hides PID
        assert!(lr.pid.is_none(), "systemd launcher should not expose pid");

        // Stdio is managed by launcher
        assert!(
            matches!(lr.stdio, StdioHandling::ManagedByLauncher),
            "expected ManagedByLauncher, got {:?}",
            lr.stdio
        );

        // Wait for exit
        let exit = RealClock
            .timeout(Duration::from_secs(5), lr.exit_rx)
            .await
            .expect("timed out waiting for exit_rx")
            .expect("exit_rx dropped");

        match exit.kind {
            ProcExitKind::Exited { code } => assert_eq!(code, 0),
            other => panic!("expected Exited(0), got {other:?}"),
        }

        // Read the output file
        let content = std::fs::read_to_string(&out_path).expect("read output file");
        let lines: Vec<&str> = content.lines().collect();

        assert!(
            lines.len() >= 3,
            "expected at least 3 lines of env output, got {lines:?}"
        );

        let bootstrap_env = lines[0];
        let proc_name_env = lines[1];
        let log_env = lines[2];

        // Validate bootstrap env round-trips
        let decoded =
            Bootstrap::from_env_safe_string(bootstrap_env).expect("child printed bootstrap env");
        let reencoded = decoded.to_env_safe_string().expect("re-encode");
        assert_eq!(
            bootstrap_env, &reencoded,
            "env-safe encoding should be deterministic/stable"
        );

        // Process name includes "proc " prefix and the proc name
        assert!(
            proc_name_env.starts_with("proc "),
            "PROCESS_NAME_ENV looks wrong: {proc_name_env:?}"
        );
        assert!(
            proc_name_env.contains("env-vars"),
            "expected proc name in process name: {proc_name_env:?}"
        );

        // Log channel propagated
        assert_eq!(log_env, log_channel.to_string().as_str());

        // Verify unit was removed after exit (monitor task cleans up)
        {
            let units = launcher.units.lock().unwrap();
            assert!(
                !units.contains_key(&proc_id),
                "proc_id should be removed from units map after exit"
            );
        }
    }

    /// Exit status mapping preserves the child's numeric exit code.
    ///
    /// This test launches a trivial process that terminates with a
    /// known non-zero exit code and asserts that the systemd launcher
    /// correctly interprets `ExecMainCode=CLD_EXITED` and
    /// `ExecMainStatus=<code>` to produce [`ProcExitKind::Exited`]
    /// with the correct code.
    #[tokio::test]
    async fn exit_kind_maps_exit_code() {
        // Skip if no session bus available (GitHub CI runners).
        let Ok(_conn) = Connection::session().await else {
            return;
        };

        let launcher = SystemdProcLauncher::new();

        // v0 bootstrap by default but it doesn't matter here.
        let bootstrap = Bootstrap::default();
        let proc_id = ProcId::Direct(any_unix_addr(), "exit-7".into());
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
            .timeout(Duration::from_secs(5), lr.exit_rx)
            .await
            .expect("timed out waiting for exit_rx")
            .expect("exit_rx dropped");

        match exit.kind {
            ProcExitKind::Exited { code } => assert_eq!(code, 7),
            other => panic!("expected Exited(7), got {other:?}"),
        }
    }

    /// Exit status mapping reports signal when child is killed.
    ///
    /// This test launches a long-lived child, kills it via
    /// [`ProcLauncher::kill`], and verifies that the systemd launcher
    /// correctly interprets `ExecMainCode=CLD_KILLED` and
    /// `ExecMainStatus=<signal>` to produce
    /// [`ProcExitKind::Signaled`].
    #[tokio::test]
    async fn kill_results_in_signaled() {
        // Skip if no session bus available (GitHub CI runners).
        let Ok(_conn) = Connection::session().await else {
            return;
        };

        let launcher = SystemdProcLauncher::new();

        // v0 bootstrap by default but it doesn't matter here.
        let bootstrap = Bootstrap::default();
        let proc_id = ProcId::Direct(any_unix_addr(), "killed".into());
        let opts = LaunchOptions {
            command: with_sh("sleep 30"),
            bootstrap_payload: bootstrap.to_env_safe_string().unwrap(),
            process_name: proc_id.to_string(),
            want_stdio: false,
            tail_lines: 0,
            log_channel: None,
        };

        let lr = launcher.launch(&proc_id, opts).await.expect("launch");

        // Verify unit is tracked
        {
            let units = launcher.units.lock().unwrap();
            assert!(
                units.contains_key(&proc_id),
                "proc_id should be in units map"
            );
        }

        // Give the process a moment to start
        RealClock.sleep(Duration::from_millis(100)).await;

        launcher.kill(&proc_id).await.expect("kill");

        let exit = RealClock
            .timeout(Duration::from_secs(5), lr.exit_rx)
            .await
            .expect("timed out waiting for exit_rx")
            .expect("exit_rx dropped");

        match exit.kind {
            ProcExitKind::Signaled { signal, .. } => {
                // systemd sends SIGTERM on StopUnit; the signal number
                // depends on how quickly the process responds
                assert!(
                    signal == libc::SIGTERM || signal == libc::SIGKILL,
                    "expected SIGTERM or SIGKILL, got {signal}"
                );
            }
            other => panic!("expected Signaled, got {other:?}"),
        }
    }

    /// Terminate sends SIGTERM via StopUnit.
    ///
    /// This test launches a process and calls terminate with a
    /// timeout. Since systemd's StopUnit sends SIGTERM, the child
    /// should be terminated gracefully if it doesn't ignore the
    /// signal.
    #[tokio::test]
    async fn terminate_stops_unit() {
        // Skip if no session bus available (GitHub CI runners).
        let Ok(_conn) = Connection::session().await else {
            return;
        };

        let launcher = SystemdProcLauncher::new();

        // v0 bootstrap by default but it doesn't matter here.
        let bootstrap = Bootstrap::default();
        let proc_id = ProcId::Direct(any_unix_addr(), "terminated".into());
        let opts = LaunchOptions {
            command: with_sh("sleep 30"),
            bootstrap_payload: bootstrap.to_env_safe_string().unwrap(),
            process_name: proc_id.to_string(),
            want_stdio: false,
            tail_lines: 0,
            log_channel: None,
        };

        let lr = launcher.launch(&proc_id, opts).await.expect("launch");

        // Give the process a moment to start
        RealClock.sleep(Duration::from_millis(100)).await;

        launcher
            .terminate(&proc_id, Duration::from_secs(5))
            .await
            .expect("terminate");

        let exit = RealClock
            .timeout(Duration::from_secs(5), lr.exit_rx)
            .await
            .expect("timed out waiting for exit_rx")
            .expect("exit_rx dropped");

        match exit.kind {
            ProcExitKind::Signaled { signal, .. } => {
                assert_eq!(signal, libc::SIGTERM, "expected SIGTERM, got {signal}");
            }
            other => panic!("expected Signaled(SIGTERM), got {other:?}"),
        }
    }

    /// Unknown proc returns error on terminate.
    #[tokio::test]
    async fn terminate_unknown_proc_is_ok() {
        // Skip if no session bus available (GitHub CI runners).
        let Ok(_conn) = Connection::session().await else {
            return;
        };

        let launcher = SystemdProcLauncher::new();

        let unknown_proc_id = ProcId::Direct(any_unix_addr(), "unknown".into());

        let result = launcher
            .terminate(&unknown_proc_id, Duration::from_secs(1))
            .await;

        assert!(
            result.is_ok(),
            "expected Ok for unknown proc (idempotent terminate)"
        );
    }

    /// Unknown proc returns error on kill.
    #[tokio::test]
    async fn kill_unknown_proc_is_ok() {
        // Skip if no session bus available (GitHub CI runners).
        let Ok(_conn) = Connection::session().await else {
            return;
        };

        let launcher = SystemdProcLauncher::new();

        let unknown_proc_id = ProcId::Direct(any_unix_addr(), "unknown".into());

        let result = launcher.kill(&unknown_proc_id).await;

        assert!(
            result.is_ok(),
            "expected Ok for unknown proc (idempotent kill)"
        );
    }

    /// Unit name generation is deterministic and collision-free.
    #[tokio::test]
    async fn unit_name_is_stable() {
        let proc_id = ProcId::Ranked(hyperactor::WorldId("my-world".into()), 42);
        let unit = SystemdProcLauncher::unit_name(&proc_id);

        assert!(unit.ends_with(".service"), "unit should be a .service");
        assert!(
            unit.starts_with("monarch-"),
            "unit should start with monarch-"
        );

        // The middle part should be hex-encoded (only hex digits)
        let middle = unit
            .strip_prefix("monarch-")
            .unwrap()
            .strip_suffix(".service")
            .unwrap();
        assert!(
            middle.chars().all(|c| c.is_ascii_hexdigit()),
            "unit name middle should be hex-encoded, got: {middle}"
        );

        // Same proc_id should produce same unit name
        let unit2 = SystemdProcLauncher::unit_name(&proc_id);
        assert_eq!(unit, unit2, "unit name should be deterministic");

        // Different proc_id should produce different unit name
        let other_proc_id = ProcId::Ranked(hyperactor::WorldId("other-world".into()), 42);
        let other_unit = SystemdProcLauncher::unit_name(&other_proc_id);
        assert_ne!(
            unit, other_unit,
            "different proc_ids should have different unit names"
        );
    }

    /// Verify that `SystemdProcLauncher::Drop` terminates the child
    /// process.
    ///
    /// This test exercises the critical cleanup path: when a launcher
    /// is dropped without explicit termination, its `Drop` impl
    /// should stop the tracked unit to prevent orphaned processes.
    ///
    /// We prove:
    /// 1. The child ran (marker contains "started")
    /// 2. The child was still alive at drop time (marker contains "running")
    /// 3. Drop killed it (exit_rx fires with clean termination, not Failed)
    /// 4. Optionally: child received SIGTERM (marker may contain "term" from trap)
    ///
    /// This validates our is_unit_gone -> Signaled(SIGTERM) fix.
    #[tokio::test]
    async fn drop_terminates_child() {
        // Skip if no session bus available
        let Ok(_conn) = Connection::session().await else {
            return;
        };

        // Use XDG_RUNTIME_DIR for marker file - guaranteed visible to
        // user units.
        // SAFETY: `libc::getuid()` is a simple syscall that returns
        // the real user ID. It has no memory safety concerns and
        // cannot fail.
        let uid = unsafe { libc::getuid() };
        let runtime_dir =
            std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| format!("/run/user/{uid}"));
        let marker = format!(
            "{}/monarch-drop-cleanup-{}-{}.marker",
            runtime_dir,
            std::process::id(),
            RealClock
                .system_time_now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // RAII guard ensures cleanup even if test panics.
        struct CleanupGuard(String);
        impl Drop for CleanupGuard {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let _cleanup = CleanupGuard(marker.clone());

        // Child writes marker twice (proves still running) and traps
        // SIGTERM. This proves: (1) it ran, (2) it was alive at drop
        // time, (3) Drop killed it.
        let script = format!(
            r#"set -e; echo started > "{}"; sleep 0.2; echo running >> "{}"; trap 'echo term >> "{}"; exit 0' TERM; sleep 60"#,
            marker, marker, marker
        );

        let bootstrap = Bootstrap::default();
        let proc_id = ProcId::Direct(any_unix_addr(), "drop-cleanup-test".into());

        let exit_rx;

        {
            let launcher = SystemdProcLauncher::new();

            let opts = LaunchOptions {
                command: with_sh(&script),
                bootstrap_payload: bootstrap.to_env_safe_string().unwrap(),
                process_name: proc_id.to_string(),
                want_stdio: false,
                tail_lines: 0,
                log_channel: None,
            };

            let lr = launcher.launch(&proc_id, opts).await.expect("launch");
            exit_rx = lr.exit_rx;

            // Verify unit is tracked before drop.
            {
                let units = launcher.units.lock().unwrap();
                assert!(
                    units.contains_key(&proc_id),
                    "proc_id should be in units map"
                );
            }

            // Poll for marker file - wait until it contains
            // "running". This proves child executed userland code AND
            // was still alive.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                if let Ok(content) = std::fs::read_to_string(&marker) {
                    if content.contains("running") {
                        break;
                    }
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "Marker file never showed 'running' - child may have failed or died early"
                );
                RealClock.sleep(Duration::from_millis(50)).await;
            }

            // Launcher drops here - Drop should stop the unit
        }

        // After drop: process should exit within timeout. Use
        // generous timeout because Drop cleanup is multi-step: spawn
        // thread → build runtime → connect D-Bus → call StopUnit.
        let exit = RealClock
            .timeout(Duration::from_secs(30), exit_rx)
            .await
            .expect(
                "timed out waiting for process to exit after Drop - cleanup may be slow or stuck",
            )
            .expect("exit_rx dropped");

        // Accept Signaled (SIGTERM/SIGKILL) or Exited - NOT Failed.
        // This verifies our is_unit_gone -> Signaled(SIGTERM) fix
        // works.
        match exit.kind {
            ProcExitKind::Signaled { signal, .. } => {
                // Drop sends StopUnit which sends SIGTERM; may escalate to SIGKILL.
                assert!(
                    signal == libc::SIGTERM || signal == libc::SIGKILL,
                    "expected SIGTERM or SIGKILL, got {signal}"
                );
            }
            ProcExitKind::Exited { .. } => {
                // Acceptable (process may have exited cleanly).
            }
            other => panic!("expected Signaled or Exited, got {other:?}"),
        }
    }

    /// Long-running procs do NOT timeout in the exit monitor.
    ///
    /// This test proves the critical invariant that the exit monitor
    /// waits indefinitely for a proc to terminate, matching the
    /// NativeProcLauncher behavior where `child.wait()` is unbounded.
    ///
    /// We:
    /// 1. Launch a `sleep 60` process
    /// 2. Wait 12 seconds (would fail older code that had a 10s
    ///    timeout bug)
    /// 3. Assert exit_rx has NOT resolved (using a timeout we expect
    ///    to timeout)
    /// 4. Kill the proc and assert exit_rx resolves properly.
    #[tokio::test]
    async fn long_running_proc_does_not_timeout() {
        // Skip if no session bus available.
        let Ok(_conn) = Connection::session().await else {
            return;
        };

        let launcher = SystemdProcLauncher::new();

        let proc_id = ProcId::Direct(any_unix_addr(), "long-running".into());
        let bootstrap = Bootstrap::default();
        let opts = LaunchOptions {
            command: with_sh("sleep 60"),
            bootstrap_payload: bootstrap.to_env_safe_string().unwrap(),
            process_name: format_process_name(&proc_id),
            want_stdio: false,
            tail_lines: 0,
            log_channel: None,
        };

        // IMPORTANT: lr must be mutable because we take &mut
        // lr.exit_rx below.
        let mut lr = launcher.launch(&proc_id, opts).await.expect("launch");

        // Wait 12 seconds — longer than the old buggy 10s timeout.
        RealClock.sleep(Duration::from_secs(12)).await;

        // Assert exit_rx has NOT resolved. We use a short timeout
        // that we EXPECT to time out.
        let poll = RealClock
            .timeout(Duration::from_millis(100), &mut lr.exit_rx)
            .await;

        assert!(
            poll.is_err(),
            "exit_rx resolved unexpectedly — proc should still be running after 12s"
        );

        // Now kill the proc and verify exit_rx resolves.
        launcher.kill(&proc_id).await.expect("kill");

        let exit = RealClock
            .timeout(Duration::from_secs(5), lr.exit_rx)
            .await
            .expect("timed out waiting for exit after kill")
            .expect("exit_rx dropped");

        match exit.kind {
            ProcExitKind::Signaled { signal, .. } => {
                assert!(
                    signal == libc::SIGTERM || signal == libc::SIGKILL,
                    "expected SIGTERM or SIGKILL, got {signal}"
                );
            }
            ProcExitKind::Exited { .. } => {
                // Also acceptable: process may have exited cleanly
                // before kill took effect.
            }
            other => panic!("expected Signaled or Exited, got {other:?}"),
        }
    }
}
