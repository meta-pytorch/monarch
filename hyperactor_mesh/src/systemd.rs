/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! D-Bus interface to systemd for managing transient units.
//!
//! This module provides minimal proxies to systemd's D-Bus API,
//! allowing us to create, monitor, and tear down **transient units**
//! (ephemeral services created programmatically) instead of forking
//! processes directly with `tokio::process`.
//!
//! # Key components
//!
//! - [`SystemdManager`]: Manage units (`start_transient_unit`,
//!   `stop_unit`, …)
//! - [`SystemdUnit`]: Query unit state (`active_state`, `sub_state`,
//!   `load_state`)
//! - [`SystemdService`]: Query service execution results
//!   (`exec_main_status`, …)
//! - [`SystemdUnitHandle`] + [`start_transient_service`]: small
//!   convenience layer that resolves the unit object path once and lets
//!   tests/builders reconstruct proxies later.
//!
//! `SystemdUnitProxy` borrows a [`Connection`], so long-lived
//! monitors should either keep the `Connection` alive or reconstruct
//! proxies inside the spawned task; `SystemdUnitHandle` makes the
//! latter pattern ergonomic.
//!
//! # Example
//!
//! ```ignore
//! 
//! // Create a transient service
//! let exec_start = vec![(
//!     "/bin/sleep".to_string(),
//!     vec!["/bin/sleep".to_string(), "10".to_string()],
//!     false,
//! )];
//! let props = vec![
//!     ("Description", Value::from("my service")),
//!     ("ExecStart", Value::from(exec_start)),
//! ];
//! let aux = Vec::new();
//!
//! let conn = Connection::session().await?;
//! let handle = start_transient_service(
//!     &conn, "my-service.service", "replace", props, aux
//! ).await?;
//! let unit = handle.unit(&conn).await?;
//!
//! // Query its state
//! assert_eq!(unit.active_state().await?, "active");
//! ```

// Treat this as a regular dep (dependencies) despite it only being
// used in the tests (dev-dependencies). This 'trick' allows the
// systemd crate to be marked 'optional' in Cargo.toml. This use is to
// assuage the "unused dependencies" linter.
#[cfg(all(target_os = "linux", feature = "systemd"))]
use ::systemd as _;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use zbus::Connection;
use zbus::Result;
use zbus::proxy;
use zbus::zvariant::OwnedObjectPath;
use zbus::zvariant::Value;

/// Minimal proxy to `org.freedesktop.systemd1.Manager`.
///
/// We use this to talk to systemd over D-Bus (either the user bus or
/// the system bus) so we can create, query, and tear down **transient
/// units** instead of forking processes ourselves.
#[proxy(
    interface = "org.freedesktop.systemd1.Manager",
    default_service = "org.freedesktop.systemd1",
    default_path = "/org/freedesktop/systemd1"
)]
pub(crate) trait SystemdManager {
    /// Create and start a transient unit, e.g. `foo.service`.
    ///
    /// `name` is the unit name (`"foo.service"`),
    /// `mode` is usually `"replace"`,
    /// `properties` is the systemd property list (Description=…,
    /// ExecStart=…, Slice=…, etc),
    /// `aux` is for auxiliary drop-ins (we usually pass `vec![]`).
    fn start_transient_unit(
        &self,
        name: &str,
        mode: &str,
        properties: Vec<(&str, Value<'_>)>,
        aux: Vec<(&str, Vec<(&str, Value<'_>)>)>,
    ) -> Result<OwnedObjectPath>;

    /// Stop an existing unit by name, e.g. `"foo.service"`
    ///
    /// `mode` is typically `"replace"` or `"fail"`.
    fn stop_unit(&self, name: &str, mode: &str) -> Result<OwnedObjectPath>;

    /// Clear the "failed" state for a single unit so it can be
    /// started again without systemd complaining.
    fn reset_failed_unit(&self, name: &str) -> Result<()>;

    /// Clear the "failed" state for *all* units owned by this
    /// manager.
    fn reset_failed(&self) -> Result<()>;

    /// Return the D-Bus object path for a unit so we can inspect it
    /// further (active state, result, etc.).
    fn get_unit(&self, name: &str) -> Result<OwnedObjectPath>;
}

/// Minimal view of a single systemd unit, used to query its state
/// over D-Bus.
#[proxy(
    interface = "org.freedesktop.systemd1.Unit",
    default_service = "org.freedesktop.systemd1"
)]
pub(crate) trait SystemdUnit {
    /// High-level unit state, e.g. "active", "inactive", "failed",
    /// "activating".
    #[zbus(property)]
    fn active_state(&self) -> Result<String>;

    /// More specific state for the unit type, e.g. "running",
    /// "exited".
    #[zbus(property)]
    fn sub_state(&self) -> Result<String>;

    /// Whether systemd has the unit loaded, e.g. "loaded",
    /// "not-found", "error".
    #[zbus(property)]
    fn load_state(&self) -> Result<String>;
}

/// Minimal view of a systemd *service* unit, used to query execution
/// results (exit status / termination).
///
/// This is the `org.freedesktop.systemd1.Service` interface, which is
/// present for units of type `.service`.
#[proxy(
    interface = "org.freedesktop.systemd1.Service",
    default_service = "org.freedesktop.systemd1"
)]
pub(crate) trait SystemdService {
    /// Exit status of the main process (like wait status for
    /// "exited").
    #[zbus(property)]
    fn exec_main_status(&self) -> zbus::Result<i32>;

    /// Encodes *how* the main process terminated (systemd enum:
    /// CLD_*). We'll usually treat `exec_main_status` as the primary
    /// signal and keep this as a hint.
    #[zbus(property)]
    fn exec_main_code(&self) -> zbus::Result<i32>;

    /// systemd's high-level service result string (e.g. "success",
    /// "exit-code", "signal", ...).
    #[zbus(property)]
    fn result(&self) -> zbus::Result<String>;
}

/// A started unit + its resolved object path.
///
/// This is just a convenience wrapper so callers don't have to
/// repeat: start_transient_unit → get_unit → stash path.
///
/// Note: currently used by tests and by the upcoming systemd-backed
/// proc launcher.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct SystemdUnitHandle {
    name: String,
    path: OwnedObjectPath,
}

#[allow(dead_code)] // Used by tests; intended for systemd-backed proc
// launching
impl SystemdUnitHandle {
    /// Access the unit's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Access the unit's owned path.
    pub fn path(&self) -> &OwnedObjectPath {
        &self.path
    }

    /// Build a `SystemdUnitProxy` for this unit.
    pub async fn unit<'c>(&self, conn: &'c Connection) -> Result<SystemdUnitProxy<'c>> {
        SystemdUnitProxy::builder(conn)
            .path(self.path.clone())?
            .build()
            .await
    }

    /// Build a `SystemdServiceProxy` for this unit (only meaningful
    /// for `.service` units).
    pub async fn service<'c>(&self, conn: &'c Connection) -> Result<SystemdServiceProxy<'c>> {
        SystemdServiceProxy::builder(conn)
            .path(self.path.clone())?
            .build()
            .await
    }
}

/// Start a transient `.service` unit and return a handle with its
/// object path.
///
/// This keeps the "systemd D-Bus ceremony" in one place:
/// - start_transient_unit
/// - get_unit (to discover the object path)
#[allow(dead_code)] // Tests and upcoming systemd-backed proc launching.
pub async fn start_transient_service(
    conn: &Connection,
    name: &str,
    mode: &str,
    properties: Vec<(&str, Value<'_>)>,
    aux: Vec<(&str, Vec<(&str, Value<'_>)>)>,
) -> Result<SystemdUnitHandle> {
    let systemd = SystemdManagerProxy::new(conn).await?;

    // We don't currently use the returned job path, but it can be
    // useful for debugging.
    let _job_path = systemd
        .start_transient_unit(name, mode, properties, aux)
        .await?;

    let path = systemd.get_unit(name).await?;

    Ok(SystemdUnitHandle {
        name: name.to_string(),
        path,
    })
}

/// Stop a systemd unit, treating "already gone" as success.
///
/// This is a convenience helper for transient units. By the time we
/// attempt to stop a unit, systemd may have already garbage-collected
/// it (e.g. due to `CollectMode=inactive-or-failed`), in which case
/// `StopUnit` returns `org.freedesktop.systemd1.NoSuchUnit`.
///
/// We treat that specific error as a no-op and return `Ok(())`,
/// while propagating any other D-Bus/systemd error to the caller.
async fn stop_unit_best_effort(conn: &Connection, name: &str) -> zbus::Result<()> {
    let systemd = SystemdManagerProxy::new(conn).await?;
    if let Err(e) = systemd.stop_unit(name, "replace").await {
        match e {
            zbus::Error::MethodError(ref err_name, ..)
                if err_name.as_str() == "org.freedesktop.systemd1.NoSuchUnit" =>
            {
                Ok(())
            }
            other => Err(other),
        }
    } else {
        Ok(())
    }
}

/// Wait until a unit is gone (unloaded) from systemd, or timeout.
///
/// This polls `GetUnit` until it fails (meaning the unit no longer
/// exists). Used after stopping a unit to ensure it's fully cleaned up.
async fn wait_unit_gone(conn: &Connection, name: &str, timeout: std::time::Duration) {
    let Ok(systemd) = SystemdManagerProxy::new(conn).await else {
        return;
    };

    let deadline = std::time::Instant::now() + timeout;
    loop {
        // get_unit fails once the unit is actually gone/unloaded.
        if systemd.get_unit(name).await.is_err() {
            return;
        }

        if std::time::Instant::now() >= deadline {
            // Best-effort: don't block forever
            return;
        }

        RealClock.sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Best-effort cleanup of a unit before (re-)starting it.
///
/// This stops the unit if running, resets any failed state, and waits
/// briefly for systemd to unload it. Used to avoid collisions when
/// reusing unit names.
async fn cleanup_unit_best_effort(conn: &Connection, name: &str) {
    let _ = stop_unit_best_effort(conn, name).await;
    if let Ok(systemd) = SystemdManagerProxy::new(conn).await {
        let _ = systemd.reset_failed_unit(name).await;
    }
    wait_unit_gone(conn, name, std::time::Duration::from_secs(2)).await;
}

/// Start a transient service after best-effort cleanup of any
/// leftover unit with the same name.
///
/// This is the recommended way to start transient units that may be
/// reused across tests or restarts, as it handles cleanup of stale
/// units that could otherwise cause `StartTransientUnit` to fail.
pub async fn start_transient_service_clean(
    conn: &Connection,
    name: &str,
    mode: &str,
    properties: Vec<(&str, zbus::zvariant::Value<'_>)>,
    aux: Vec<(&str, Vec<(&str, zbus::zvariant::Value<'_>)>)>,
) -> zbus::Result<SystemdUnitHandle> {
    cleanup_unit_best_effort(conn, name).await;
    start_transient_service(conn, name, mode, properties, aux).await
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::io::BufRead;
    use std::os::fd::OwnedFd;
    use std::os::unix::io::FromRawFd;
    use std::os::unix::io::IntoRawFd;
    use std::os::unix::net::UnixStream;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use futures::StreamExt;
    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;
    use tokio::io::AsyncBufReadExt;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use zbus::Connection;
    use zbus::zvariant::Fd;

    use super::*;

    // Helpers

    /// Convert a `UnixStream` into an `Fd` suitable for passing to
    /// systemd over D-Bus.
    ///
    /// This consumes the stream and takes over unique ownership of
    /// its file descriptor.
    fn owned_fd_from_unixstream(stream: UnixStream) -> OwnedFd {
        let raw_fd = stream.into_raw_fd();
        // SAFETY: `raw_fd` was just returned from `UnixStream::into_raw_fd`,
        // so it's a valid, open file descriptor that is not yet managed by
        // any RAII type. We immediately wrap it in `OwnedFd` and never use
        // `raw_fd` again, so there is exactly one owner and we won't
        // double-close it.
        unsafe { OwnedFd::from_raw_fd(raw_fd) }
    }

    /// Start a transient service after best-effort cleanup of any
    /// leftover unit with the same name.
    pub async fn start_transient_service_clean(
        conn: &Connection,
        name: &str,
        mode: &str,
        properties: Vec<(&str, Value<'_>)>,
        aux: Vec<(&str, Vec<(&str, Value<'_>)>)>,
    ) -> Result<SystemdUnitHandle> {
        cleanup_unit_best_effort(conn, name).await;
        start_transient_service(conn, name, mode, properties, aux).await
    }

    /// Stop a systemd unit, treating "already gone" as success.
    ///
    /// This is a convenience helper for tests that create **transient
    /// units**. By the time we attempt to stop a unit, systemd may
    /// have already garbage-collected it (e.g. due to
    /// `CollectMode=inactive-or-failed`), in which case `StopUnit`
    /// returns `org.freedesktop.systemd1.NoSuchUnit`.
    ///
    /// We treat that specific error as a no-op and return `Ok(())`,
    /// while propagating any other D-Bus/systemd error to the caller.
    async fn stop_unit_best_effort(conn: &Connection, name: &str) -> Result<()> {
        let systemd = SystemdManagerProxy::new(conn).await?;
        if let Err(e) = systemd.stop_unit(name, "replace").await {
            match e {
                zbus::Error::MethodError(name, ..)
                    if name.as_str() == "org.freedesktop.systemd1.NoSuchUnit" =>
                {
                    Ok(())
                }
                other => Err(other),
            }
        } else {
            Ok(())
        }
    }

    /// Monotonically increasing per-process counter for unit-name
    /// uniqueness.
    ///
    /// Combined with `std::process::id()` this avoids name collisions
    /// across:
    /// - parallel test execution in the same process, and
    /// - retries / stress-runs that re-enter the same test logic.
    static UNIT_SEQ: AtomicU64 = AtomicU64::new(0);

    /// Generate a unique transient `.service` unit name for tests.
    ///
    /// The returned name has the form:
    /// `{prefix}-{pid}-{seq}.service`
    ///
    /// This avoids accidental reuse of unit names across parallel
    /// tests or repeated runs, which is important because systemd may
    /// keep units around briefly (or in a failed state) and
    /// `StartTransientUnit` will otherwise collide.
    fn unique_unit_name(prefix: &str) -> String {
        let seq = UNIT_SEQ.fetch_add(1, Ordering::Relaxed);
        format!("{}-{}-{}.service", prefix, std::process::id(), seq)
    }

    /// Wait until `GetUnit(name)` fails, indicating the unit is
    /// gone/unloaded.
    ///
    /// This is a *best-effort* helper used by tests to reduce
    /// flakiness when systemd garbage-collects transient units
    /// asynchronously (especially under
    /// `CollectMode=inactive-or-failed`).
    ///
    /// If we cannot connect to the systemd manager or the timeout
    /// expires, we return without failing the test here; callers can
    /// assert more meaningfully elsewhere.
    async fn wait_unit_gone(conn: &Connection, name: &str, timeout: Duration) {
        let Ok(systemd) = SystemdManagerProxy::new(conn).await else {
            return;
        };

        let deadline = std::time::Instant::now() + timeout;
        loop {
            // get_unit fails once the unit is actually gone/unloaded.
            if systemd.get_unit(name).await.is_err() {
                return;
            }

            if std::time::Instant::now() >= deadline {
                // Best-effort: don't panic here; let the test fail in
                // a clearer place if needed.
                eprintln!("wait_unit_gone: unit still present after {:?}", timeout);
                return;
            }

            RealClock.sleep(Duration::from_millis(50)).await;
        }
    }

    /// Best-effort cleanup for transient units created by tests.
    ///
    /// Attempts to:
    /// - stop the unit (ignoring "already gone"),
    /// - clear its failed state (ignoring errors), and
    /// - wait briefly for systemd to unload/garbage-collect the unit.
    ///
    /// This helper is intentionally tolerant: transient units may
    /// already have been collected by the time cleanup runs, and we
    /// prefer tests to fail on their primary assertions rather than
    /// on teardown.
    async fn cleanup_unit_best_effort(conn: &Connection, name: &str) {
        let _ = stop_unit_best_effort(conn, name).await;
        if let Ok(systemd) = SystemdManagerProxy::new(conn).await {
            let _ = systemd.reset_failed_unit(name).await;
        }
        wait_unit_gone(conn, name, Duration::from_secs(2)).await;
    }

    /// Poll a unit's `ActiveState` and `SubState` until it matches
    /// the expected pair.
    ///
    /// This is used instead of assuming immediate transitions, since
    /// systemd state changes can lag under load (or on slower CI
    /// hosts).
    ///
    /// Unlike `wait_unit_gone`/`cleanup_unit_best_effort`, this
    /// helper is used for *positive* assertions; if the unit does not
    /// reach the desired state within `timeout`, the test fails with
    /// the last observed state for debugging.
    async fn wait_unit_state(
        unit: &SystemdUnitProxy<'_>,
        want_active: &str,
        want_sub: &str,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let active = unit.active_state().await?;
            let sub = unit.sub_state().await?;
            if active == want_active && sub == want_sub {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "unit did not reach {}/{} within {:?}; last seen {}/{}",
                    want_active, want_sub, timeout, active, sub
                );
            }
            RealClock.sleep(Duration::from_millis(50)).await;
        }
    }

    // Tests

    /// Test creating and stopping a transient systemd unit.
    ///
    /// Creates a simple `sleep` service, verifies it's running, stops
    /// it, and confirms the transient unit is cleaned up afterward.
    #[tokio::test]
    async fn test_start_transient_unit() -> Result<()> {
        // Skip if no session bus available (GitHub CI runners).
        let conn = match Connection::session().await {
            Ok(conn) => conn,
            Err(_) => {
                eprintln!("Skipping test: D-Bus session bus not available");
                return Ok(());
            }
        };

        let unit_name = unique_unit_name("test-sleep-monitor");
        let exec_start = vec![(
            "/bin/sleep".to_string(),
            vec!["/bin/sleep".to_string(), "30".to_string()],
            false,
        )];
        let props = vec![
            ("Description", Value::from("transient sleep 30")),
            ("ExecStart", Value::from(exec_start)),
            ("CollectMode", Value::from("inactive-or-failed")),
        ];
        let aux = Vec::new();

        // Start the unit and resolve its object path once.
        let handle =
            start_transient_service_clean(&conn, &unit_name, "replace", props, aux).await?;

        // Get unit proxy for monitoring.
        let unit = handle.unit(&conn).await?;

        // Verify initial state.
        wait_unit_state(&unit, "active", "running", Duration::from_secs(3)).await?;

        // Stop the unit.
        cleanup_unit_best_effort(&conn, &unit_name).await;

        Ok(())
    }

    /// Test monitoring transient unit state transitions via D-Bus
    /// property signals.
    ///
    /// Starts a simple `sleep` service, waits until it reaches
    /// `active/running`, then spawns a background monitor that
    /// subscribes to `ActiveState` and `SubState` change
    /// notifications and records observed states.
    ///
    /// After issuing `StopUnit`, the test verifies that shutdown
    /// completes as observed by either:
    /// - reaching an "inactive-ish" state (e.g. `inactive/*`), OR
    /// - the unit disappearing ("Gone") due to systemd garbage collection.
    ///
    /// Note: with `CollectMode=inactive-or-failed`, intermediate
    /// states like `deactivating` or even `inactive` can be extremely
    /// brief and may be missed under load, so the assertions are
    /// intentionally tolerant.
    #[tokio::test]
    async fn test_monitor_unit_state_transitions() -> Result<()> {
        // State enum to track unit lifecycle.
        #[derive(Debug, Clone, PartialEq)]
        enum UnitState {
            Active { sub_state: String },
            Deactivating { sub_state: String },
            Inactive { sub_state: String },
            Gone,
        }

        impl UnitState {
            fn from_states(active: String, sub: String) -> Self {
                match active.as_str() {
                    "active" => UnitState::Active { sub_state: sub },
                    "deactivating" => UnitState::Deactivating { sub_state: sub },
                    "inactive" => UnitState::Inactive { sub_state: sub },
                    // We don't model every systemd active-state
                    // variant here; treat the rest as "inactive-ish".
                    _ => UnitState::Inactive { sub_state: sub },
                }
            }
        }

        // Skip if no session bus available (GitHub CI runners).
        let conn = match Connection::session().await {
            Ok(conn) => conn,
            Err(_) => {
                eprintln!("Skipping test: D-Bus session bus not available");
                return Ok(());
            }
        };

        let unit_name = unique_unit_name("test-sleep-monitor");

        let exec_start = vec![(
            "/bin/sleep".to_string(),
            vec!["/bin/sleep".to_string(), "30".to_string()],
            false,
        )];
        let props = vec![
            ("Description", Value::from("monitor state transitions")),
            ("ExecStart", Value::from(exec_start)),
            // NOTE: this can make "inactive" extremely brief; the
            // monitor must tolerate missing it.
            ("CollectMode", Value::from("inactive-or-failed")),
        ];
        let aux = Vec::new();

        // Start the unit and resolve its object path once.
        let handle =
            start_transient_service_clean(&conn, &unit_name, "replace", props, aux).await?;

        // Build a unit proxy for initial read.
        let unit = handle.unit(&conn).await?;

        // Verify it *eventually* reaches active/running (don’t assume
        // it's instantaneous under load).
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let (_initial_active, initial_sub) = loop {
            let a = unit.active_state().await?;
            let s = unit.sub_state().await?;
            if a == "active" && s == "running" {
                break (a, s);
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "unit did not reach active/running within 3s; last seen active_state={}, sub_state={}",
                    a, s
                );
            }
            RealClock.sleep(Duration::from_millis(50)).await;
        };

        let initial_state = UnitState::Active {
            sub_state: initial_sub.clone(),
        };
        let states = Arc::new(Mutex::new(vec![initial_state.clone()]));

        // Spawn background task to monitor property changes.
        let conn2 = conn.clone();
        let path2 = handle.path().clone();
        let states_clone = states.clone();

        // Small handshake so we don’t race "stop" against the monitor
        // setup.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();

        let monitor_task = tokio::spawn(async move {
            let unit = SystemdUnitProxy::builder(&conn2)
                .path(path2)
                .expect("unit path")
                .build()
                .await
                .expect("build unit proxy");

            let _ = ready_tx.send(());

            let mut last_state = Some(UnitState::Active {
                sub_state: initial_sub,
            });

            let mut active_stream = unit.receive_active_state_changed().await;
            let mut sub_stream = unit.receive_sub_state_changed().await;

            loop {
                tokio::select! {
                    Some(active_change) = active_stream.next() => {
                        let Ok(active) = active_change.get().await else { continue };

                        // If the unit disappears between signal and
                        // query, treat as Gone.
                        let sub = match unit.sub_state().await {
                            Ok(s) => s,
                            Err(_) => {
                                states_clone.lock().unwrap().push(UnitState::Gone);
                                break;
                            }
                        };

                        let state = UnitState::from_states(active, sub);
                        if last_state.as_ref() != Some(&state) {
                            states_clone.lock().unwrap().push(state.clone());
                            last_state = Some(state);
                        }
                    }

                    Some(sub_change) = sub_stream.next() => {
                        let Ok(sub) = sub_change.get().await else { continue };

                        // If the unit disappears between signal and
                        // query, treat as Gone.
                        let active = match unit.active_state().await {
                            Ok(a) => a,
                            Err(_) => {
                                states_clone.lock().unwrap().push(UnitState::Gone);
                                break;
                            }
                        };

                        let state = UnitState::from_states(active, sub);
                        if last_state.as_ref() != Some(&state) {
                            states_clone.lock().unwrap().push(state.clone());
                            last_state = Some(state);
                        }
                    }

                    else => break,
                }
            }
        });

        // Wait for monitor to be set up (or time out and keep going;
        // the test will fail meaningfully).
        let _ = RealClock.timeout(Duration::from_secs(1), ready_rx).await;

        // Stop the unit — IMPORTANT: do NOT
        // "cleanup_unit_best_effort" yet, it races away the
        // transitions.
        stop_unit_best_effort(&conn, &unit_name).await?;

        // Give the monitor a window to observe shutdown progress OR
        // disappearance. We accept that "inactive" / "deactivating"
        // may be missed under CollectMode+load.
        let wait_deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            {
                let s = states.lock().unwrap();
                if s.iter().any(|x| {
                    matches!(
                        x,
                        UnitState::Deactivating { .. }
                            | UnitState::Inactive { .. }
                            | UnitState::Gone
                    )
                }) {
                    break;
                }
            }

            // Also accept "gone" as detected via the manager if it
            // vanished too quickly.
            if SystemdManagerProxy::new(&conn)
                .await?
                .get_unit(&unit_name)
                .await
                .is_err()
            {
                states.lock().unwrap().push(UnitState::Gone);
                break;
            }

            if std::time::Instant::now() >= wait_deadline {
                break;
            }
            RealClock.sleep(Duration::from_millis(50)).await;
        }

        // Now do best-effort cleanup (stop/reset/wait-gone).
        cleanup_unit_best_effort(&conn, &unit_name).await;

        // Stop monitoring.
        monitor_task.abort();

        // Take a snapshot and drop the lock BEFORE any awaits.
        let collected_states = {
            let guard = states.lock().unwrap();
            guard.clone()
        }; // guard dropped here

        let has_active = collected_states
            .iter()
            .any(|s| matches!(s, UnitState::Active { .. }));
        let _has_deactivation = collected_states
            .iter()
            .any(|s| matches!(s, UnitState::Deactivating { .. }));
        let has_inactive = collected_states
            .iter()
            .any(|s| matches!(s, UnitState::Inactive { .. }));
        let has_gone = collected_states
            .iter()
            .any(|s| matches!(s, UnitState::Gone));
        assert!(
            has_active,
            "Should observe active; states={:?}",
            &*collected_states
        );

        // After cleanup, shutdown must be complete. Prefer a
        // deterministic manager/unit poll over relying on the monitor
        // seeing "Gone".
        let systemd = SystemdManagerProxy::new(&conn).await?;
        let shutdown_complete = match systemd.get_unit(&unit_name).await {
            Err(_) => true, // unit disappeared: fine
            Ok(path) => {
                // Unit still exists; it must not be active/running
                // anymore.
                let unit2 = SystemdUnitProxy::builder(&conn).path(path)?.build().await?;
                let a = unit2.active_state().await.unwrap_or_default();
                a != "active"
            }
        };

        assert!(
            has_inactive || has_gone || shutdown_complete,
            "Should observe shutdown completion (inactive/gone) or confirm it via polling; states={:?}",
            &*collected_states
        );

        Ok(())
    }

    /// Test tailing unit logs from journald (OS thread version).
    ///
    /// This test uses the systemd journal API to read logs from
    /// transient units. It's done with `std::thread::spawn` because
    /// the `systemd::journal::Journal` type is !Send and cannot be
    /// used across async tasks.
    ///
    /// NOTE: I've been unable to make this work on Meta devgpu/devvm
    /// infrastructure due to journal configuration/permission quirks
    /// (for a starting point on this goto
    /// https://fb.workplace.com/groups/systemd.and.friends/permalink/3781106268771810/).
    /// See the `test_tail_unit_logs_via_fd*` tests for a working
    /// alternative that uses FD-passing instead of journald.
    ///
    /// The code uses the crate 'systemd'. I avoid "failed to run
    /// custom build command libsystemd-sys" in GitHub CI where the
    /// 'libsystemd-dev' package is not installed by gating it on a
    /// feature.
    #[cfg(all(target_os = "linux", feature = "systemd"))]
    #[tokio::test]
    async fn test_tail_unit_logs_via_journal() -> Result<()> {
        use systemd::journal;
        use systemd::journal::JournalWaitResult;

        // Skip if no session bus available (GitHub CI runners).
        let conn = match Connection::session().await {
            Ok(conn) => conn,
            Err(_) => {
                eprintln!("Skipping test: D-Bus session bus not available");
                return Ok(());
            }
        };

        // Skip if we can't open the journal (no systemd-journald).
        if journal::OpenOptions::default()
            .current_user(true)
            .local_only(true)
            .open()
            .is_err()
        {
            eprintln!("Skipping test: systemd journal not available");
            return Ok(());
        }

        let unit_name = unique_unit_name("test-tail-logs");
        let marker = "TAIL_MARKER_TEST";

        let (log_tx, mut log_rx) = mpsc::channel::<String>(128);
        let cancel = CancellationToken::new();

        // Spawn an OS thread to read from journald (`Journal` is
        // `!Send`).
        let journal_forwarder = std::thread::spawn({
            let cancel = cancel.clone();
            let log_tx = log_tx.clone();
            let unit_name = unit_name.clone();

            move || -> anyhow::Result<()> {
                let mut journal = journal::OpenOptions::default()
                    .current_user(true)
                    .local_only(true)
                    .open()?;

                // Per
                // https://www.internalfb.com/wiki/Development_Environment/Debugging_systemd_Services/#examples
                // we are setting up for the equivalent of
                // `journalctl _UID=$(id -u $USER) _SYSTEMD_USER_UNIT=test-tail-logs.service -f`
                // but (on Meta infra) that needs to be run under `sudo`
                // and there's nothing we can do here to elevate our
                // privilges like that.
                let uid = nix::unistd::Uid::current();
                journal.match_add("_UID", uid.to_string().as_bytes())?;
                journal.match_add("_SYSTEMD_USER_UNIT", unit_name.as_bytes())?;

                journal.seek_tail()?;
                journal.next()?;

                loop {
                    if cancel.is_cancelled() {
                        break;
                    }

                    match journal.wait(Some(Duration::from_secs(1)))? {
                        JournalWaitResult::Nop => {}
                        JournalWaitResult::Invalidate => {
                            journal.seek_tail()?;
                            journal.next()?;
                        }
                        JournalWaitResult::Append => {
                            while let Some(rec) = journal.next_entry()? {
                                if let Some(msg) = rec.get("MESSAGE") {
                                    let _ = log_tx.blocking_send(msg.to_string());
                                }
                            }
                        }
                    }
                }

                Ok(())
            }
        });

        // This unit prints a marker several times, then exits.
        let exec_start = vec![(
            "/bin/sh".to_string(),
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                format!("for i in 1 2 3 4 5; do echo {}; sleep 1; done", marker),
            ],
            false,
        )];
        let props = vec![
            (
                "Description",
                Value::from("unit that logs to stdout via journald"),
            ),
            ("StandardOutput", Value::from("journal")),
            ("StandardError", Value::from("journal")),
            ("ExecStart", Value::from(exec_start)),
            ("CollectMode", Value::from("inactive-or-failed")),
        ];
        let aux: Vec<(&str, Vec<(&str, Value<'_>)>)> = Vec::new();

        let _handle =
            start_transient_service_clean(&conn, &unit_name, "replace", props, aux).await?;

        // Wait for the marker to appear in the forwarded logs (up to
        // ~4s).
        let mut seen_marker = false;
        for _ in 0..4 {
            match RealClock
                .timeout(Duration::from_secs(1), log_rx.recv())
                .await
            {
                Ok(Some(line)) => {
                    println!("[{}] {}", unit_name, line);
                    if line.contains(marker) {
                        seen_marker = true;
                        break;
                    }
                }
                Ok(None) => {
                    // Journal forwarder closed the channel; nothing
                    // more to read.
                    break;
                }
                Err(_) => {
                    // Timeout: just loop and try again.
                }
            }
        }

        // Stop the unit and let systemd clean it up.
        cleanup_unit_best_effort(&conn, &unit_name).await;

        // Tell the journal forwarder to exit and wait for it.
        cancel.cancel();
        drop(log_tx); // In case the journal forwarder is blocked on
        // `blocking_send`.
        let _ = journal_forwarder
            .join()
            .expect("journald forwarder thread panicked");

        // If we never saw the marker, don't fail the test outright.
        // In practice this probably means journald isn't configured
        // to expose this user's logs (e.g. requires sudo, different
        // journal namespaces, etc.), not that the logic is wrong.
        if !seen_marker {
            eprintln!(
                "test_tail_unit_logs_to_parent_stdout: did not observe marker '{}' in journald logs.\n\
                 This is most likely due to journal visibility/permissions in this environment.\n\
                 Treating this as a soft skip instead of a hard failure.",
                marker,
            );
        }

        Ok(())
    }

    /// Test tailing unit logs via file descriptor passing (sync
    /// thread version).
    ///
    /// This test works around journal permission issues by having
    /// systemd write the unit's output directly to a file descriptor
    /// we control, instead of trying to read it back from journald.
    ///
    /// This version uses `std::thread::spawn` with blocking I/O. See
    /// `test_tail_unit_logs_via_fd` for the async version.
    #[tokio::test]
    async fn test_tail_unit_logs_via_fd_sync() -> Result<()> {
        // Skip if no session bus available (GitHub CI runners).
        let conn = match Connection::session().await {
            Ok(conn) => conn,
            Err(_) => {
                eprintln!("Skipping test: D-Bus session bus not available");
                return Ok(());
            }
        };

        let unit_name = unique_unit_name("test-tail-fd");
        let marker = "FD_TAIL_MARKER_SYNC";

        // Create a Unix socket pair for capturing output.
        let (log_reader, log_writer) = UnixStream::pair()?;

        // Transfer ownership of write FD to systemd via D-Bus.
        let fd_for_dbus = Fd::from(owned_fd_from_unixstream(log_writer));

        let (log_tx, mut log_rx) = mpsc::channel::<String>(128);
        let cancel = CancellationToken::new();

        // Spawn a thread to read from the socket.
        let log_forwarder = std::thread::spawn({
            let cancel = cancel.clone();
            move || {
                let reader = std::io::BufReader::new(log_reader);
                for line_result in reader.lines() {
                    if cancel.is_cancelled() {
                        break;
                    }
                    if let Ok(line) = line_result {
                        println!("  [fd-sync] {}", line);
                        let _ = log_tx.blocking_send(line);
                    }
                }
            }
        });

        // Create a unit that writes to our file descriptor.
        let exec_start = vec![(
            "/bin/sh".to_string(),
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                format!("for i in 1 2 3 4 5; do echo {}; sleep 1; done", marker),
            ],
            false,
        )];

        let props = vec![
            ("Description", Value::from("unit that logs via FD")),
            (
                "StandardOutputFileDescriptor",
                Value::from(fd_for_dbus.try_clone()?),
            ),
            ("StandardErrorFileDescriptor", Value::from(fd_for_dbus)),
            ("ExecStart", Value::from(exec_start)),
            ("CollectMode", Value::from("inactive-or-failed")),
        ];
        let aux: Vec<(&str, Vec<(&str, Value<'_>)>)> = Vec::new();

        let _handle =
            start_transient_service_clean(&conn, &unit_name, "replace", props, aux).await?;

        // Wait for the marker to appear in the forwarded logs.
        let mut seen_marker = false;
        for _ in 0..10 {
            match RealClock
                .timeout(Duration::from_secs(1), log_rx.recv())
                .await
            {
                Ok(Some(line)) => {
                    if line.contains(marker) {
                        seen_marker = true;
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => continue,
            }
        }

        // Stop the unit.
        cleanup_unit_best_effort(&conn, &unit_name).await;

        // Stop the log forwarder.
        cancel.cancel();
        log_forwarder.join().expect("log forwarder thread panicked");

        assert!(
            seen_marker,
            "expected to see marker line from unit's FD output"
        );

        Ok(())
    }

    /// Test tailing unit logs via file descriptor passing (async
    /// version).
    ///
    /// This test works around journal permission issues by having
    /// systemd write the unit's output directly to a file descriptor
    /// we control.
    ///
    /// This version uses `tokio::spawn` with async I/O, which is more
    /// efficient than the sync thread version.
    #[tokio::test]
    async fn test_tail_unit_logs_via_fd() -> Result<()> {
        // Skip if no session bus available (GitHub CI runners).
        let conn = match Connection::session().await {
            Ok(conn) => conn,
            Err(_) => {
                eprintln!("Skipping test: D-Bus session bus not available");
                return Ok(());
            }
        };

        let unit_name = unique_unit_name("test-tail-fd-async");
        let marker = "FD_TAIL_MARKER_ASYNC";

        // Create a Unix socket pair for capturing output
        let (log_reader, log_writer) = UnixStream::pair()?;

        // Transfer ownership of write FD to systemd via D-Bus.
        let fd_for_dbus = Fd::from(owned_fd_from_unixstream(log_writer));

        let (log_tx, mut log_rx) = mpsc::channel::<String>(128);
        let cancel = CancellationToken::new();

        // Spawn async task to read from the socket.
        log_reader.set_nonblocking(true)?;
        let async_log_reader = tokio::net::UnixStream::from_std(log_reader)?;
        let log_forwarder_handle = tokio::spawn({
            let cancel = cancel.clone();
            async move {
                let mut lines = tokio::io::BufReader::new(async_log_reader).lines();
                loop {
                    if cancel.is_cancelled() {
                        break;
                    }

                    match RealClock
                        .timeout(Duration::from_millis(100), lines.next_line())
                        .await
                    {
                        Ok(Ok(Some(line))) => {
                            println!("  [fd-async] {}", line);
                            let _ = log_tx.send(line).await;
                        }
                        Ok(Ok(None)) => break, // Stream ended
                        Ok(Err(_)) => break,   // Error reading
                        Err(_) => {
                            // Timeout, continue
                        }
                    }
                }
            }
        });

        // Create a unit that writes to our file descriptor.
        let exec_start = vec![(
            "/bin/sh".to_string(),
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                format!("for i in 1 2 3 4 5; do echo {}; sleep 1; done", marker),
            ],
            false,
        )];

        let props = vec![
            ("Description", Value::from("unit that logs via FD (async)")),
            (
                "StandardOutputFileDescriptor",
                Value::from(fd_for_dbus.try_clone()?),
            ),
            ("StandardErrorFileDescriptor", Value::from(fd_for_dbus)),
            ("ExecStart", Value::from(exec_start)),
            ("CollectMode", Value::from("inactive-or-failed")),
        ];

        let aux = Vec::new();

        let _handle =
            start_transient_service_clean(&conn, &unit_name, "replace", props, aux).await?;

        // Wait for the marker to appear in the forwarded logs.
        let mut seen_marker = false;
        for _ in 0..10 {
            match RealClock
                .timeout(Duration::from_secs(1), log_rx.recv())
                .await
            {
                Ok(Some(line)) => {
                    if line.contains(marker) {
                        seen_marker = true;
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => continue,
            }
        }

        // Stop the unit.
        cleanup_unit_best_effort(&conn, &unit_name).await;

        // Stop the reader.
        cancel.cancel();
        let _ = log_forwarder_handle.await;

        assert!(
            seen_marker,
            "expected to see marker line from unit's FD output"
        );

        Ok(())
    }

    /// Test aggregating logs from multiple units via a single
    /// forwarder.
    ///
    /// This simulates a real-world scenario where one log aggregator
    /// collects output from multiple transient units (like a systemd
    /// slice), rather than spawning one thread per unit.
    #[tokio::test]
    async fn test_tail_multiple_unit_logs_via_fd() -> Result<()> {
        // Skip if no session bus available (GitHub CI runners).
        let conn = match Connection::session().await {
            Ok(conn) => conn,
            Err(_) => {
                eprintln!("Skipping test: D-Bus session bus not available");
                return Ok(());
            }
        };

        // Unique run id so stress-runs / concurrent tests don't collide on unit names.
        let run_id = format!(
            "{}-{}",
            std::process::id(),
            UNIT_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );

        // Define multiple units to launch (unique per run).
        let units: [(String, &'static str); 3] = [
            (format!("test-multi-a-{}.service", run_id), "MARKER_A"),
            (format!("test-multi-b-{}.service", run_id), "MARKER_B"),
            (format!("test-multi-c-{}.service", run_id), "MARKER_C"),
        ];

        // Pre-clean any leftovers (best effort).
        for (unit_name, _) in &units {
            cleanup_unit_best_effort(&conn, unit_name).await;
        }

        // Create socket pairs for each unit.
        let mut unit_log_readers = Vec::new();
        let mut unit_output_fds = Vec::new();

        for _ in &units {
            let (log_reader, log_writer) = UnixStream::pair()?;
            unit_log_readers.push(log_reader);

            // Transfer ownership of write FD to systemd via D-Bus.
            let fd_for_dbus = Fd::from(owned_fd_from_unixstream(log_writer));
            unit_output_fds.push(fd_for_dbus);
        }

        let (log_tx, mut log_rx) = mpsc::channel::<(String, String)>(128);
        let cancel = CancellationToken::new();

        // ONE aggregator task for ALL units.
        let aggregator = tokio::spawn({
            let cancel = cancel.clone();
            let units = units
                .iter()
                .map(|(name, marker)| (name.clone(), *marker))
                .collect::<Vec<(String, &'static str)>>();

            async move {
                // Convert to async BufReaders.
                let mut readers: Vec<_> = unit_log_readers
                    .into_iter()
                    .enumerate()
                    .map(|(idx, reader)| {
                        // Set non-blocking for async use.
                        let _ = reader.set_nonblocking(true);
                        let async_reader =
                            tokio::net::UnixStream::from_std(reader).expect("from_std");
                        (idx, tokio::io::BufReader::new(async_reader).lines())
                    })
                    .collect();

                // Poll the readers round-robin.
                loop {
                    if cancel.is_cancelled() {
                        break;
                    }

                    let mut any_read = false;
                    for (idx, lines_reader) in readers.iter_mut() {
                        // Try to read with a small timeout.
                        match RealClock
                            .timeout(Duration::from_millis(10), lines_reader.next_line())
                            .await
                        {
                            Ok(Ok(Some(line))) => {
                                let unit_name = units[*idx].0.clone();
                                println!("  [aggregator] {}: {}", unit_name, line);
                                let _ = log_tx.send((unit_name, line)).await;
                                any_read = true;
                            }
                            Ok(Ok(None)) => {
                                // Stream ended
                            }
                            Ok(Err(_)) => {
                                // Error reading; ignore
                            }
                            Err(_) => {
                                // Timeout - no data available from this reader
                            }
                        }
                    }

                    // If no data from any reader, sleep briefly.
                    if !any_read {
                        RealClock.sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });

        // Start all units.
        for ((unit_name, marker), owned_fd) in units.iter().zip(unit_output_fds) {
            let exec_start = vec![(
                "/bin/sh".to_string(),
                vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    format!("for i in 1 2 3; do echo {}; sleep 1; done", marker),
                ],
                false,
            )];

            let fd_for_dbus = owned_fd;
            let props = vec![
                (
                    "Description",
                    Value::from(format!("multi-unit test {}", unit_name)),
                ),
                (
                    "StandardOutputFileDescriptor",
                    Value::from(fd_for_dbus.try_clone()?),
                ),
                ("StandardErrorFileDescriptor", Value::from(fd_for_dbus)),
                ("ExecStart", Value::from(exec_start)),
                ("CollectMode", Value::from("inactive-or-failed")),
            ];

            let _handle =
                start_transient_service_clean(&conn, unit_name, "replace", props, Vec::new())
                    .await?;
        }

        // Collect logs and track which units we've seen.
        let mut seen_markers: HashMap<String, bool> = units
            .iter()
            .map(|(name, _)| (name.clone(), false))
            .collect();

        // Wait for markers from all units (up to 15 seconds total).
        for _ in 0..150 {
            match RealClock
                .timeout(Duration::from_millis(100), log_rx.recv())
                .await
            {
                Ok(Some((unit_name, line))) => {
                    // Check if this line contains the expected marker
                    // for this unit.
                    if let Some((_, expected_marker)) =
                        units.iter().find(|(name, _)| name == &unit_name)
                    {
                        if line.contains(expected_marker) {
                            seen_markers.insert(unit_name.clone(), true);
                        }
                    }

                    // Check if we've seen all markers.
                    if seen_markers.values().all(|&seen| seen) {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    // Timeout, continue waiting.
                }
            }
        }

        // Stop all units (best effort).
        for (unit_name, _) in &units {
            cleanup_unit_best_effort(&conn, unit_name).await;
        }

        // Stop the aggregator.
        cancel.cancel();
        let _ = aggregator.await;

        // Verify we saw markers from all units.
        for (unit_name, marker) in &units {
            assert!(
                seen_markers.get(unit_name).copied().unwrap_or(false),
                "did not see marker {} from unit {}",
                marker,
                unit_name
            );
        }

        Ok(())
    }

    /// Test that `org.freedesktop.systemd1.Service` exposes exit
    /// results for a transient service.
    ///
    /// We use `Type=oneshot` so systemd waits for the command to
    /// complete and records the main process exit status. We also set
    /// `RemainAfterExit=true` so the unit sticks around long enough
    /// for us to query `ExecMain*` properties.
    #[tokio::test]
    async fn test_service_exec_main_status_nonzero_exit() -> Result<()> {
        use std::time::Duration;

        use hyperactor::clock::Clock;
        use hyperactor::clock::RealClock;
        use zbus::Connection;
        use zbus::zvariant::Value;

        // Skip if no session bus available (GitHub CI runners).
        let conn = match Connection::session().await {
            Ok(conn) => conn,
            Err(_) => {
                eprintln!("Skipping test: D-Bus session bus not available");
                return Ok(());
            }
        };

        // Make the unit name unique to reduce collisions across
        // retries/parallelism.
        let unit_name = format!("test-exit-status-{}.service", std::process::id());

        // Make the unit exit quickly with a known status code.
        let exit_code: i32 = 17;
        let exec_start = vec![(
            "/bin/sh".to_string(),
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                format!("exit {}", exit_code),
            ],
            false,
        )];

        let props = vec![
            (
                "Description",
                Value::from("service that exits with known status"),
            ),
            ("Type", Value::from("oneshot")),
            ("RemainAfterExit", Value::from(true)),
            ("ExecStart", Value::from(exec_start)),
        ];

        // Start the transient unit.
        let handle =
            start_transient_service_clean(&conn, &unit_name, "replace", props, Vec::new()).await?;

        // Ensure we try to clean up even if assertions fail.
        struct Cleanup<'a> {
            conn: &'a Connection,
            name: String,
        }
        impl<'a> Cleanup<'a> {
            async fn run(self) {
                let _ = stop_unit_best_effort(self.conn, &self.name).await;
            }
        }
        let cleanup = Cleanup {
            conn: &conn,
            name: unit_name.clone(),
        };

        let unit = handle.unit(&conn).await?;
        let svc = handle.service(&conn).await?;

        // Wait for unit to reach a terminal state. For non-zero exit
        // on oneshot, we expect "failed".
        let mut active = "<unknown>".to_string();
        let mut sub = "<unknown>".to_string();
        for _ in 0..200 {
            active = unit.active_state().await?;
            sub = unit.sub_state().await?;
            if active == "failed" {
                break;
            }
            RealClock.sleep(Duration::from_millis(50)).await;
        }

        // Now wait for ExecMainStatus to reflect the exit code
        // (property update lag).
        let mut status: i32 = 0;
        let mut code: i32 = 0;
        let mut result = String::new();
        for _ in 0..200 {
            status = svc.exec_main_status().await?;
            code = svc.exec_main_code().await?;
            result = svc.result().await?;
            if status == exit_code && !result.is_empty() {
                break;
            }
            RealClock.sleep(Duration::from_millis(25)).await;
        }

        // Assertions.
        assert_eq!(
            active, "failed",
            "expected unit to fail for non-zero exit; got active_state={}, sub_state={}, result={}, exec_main_code={}, exec_main_status={}",
            active, sub, result, code, status
        );
        assert_eq!(
            status, exit_code,
            "unexpected ExecMainStatus; active_state={}, sub_state={}, result={}, exec_main_code={}",
            active, sub, result, code
        );
        assert!(
            !result.is_empty(),
            "expected non-empty systemd Service.Result; active_state={}, sub_state={}, exec_main_code={}, exec_main_status={}",
            active,
            sub,
            code,
            status
        );

        // Best-effort cleanup.
        cleanup.run().await;

        Ok(())
    }
}
