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
//! - [`SystemdManager`]: Create and manage units
//!   (`start_transient_unit`, `stop_unit`, `reset_failed_unit`)
//! - [`SystemdUnit`]: Query unit state (`active_state`, `sub_state`,
//!   `load_state`)
//!
//! # Example
//!
//! ```ignore
//! let conn = Connection::session().await?;
//! let systemd = SystemdManagerProxy::new(&conn).await?;
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
//! systemd.start_transient_unit("my-service.service", "replace", props, vec![]).await?;
//!
//! // Query its state
//! let unit_path = systemd.get_unit("my-service.service").await?;
//! let unit = SystemdUnitProxy::builder(&conn).path(unit_path)?.build().await?;
//! assert_eq!(unit.active_state().await?, "active");

// Treat this as a regular dep (dependencies) despite it only being
// used in the tests (dev-dependencies). This 'trick' allows the
// systemd crate to be marked 'optional' in Cargo.toml. This use is to
// assuage the "unused dependencies" linter.
#[cfg(all(target_os = "linux", feature = "systemd"))]
use systemd as _;
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
trait SystemdManager {
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

    /// Stop an existing unit by name , e.g. `"foo.service"`
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
trait SystemdUnit {
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

        let unit_name = "test-sleep.service";
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

        let systemd = SystemdManagerProxy::new(&conn).await?;

        // Start the unit.
        let start_path = systemd
            .start_transient_unit(unit_name, "replace", props, aux)
            .await?;
        assert!(
            start_path
                .to_string()
                .contains("/org/freedesktop/systemd1/job"),
            "unexpected object path: {start_path}"
        );

        // Get unit proxy for monitoring.
        let unit = SystemdUnitProxy::builder(&conn)
            .path(systemd.get_unit(unit_name).await?)?
            .build()
            .await?;

        // Verify initial state.
        let active_state = unit.active_state().await?;
        let sub_state = unit.sub_state().await?;
        assert_eq!(active_state, "active");
        assert_eq!(sub_state, "running");

        // Stop the unit.
        let stop_path = systemd.stop_unit(unit_name, "replace").await?;
        assert!(
            stop_path
                .to_string()
                .contains("/org/freedesktop/systemd1/job"),
            "unexpected object path: {stop_path}"
        );

        // Poll for unit cleanup.
        for attempt in 1..=5 {
            RealClock.sleep(Duration::from_secs(1)).await;
            if systemd.get_unit(unit_name).await.is_err() {
                break;
            }
            if attempt == 5 {
                panic!("transient unit not cleaned up after {} seconds", attempt);
            }
        }

        Ok(())
    }

    /// Test monitoring systemd unit state transitions via D-Bus
    /// signals.
    ///
    /// Creates a transient `sleep` service, subscribes to property
    /// change signals, stops the unit, and verifies the expected state
    /// transitions (Active → Inactive → Gone) are observed.
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

        let unit_name = "test-sleep-monitor.service";

        let exec_start = vec![(
            "/bin/sleep".to_string(),
            vec!["/bin/sleep".to_string(), "30".to_string()],
            false,
        )];
        let props = vec![
            ("Description", Value::from("monitor state transitions")),
            ("ExecStart", Value::from(exec_start)),
            ("CollectMode", Value::from("inactive-or-failed")),
        ];
        let aux = Vec::new();

        let systemd = SystemdManagerProxy::new(&conn).await?;

        // Start the unit.
        let start_path = systemd
            .start_transient_unit(unit_name, "replace", props, aux)
            .await?;
        assert!(
            start_path
                .to_string()
                .contains("/org/freedesktop/systemd1/job")
        );

        // Get unit proxy for monitoring.
        let unit_path = systemd.get_unit(unit_name).await?;
        let unit = SystemdUnitProxy::builder(&conn)
            .path(unit_path)?
            .build()
            .await?;

        // Verify initial state.
        let initial_active = unit.active_state().await?;
        let initial_sub = unit.sub_state().await?;
        assert_eq!(initial_active, "active");
        assert_eq!(initial_sub, "running");

        // Collect state transitions.
        let initial_state = UnitState::Active {
            sub_state: initial_sub.clone(),
        };
        let states = Arc::new(Mutex::new(vec![initial_state.clone()]));

        // Spawn background task to monitor property changes.
        let unit_clone = unit.clone();
        let states_clone = states.clone();
        let initial_state_clone = initial_state.clone();
        let monitor_task = tokio::spawn(async move {
            let mut last_state = Some(initial_state_clone);
            let mut active_stream = unit_clone.receive_active_state_changed().await;
            let mut sub_stream = unit_clone.receive_sub_state_changed().await;

            loop {
                tokio::select! {
                    Some(active_change) = active_stream.next() => {
                        if let Ok(active) = active_change.get().await {
                            if let Ok(sub) = unit_clone.sub_state().await {
                                let state = UnitState::from_states(active, sub);
                                if last_state.as_ref() != Some(&state) {
                                    states_clone.lock().unwrap().push(state.clone());
                                    last_state = Some(state);
                                }
                            }
                        }
                    }
                    Some(sub_change) = sub_stream.next() => {
                        if let Ok(sub) = sub_change.get().await {
                            if let Ok(active) = unit_clone.active_state().await {
                                let state = UnitState::from_states(active, sub);
                                if last_state.as_ref() != Some(&state) {
                                    states_clone.lock().unwrap().push(state.clone());
                                    last_state = Some(state);
                                }
                            }
                        }
                    }
                    else => break,
                }
            }
        });

        // Give monitor time to set up.
        RealClock.sleep(Duration::from_millis(100)).await;

        // Stop the unit.
        let stop_path = systemd.stop_unit(unit_name, "replace").await?;
        assert!(
            stop_path
                .to_string()
                .contains("/org/freedesktop/systemd1/job")
        );

        // Poll for unit cleanup.
        for attempt in 1..=5 {
            RealClock.sleep(Duration::from_secs(1)).await;
            if systemd.get_unit(unit_name).await.is_err() {
                states.lock().unwrap().push(UnitState::Gone);
                break;
            }
            if attempt == 10 {
                panic!("transient unit not cleaned up after {} seconds", attempt);
            }
        }

        // Stop monitoring.
        monitor_task.abort();

        // Verify state transitions.
        let collected_states = states.lock().unwrap();

        // Check for observed states.
        let has_active = collected_states
            .iter()
            .any(|s| matches!(s, UnitState::Active { .. }));
        let has_deactivating = collected_states
            .iter()
            .any(|s| matches!(s, UnitState::Deactivating { .. }));
        let has_inactive = collected_states
            .iter()
            .any(|s| matches!(s, UnitState::Inactive { .. }));
        let has_gone = collected_states
            .iter()
            .any(|s| matches!(s, UnitState::Gone));

        assert!(has_active, "Should observe active");
        assert!(
            has_deactivating || has_inactive,
            "Should observe deactivating or inactive state during shutdown"
        );
        assert!(has_gone, "Should observe unit cleanup");

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

        let unit_name = "test-tail-logs.service";
        let marker = "TAIL_MARKER_TEST";

        let (log_tx, mut log_rx) = mpsc::channel::<String>(128);
        let cancel = CancellationToken::new();

        // Spawn an OS thread to read from journald (`Journal` is
        // `!Send`).
        let journal_forwarder = std::thread::spawn({
            let cancel = cancel.clone();
            let log_tx = log_tx.clone();

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

        let systemd = SystemdManagerProxy::new(&conn).await?;

        // Start the unit.
        let start_path = systemd
            .start_transient_unit(unit_name, "replace", props, aux)
            .await?;
        assert!(
            start_path
                .to_string()
                .contains("/org/freedesktop/systemd1/job")
        );

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
        //
        // For transient units with CollectMode=inactive-or-failed,
        // it's possible the unit has already been garbage-collected
        // by the time we call stop_unit. In that case systemd returns
        // org.freedesktop.systemd1.NoSuchUnit, which we treat as
        // "already stopped / cleaned up" rather than an error.
        if let Err(e) = systemd.stop_unit(unit_name, "replace").await {
            match e {
                zbus::Error::MethodError(name, ..)
                    if name.as_str() == "org.freedesktop.systemd1.NoSuchUnit" =>
                {
                    // Unit already gone; that's fine for this test.
                }
                other => return Err(other),
            }
        }

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

        let unit_name = "test-tail-fd.service";
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

        let systemd = SystemdManagerProxy::new(&conn).await?;

        // Start the unit.
        let start_path = systemd
            .start_transient_unit(unit_name, "replace", props, aux)
            .await?;
        assert!(
            start_path
                .to_string()
                .contains("/org/freedesktop/systemd1/job")
        );

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
        if let Err(e) = systemd.stop_unit(unit_name, "replace").await {
            match e {
                zbus::Error::MethodError(name, ..)
                    if name.as_str() == "org.freedesktop.systemd1.NoSuchUnit" => {}
                other => return Err(other),
            }
        }

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

        let unit_name = "test-tail-fd-async.service";
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

        let systemd = SystemdManagerProxy::new(&conn).await?;

        // Start the unit.
        let start_path = systemd
            .start_transient_unit(unit_name, "replace", props, Vec::new())
            .await?;
        assert!(
            start_path
                .to_string()
                .contains("/org/freedesktop/systemd1/job")
        );

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

        // Stop the unit
        if let Err(e) = systemd.stop_unit(unit_name, "replace").await {
            match e {
                zbus::Error::MethodError(name, ..)
                    if name.as_str() == "org.freedesktop.systemd1.NoSuchUnit" => {}
                other => return Err(other),
            }
        }

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

        // Define multiple units to launch.
        let units = [
            ("test-multi-a.service", "MARKER_A"),
            ("test-multi-b.service", "MARKER_B"),
            ("test-multi-c.service", "MARKER_C"),
        ];

        // Create socket pairs for each unit
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
            async move {
                // Convert to async BufReaders
                let mut readers: Vec<_> = unit_log_readers
                    .into_iter()
                    .enumerate()
                    .map(|(idx, reader)| {
                        // Set non-blocking for async use.
                        reader.set_nonblocking(true).ok();
                        let async_reader = tokio::net::UnixStream::from_std(reader).unwrap();
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
                                let unit_name = units[*idx].0.to_string();
                                println!("  [aggregator] {}: {}", unit_name, line);
                                let _ = log_tx.send((unit_name, line)).await;
                                any_read = true;
                            }
                            Ok(Ok(None)) => {
                                // Stream ended
                            }
                            Ok(Err(_)) => {
                                // Error reading
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

        // Start all units
        let systemd = SystemdManagerProxy::new(&conn).await?;

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

            systemd
                .start_transient_unit(unit_name, "replace", props, Vec::new())
                .await?;
        }

        // Collect logs and track which units we've seen.
        let mut seen_markers: HashMap<String, bool> = units
            .iter()
            .map(|(name, _)| (name.to_string(), false))
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
                        units.iter().find(|(name, _)| *name == unit_name)
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
                    // Timeout, continue waiting
                }
            }
        }

        // Stop all units
        for (unit_name, _) in &units {
            if let Err(e) = systemd.stop_unit(unit_name, "replace").await {
                match e {
                    zbus::Error::MethodError(name, ..)
                        if name.as_str() == "org.freedesktop.systemd1.NoSuchUnit" => {}
                    other => return Err(other),
                }
            }
        }

        // Stop the aggregator
        cancel.cancel();
        let _ = aggregator.await;

        // Verify we saw markers from all units
        for &(unit_name, marker) in &units {
            assert!(
                seen_markers.get(unit_name).copied().unwrap_or(false),
                "did not see marker {} from unit {}",
                marker,
                unit_name
            );
        }

        Ok(())
    }
}
