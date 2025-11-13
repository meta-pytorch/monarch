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
    use std::sync::Arc;
    use std::sync::Mutex;

    use futures::StreamExt;
    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;
    use zbus::Connection;

    use super::*;

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
            RealClock.sleep(tokio::time::Duration::from_secs(1)).await;
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
        RealClock
            .sleep(tokio::time::Duration::from_millis(100))
            .await;

        // Stop the unit.
        let stop_path = systemd.stop_unit(unit_name, "replace").await?;
        assert!(
            stop_path
                .to_string()
                .contains("/org/freedesktop/systemd1/job")
        );

        // Poll for unit cleanup.
        for attempt in 1..=5 {
            RealClock.sleep(tokio::time::Duration::from_secs(1)).await;
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
}
