/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Prepare connection addresses and host-side listeners for one proc launch.
//!
//! The [`BootstrapProcManager`](super::BootstrapProcManager) calls
//! [`ProcConnectionSetup::create_unix`] or
//! [`ProcConnectionSetup::create_hybrid_vsock`] from its `spawn` method.
//! This module selects the addresses for messages in both directions and
//! for readiness,
//! creates the additional host-side listeners needed by the selected
//! scheme, and returns the addresses, readiness receiver, and per-proc
//! resources to the manager. It does not launch the proc.
//!
//! A Monarch host (the [`Host<M>`](crate::host::Host) struct) is the software
//! manager that starts and supervises procs. It is not the physical machine.
//! It owns a [`Gateway`], a message router containing code and routing state,
//! not a socket. Socket listeners receive messages and pass them to this
//! router.
//!
//! The host uses [`BootstrapProcManager`](super::BootstrapProcManager) to
//! launch and track the procs handled here. Each proc needs three paths:
//!
//! - Send messages to the host's gateway.
//! - Report to the bootstrap manager that startup is complete.
//! - Receive messages from the host.
//!
//! A launcher declares how the host and its procs reach each other through
//! [`ProcReachability`](crate::proc_launcher::ProcReachability).
//!
//! [`VsockAddr`](hyperactor::channel::VsockAddr) defines the kernel-vsock
//! and VMM Unix-socket address forms used here. [`ChannelAddr::Alias`]
//! pairs the address the proc listens on with the address the host uses
//! to connect when they differ, as they do with Firecracker.
//!
//! ## Unix sockets
//!
//! With `Unix`, the proc connects directly to the host's existing Unix
//! listener, which passes messages to the gateway. This module creates a
//! separate Unix listener for its readiness report. The proc listens for
//! incoming messages on a Unix socket whose path is derived from its proc
//! ID in the host's socket directory.
//!
//! ## Firecracker's hybrid vsock
//!
//! With `HybridVsock`, the proc runs inside a VM. Monarch's host and the
//! Firecracker program run outside it. Firecracker bridges connections
//! between the guest's vsock sockets and Unix sockets outside the VM.
//!
//! This module gives each VM its own socket directory. It supplies a path
//! such as `/run/vm1/v.sock` to the launcher, which configures Firecracker
//! with that path. Firecracker uses a filename rule for guest-to-host
//! connections: append an underscore and the destination port to that path.
//! A guest connection to host vsock port `10000` therefore reaches
//! `/run/vm1/v.sock_10000`.
//!
//! Monarch creates a Unix socket and listens at that exact path. Messages
//! then travel from the guest to the host as follows:
//!
//! 1. The proc opens a vsock connection, specifying the host as the
//!    destination and `10000` as the destination port. It then writes
//!    bytes to that connection.
//! 2. Firecracker receives those bytes and writes them to its Unix-socket
//!    connection to `/run/vm1/v.sock_10000`.
//! 3. Monarch's receive loop reads messages from that socket and routes
//!    them through the host's shared [`Gateway`] router.
//!
//! A second VM uses a different path, such as `/run/vm2/v.sock_10000`. Both
//! guests use port `10000`, but each Firecracker connects to its own VM's
//! listener. Each listener has its own receive loop, started through
//! `Gateway::serve_rx`, and all use the same host gateway.
//! Passing a message from a receive loop to the gateway does not make
//! another socket connection.
//!
//! Readiness uses the same filename rule with port `10001`. This module
//! creates the Unix listener at `/run/vm1/v.sock_10001` and returns its
//! receiver to the bootstrap manager.
//! The manager starts a task that owns the receiver and a cloned proc
//! handle ([`BootstrapProcHandle`](super::BootstrapProcHandle)). The handle
//! shares readiness state with the manager and other handle holders.
//! The task reads the startup report, records `Ready` in that shared state,
//! and notifies tasks waiting for readiness.
//! The report does not pass through the gateway.
//!
//! In the other direction, the proc listens on guest vsock port `10002`.
//! Firecracker listens on the Unix socket `/run/vm1/v.sock`. The host
//! connects there and sends `CONNECT 10002`; Firecracker bridges the
//! connection to the proc's listener inside the VM.
//!
//! No `v.sock_10002` listener is needed. The `_PORT` filename rule applies
//! to guest-to-host connections. In this direction, the host specifies
//! the guest port in the `CONNECT` request instead.
//!
//! The bootstrap manager owns launch and cleanup: it passes the prepared
//! addresses to the proc and launcher, and manages the lifetime of the
//! per-proc listeners and socket directory.

use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use hyperactor::ActorRef;
use hyperactor::ProcAddr;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelRx;
use hyperactor::channel::ChannelTransport;
use hyperactor::gateway::Gateway;

use super::hybrid_vsock::HybridVsockProc;
use super::hybrid_vsock::ReleaseOutcome;
use crate::host::HostError;
use crate::proc_agent::ProcAgent;

/// Communication setup for launching one proc.
///
/// The proc gets addresses for connecting and listening. The bootstrap
/// manager keeps the readiness receiver and host-side resources.
pub(crate) struct ProcConnectionSetup {
    /// Addresses included in the proc's bootstrap payload.
    pub(crate) proc: ProcStartupAddresses,

    /// Host-owned receiver and resources for readiness and cleanup.
    pub(crate) host: HostLaunchResources,
}

/// Addresses supplied to the proc for connecting and listening.
pub(crate) struct ProcStartupAddresses {
    /// Where the proc connects to send messages to the host's gateway.
    /// With Firecracker, this is a [`Native`][native] address using
    /// [`HybridVsockProc::HOST_CID`] and [`HybridVsockProc::BACKEND_PORT`].
    /// The guest opens that vsock connection; Firecracker connects it to
    /// the host's Unix listener at `<uds>_BACKEND_PORT`.
    ///
    /// [native]: hyperactor::channel::VsockAddr::Native
    pub(crate) backend_addr: ChannelAddr,

    /// Where the proc connects to send its startup readiness report.
    /// With Firecracker, this is a [`Native`][native] address using
    /// [`HybridVsockProc::HOST_CID`] and [`HybridVsockProc::READINESS_PORT`].
    /// The guest opens that vsock connection; Firecracker connects it to
    /// the host's Unix listener at `<uds>_READINESS_PORT`.
    ///
    /// [native]: hyperactor::channel::VsockAddr::Native
    pub(crate) readiness_addr: ChannelAddr,

    /// Where the proc opens its message listener. For Unix, this is the
    /// proc-ID-derived socket in the manager's socket directory, and the
    /// host connects using that same address.
    /// With Firecracker, this holds a pair of addresses
    /// ([`ChannelAddr::Alias`]): the guest listens on
    /// [`Native { cid: CID_ANY, port: PROC_PORT }`][native], and the host
    /// connects through [`Hybrid { uds, port: PROC_PORT }`][hybrid]. Here
    /// `uds` is Firecracker's Unix socket path; [`HybridVsockProc::PROC_PORT`]
    /// is the guest's listening port.
    ///
    /// [native]: hyperactor::channel::VsockAddr::Native
    /// [hybrid]: hyperactor::channel::VsockAddr::Hybrid
    pub(crate) serve_addr: ChannelAddr,
}

/// Host-side state prepared for launch, not sent to the proc.
pub(crate) struct HostLaunchResources {
    /// Host-side receiver for the proc's address and agent reference.
    /// With Firecracker, it receives through the Unix listener at
    /// `<uds>_READINESS_PORT` ([`HybridVsockProc::READINESS_PORT`]).
    /// The reported address is [`Hybrid { uds, port: PROC_PORT }`][hybrid]
    /// ([`HybridVsockProc::PROC_PORT`]), which the host uses to reach
    /// the guest's message listener.
    /// Moved into the manager's readiness task.
    ///
    /// [hybrid]: hyperactor::channel::VsockAddr::Hybrid
    pub(crate) readiness_rx: ChannelRx<(ChannelAddr, ActorRef<ProcAgent>)>,

    /// Kept by the manager until launch fails or the proc exits.
    pub(crate) resources: ProcResources,
}

impl ProcConnectionSetup {
    /// Maximum time to wait for a host-side receive task to finish
    /// during cleanup.
    pub(crate) const RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

    /// Prepares communication for a proc that will use Unix sockets.
    ///
    /// The proc will send messages to the host's existing listener at
    /// `host_backend`. This function chooses where the proc will listen for
    /// incoming messages and opens a host listener for its readiness report.
    ///
    /// Returns the three addresses for the proc's bootstrap payload and the
    /// readiness receiver the manager will use. The proc opens its own message
    /// listener during startup, at the path derived from its ID in
    /// `socket_dir`.
    pub(super) fn create_unix(
        proc_id: &ProcAddr,
        socket_dir: &Path,
        host_backend: ChannelAddr,
    ) -> Result<Self, HostError> {
        // Host -> proc: choose where the proc will listen for messages.
        // This computes an address; the proc opens the listener during startup.
        let serve_addr = super::local_proc_addr(socket_dir, proc_id.id())
            .map_err(|error| HostError::ProcessConfigurationFailure(proc_id.clone(), error))?
            .0;

        // Proc -> manager: open the host listener for the readiness report.
        let (readiness_addr, readiness_rx) =
            channel::serve(ChannelAddr::any(ChannelTransport::Unix))?;

        Ok(Self {
            proc: ProcStartupAddresses {
                // Proc -> host: use the host's existing message listener.
                backend_addr: host_backend,
                readiness_addr,
                serve_addr,
            },
            host: HostLaunchResources {
                readiness_rx,
                resources: ProcResources::Unix,
            },
        })
    }

    /// Prepares communication for a proc inside a Firecracker VM.
    ///
    /// Creates the VM's socket directory under `socket_dir` and a Unix
    /// message listener on the host. Firecracker bridges the proc's vsock
    /// connection to this listener. The listener's receive task calls
    /// `gateway` to route messages. A second Unix listener on the host
    /// receives the proc's readiness report.
    ///
    /// Returns the three addresses for the proc's bootstrap payload. The
    /// manager keeps the readiness receiver, socket directory, and handle
    /// for the host's message receive task. The proc opens its own vsock
    /// message listener during startup.
    pub(super) fn create_hybrid_vsock(
        proc_id: &ProcAddr,
        socket_dir: &Path,
        gateway: &Gateway,
    ) -> Result<Self, HostError> {
        // Proc -> host: create the VM's socket directory and host listener
        // at <uds>_10000, with a receive task that routes through `gateway`.
        let hybrid_vsock = HybridVsockProc::create(proc_id, socket_dir, gateway)?;

        // Proc -> manager: listen at <uds>_10001 for the readiness report.
        // Firecracker bridges the guest's connection to host port 10001 here.
        let (_, readiness_rx) = channel::serve(hybrid_vsock.readiness_listen_addr())?;

        // Give the proc host vsock ports 10000 and 10001 to connect to.
        // For host -> proc, pair a Native listen address on guest port 10002
        // with the Hybrid address the host uses to connect through Firecracker.
        let (backend_addr, readiness_addr, serve_addr) = hybrid_vsock.guest_addrs();

        // Return the proc's startup addresses separately from the host's
        // readiness receiver, socket directory, and receive-task handle.
        Ok(Self {
            proc: ProcStartupAddresses {
                backend_addr,
                readiness_addr,
                serve_addr,
            },
            host: HostLaunchResources {
                readiness_rx,
                resources: ProcResources::HybridVsock(hybrid_vsock),
            },
        })
    }
}

/// Host resources the bootstrap manager keeps until launch fails
/// or the proc exits.
pub(crate) enum ProcResources {
    /// Uses the host's existing message listener; nothing extra to retain here.
    Unix,

    /// A directory for this VM's sockets and a handle to the mailbox
    /// server task serving `<dir>/v.sock_BACKEND_PORT` on the host
    /// ([`HybridVsockProc::BACKEND_PORT`]).
    ///
    /// Firecracker bridges the proc's vsock connection to this Unix socket.
    /// The task reads incoming messages and passes them to the host's gateway.
    ///
    /// See [`HybridVsockProc`] for how these resources are released.
    HybridVsock(HybridVsockProc),
}

impl ProcResources {
    /// The VMM socket path supplied to the launcher for a hybrid-vsock proc.
    pub(crate) fn vsock_uds_path(&self) -> Option<PathBuf> {
        match self {
            Self::Unix => None,
            Self::HybridVsock(resources) => Some(resources.uds()),
        }
    }

    /// Cleans up host resources after a failed launch or a proc exit.
    ///
    /// For hybrid vsock, uses the `MailboxServerHandle` to stop the task
    /// serving `<dir>/v.sock_BACKEND_PORT` ([`HybridVsockProc::BACKEND_PORT`]),
    /// which routes incoming messages through the host's gateway. Waits up to
    /// [`ProcConnectionSetup::RELEASE_TIMEOUT`] for the task to finish
    /// draining, then removes the VM's socket directory.
    ///
    /// Returns `Some` with the release outcome for hybrid vsock, or `None`
    /// for Unix, which has no resources retained here.
    pub(crate) async fn release(self) -> Option<ReleaseOutcome> {
        match self {
            Self::Unix => None,
            Self::HybridVsock(resources) => Some(resources.release().await),
        }
    }
}
