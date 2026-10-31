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
//! for readiness, creates the additional host-side listeners needed by the
//! selected scheme, and returns the addresses, readiness receiver, and
//! per-proc resources to the manager.
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
//! With `Unix`, the proc and host connect directly to each other's Unix
//! sockets. There is no VMM bridge between them. The diagrams show the
//! [`ProcStartupAddresses`] prepared here for the proc's bootstrap payload.
//!
//! ### Proc to host: messages
//!
//! The proc connects to the host's existing message listener at
//! `backend_addr`. This module does not create another message listener
//! on the host.
//!
//! ```text
//! PROC                                      MONARCH HOST
//! ────                                      ────────────
//!
//! backend_addr = host's existing Unix socket
//!
//! proc ─────────── Unix ────────────────► existing listener
//!                                                │
//!                                           receive loop
//!                                                │
//!                                           host Gateway
//! ```
//!
//! ### Proc to host: readiness report
//!
//! This module creates a separate host Unix listener for the proc's
//! readiness report and returns its receiver to the bootstrap manager.
//!
//! ```text
//! PROC                                      MONARCH HOST
//! ────                                      ────────────
//!
//! readiness_addr = new host Unix socket
//!
//! proc ─────────── Unix ────────────────► readiness listener
//!                                                │
//!                                          readiness task
//!                                                │
//!                                      proc handle marked ready
//! ```
//!
//! The manager's readiness task records startup completion in the proc
//! handle's shared state and notifies tasks waiting for it. The report
//! does not pass through the gateway.
//!
//! ### Host to proc: messages
//!
//! This module derives the proc's message socket path from its ID in
//! the manager's `socket_dir`. The proc opens that listener during
//! startup; the host connects using the same address.
//!
//! ```text
//! PROC                                      MONARCH HOST
//! ────                                      ────────────
//!
//! serve_addr = proc's Unix socket
//!
//! proc listener ◄───────── Unix ───────── host
//! ```
//!
//! ## Firecracker's hybrid vsock
//!
//! With `HybridVsock`, the proc runs inside a VM. Monarch's host and the
//! Firecracker program run outside it. Firecracker bridges connections
//! between the guest's vsock sockets and Unix sockets outside the VM.
//!
//! Each VM gets a socket directory. Below, `<uds>` stands for
//! `<socket_dir>/vm-XXXX/v.sock`. Monarch supplies this path to the launcher
//! in `LaunchOptions.vsock_uds_path`; the launcher configures Firecracker's
//! vsock device with it as `uds_path`. The addresses shown are the
//! [`ProcStartupAddresses`] prepared here for the proc's bootstrap payload.
//!
//! ### Proc to host: messages
//!
//! Firecracker maps a guest connection to host port `P` to `<uds>_P`.
//! Monarch listens at `<uds>_10000`, so the proc reaches that listener by
//! opening a vsock connection to the host (CID `2`) on port `10000`.
//!
//! ```text
//! backend_addr = Native { cid: 2, port: 10000 }
//!
//!  GUEST VM                                      HOST
//!                 ║
//! ┌───────┐       ║   ┌───────────────────────┐      ┌───────────────────────┐
//! │ proc  │       ║   │ Firecracker           │      │ Monarch host          │
//! │       │       ║   │                       │      │                       │
//! │       │ vsock ║   │  vsock device         │      │                       │
//! │     ●─┼───────╫───┼──►● cid 2, port 10000 │      │                       │
//! │       │       ║   │   ┆                   │      │                       │
//! └───────┘       ║   │   ┆ relays bytes      │      │                       │
//!                 ║   │   ┆                   │      │                       │
//!                 ║   │   ┆                   │ Unix │                       │
//!                 ║   │   ●───────────────────┼──────┼─►● <uds>_10000        │
//!                 ║   │  Unix socket          │      │  ┆                    │
//!                 ║   └───────────────────────┘      │  receive loop         │
//!                 ║                                  │  ┆                    │
//!                 ║                                  │  host Gateway         │
//!                 ║                                  └───────────────────────┘
//!
//! ●──►●  a connection between two endpoints; the arrow points at the listener
//! ║      the VM boundary: guest kernel on the left, host on the right
//! ┆      inside one process, not a connection
//! ```
//!
//! Each VM has a different `<uds>`, so guests can use the same ports.
//! Each message listener has its own receive loop, started through
//! `Gateway::serve_rx`, and all route through the host's shared [`Gateway`].
//! This routing does not open another socket connection.
//!
//! ### Proc to host: readiness report
//!
//! This module creates a second Unix listener at `<uds>_10001` for the
//! proc's readiness report. Firecracker uses the same filename rule to
//! connect the guest to this listener.
//!
//! ```text
//! readiness_addr = Native { cid: 2, port: 10001 }
//!
//!  GUEST VM                                      HOST
//!                 ║
//! ┌───────┐       ║   ┌───────────────────────┐      ┌───────────────────────┐
//! │ proc  │       ║   │ Firecracker           │      │ Monarch host          │
//! │       │       ║   │                       │      │                       │
//! │       │ vsock ║   │  vsock device         │      │                       │
//! │     ●─┼───────╫───┼──►● cid 2, port 10001 │      │                       │
//! │       │       ║   │   ┆                   │      │                       │
//! └───────┘       ║   │   ┆ relays bytes      │      │                       │
//!                 ║   │   ┆                   │      │                       │
//!                 ║   │   ┆                   │ Unix │                       │
//!                 ║   │   ●───────────────────┼──────┼─►● <uds>_10001        │
//!                 ║   │  Unix socket          │      │  ┆                    │
//!                 ║   └───────────────────────┘      │  readiness task       │
//!                 ║                                  │  ┆                    │
//!                 ║                                  │  handle marked ready  │
//!                 ║                                  └───────────────────────┘
//! ```
//!
//! The readiness task owns the receiver and a cloned
//! [`BootstrapProcHandle`](super::BootstrapProcHandle). It records readiness
//! in the state shared by handle holders and notifies tasks waiting for it.
//! The readiness report does not pass through the gateway.
//!
//! ### Host to proc: messages
//!
//! The proc listens on guest vsock port `10002`. The host connects to
//! Firecracker's own Unix listener at `<uds>`, not to a Monarch listener.
//! The vsock transport's `hybrid_connect` opens this connection and
//! performs the one-time handshake shown below.
//!
//! ```text
//! GUEST VM                 FIRECRACKER                 MONARCH HOST
//! ────────                 ───────────                 ────────────
//!
//! serve_addr = Alias {
//!     bind_to: Native { cid: CID_ANY, port: 10002 }, // guest listens
//!     dial_to: Hybrid { uds, port: 10002 },         // host connects
//! }
//!
//! Opening the connection, once per connection:
//!
//!  guest listener             <uds>                   host
//!     accept()        Firecracker listener      hybrid_connect()
//!         │                     │                       │
//!   │     │                     │◄── connect(<uds>) ────┤
//!   │     │                     │◄── CONNECT 10002 ─────┤
//!   │     │◄── connect vsock ───┤                       │
//!   │     │                     ├─── OK <port> ────────►│
//!   ▼     │                     │                       │ returns
//!  time
//!
//! Then, over that same connection:
//!
//!  guest listener            bridge                   host
//!         │◄── vsock ───────────┤◄── Unix: messages ────┤
//! ```
//!
//! No `<uds>_10002` listener is needed: the `_PORT` filename rule is for
//! guest-to-host connections; the host names the guest port in `CONNECT`.
//!
//! The bootstrap manager owns launch and cleanup: it passes the prepared
//! addresses to the proc and launcher, and manages the lifetime of the
//! per-proc listeners and socket directory.
//!
//! ## Invariants (RCH-*)
//!
//! Code enforcing these guarantees is tagged `// RCH-N`. Each witness
//! names the IDs it checks and refers back to this registry.
//!
//! - **RCH-1 (Unix addresses):** Use the supplied host backend address,
//!   create a Unix readiness listener, and derive the proc's listen address
//!   from its ID in the manager's socket directory. Retain no VM resources.
//! - **RCH-2 (hybrid addresses and routing):** The backend and readiness
//!   addresses use native vsock on [`HybridVsockProc::HOST_CID`], at
//!   [`HybridVsockProc::BACKEND_PORT`] and [`HybridVsockProc::READINESS_PORT`].
//!   Their host Unix listeners use the same VM path and corresponding ports.
//!   The message listener routes through the supplied gateway; the readiness
//!   listener receives startup reports. The proc's serve alias pairs a native
//!   listener on [`HybridVsockProc::PROC_PORT`] with a hybrid address using
//!   that same port and the VMM socket path supplied to the launcher.
//! - **RCH-3 (separate VM sockets):** Each hybrid setup gets its own directory
//!   under the manager's socket directory. Using the same reserved ports for
//!   different VMs must not give them the same host socket paths.
//! - **RCH-4 (invalid paths fail during preparation):** Reject socket paths
//!   that do not fit a Unix socket address, releasing partial VM resources.
//!   The manager prepares connections before calling the launcher.
//! - **RCH-5 (resource lifetime):** The setup owns the per-VM resources until
//!   the manager transfers them to its exit monitor. Launch failure releases
//!   them; cancellation or unwinding drops them. Proc exit, including loss of
//!   the launcher's exit sender, releases them. Cleanup signals the message
//!   receive task to stop and releases the socket directory.
//! - **RCH-6 (bounded message-task join):** Explicit release signals the
//!   message receive task to stop, then waits at most
//!   [`ProcConnectionSetup::RELEASE_TIMEOUT`] for it to finish. Release drops
//!   the socket directory whether the join completes or times out. A timeout
//!   does not mean the task has finished. Drop signals stop and releases the
//!   directory without awaiting the task; it does not perform a bounded join.
//! - **RCH-7 (readiness listener lifetime):** The readiness receiver moves into
//!   the manager's readiness task. The task stops when the report arrives or
//!   the proc becomes terminal, then waits at most
//!   [`ProcConnectionSetup::RELEASE_TIMEOUT`] for the listener to stop. A
//!   report that arrives after the proc is terminal does not make it ready.

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
        // RCH-1, RCH-4: choose and validate the proc's Unix listen address.
        // This computes an address; the proc opens the listener during startup.
        let serve_addr = super::local_proc_addr(socket_dir, proc_id.id())
            .map_err(|error| HostError::ProcessConfigurationFailure(proc_id.clone(), error))?
            .0;

        // RCH-1: open the host listener for the readiness report.
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
        // RCH-2, RCH-3: create the VM's socket directory and host listener
        // at <uds>_10000, with a receive task that routes through `gateway`.
        let hybrid_vsock = HybridVsockProc::create(proc_id, socket_dir, gateway)?;

        // RCH-2: listen at <uds>_10001 for the readiness report.
        // Firecracker bridges the guest's connection to host port 10001 here.
        let (_, readiness_rx) = channel::serve(hybrid_vsock.readiness_listen_addr())?;

        // RCH-2: give the proc host vsock ports 10000 and 10001 to connect to.
        // For host -> proc, pair a Native listen address on guest port 10002
        // with the Hybrid address the host uses to connect through Firecracker.
        let (backend_addr, readiness_addr, serve_addr) = hybrid_vsock.guest_addrs();

        // RCH-5: return the proc's startup addresses separately from the host's
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
        // RCH-5, RCH-6: only hybrid setups retain a per-VM server to release.
        match self {
            Self::Unix => None,
            Self::HybridVsock(resources) => Some(resources.release().await),
        }
    }
}
