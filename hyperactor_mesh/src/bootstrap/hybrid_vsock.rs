/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Host-side resources for procs that run behind a hybrid-vsock VMM.
//!
//! A guest connection to the host's vsock port P arrives on the host as a
//! connection to `<uds>_P`, and the host reaches guest port P by connecting
//! to `<uds>` (Firecracker's hybrid vsock). Each such proc gets a private
//! directory holding `<uds>`'s neighbors: the backend ingress and readiness
//! report listeners.
//!
//! ```text
//! <socket_dir>/vm-XXXX/   HybridVsockProc::dir; removed by release or drop
//! ├── v.sock              Firecracker's listener, for host-to-guest CONNECT
//! ├── v.sock_10000        Monarch's message listener, served by a task that
//! │                       routes to the Gateway; HybridVsockProc::ingress
//! │                       holds the task's handle
//! ├── v.sock_10001        Monarch's readiness listener; its receiver is in
//! │                       the bootstrap manager's readiness task
//! └── v.sock_P            any other guest-to-host port the launcher serves
//! ```
//!
//! ```text
//!              ingress receive task                       directory
//! release()    signal stop, wait at most RELEASE_TIMEOUT  removed
//! drop         signal stop, no wait                       removed
//! ```
//!
//! The connection and resource guarantees (RCH-*) are defined in
//! [`super::reachability`]; tagged sites below enforce that contract.

use std::os::unix::net::SocketAddr as StdSocketAddr;
use std::path::Path;
use std::path::PathBuf;

use hyperactor::ProcAddr;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::VsockAddr;
use hyperactor::gateway::Gateway;
use hyperactor::mailbox::MailboxServerHandle;
use tempfile::TempDir;

use super::reachability::ProcConnectionSetup;
use crate::host::HostError;

/// Whether releasing a hybrid-vsock proc joined its ingress server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReleaseOutcome {
    /// The ingress server finished draining.
    Joined,
    /// The ingress server did not finish draining within
    /// [`ProcConnectionSetup::RELEASE_TIMEOUT`].
    TimedOut,
}

/// Host-side socket directory and receive-task handle for one VM.
///
/// Held by [`ProcResources::HybridVsock`][resources].
/// [`Self::release`] stops the task, waits for it to finish draining,
/// and removes the directory. Dropping this value stops the task and
/// removes the directory without waiting.
///
/// [resources]: super::reachability::ProcResources::HybridVsock
pub(crate) struct HybridVsockProc {
    /// Directory reserved for this VM's Unix sockets.
    dir: TempDir,

    /// Handle to the mailbox server task serving `<dir>/v.sock_BACKEND_PORT`
    /// ([`Self::BACKEND_PORT`]).
    /// Firecracker bridges the proc's vsock connection to this socket.
    /// The task reads messages and passes them to the host's gateway.
    ingress: Option<MailboxServerHandle>,
}

impl HybridVsockProc {
    /// The host port on which the guest reaches the host's backend.
    pub(crate) const BACKEND_PORT: u32 = 10000;
    /// The host port on which the guest reports readiness.
    pub(crate) const READINESS_PORT: u32 = 10001;
    /// The guest port on which the proc serves.
    pub(crate) const PROC_PORT: u32 = 10002;
    /// The host's vsock CID as seen from a guest (`VMADDR_CID_HOST`).
    pub(crate) const HOST_CID: u32 = 2;

    /// Creates a `vm-*` directory under `socket_dir`, checks that every
    /// per-port socket path fits, and serves the backend ingress into
    /// `gateway`.
    pub(crate) fn create(
        proc_id: &ProcAddr,
        socket_dir: &Path,
        gateway: &Gateway,
    ) -> Result<Self, HostError> {
        let config_failure =
            |err: anyhow::Error| HostError::ProcessConfigurationFailure(proc_id.clone(), err);
        // RCH-3: a distinct directory gives each VM distinct host socket paths.
        let dir = tempfile::Builder::new()
            .prefix("vm-")
            .tempdir_in(socket_dir)
            .map_err(|err| config_failure(err.into()))?;
        let uds = dir.path().join("v.sock");
        // RCH-4: validate the longest reserved suffix before opening
        // a listener.
        // `PROC_PORT` has the longest suffix of the reserved ports.
        port_path(&uds, Self::PROC_PORT).map_err(config_failure)?;

        let backend = port_path(&uds, Self::BACKEND_PORT).map_err(config_failure)?;
        // RCH-2: this VM's message listener uses the supplied host gateway.
        let (_, ingress) = gateway
            .serve_ingress(backend)
            .map_err(|err| config_failure(err.into()))?;
        Ok(Self {
            dir,
            ingress: Some(ingress),
        })
    }

    /// `<dir>/v.sock`, the VMM's hybrid-vsock socket.
    pub(crate) fn uds(&self) -> PathBuf {
        self.dir.path().join("v.sock")
    }

    /// Host Unix-socket address at `<uds>_READINESS_PORT`, where Monarch
    /// listens for this proc's readiness report. Firecracker bridges the
    /// guest's connection to host vsock port [`Self::READINESS_PORT`]
    /// to this socket.
    pub(crate) fn readiness_listen_addr(&self) -> ChannelAddr {
        port_path(&self.uds(), Self::READINESS_PORT)
            .expect("should fit: create checked the longest per-port path")
    }

    /// The payload's `(backend_addr, callback_addr, serve_addr)`: the host's
    /// message and readiness ports on [`Self::HOST_CID`], and a serve address
    /// that binds the guest's `PROC_PORT` while advertising the hybrid address
    /// the host dials.
    pub(crate) fn guest_addrs(&self) -> (ChannelAddr, ChannelAddr, ChannelAddr) {
        // RCH-2: host ports are native; the proc's listen/connect pair shares
        // one guest port and this VM's VMM socket path.
        let host_port = |port| {
            ChannelAddr::Vsock(VsockAddr::Native {
                cid: Self::HOST_CID,
                port,
            })
        };
        let serve_addr = ChannelAddr::Alias {
            dial_to: Box::new(ChannelAddr::Vsock(VsockAddr::Hybrid {
                uds: self.uds(),
                port: Self::PROC_PORT,
            })),
            bind_to: Box::new(ChannelAddr::Vsock(VsockAddr::Native {
                cid: VsockAddr::CID_ANY,
                port: Self::PROC_PORT,
            })),
        };
        (
            host_port(Self::BACKEND_PORT),
            host_port(Self::READINESS_PORT),
            serve_addr,
        )
    }

    /// Stops the ingress server, waits up to
    /// [`ProcConnectionSetup::RELEASE_TIMEOUT`] for it to finish draining, then
    /// removes the directory.
    pub(crate) async fn release(mut self) -> ReleaseOutcome {
        // RCH-6: bound the join, not the lifetime of a task that ignores stop.
        let Some(mut ingress) = self.ingress.take() else {
            return ReleaseOutcome::Joined;
        };
        ingress.stop("hybrid vsock proc released");
        match tokio::time::timeout(ProcConnectionSetup::RELEASE_TIMEOUT, &mut ingress).await {
            Ok(_) => ReleaseOutcome::Joined,
            Err(_) => {
                tracing::warn!(
                    dir = %self.dir.path().display(),
                    "hybrid vsock ingress did not finish draining within {:?}",
                    ProcConnectionSetup::RELEASE_TIMEOUT
                );
                ReleaseOutcome::TimedOut
            }
        }
    }
}

impl Drop for HybridVsockProc {
    fn drop(&mut self) {
        // RCH-5, RCH-6: Drop is synchronous, so it stops the task without
        // awaiting it; TempDir releases the directory.
        if let Some(ingress) = &self.ingress {
            ingress.stop("hybrid vsock proc dropped");
        }
    }
}

/// The Unix socket address `<uds>_<port>`.
fn port_path(uds: &Path, port: u32) -> anyhow::Result<ChannelAddr> {
    let mut path = uds.as_os_str().to_owned();
    path.push(format!("_{port}"));
    let path = PathBuf::from(path);
    let addr = StdSocketAddr::from_pathname(&path).map_err(|err| {
        anyhow::anyhow!(
            "hybrid vsock socket path {} ({} bytes) does not fit a unix socket address: {err}",
            path.display(),
            path.as_os_str().len()
        )
    })?;
    Ok(addr.into())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyperactor::mailbox::MailboxServerError;
    use tokio::sync::oneshot;
    use tokio::sync::watch;

    use super::*;

    /// RCH-6 ([registry](super::super::reachability)):
    /// a task that ignores stop cannot make release wait forever.
    #[tokio::test(start_paused = true)]
    async fn test_release_times_out_on_pending_join() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_owned();
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let (done_tx, done_rx) = oneshot::channel::<()>();
        let (stopped_tx, _stopped_rx) = watch::channel(false);
        // The task ignores the stop signal until the gate opens.
        let task = tokio::spawn(async move {
            let _ = gate_rx.await;
            let _ = done_tx.send(());
            Ok::<(), MailboxServerError>(())
        });
        let proc = HybridVsockProc {
            dir,
            ingress: Some(MailboxServerHandle::from_parts(task, stopped_tx)),
        };

        assert_eq!(proc.release().await, ReleaseOutcome::TimedOut);
        assert!(!path.exists(), "release must remove the directory");

        gate_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), done_rx)
            .await
            .expect("the injected task should complete once the gate opens")
            .unwrap();
    }

    /// RCH-4 ([registry](super::super::reachability)):
    /// reject an overlong socket path while accepting a fitting one.
    #[test]
    fn test_port_path_rejects_overlong_paths() {
        let long = PathBuf::from(format!("/{}/v.sock", "d".repeat(120)));
        assert!(port_path(&long, HybridVsockProc::PROC_PORT).is_err());
        assert!(port_path(Path::new("/tmp/v.sock"), HybridVsockProc::PROC_PORT).is_ok());
    }
}
