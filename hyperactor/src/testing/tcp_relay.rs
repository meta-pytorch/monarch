/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A TCP relay for simulating peer restarts and connection loss.

use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::watch;
use tokio::task::AbortHandle;

use crate::channel::ChannelAddr;

/// An IPv4 loopback TCP address with an ephemeral port. OSS CI has no
/// IPv6 loopback, which `TcpMode::Localhost` uses.
pub fn tcp_loopback() -> ChannelAddr {
    ChannelAddr::Tcp(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
}

/// A TCP address on which nothing listens.
pub fn unreachable_tcp_addr() -> ChannelAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    ChannelAddr::Tcp(listener.local_addr().unwrap())
}

/// A TCP relay that forwards each connection to its current target.
/// Aborting its connections drops them without a channel-level close, as
/// a crashed peer or a network failure would.
pub struct TcpRelay {
    /// The address that clients dial.
    pub addr: ChannelAddr,
    target: Arc<Mutex<SocketAddr>>,
    connections: Arc<Mutex<Vec<AbortHandle>>>,
    accepted: watch::Receiver<usize>,
}

impl TcpRelay {
    /// Start a relay to `target`, which must be a TCP address.
    pub async fn start(target: &ChannelAddr) -> Self {
        let ChannelAddr::Tcp(target) = target else {
            panic!("relay target should be a TCP address, got {target}")
        };
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let addr = ChannelAddr::Tcp(listener.local_addr().unwrap());
        let target = Arc::new(Mutex::new(*target));
        let connections = Arc::new(Mutex::new(Vec::new()));
        let (accepted_tx, accepted) = watch::channel(0);
        {
            let target = Arc::clone(&target);
            let connections = Arc::clone(&connections);
            tokio::spawn(async move {
                while let Ok((mut inbound, _)) = listener.accept().await {
                    accepted_tx.send_modify(|count| *count += 1);
                    let target = *target.lock().unwrap();
                    let connection = tokio::spawn(async move {
                        if let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await {
                            let _ =
                                tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                        }
                    });
                    connections.lock().unwrap().push(connection.abort_handle());
                }
            });
        }
        Self {
            addr,
            target,
            connections,
            accepted,
        }
    }

    /// Drop every relayed connection and forward new ones to `target`.
    pub fn restart_peer_at(&mut self, target: &ChannelAddr) {
        let ChannelAddr::Tcp(target) = target else {
            panic!("relay target should be a TCP address, got {target}")
        };
        *self.target.lock().unwrap() = *target;
        self.drop_connections();
    }

    /// Drop every relayed connection, keeping the target.
    pub fn drop_connections(&mut self) {
        self.accepted.borrow_and_update();
        for connection in self.connections.lock().unwrap().drain(..) {
            connection.abort();
        }
    }

    /// Wait until the relay accepts a connection after the last
    /// [`Self::restart_peer_at`] or [`Self::drop_connections`], as a
    /// client that observed the disconnect reconnects.
    pub async fn reconnected(&mut self) {
        self.accepted.changed().await.unwrap();
    }
}
