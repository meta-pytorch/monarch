/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::UdpSocket as StdUdpSocket;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use chrysalis::DatagramSocketSet;
use chrysalis::Pid;
use chrysalis::UdpSocket;
use chrysalis::UnixDatagramSocket;

use crate::address::CarrierAddr;
use crate::address::CarrierSpec;
use crate::address::format_pid;

/// One multi-carrier binding for a demo node.
pub(crate) struct BoundSockets {
    pub(crate) socket: Arc<DatagramSocketSet>,
    pub(crate) address: CarrierAddr,
    unix_paths: Vec<PathBuf>,
}

impl BoundSockets {
    /// Binds the requested public carrier plus a hidden carrier of the other kind.
    pub async fn bind(
        spec: &CarrierSpec,
        pid: Pid,
        advertise_to: Option<&CarrierAddr>,
    ) -> Result<Self> {
        match spec {
            CarrierSpec::Udp(authority) => Self::bind_udp(authority, pid, advertise_to).await,
            CarrierSpec::UnixAuto => {
                let path = generated_path(pid, "carrier");
                Self::bind_unix(&path, true).await
            }
            CarrierSpec::Unix(path) => Self::bind_unix(path, false).await,
        }
    }

    async fn bind_udp(
        authority: &str,
        pid: Pid,
        advertise_to: Option<&CarrierAddr>,
    ) -> Result<Self> {
        let requested = resolve_udp(authority).await?;
        let primary = Arc::new(UdpSocket::bind(requested).await?);
        let helper_path = generated_path(pid, "unix-helper");
        let helper = Arc::new(UnixDatagramSocket::bind(&helper_path)?);
        let address = CarrierAddr::Udp(advertised_udp_address(primary.address(), advertise_to)?);
        Ok(Self {
            socket: Arc::new(DatagramSocketSet::new(primary, vec![helper])?),
            address,
            unix_paths: vec![helper_path],
        })
    }

    async fn bind_unix(path: &Path, remove_on_drop: bool) -> Result<Self> {
        let primary = Arc::new(UnixDatagramSocket::bind(path)?);
        let helper =
            Arc::new(UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?);
        let address = CarrierAddr::Unix(path.to_owned());
        Ok(Self {
            socket: Arc::new(DatagramSocketSet::new(primary, vec![helper])?),
            address,
            unix_paths: remove_on_drop
                .then(|| path.to_owned())
                .into_iter()
                .collect(),
        })
    }
}

fn advertised_udp_address(
    bound: SocketAddr,
    advertise_to: Option<&CarrierAddr>,
) -> io::Result<SocketAddr> {
    if !bound.ip().is_unspecified() {
        return Ok(bound);
    }
    let Some(CarrierAddr::Udp(peer)) = advertise_to else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "wildcard UDP carrier requires a UDP peer to derive its advertised address",
        ));
    };
    if bound.is_ipv4() != peer.is_ipv4() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "wildcard UDP carrier and peer must use the same address family",
        ));
    }
    let unspecified = match peer {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    };
    let probe = StdUdpSocket::bind(unspecified)?;
    probe.connect(peer)?;
    Ok(SocketAddr::new(probe.local_addr()?.ip(), bound.port()))
}

impl Drop for BoundSockets {
    fn drop(&mut self) {
        for path in &self.unix_paths {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => eprintln!("failed to remove {}: {error}", path.display()),
            }
        }
    }
}

fn generated_path(pid: Pid, role: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "chrysalis-demo-{}-{}-{role}.sock",
        format_pid(pid),
        std::process::id(),
    ))
}

async fn resolve_udp(authority: &str) -> Result<SocketAddr> {
    tokio::net::lookup_host(authority)
        .await
        .with_context(|| format!("resolve UDP carrier {authority}"))?
        .next()
        .with_context(|| format!("UDP carrier resolved no addresses: {authority}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_explicit_udp_advertisement() {
        let bound = "127.0.0.1:1234".parse().expect("parse bound address");
        let peer = CarrierAddr::Udp("127.0.0.1:26600".parse().expect("parse peer address"));

        assert_eq!(
            advertised_udp_address(bound, Some(&peer)).expect("derive advertisement"),
            bound
        );
    }

    #[test]
    fn derives_wildcard_udp_advertisement_from_peer_route() {
        let bound = "0.0.0.0:1234".parse().expect("parse bound address");
        let peer = CarrierAddr::Udp("127.0.0.1:26600".parse().expect("parse peer address"));

        assert_eq!(
            advertised_udp_address(bound, Some(&peer)).expect("derive advertisement"),
            "127.0.0.1:1234".parse().expect("parse expected address")
        );
    }

    #[test]
    fn rejects_wildcard_udp_advertisement_for_another_address_family() {
        let bound = "[::]:1234".parse().expect("parse bound address");
        let peer = CarrierAddr::Udp("127.0.0.1:26600".parse().expect("parse peer address"));

        assert!(advertised_udp_address(bound, Some(&peer)).is_err());
    }

    #[test]
    fn rejects_wildcard_udp_advertisement_without_a_peer() {
        let bound = "[::]:1234".parse().expect("parse bound address");

        assert!(advertised_udp_address(bound, None).is_err());
    }
}
