/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::ffi::OsStr;
use std::fmt;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use chrysalis::DatagramAddr;
use chrysalis::Pid;
use chrysalis::PidPrefix;
use chrysalis::UdpSocket;
use chrysalis::UnixDatagramSocket;
use chrysalis_resolver::ResolverSpec;

/// A carrier binding requested on the command line.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CarrierSpec {
    Udp(String),
    UnixAuto,
    Unix(PathBuf),
}

impl FromStr for CarrierSpec {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(authority) = value.strip_prefix("udp://") {
            if authority.is_empty() {
                bail!("UDP carrier requires host:port");
            }
            return Ok(Self::Udp(authority.into()));
        }
        if let Some(path) = value.strip_prefix("unix://") {
            if path == "*" {
                return Ok(Self::UnixAuto);
            }
            if path.is_empty() {
                bail!("Unix carrier requires a path or *");
            }
            return Ok(Self::Unix(path.into()));
        }
        bail!("carrier must use udp:// or unix://")
    }
}

/// One concrete, printable carrier address.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CarrierAddr {
    Udp(SocketAddr),
    Unix(PathBuf),
}

impl CarrierAddr {
    /// Converts this address to its transport representation.
    pub(crate) fn datagram_addr(&self) -> DatagramAddr {
        match self {
            Self::Udp(address) => UdpSocket::datagram_addr(*address),
            Self::Unix(path) => UnixDatagramSocket::datagram_addr(path),
        }
    }
}

impl fmt::Display for CarrierAddr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Udp(address) => write!(formatter, "udp://{address}"),
            Self::Unix(path) => write!(formatter, "unix://{}", path.display()),
        }
    }
}

impl FromStr for CarrierAddr {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(address) = value.strip_prefix("udp://") {
            return Ok(Self::Udp(address.parse()?));
        }
        if let Some(path) = value.strip_prefix("unix://") {
            if path.is_empty() {
                bail!("Unix address requires a path");
            }
            return Ok(Self::Unix(path.into()));
        }
        bail!("address must use udp:// or unix://")
    }
}

/// A copyable bootstrap token for one running node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NodeAddr {
    pub(crate) pid: Pid,
    pub(crate) address: CarrierAddr,
}

impl fmt::Display for NodeAddr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}?authority={}",
            self.address,
            format_pid(self.pid)
        )
    }
}

impl FromStr for NodeAddr {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (address, pid) = parse_bootstrap(value)?;
        let pid = pid.ok_or_else(|| anyhow!("bootstrap locator requires an authority query"))?;
        Ok(Self { pid, address })
    }
}

/// A pinned or address-only parent bootstrap token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JoinToken {
    Pinned(NodeAddr),
    Discover(CarrierAddr),
}

/// A direct cluster bootstrap or a deployment resolver locator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClusterLocator {
    Direct(JoinToken),
    Resolver(ResolverSpec),
}

impl FromStr for JoinToken {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (address, authority) = parse_bootstrap(value)?;
        match authority {
            Some(pid) => Ok(Self::Pinned(NodeAddr { pid, address })),
            None => Ok(Self::Discover(address)),
        }
    }
}

impl FromStr for ClusterLocator {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if ResolverSpec::recognizes(value) {
            value
                .parse()
                .map(Self::Resolver)
                .map_err(anyhow::Error::new)
        } else {
            value.parse().map(Self::Direct)
        }
    }
}

/// A process PID prefix with an optional cluster locator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Reference {
    pub(crate) pid: PidPrefix,
    pub(crate) cluster: Option<ClusterLocator>,
}

impl FromStr for Reference {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (pid, cluster) = match value.split_once('@') {
            Some((pid, locator)) => (pid, Some(locator.parse()?)),
            None => (value, None),
        };
        Ok(Self {
            pid: pid.parse()?,
            cluster,
        })
    }
}

fn parse_bootstrap(value: &str) -> Result<(CarrierAddr, Option<Pid>)> {
    let (locator, authority) = if value.starts_with("udp://") {
        match value.split_once('?') {
            Some((locator, query)) => (locator, Some(parse_authority_query(query)?)),
            None => (value, None),
        }
    } else if value.starts_with("unix://") {
        match value.rsplit_once("?authority=") {
            Some((locator, authority)) => (locator, Some(parse_pid(authority)?)),
            None => (value, None),
        }
    } else {
        (value, None)
    };
    let address = locator.parse()?;
    Ok((address, authority))
}

fn parse_authority_query(query: &str) -> Result<Pid> {
    let (key, value) = query
        .split_once('=')
        .ok_or_else(|| anyhow!("bootstrap query must have the form authority=PID"))?;
    if key != "authority" || value.contains('&') {
        bail!("bootstrap locator only supports the authority query");
    }
    parse_pid(value)
}

/// Formats a PID as 32 lowercase hexadecimal digits.
pub(crate) fn format_pid(pid: Pid) -> String {
    let mut output = String::with_capacity(32);
    for byte in pid.as_bytes() {
        use fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}

/// Formats a transport-qualified datagram address for terminal output.
pub(crate) fn format_datagram_addr(address: &DatagramAddr) -> String {
    match address.scheme() {
        "udp" => decode_udp_addr(address)
            .map(|address| format!("udp://{address}"))
            .unwrap_or_else(|| format_opaque_addr(address)),
        "unixgram" => format!(
            "unix://{}",
            Path::new(OsStr::from_bytes(address.opaque())).display()
        ),
        _ => format_opaque_addr(address),
    }
}

fn decode_udp_addr(address: &DatagramAddr) -> Option<SocketAddr> {
    let bytes = address.opaque();
    match (bytes.first().copied(), bytes.len()) {
        (Some(4), 7) => {
            let ip = Ipv4Addr::from(<[u8; 4]>::try_from(&bytes[1..5]).ok()?);
            let port = u16::from_be_bytes(<[u8; 2]>::try_from(&bytes[5..7]).ok()?);
            Some(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        (Some(6), 27) => {
            let ip = Ipv6Addr::from(<[u8; 16]>::try_from(&bytes[1..17]).ok()?);
            let port = u16::from_be_bytes(<[u8; 2]>::try_from(&bytes[17..19]).ok()?);
            let flowinfo = u32::from_be_bytes(<[u8; 4]>::try_from(&bytes[19..23]).ok()?);
            let scope_id = u32::from_be_bytes(<[u8; 4]>::try_from(&bytes[23..27]).ok()?);
            Some(SocketAddr::V6(SocketAddrV6::new(
                ip, port, flowinfo, scope_id,
            )))
        }
        _ => None,
    }
}

fn format_opaque_addr(address: &DatagramAddr) -> String {
    let mut output = format!("{}://", address.scheme());
    for byte in address.opaque() {
        use fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}

fn parse_pid(value: &str) -> Result<Pid> {
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("PID must contain 32 hexadecimal digits");
    }
    let mut bytes = [0; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .expect("ASCII hexadecimal PID should have been validated above");
    }
    let pid = Pid::from_bytes(bytes);
    if pid.is_link_local() {
        bail!("link-local PID is reserved");
    }
    Ok(pid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_address_round_trips() {
        let address = NodeAddr {
            pid: Pid::from_bytes([0x42; 16]),
            address: CarrierAddr::Udp("127.0.0.1:1234".parse().unwrap()),
        };

        assert_eq!(
            address
                .to_string()
                .parse::<NodeAddr>()
                .expect("parse node address"),
            address
        );
    }

    #[test]
    fn bootstrap_accepts_authority_query_and_address_only_forms() {
        let address = CarrierAddr::Udp("127.0.0.1:1234".parse().unwrap());
        assert!("127.0.0.1:1234".parse::<JoinToken>().is_err());
        assert_eq!(
            "udp://127.0.0.1:1234"
                .parse::<JoinToken>()
                .expect("parse discovered bootstrap"),
            JoinToken::Discover(address.clone())
        );
        assert_eq!(
            "udp://127.0.0.1:1234?authority=42424242424242424242424242424242"
                .parse::<JoinToken>()
                .expect("parse authoritative bootstrap"),
            JoinToken::Pinned(NodeAddr {
                pid: Pid::from_bytes([0x42; 16]),
                address,
            })
        );
        assert!(
            "42424242@udp://127.0.0.1:1234"
                .parse::<JoinToken>()
                .is_err()
        );
        assert_eq!(
            "unix:///tmp/chrysalis?socket"
                .parse::<JoinToken>()
                .expect("parse Unix path containing a question mark"),
            JoinToken::Discover(CarrierAddr::Unix("/tmp/chrysalis?socket".into()))
        );
        assert!(
            "udp://127.0.0.1:1234?authority=42424242424242424242424242424242&extra=1"
                .parse::<JoinToken>()
                .is_err()
        );
    }

    #[test]
    fn pid_parser_rejects_non_ascii_input_without_panicking() {
        let value = format!("aé{}", "a".repeat(29));

        assert!(parse_pid(&value).is_err());
    }

    #[test]
    fn cluster_locator_accepts_direct_and_resolved_deployments() {
        assert_eq!(
            "udp://127.0.0.1:1234"
                .parse::<ClusterLocator>()
                .expect("parse direct cluster"),
            ClusterLocator::Direct(JoinToken::Discover(CarrierAddr::Udp(
                "127.0.0.1:1234".parse().expect("parse UDP address")
            )))
        );
        assert_eq!(
            "mast://scale_job"
                .parse::<ClusterLocator>()
                .expect("parse resolved cluster"),
            ClusterLocator::Resolver(ResolverSpec::Mast {
                job: "scale_job".into()
            })
        );
    }

    #[test]
    fn carrier_spec_accepts_auto_unix_and_udp() {
        assert_eq!(
            "unix://*"
                .parse::<CarrierSpec>()
                .expect("parse automatic Unix carrier"),
            CarrierSpec::UnixAuto
        );
        assert_eq!(
            "udp://localhost:0"
                .parse::<CarrierSpec>()
                .expect("parse UDP carrier"),
            CarrierSpec::Udp("localhost:0".into())
        );
    }

    #[test]
    fn references_accept_prefixes_and_qualified_clusters() {
        let reference: Reference = "4242@mast://scale_job"
            .parse()
            .expect("parse qualified reference");
        assert_eq!(reference.pid, "4242".parse().expect("parse PID prefix"));
        assert_eq!(
            reference.cluster,
            Some(ClusterLocator::Resolver(ResolverSpec::Mast {
                job: "scale_job".into()
            }))
        );
        assert!("0".parse::<Reference>().is_ok());
        assert!("zzzzzzzz".parse::<Reference>().is_err());
        assert!("0".repeat(33).parse::<Reference>().is_err());
    }

    #[test]
    fn formats_known_and_opaque_datagram_addresses() {
        assert_eq!(
            format_datagram_addr(&UdpSocket::datagram_addr("127.0.0.1:1234".parse().unwrap())),
            "udp://127.0.0.1:1234"
        );
        assert_eq!(
            format_datagram_addr(&UnixDatagramSocket::datagram_addr("/tmp/chrysalis.sock")),
            "unix:///tmp/chrysalis.sock"
        );
        assert_eq!(
            format_datagram_addr(&DatagramAddr::new("test", [0xab, 0xcd])),
            "test://abcd"
        );
    }
}
