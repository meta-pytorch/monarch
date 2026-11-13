/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Minimal asyncio bindings for Chrysalis nodes and streams.
//!
//! This is an internal, version-zero API. It contains only the identity, node
//! and stream operations needed by the first Python programs. Both the Python
//! surface and its runtime bridge are experimental and subject to change.
//!
//! This version uses `pyo3-async-runtimes` directly and exposes only asyncio.
//! Future bindings may instead use the shared "get or await" completion model,
//! which supports blocking, polling, and awaiting the same Rust operation.
//!
//! For now, Python enters each async method while holding the GIL. The method
//! validates its arguments and moves owned Rust values into `future_into_py`,
//! which creates an asyncio future and schedules the Chrysalis operation on
//! the Tokio runtime owned by `pyo3-async-runtimes`.
//!
//! The Rust operation runs without the GIL and holds no Python borrows across
//! an `.await`. When it finishes, the bridge briefly reacquires the GIL to
//! resolve the asyncio future.
//!
//! `Node(...)` is a normal synchronous Python constructor. Its Rust setup must
//! start background Tokio tasks, so it briefly enters the runtime owned by
//! `pyo3-async-runtimes`. It leaves that runtime before returning to Python.
//!
//! A node binds one carrier, or a pair. `UdpCarrier`, the default, sends each
//! QUIC packet as one UDP datagram. `VsockCarrier` exists for Firecracker,
//! whose vsock device provides byte streams rather than the datagrams
//! Chrysalis expects. It preserves packet boundaries by framing each datagram
//! onto that stream. Its Unix-stream mode exercises the same adapter without a
//! VM.
//!
//! A node at a VM boundary needs two communication paths: one toward the rest
//! of the mesh, and one toward nodes that are local to it. A carrier pair
//! provides those paths.
//!
//! The first carrier faces the rest of the mesh. If the node has a parent, it
//! joins through this carrier. Other nodes also use it to reach this node and
//! everything below it. The second carrier is the additional path normally
//! used by local children. Both paths carry traffic in both directions.
//!
//! Inside a guest, Alice uses `(VsockCarrier, UdpCarrier)`: vsock reaches the
//! host Relay, while UDP reaches Carol over guest loopback. A host Relay uses
//! `(UdpCarrier, VsockCarrier)`: UDP reaches other Relays, while vsock reaches
//! its VMs.
//!
//! `DatagramSocketSet` receives from both carriers. When sending, the
//! destination address selects UDP or vsock. `node:`, `unix:`, `native:`, and
//! `hybrid:` addresses all select the vsock carrier.
//!
//! Cancelling a Python awaitable drops its Rust future. Work already completed
//! by that operation is not undone.
//!
//! ## Invariants (CPY-*)
//!
//! Code that enforces or relies on these guarantees is tagged `// CPY-N`.
//! Each test that witnesses one names its IDs and points back to this registry.
//!
//! - **CPY-1 (async boundary):** Every operation that waits returns an asyncio
//!   future. Rust owns everything carried across an `.await`, runs without the
//!   GIL, and reacquires the GIL only to complete the Python future. Synchronous
//!   `Node` construction enters the Tokio runtime only while starting native
//!   work and leaves it before returning to Python.
//! - **CPY-2 (typed identity):** Python PIDs are complete, validated 32-digit
//!   values. `Identity.pid`, parent configuration, `dial`, and `accept` use the
//!   `Pid` type rather than unvalidated strings. An identity's leaf certificate
//!   determines its PID.
//! - **CPY-3 (carriers, address roles and parent authentication):** A node
//!   binds one carrier, or a pair: a primary carrier and one carrier of the
//!   other kind. A `UdpCarrier` binds one specific IP and port. A
//!   `VsockCarrier` listens only on Unix or native-vsock addresses. A parent
//!   address must belong to the node's carrier: `udp://<ip>:<port>` with a
//!   specific IP and a non-zero port for UDP, and Unix, native-vsock, or
//!   hybrid-vsock for vsock. A pair node joins its parent over its primary, by
//!   that carrier's rule. The child requires its parent to authenticate as the
//!   configured PID. `node:` values name connected peers and are not
//!   caller-supplied addresses.
//! - **CPY-4 (stream behavior):** A stream's send and receive sides have
//!   separate locks, so one read and one send can run together while calls in
//!   the same direction take turns. An empty read means the peer finished;
//!   connection loss before that is an error.
//! - **CPY-5 (cancellation does not roll back):** Cancelling a Python future
//!   drops the Rust future. Work already completed may remain visible. A
//!   cancelled stream operation gives no guarantee about that stream's state.
//! - **CPY-6 (one-way close):** `close` changes the node to closing only after
//!   it has created a Python future. From then on, new `dial` and `accept` calls
//!   fail. At most one shutdown attempt runs at a time. Once it finishes, later
//!   `close` calls return the stored completion; if an attempt is cancelled, a
//!   later call continues shutdown. If future creation fails, the node remains
//!   open.
//! - **CPY-7 (Python errors):** Invalid PID text, addresses, PEM bundles, and
//!   read sizes raise `ValueError`. Wrong Python types and out-of-range integers
//!   raise PyO3's `TypeError` or `OverflowError`, and a carrier pair of one kind
//!   raises `TypeError`. Chrysalis node and transport failures raise
//!   `ChrysalisError`.
//! - **CPY-8 (private Python identity):** Native types and exceptions belong to
//!   `chrysalis._rust_bindings`. The module is private and experimental; no
//!   stable top-level `chrysalis` package API is provided.
//! - **CPY-9 (UDP by default, and UDP nodes forward):** A node given no carrier
//!   binds a fresh UDP socket on 127.0.0.1, on a port the system chooses.
//!   Every single-carrier UDP node is built with `TransportConfig::routed_udp`,
//!   so it forwards PID-routed packets for its children. A pair node is built
//!   as a `DatagramSocketSet` on the switch backend, which forwards by
//!   connection ID across both carriers. A UDP node's `Node.addresses` is the
//!   address it actually bound, which other nodes dial; a `VsockCarrier`
//!   node's is its configured listen addresses, whose dial form depends on the
//!   deployment. A pair node's advertised locator is its primary's address, and
//!   its `Node.addresses` is the primary's addresses followed by the other
//!   carrier's.

use std::borrow::Cow;
use std::fmt::Display;
use std::io;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use chrysalis::DatagramAddr;
use chrysalis::DatagramSocket;
use chrysalis::DatagramSocketSet;
use chrysalis::NamespaceConfig;
use chrysalis::Node;
use chrysalis::NodeConfig;
use chrysalis::ParentEndpoint;
use chrysalis::Pid;
use chrysalis::PidPrefix;
use chrysalis::QuicIdentity;
use chrysalis::RecvStream;
use chrysalis::ResolveConsistency;
use chrysalis::SendStream;
use chrysalis::Stream;
use chrysalis::TransportConfig;
use chrysalis::UdpSocket;
use chrysalis::VsockDatagramAddr;
use chrysalis::VsockDatagramSocket;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_async_runtimes::tokio::get_runtime;
use rcgen::BasicConstraints;
use rcgen::CertificateParams;
use rcgen::CertifiedIssuer;
use rcgen::ExtendedKeyUsagePurpose;
use rcgen::IsCa;
use rcgen::KeyPair;
use rcgen::KeyUsagePurpose;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;

/// TLS server name currently used when loading every identity.
///
/// Generated test certificates name `localhost`. A future revision will derive
/// this name from the certificate so identities with other names can be used.
const SERVER_NAME: &str = "localhost";

/// Python-bindings policy limiting one `Stream.read` allocation.
///
/// Python controls `max_bytes`; cap it so an accidental large value cannot
/// allocate an equally large Rust buffer. This is not a transport limit.
const READ_BUFFER_LIMIT: usize = 1 << 20;

/// Default bind for `UdpCarrier()` and for `Node` when no carrier is supplied.
///
/// This is Python-binding policy, not a transport default. `127.0.0.1` makes
/// the default node reachable only by programs sharing the Python process's
/// loopback network; it does not expose the node on an external network
/// interface. Port 0 lets the system choose an available port. Callers that
/// need another interface pass `UdpCarrier(bind=...)`.
const DEFAULT_UDP_BIND: &str = "127.0.0.1:0";

/// Prefix used to expose and parse UDP addresses in the Python API.
///
/// This private parser mirrors the established spelling used by other
/// Chrysalis clients. Moving it into the transport requires a shared concrete
/// UDP address type; bind specifications and bootstrap locators have different
/// validation rules and cannot simply share this constant.
const UDP_PREFIX: &str = "udp://";

// CPY-7, CPY-8: defines `chrysalis._rust_bindings.ChrysalisError`, a Python `Exception`
// subclass. Call sites convert errors to it explicitly through `chrysalis_error`.
create_exception!(
    chrysalis._rust_bindings,
    ChrysalisError,
    PyException,
    "A Chrysalis node or transport failure."
);

/// Wraps a displayable node or transport failure as `ChrysalisError`.
fn chrysalis_error(error: impl Display) -> PyErr {
    ChrysalisError::new_err(error.to_string())
}

/// Wraps a displayable input-validation failure as Python `ValueError`.
fn value_error(error: impl Display) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// A validated, complete Chrysalis process identifier exposed to Python.
///
/// Construction accepts exactly 32 hexadecimal digits. Equality and hashing
/// use the underlying Chrysalis PID.
// CPY-2, CPY-8: keep PID validation in one Python value type with the private
// module identity shared by every binding type.
#[pyclass(
    name = "Pid",
    module = "chrysalis._rust_bindings",
    frozen,
    eq,
    hash,
    from_py_object
)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct PyPid {
    /// Validated identifier passed to Chrysalis APIs.
    inner: Pid,
}

#[pymethods]
impl PyPid {
    /// Creates a PID from exactly 32 hexadecimal digits.
    #[new]
    fn new(value: &str) -> PyResult<Self> {
        Ok(Self {
            inner: parse_pid(value)?,
        })
    }

    /// Returns the PID as 32 lowercase hexadecimal digits.
    fn __str__(&self) -> String {
        PidPrefix::from(self.inner).to_string()
    }

    /// Returns the PID as `Pid('...')`.
    fn __repr__(&self) -> String {
        format!("Pid('{}')", self.__str__())
    }
}

/// Wraps an existing Rust PID for Python.
impl From<Pid> for PyPid {
    fn from(inner: Pid) -> Self {
        Self { inner }
    }
}

/// The credentials a node uses for mutual TLS.
///
/// The leaf certificate determines the node's PID. The node uses its
/// certificate chain and private key to identify itself, and its trust roots
/// to decide which peer certificates to accept.
// CPY-2, CPY-8: the certificate-derived PID and credentials remain one typed
// Python identity in the private bindings module.
#[pyclass(name = "Identity", module = "chrysalis._rust_bindings", frozen)]
struct PyIdentity {
    /// Parsed credentials passed to Chrysalis when creating a `Node`.
    inner: QuicIdentity,
}

#[pymethods]
impl PyIdentity {
    /// Loads an identity and trust policy from a PEM certificate chain and
    /// private key.
    ///
    /// A malformed bundle raises Python `ValueError`; any other loading
    /// failure raises `ChrysalisError`.
    #[staticmethod]
    fn from_pem(bundle: &str) -> PyResult<Self> {
        let inner =
            QuicIdentity::from_pem_bundle(bundle, SERVER_NAME).map_err(|error| {
                match error.kind() {
                    io::ErrorKind::InvalidData => value_error(error),
                    _ => chrysalis_error(error),
                }
            })?;
        Ok(Self { inner })
    }

    /// Returns the certificate-derived PID.
    #[getter]
    fn pid(&self) -> PyPid {
        self.inner.pid().into()
    }
}

/// Generates `count` mutually trusted identity bundles for tests.
///
/// Each PEM bundle contains a new node certificate and private key. The same
/// newly generated CA signs every node certificate.
#[pyfunction]
fn generate_test_identities(count: usize) -> PyResult<Vec<String>> {
    test_bundles(count).map_err(chrysalis_error)
}

/// Builds the PEM text returned by `generate_test_identities`.
///
/// Each bundle contains, in order, the node certificate, the shared CA
/// certificate and the node's private key, as `QuicIdentity` expects.
fn test_bundles(count: usize) -> Result<Vec<String>, rcgen::Error> {
    // One fresh CA signs every node certificate and is included in every
    // bundle as the trust root.
    let mut issuer_params = CertificateParams::default();
    issuer_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    issuer_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let issuer = CertifiedIssuer::self_signed(issuer_params, KeyPair::generate()?)?;
    // The CA certificate becomes the trust root in every identity bundle.
    let trust_roots = issuer.pem();
    (0..count)
        .map(|_| {
            // Each node gets its own private key.
            let key = KeyPair::generate()?;
            let mut params = CertificateParams::new(vec![SERVER_NAME.to_owned()])?;
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ClientAuth,
                ExtendedKeyUsagePurpose::ServerAuth,
            ];
            // The CA signs a node certificate for this key. It can identify
            // the node on either side of a mutual-TLS connection.
            let certificate = params.signed_by(&key, &issuer)?;
            // QuicIdentity expects the node certificate, trust root and private
            // key in this order.
            Ok(format!(
                "{}\n{}\n{}",
                certificate.pem().trim_end(),
                trust_roots.trim_end(),
                key.serialize_pem().trim_end(),
            ))
        })
        .collect()
}

/// Parses a complete PID from Python text.
///
/// Malformed or abbreviated values raise Python `ValueError`.
fn parse_pid(text: &str) -> PyResult<Pid> {
    // CPY-2, CPY-7: accept only a complete PID and report malformed text as
    // Python input error.
    PidPrefix::from_str(text)
        .map_err(value_error)?
        .as_pid()
        .ok_or_else(|| PyValueError::new_err(format!("a PID has 32 hex digits, not {text:?}")))
}

/// Parses a caller-supplied vsock address and rejects `node:` peer labels.
///
/// `VsockDatagramAddr` owns the address syntax. This binding owns which
/// variants Python callers may supply.
fn vsock_address(text: &str) -> PyResult<VsockDatagramAddr> {
    // CPY-3, CPY-7: parse caller-supplied addresses and reject peer names.
    let address = text.parse().map_err(value_error)?;
    match address {
        VsockDatagramAddr::Node(_) => Err(PyValueError::new_err(format!(
            "{text:?} is a peer's name, not an address"
        ))),
        address => Ok(address),
    }
}

/// Parses a vsock carrier address a node can listen on.
///
/// Hybrid addresses are dial-only, so only Unix and native addresses are
/// accepted.
fn vsock_listen_address(text: &str) -> PyResult<VsockDatagramAddr> {
    // CPY-3: hybrid addresses are dial-only.
    match vsock_address(text)? {
        VsockDatagramAddr::Hybrid { .. } => Err(PyValueError::new_err(format!(
            "cannot listen on {text:?}: use a unix: or native: address"
        ))),
        address => Ok(address),
    }
}

/// Returns the canonical text stored in a vsock [`DatagramAddr`].
///
/// Addresses parsed from Python text already have nonempty UTF-8 paths, the
/// only conditions this conversion can reject.
fn vsock_address_text(address: &VsockDatagramAddr) -> PyResult<String> {
    let address = DatagramAddr::try_from(address).map_err(chrysalis_error)?;
    std::str::from_utf8(address.opaque())
        .map(str::to_owned)
        .map_err(chrysalis_error)
}

/// Parses a UDP node's parent address, `udp://<ip>:<port>`.
///
/// The IP must be specific and the port non-zero: no node can be reached at
/// an unspecified IP or at port 0.
fn udp_parent_address(text: &str) -> PyResult<DatagramAddr> {
    // CPY-3, CPY-7: a UDP node joins its parent over UDP, at an address a
    // parent can have.
    let address = text
        .strip_prefix(UDP_PREFIX)
        .and_then(|address| SocketAddr::from_str(address).ok())
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "a UDP node's parent address has the form udp://<ip>:<port>, not {text:?}"
            ))
        })?;
    if address.ip().is_unspecified() || address.port() == 0 {
        return Err(PyValueError::new_err(format!(
            "a UDP node's parent address names a specific IP and a non-zero port, not {text:?}"
        )));
    }
    Ok(UdpSocket::datagram_addr(address))
}

/// Parses a `VsockCarrier` node's parent address: `unix:`, `native:` or
/// `hybrid:`.
fn vsock_parent_address(text: &str) -> PyResult<DatagramAddr> {
    // CPY-3, CPY-7: a vsock node can't join a parent over UDP.
    if text.starts_with(UDP_PREFIX) {
        return Err(PyValueError::new_err(format!(
            "a VsockCarrier node's parent address starts with unix:, native:, or hybrid:, not {text:?}"
        )));
    }
    DatagramAddr::try_from(&vsock_address(text)?).map_err(value_error)
}

/// Builds the configuration used when a new node joins a parent.
///
/// `pid` is the identity the parent must authenticate as; `address` is where
/// the new child connects to it.
fn parent_namespace(pid: PyPid, address: DatagramAddr) -> PyResult<NamespaceConfig> {
    // CPY-2, CPY-3: bind the dial address to the identity the parent must
    // authenticate as.
    NamespaceConfig::try_new(pid.inner, vec![ParentEndpoint::new(address)]).map_err(value_error)
}

/// Configuration for a node's UDP carrier.
///
/// UDP carries each QUIC packet as one datagram. This value owns no socket; it
/// stores the requested local bind address. `Node` opens the socket during
/// construction, and `Node.addresses` reports the actual address after port 0
/// is resolved.
// CPY-3, CPY-8: a UDP carrier names one specific local IP and port.
#[pyclass(
    name = "UdpCarrier",
    module = "chrysalis._rust_bindings",
    frozen,
    from_py_object
)]
#[derive(Clone, Copy)]
struct PyUdpCarrier {
    /// Requested local IP and port. The IP is specific; port 0 asks the system
    /// to choose an available port.
    bind: SocketAddr,
}

#[pymethods]
impl PyUdpCarrier {
    /// Describes a UDP socket bound at `bind`, a specific IP and port.
    ///
    /// Port 0 lets the system choose a free port. A malformed address or an
    /// unspecified IP raises Python `ValueError`, because a node advertises
    /// the address it binds.
    #[new]
    #[pyo3(signature = (bind = DEFAULT_UDP_BIND))]
    fn new(bind: &str) -> PyResult<Self> {
        // CPY-3, CPY-7: other nodes can't reach an unspecified IP.
        let bind = SocketAddr::from_str(bind).map_err(|error| {
            PyValueError::new_err(format!("{bind:?} is not an IP and port: {error}"))
        })?;
        if bind.ip().is_unspecified() {
            return Err(PyValueError::new_err(format!(
                "a UDP carrier binds a specific IP, not {}",
                bind.ip()
            )));
        }
        Ok(Self { bind })
    }
}

/// The vsock carrier, which frames datagrams onto byte streams.
///
/// It connects nodes over Firecracker's vsock links, or over Unix stream
/// sockets on one host. The carrier only describes its endpoints. The `Node`
/// that uses it binds them.
// CPY-3, CPY-8: a vsock carrier listens only on Unix or native-vsock addresses.
#[pyclass(
    name = "VsockCarrier",
    module = "chrysalis._rust_bindings",
    frozen,
    from_py_object
)]
#[derive(Clone)]
struct PyVsockCarrier {
    /// Parsed listen addresses. `Node.addresses` reports their canonical text.
    listen: Vec<VsockDatagramAddr>,
}

#[pymethods]
impl PyVsockCarrier {
    /// Describes a carrier that accepts connections at `listen`.
    ///
    /// Each address is `unix:<path>` or `native:<cid>:<port>`. With none, the
    /// node only connects to its parent. Any other address raises Python
    /// `ValueError`.
    #[new]
    // PyO3 evaluates `Vec::new()` for each call, so callers do not share a
    // mutable default.
    #[pyo3(signature = (listen = Vec::new()))]
    fn new(listen: Vec<String>) -> PyResult<Self> {
        let listen = listen
            .iter()
            .map(|text| vsock_listen_address(text))
            .collect::<PyResult<_>>()?;
        Ok(Self { listen })
    }
}

/// One carrier: a `UdpCarrier` or a `VsockCarrier`.
#[derive(FromPyObject)]
enum OneCarrier {
    /// A UDP socket at the carrier's bind address.
    Udp(PyUdpCarrier),
    /// The vsock carrier with the caller's listen addresses.
    Vsock(PyVsockCarrier),
}

impl OneCarrier {
    /// Parses a parent address in this carrier's form.
    fn parent_address(&self, text: &str) -> PyResult<DatagramAddr> {
        match self {
            Self::Udp(_) => udp_parent_address(text),
            Self::Vsock(_) => vsock_parent_address(text),
        }
    }

    /// Binds this carrier as a node's only carrier, returning the node's
    /// transport and the addresses `Node.addresses` reports.
    fn bind_transport(&self, identity: QuicIdentity) -> PyResult<(TransportConfig, Vec<String>)> {
        match self {
            Self::Udp(udp) => {
                let socket = std::net::UdpSocket::bind(udp.bind).map_err(chrysalis_error)?;
                let local = socket.local_addr().map_err(chrysalis_error)?;
                // CPY-9: a routed UDP transport forwards PID-routed packets for
                // the node's children; a direct one would answer them itself.
                let transport =
                    TransportConfig::routed_udp(socket, None, identity).map_err(chrysalis_error)?;
                Ok((transport, vec![format!("{UDP_PREFIX}{local}")]))
            }
            Self::Vsock(vsock) => {
                let (socket, addresses) = bind_vsock(vsock, identity.pid())?;
                Ok((TransportConfig::new(Arc::new(socket), identity), addresses))
            }
        }
    }

    /// Binds this carrier as one socket of a pair, returning the socket and
    /// the addresses it reports.
    fn bind_socket(&self, pid: Pid) -> PyResult<(Arc<dyn DatagramSocket>, Vec<String>)> {
        match self {
            Self::Udp(udp) => {
                // The bind completes at once; `block_on` only supplies the
                // runtime whose reactor the socket registers with.
                let socket = get_runtime()
                    .block_on(UdpSocket::bind(udp.bind))
                    .map_err(chrysalis_error)?;
                let address = format!("{UDP_PREFIX}{}", socket.address());
                Ok((Arc::new(socket), vec![address]))
            }
            Self::Vsock(vsock) => {
                let (socket, addresses) = bind_vsock(vsock, pid)?;
                Ok((Arc::new(socket), addresses))
            }
        }
    }
}

/// Binds a vsock carrier's socket, named after the node's PID, and returns it
/// with its listen addresses in canonical form.
fn bind_vsock(vsock: &PyVsockCarrier, pid: Pid) -> PyResult<(VsockDatagramSocket, Vec<String>)> {
    // Format every reported address before binding opens any listener, so a
    // conversion error leaves no carrier resources.
    let addresses = vsock
        .listen
        .iter()
        .map(vsock_address_text)
        .collect::<PyResult<_>>()?;
    let name = PidPrefix::from(pid).to_string();
    let socket = VsockDatagramSocket::bind(&name, &vsock.listen).map_err(chrysalis_error)?;
    Ok((socket, addresses))
}

/// The carrier a node binds, from the `carrier` argument of `Node(...)`.
#[derive(FromPyObject)]
enum Carrier {
    /// One carrier.
    One(OneCarrier),
    /// One `UdpCarrier` and one `VsockCarrier`, in either order. The first is
    /// the primary.
    Pair(OneCarrier, OneCarrier),
}

impl Carrier {
    /// Rejects a pair of two carriers of one kind, before anything binds.
    fn check(&self) -> PyResult<()> {
        // CPY-3, CPY-7: a pair has one carrier of each kind.
        match self {
            Self::Pair(OneCarrier::Udp(_), OneCarrier::Udp(_))
            | Self::Pair(OneCarrier::Vsock(_), OneCarrier::Vsock(_)) => Err(PyTypeError::new_err(
                "a carrier pair is one UdpCarrier and one VsockCarrier",
            )),
            _ => Ok(()),
        }
    }

    /// The carrier the node joins its parent through and advertises.
    fn primary(&self) -> &OneCarrier {
        match self {
            Self::One(carrier) | Self::Pair(carrier, _) => carrier,
        }
    }

    /// Parses a parent address in the primary carrier's form.
    fn parent_address(&self, text: &str) -> PyResult<DatagramAddr> {
        // CPY-3: a pair node joins its parent over its primary.
        self.primary().parent_address(text)
    }

    /// Binds the carrier, returning the node's transport and the addresses
    /// `Node.addresses` reports.
    ///
    /// Call this inside the Tokio runtime: binding a vsock carrier starts
    /// tasks on it.
    fn bind(&self, identity: QuicIdentity) -> PyResult<(TransportConfig, Vec<String>)> {
        match self {
            Self::One(carrier) => carrier.bind_transport(identity),
            Self::Pair(primary, other) => {
                let pid = identity.pid();
                let (primary, mut addresses) = primary.bind_socket(pid)?;
                let (other, other_addresses) = other.bind_socket(pid)?;
                addresses.extend(other_addresses);
                // CPY-9: the set's local address, which the node advertises, is
                // its primary's; the switch backend forwards across both.
                let set = DatagramSocketSet::new(primary, vec![other]).map_err(chrysalis_error)?;
                Ok((TransportConfig::new(Arc::new(set), identity), addresses))
            }
        }
    }
}

/// A running Chrysalis node exposed to Python.
///
/// A node may join a parent, resolve other nodes by PID, open application
/// streams to them, and accept streams from authenticated peers. `close`
/// shuts down all work owned by the node.
#[pyclass(name = "Node", module = "chrysalis._rust_bindings", frozen)]
struct PyNode {
    /// Shared node and shutdown state captured by its async operations.
    inner: Arc<NodeInner>,
    /// The addresses `addresses` reports: a UDP node's bound address, a
    /// `VsockCarrier` node's listen addresses, or a pair's first carrier's
    /// addresses followed by its second's.
    addresses: Vec<String>,
}

/// State shared by the Python node and its in-flight async operations.
struct NodeInner {
    /// Native node used by every Python operation.
    node: Node,
    /// Set when `close` successfully returns its Python awaitable.
    ///
    /// New `dial` and `accept` calls fail immediately, even before the caller
    /// awaits the shutdown.
    // CPY-6: publishing this state prevents new work before shutdown finishes.
    closing: AtomicBool,
    /// Stores the completion after the node has shut down and joined.
    // CPY-6: one initializer owns an attempt; completed shutdown is shared.
    closed: OnceCell<()>,
}

impl NodeInner {
    /// Rejects new operations after `close` has returned an awaitable.
    fn ensure_open(&self) -> PyResult<()> {
        if self.closing.load(Ordering::Acquire) {
            return Err(ChrysalisError::new_err("the node is closed"));
        }
        Ok(())
    }
}

#[pymethods]
impl PyNode {
    /// Creates and starts a Chrysalis node.
    ///
    /// `carrier` is the node's `UdpCarrier` or `VsockCarrier`, or a pair of one
    /// of each in either order, `(UdpCarrier, VsockCarrier)` or
    /// `(VsockCarrier, UdpCarrier)`. Without one, the node binds a fresh UDP
    /// socket on 127.0.0.1. `parent`, when present, gives the PID and address
    /// of the parent to join; without it, the node is a namespace root. A pair
    /// node joins its parent through its first carrier, so its parent address
    /// takes that carrier's form.
    ///
    /// Invalid input raises Python `ValueError`. A carrier of another type, or
    /// a pair of two carriers of one kind, raises `TypeError`. A failure to
    /// bind a carrier or start the node, including a kernel that refuses
    /// io_uring to a single-carrier UDP node, raises `ChrysalisError`.
    #[new]
    // Optional configuration is keyword-only.
    #[pyo3(signature = (identity, *, carrier = None, parent = None))]
    fn new(
        identity: &PyIdentity,
        carrier: Option<Carrier>,
        parent: Option<(PyPid, String)>,
    ) -> PyResult<Self> {
        // CPY-9: a node given no carrier binds a fresh loopback UDP socket.
        let carrier = carrier.unwrap_or_else(|| {
            Carrier::One(OneCarrier::Udp(
                PyUdpCarrier::new(DEFAULT_UDP_BIND)
                    .expect("should parse the default UDP bind address"),
            ))
        });
        carrier.check()?;
        let parent = parent
            .map(|(pid, address)| parent_namespace(pid, carrier.parent_address(&address)?))
            .transpose()?;
        let identity = identity.inner.clone();

        // CPY-1: binding a vsock carrier and `Node::create` start tasks on the
        // current Tokio runtime, and this Python thread has none.
        let _runtime = get_runtime().enter();
        let (transport, addresses) = carrier.bind(identity)?;
        let mut config = NodeConfig::new(transport);
        // CPY-3: adding a parent turns this node from a namespace root into an
        // authenticated child.
        if let Some(parent) = parent {
            config = config.with_parent(parent);
        }
        let node = Node::create(config).map_err(chrysalis_error)?;
        Ok(Self {
            inner: Arc::new(NodeInner {
                node,
                closing: AtomicBool::new(false),
                closed: OnceCell::new(),
            }),
            addresses,
        })
    }

    /// Returns this node's addresses.
    ///
    /// A UDP node has one, `udp://<ip>:<port>`, with the port it bound; other
    /// nodes dial it there. A `VsockCarrier` node has its listen addresses in
    /// canonical form, or none if it only connects. How another node dials a
    /// vsock listener depends on the deployment: a host reaches a guest's
    /// `native:` listener through a `hybrid:` address. A pair node has its
    /// first carrier's addresses, then its second's.
    #[getter]
    fn addresses(&self) -> Vec<String> {
        // CPY-9: a UDP node reports the port the system chose, not port 0, and
        // a pair reports its first carrier's addresses before its second's.
        self.addresses.clone()
    }

    /// Opens an application stream to `pid` using this node's current
    /// namespace view.
    ///
    /// Raises `ChrysalisError` if the PID is not visible or the connection
    /// cannot be opened.
    fn dial<'py>(&self, py: Python<'py>, pid: PyPid) -> PyResult<Bound<'py, PyAny>> {
        // CPY-2, CPY-6, CPY-7: accept only a typed PID and refuse all new work
        // once close has published the node's closing state.
        self.inner.ensure_open()?;
        let inner = Arc::clone(&self.inner);
        // CPY-1, CPY-5: move owned Rust state across the async boundary;
        // cancellation drops this future without rollback.
        future_into_py(py, async move {
            let stream = inner
                .node
                .dial(pid.inner, ResolveConsistency::Cached)
                .await
                .map_err(chrysalis_error)?;
            Ok(PyStream::new(stream))
        })
    }

    /// Waits until another node opens a bidirectional stream to this node.
    ///
    /// Returns the stream and the authenticated PID of the node that opened
    /// it. Raises `ChrysalisError` if this node is closed or the wait fails.
    fn accept<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // CPY-6, CPY-7: refuse new incoming work after close starts.
        self.inner.ensure_open()?;
        let inner = Arc::clone(&self.inner);
        // CPY-1, CPY-2, CPY-5: the Rust future owns its state and returns the
        // authenticated peer as a typed PID.
        future_into_py(py, async move {
            let incoming = inner.node.accept().await.map_err(chrysalis_error)?;
            let (source, stream) = incoming.into_parts();
            Ok((PyPid::from(source), PyStream::new(stream)))
        })
    }

    /// Starts shutting down the node and returns an awaitable for its
    /// completion.
    ///
    /// Shutdown is scheduled before this call returns; it does not wait for
    /// the caller to await the result. Once this call returns, new `dial` and
    /// `accept` calls fail. Awaiting the result waits for all work owned by the
    /// node to finish. Repeated calls share completed shutdown; if one wait is
    /// cancelled, a later call continues shutdown.
    ///
    /// Without a running asyncio loop, this raises Python `RuntimeError` and
    /// leaves the node open.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        // CPY-1, CPY-5, CPY-6: create the Python future before publishing the
        // closing state, and share completed shutdown across callers.
        let closed = future_into_py(py, async move {
            inner
                .closed
                .get_or_init(|| async {
                    inner.node.shutdown();
                    inner.node.join().await;
                })
                .await;
            Ok(())
        })?;
        // `future_into_py` fails without a running event loop, and a call
        // that returns no awaitable must leave the node open.
        self.inner.closing.store(true, Ordering::Release);
        Ok(closed)
    }
}

/// A bidirectional byte stream between two authenticated nodes.
///
/// Sending and receiving are independent: one read can run while one write or
/// `finish` runs. Calls in the same direction take turns.
// CPY-4, CPY-8: expose the two independently locked halves as one private
// Python stream type.
#[pyclass(name = "Stream", module = "chrysalis._rust_bindings", frozen)]
struct PyStream {
    /// The send side used by `write` and `finish`. Only one of those calls runs
    /// at a time.
    send: Arc<Mutex<SendStream>>,
    /// The receive side used by `read`. Only one read runs at a time.
    recv: Arc<Mutex<RecvStream>>,
}

impl PyStream {
    /// Wraps a native stream for Python, with separate locks for reading and
    /// writing.
    fn new(stream: Stream) -> Self {
        let (send, recv) = stream.into_parts();
        Self {
            send: Arc::new(Mutex::new(send)),
            recv: Arc::new(Mutex::new(recv)),
        }
    }
}

#[pymethods]
impl PyStream {
    /// Writes all of `data` to the other end of the stream.
    ///
    /// Returns an asyncio future. Await it until the stream has accepted every
    /// byte. This does not end the stream; call `finish` when no more bytes
    /// will be sent.
    fn write<'py>(&self, py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        let data = data.to_vec();
        let send = Arc::clone(&self.send);
        // CPY-1, CPY-4, CPY-5: one send operation owns the copied bytes and
        // the send lock until it completes or is cancelled.
        future_into_py(py, async move {
            send.lock()
                .await
                .write_all(&data)
                .await
                .map_err(chrysalis_error)
        })
    }

    /// Tells the other end that this side will send no more bytes.
    ///
    /// Returns an asyncio future. Await it until the send side has closed.
    /// The receive side stays open, so the caller may continue reading.
    fn finish<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let send = Arc::clone(&self.send);
        // CPY-1, CPY-4, CPY-5: finish shares the send-side lock with write.
        future_into_py(py, async move {
            send.lock().await.finish().await.map_err(chrysalis_error)
        })
    }

    /// Reads up to `max_bytes` from the other end of the stream.
    ///
    /// Returns an asyncio future for one read. It completes when at least one
    /// byte is available, the other end has finished sending, or the read
    /// fails. It does not wait to fill `max_bytes`; call `read` again to
    /// continue. An empty result means the other end has finished sending.
    ///
    /// `max_bytes` must be at least 1. This version allocates at most 1 MiB for
    /// one read, so a larger request still returns at most 1 MiB. If the
    /// connection closes before the other end finishes, the future raises
    /// `ChrysalisError`.
    #[pyo3(signature = (max_bytes = 65536))]
    fn read<'py>(&self, py: Python<'py>, max_bytes: i64) -> PyResult<Bound<'py, PyAny>> {
        // Reject an empty buffer, which would otherwise look like end of
        // stream, and apply the bindings' allocation limit.
        let capacity = usize::try_from(max_bytes)
            .ok()
            .filter(|&count| count > 0)
            .ok_or_else(|| {
                PyValueError::new_err(format!("max_bytes must be at least 1, not {max_bytes}"))
            })?
            .min(READ_BUFFER_LIMIT);
        let recv = Arc::clone(&self.recv);
        // CPY-1, CPY-4, CPY-5: reads own only the receive half and preserve
        // the distinction between peer finish and connection failure.
        future_into_py(py, async move {
            let mut buffer = vec![0; capacity];
            let count = recv
                .lock()
                .await
                .read(&mut buffer)
                .await
                .map_err(chrysalis_error)?;
            buffer.truncate(count);
            // Return Python bytes, not a list of integer byte values.
            Ok(Cow::<'static, [u8]>::Owned(buffer))
        })
    }
}

#[pymodule]
#[pyo3(name = "_rust_bindings")]
fn mod_init(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // CPY-8: every exported native type belongs to the private bindings module.
    let py = module.py();
    module.add("ChrysalisError", py.get_type::<ChrysalisError>())?;
    module.add_class::<PyPid>()?;
    module.add_class::<PyIdentity>()?;
    module.add_class::<PyUdpCarrier>()?;
    module.add_class::<PyVsockCarrier>()?;
    module.add_class::<PyNode>()?;
    module.add_class::<PyStream>()?;
    module.add_function(wrap_pyfunction!(generate_test_identities, module)?)?;
    Ok(())
}
