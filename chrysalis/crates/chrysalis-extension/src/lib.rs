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
//! - **CPY-3 (address roles and parent authentication):** A node listens only
//!   on Unix or native-vsock addresses. A child may dial Unix, native-vsock, or
//!   hybrid-vsock parent addresses and requires the parent to authenticate as
//!   the configured PID. `node:` values name connected peers and are not
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
//!   raise PyO3's `TypeError` or `OverflowError`. Chrysalis node and transport
//!   failures raise `ChrysalisError`.
//! - **CPY-8 (private Python identity):** Native types and exceptions belong to
//!   `chrysalis._rust_bindings`. The module is private and experimental; no
//!   stable top-level `chrysalis` package API is provided.

use std::borrow::Cow;
use std::fmt::Display;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use chrysalis::DatagramAddr;
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
use chrysalis::VSOCK_SCHEME;
use chrysalis::VsockDatagramAddr;
use chrysalis::VsockDatagramSocket;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
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

/// Parses a carrier address supplied by Python.
///
/// Accepts `unix:<path>`, `native:<cid>:<port>` and
/// `hybrid:<port>:<uds>`. A `node:` value names a connected peer rather than
/// an address to bind or dial, so callers cannot supply one.
fn carrier_address(text: &str) -> PyResult<VsockDatagramAddr> {
    // CPY-3, CPY-7: parse caller-supplied addresses and reject peer names.
    let address = VsockDatagramAddr::try_from(&DatagramAddr::new(VSOCK_SCHEME, text.as_bytes()))
        .map_err(value_error)?;
    match address {
        VsockDatagramAddr::Node(_) => Err(PyValueError::new_err(format!(
            "{text:?} is a peer's name, not an address"
        ))),
        address => Ok(address),
    }
}

/// Parses a carrier address a node can listen on.
///
/// Hybrid addresses are dial-only, so only Unix and native addresses are
/// accepted.
fn listen_address(text: &str) -> PyResult<VsockDatagramAddr> {
    // CPY-3: hybrid addresses are dial-only.
    match carrier_address(text)? {
        VsockDatagramAddr::Hybrid { .. } => Err(PyValueError::new_err(format!(
            "cannot listen on {text:?}: use a unix: or native: address"
        ))),
        address => Ok(address),
    }
}

/// Builds the configuration used when a new node joins a parent.
///
/// `pid` is the identity the parent must authenticate as; `address` is where
/// the new child connects to it.
fn parent_namespace(pid: PyPid, address: &str) -> PyResult<NamespaceConfig> {
    // CPY-2, CPY-3: bind the dial address to the identity the parent must
    // authenticate as.
    let address = DatagramAddr::try_from(&carrier_address(address)?).map_err(value_error)?;
    NamespaceConfig::try_new(pid.inner, vec![ParentEndpoint::new(address)]).map_err(value_error)
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
    /// `listen` lists the addresses where this node accepts carrier
    /// connections. `parent`, when present, gives the PID and address of the
    /// parent to join; without it, the node is a namespace root.
    ///
    /// Invalid input raises Python `ValueError`. A failure to bind or start the
    /// node raises `ChrysalisError`.
    #[new]
    // Optional configuration is keyword-only. PyO3 evaluates `Vec::new()` for
    // each call, so callers do not share a mutable default.
    #[pyo3(signature = (identity, *, listen = Vec::new(), parent = None))]
    fn new(
        identity: &PyIdentity,
        listen: Vec<String>,
        parent: Option<(PyPid, String)>,
    ) -> PyResult<Self> {
        let listen = listen
            .iter()
            .map(|text| listen_address(text))
            .collect::<PyResult<Vec<_>>>()?;
        let parent = parent
            .map(|(pid, address)| parent_namespace(pid, &address))
            .transpose()?;
        let identity = identity.inner.clone();
        let name = PidPrefix::from(identity.pid()).to_string();

        // CPY-1: `bind` and `Node::create` start tasks on the current Tokio runtime,
        // and this Python thread has none.
        let _runtime = get_runtime().enter();
        let socket = VsockDatagramSocket::bind(&name, &listen).map_err(chrysalis_error)?;
        let mut config = NodeConfig::new(TransportConfig::new(Arc::new(socket), identity));
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
        })
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
    module.add_class::<PyNode>()?;
    module.add_class::<PyStream>()?;
    module.add_function(wrap_pyfunction!(generate_test_identities, module)?)?;
    Ok(())
}
