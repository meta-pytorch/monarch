/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Separate-process implementations of Alice, Relay, and Bob.
//!
//! `main.rs` parses one role command and calls into this module. The roles form
//! the same namespace tree as the in-process walkthrough:
//!
//! ```text
//! Alice (root)
//! └── Relay
//!     └── Bob
//! ```
//!
//! Alice sends the postcard and numbered streams. Bob receives and answers
//! them. Relay is their routing hop: it maintains the parent/child links and
//! forwards encrypted carrier datagrams without terminating Alice and Bob's
//! end-to-end QUIC connection.
//!
//! Each process loads its own certificate and private key, then names its
//! carrier `node:<its certificate-derived PID>`. Bob opens a parent link to
//! Relay, and Relay opens one to Alice. Those links publish Bob's PID through
//! the namespace. Alice waits for that publication, resolves Bob's PID to Relay
//! as the next hop, and opens an authenticated application stream whose QUIC
//! peer is Bob rather than Relay.
//!
//! To demonstrate what Relay can see, it wraps its carrier in `ObservedSocket`,
//! defined in the sibling `observed_socket.rs`. That wrapper forwards datagrams
//! unchanged while retaining bounded copies for Relay's final plaintext check.
//!
//! Alice exits after the postcard and 32 numbered echo streams complete. Relay
//! and Bob keep serving until their stop future completes; they then stop their
//! node, wait for its background work, and return a report that `main.rs` prints
//! and turns into the process exit status.

use std::fs::OpenOptions;
use std::future::Future;
use std::io;
use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrysalis::DatagramAddr;
use chrysalis::DatagramSocket;
use chrysalis::IncomingStream;
use chrysalis::NamespaceConfig;
use chrysalis::Node;
use chrysalis::NodeConfig;
use chrysalis::ParentEndpoint;
use chrysalis::Pid;
use chrysalis::PidPrefix;
use chrysalis::QuicIdentity;
use chrysalis::ResolveConsistency;
use chrysalis::TransportConfig;
use chrysalis::VsockDatagramAddr;
use chrysalis::VsockDatagramSocket;
use tokio::io::AsyncWriteExt as _;

use super::ACKNOWLEDGEMENT;
use super::POSTCARD;
use super::STREAM_COUNT;
use super::identity;
use super::observed_socket::ObservedSocket;
use super::read_bounded;

/// Maximum time Alice waits for Bob's namespace entry to reach her.
///
/// This includes parent-link retries and, in the VM run, guest startup.
const PEER_WAIT: Duration = Duration::from_secs(120);

/// Maximum time Alice waits for the completed exchange to appear in connection
/// statistics, which are published periodically.
const POOL_WAIT: Duration = Duration::from_secs(5);

/// Delay between local namespace checks while waiting for a peer publication.
const NAMESPACE_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Delay between checks for the transport's periodically published statistics.
const CONNECTION_STATS_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Result returned after Alice completes the exchange and stops her node.
///
/// The command needs only success or failure. The in-process role test also
/// checks the advertised address to prove the carrier was named from Alice's
/// certificate-derived PID.
#[derive(Debug)]
pub(crate) struct AliceReport {
    /// Alice's advertised `node:<pid>` carrier address.
    pub(crate) advertised: DatagramAddr,
}

/// Relay's traffic observations after it has stopped and drained its recorder.
#[derive(Debug)]
pub(crate) struct RelayReport {
    /// Relay's advertised `node:<pid>` carrier address.
    pub(crate) advertised: DatagramAddr,

    /// Total datagrams removed from `ObservedSocket` across every drain.
    pub(crate) observed: usize,

    /// Whether any recorded datagram contained the postcard's literal bytes.
    pub(crate) plaintext_seen: bool,

    /// Whether every recorder drain fit within its 4,096-datagram capacity.
    pub(crate) complete: bool,
}

/// Bob's stream results after he has stopped serving and shut down his node.
#[derive(Debug)]
pub(crate) struct BobReport {
    /// Bob's advertised `node:<pid>` carrier address.
    pub(crate) advertised: DatagramAddr,

    /// Number of completed application streams: one postcard and the echoes.
    pub(crate) served: usize,

    /// Whether every accepted stream authenticated Alice as its source.
    pub(crate) all_from_alice: bool,
}

/// Role name and certificate-derived PID for one generated identity bundle.
#[derive(Clone, Copy, Debug)]
pub(crate) struct NamedIdentity {
    /// Stable command-line role name: `alice`, `relay`, or `bob`.
    pub(crate) name: &'static str,

    /// PID derived from the role's generated leaf certificate.
    pub(crate) pid: Pid,
}

/// Creates a logical datagram endpoint over framed byte-stream connections.
///
/// Alice passes one listening address and Relay may pass several. Bob passes
/// none, so he cannot accept a new connection; he can still open a bidirectional
/// parent connection to Relay and send and receive datagrams over it.
///
/// The carrier announces `node:<pid>` on every connection it opens. Naming it
/// from the same certificate-derived PID that the Chrysalis node uses keeps
/// carrier replies and authenticated process identity aligned.
pub(crate) fn carrier(
    identity: &QuicIdentity,
    listen: &[VsockDatagramAddr],
) -> io::Result<Arc<VsockDatagramSocket>> {
    // VsockDatagramSocket takes the name without the `node:` prefix.
    let name = PidPrefix::from(identity.pid()).to_string();
    // Alice and Bob pass this shared socket directly to their Node. Relay first
    // wraps it in ObservedSocket so the Node's traffic can also be inspected.
    Ok(Arc::new(VsockDatagramSocket::bind(&name, listen)?))
}

/// Creates Alice, Relay, and Bob's identity-bundle files.
///
/// All three certificates are signed by one fresh test CA, so the roles trust
/// one another. Each file is created privately and is never overwritten. The
/// `identities` prints each returned value as `<role> <pid>`. Those PIDs become
/// Alice's `--bob-pid`, Relay's `--parent-pid`, and Bob's `--parent-pid` and
/// `--alice-pid` arguments.
pub(crate) fn write_identities(out: &Path) -> io::Result<Vec<NamedIdentity>> {
    // The caller supplies the run's identity directory. Reusing an existing
    // directory is safe because each bundle file below uses `create_new`.
    std::fs::create_dir_all(out)?;
    // Generate all bundles together so they share the same test CA.
    let bundles = identity::mutually_trusted_bundles::<3>();
    ["alice", "relay", "bob"]
        .into_iter()
        .zip(bundles)
        .map(|(name, bundle)| {
            let path = out.join(format!("{name}.pem"));
            // Set mode 0600 as part of file creation, before any private-key
            // bytes exist. `create_new` also refuses files and symlinks already
            // present at this path.
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)?;
            file.write_all(bundle.pem.as_bytes())?;
            Ok(NamedIdentity {
                name,
                pid: bundle.pid,
            })
        })
        .collect()
}

/// The parent identity and carrier destination configured for a child node.
struct ParentLink {
    /// PID the child requires the parent to authenticate as.
    pid: Pid,
    /// Carrier destination the child uses to reach the parent.
    address: VsockDatagramAddr,
}

/// Creates one role's Chrysalis node on a vsock datagram carrier.
///
/// `None` creates root Alice. A [`ParentLink`] creates a child and starts its
/// retrying parent connection. The returned address is the `node:<pid>` address
/// the carrier advertises to peers.
fn create_node(
    identity: QuicIdentity,
    listen: &[VsockDatagramAddr],
    parent: Option<ParentLink>,
) -> io::Result<(Node, DatagramAddr)> {
    // Bind zero or more physical stream listeners and name the logical carrier
    // after this identity's PID.
    let socket = carrier(&identity, listen)?;
    // Retain the logical address for reports before the socket moves into Node.
    let advertised = socket.local_addr().clone();
    // TransportConfig pairs the datagram carrier with this role's mutual-TLS
    // identity. NodeConfig::new starts as a root configuration and advertises
    // the carrier's local address; the block below turns it into a child when a
    // parent was supplied.
    let mut config = NodeConfig::new(TransportConfig::new(socket, identity));
    if let Some(parent) = parent {
        // VsockDatagramAddr is the structured, vsock-specific form used by the
        // CLI. Nameserver configuration is carrier-neutral, so encode it as a
        // DatagramAddr with scheme `vsock` and the corresponding address bytes.
        let parent_address = DatagramAddr::try_from(&parent.address)?;
        // One ParentEndpoint is one possible path for both control traffic and
        // application traffic to the parent. The API accepts several fallback
        // paths; this example supplies one.
        let parent_endpoint = ParentEndpoint::new(parent_address);
        // Pin the parent to its certificate-derived PID and associate that
        // identity with the allowed path. This also installs the nameserver's
        // default retry policy and rejects a reserved PID or an empty path list.
        let parent_namespace = NamespaceConfig::try_new(parent.pid, vec![parent_endpoint])
            .map_err(io::Error::other)?;
        // Mark this NodeConfig as a child. Node::create below uses this policy
        // to start the manager that repeatedly connects and authenticates the
        // parent, then exchanges namespace updates over that link.
        config = config.with_parent(parent_namespace);
    }
    // Node::create starts the transport and namespace background tasks. For a
    // child, it also starts ParentLinkManager: that task retries the configured
    // paths, authenticates the pinned parent PID, publishes this node upward,
    // receives namespace updates, and reconnects after failure. Node::create
    // returns before the parent connection has necessarily succeeded.
    Ok((Node::create(config).map_err(io::Error::other)?, advertised))
}

/// Waits until namespace propagation gives `node` a route to `pid`.
///
/// A child publishes itself through its parent link asynchronously. For
/// example, Bob first reaches Relay, then Relay republishes Bob to Alice.
/// `Node::create` returning therefore does not mean Alice can resolve Bob yet.
/// This polls Alice's local namespace until the entry arrives or `timeout`
/// expires.
///
/// # Errors
///
/// Returns [`io::ErrorKind::TimedOut`] when the entry does not arrive in time.
async fn wait_until_visible(node: &Node, pid: Pid, timeout: Duration) -> io::Result<()> {
    tokio::time::timeout(timeout, async {
        while node.nameserver().get(pid).await.is_none() {
            // Sleep between checks so waiting does not spin on the runtime.
            tokio::time::sleep(NAMESPACE_POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "peer did not become visible"))
}

/// Waits until the transport's latest statistics include a connection to `pid`.
///
/// QUIC keeps a connection to each peer so later streams can reuse it. After
/// Alice's 32 echo streams finish, this check confirms that her transport still
/// has a reusable connection to Bob. The statistics are refreshed periodically,
/// so the connection may appear shortly after the streams complete.
///
/// # Errors
///
/// Returns [`io::ErrorKind::TimedOut`] when no connection snapshot appears
/// within [`POOL_WAIT`].
async fn wait_until_pooled(node: &Node, pid: Pid) -> io::Result<()> {
    tokio::time::timeout(POOL_WAIT, async {
        while node.transport().connection_stats(pid).is_none() {
            // Sleep between snapshots so waiting does not spin on the runtime.
            tokio::time::sleep(CONNECTION_STATS_POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connection was not pooled"))
}

/// Runs root Alice through the complete application exchange with Bob.
///
/// Alice listens for Relay, waits for Bob's namespace entry to propagate
/// through Relay, sends the postcard, opens 32 numbered streams, and verifies
/// that Bob echoes each number. Alice always stops her node and waits for its
/// background work before returning.
pub(crate) async fn run_alice(
    identity: QuicIdentity,
    listen: VsockDatagramAddr,
    bob: Pid,
) -> io::Result<AliceReport> {
    // Alice has no parent. Her listener lets Relay establish the first link in
    // the namespace tree.
    let (node, advertised) = create_node(identity, &[listen], None)?;
    // Keep the result instead of returning immediately, so cleanup below runs
    // after both success and failure. Propagate the saved error only afterward.
    let result = alice_exchange(&node, bob).await;
    // Cleanup is unconditional: a failed postcard, stream, or pool check still
    // stops every task Alice started.
    node.shutdown();
    node.join().await;
    result?;
    Ok(AliceReport { advertised })
}

/// Waits for Bob, completes both application exchanges, and checks connection
/// reuse.
async fn alice_exchange(node: &Node, bob: Pid) -> io::Result<()> {
    // Bob publishes his PID to Relay, which propagates it to root Alice.
    wait_until_visible(node, bob, PEER_WAIT).await?;
    exchange_postcard(node, bob).await?;
    exchange_numbered_streams(node, bob).await?;
    wait_until_pooled(node, bob).await
}

/// Exchanges one postcard and acknowledgement with authenticated peer Bob.
///
/// Alice opens one bidirectional stream to Bob's PID. Relay is only the route;
/// Bob is the QUIC peer reported by the end-to-end connection.
async fn exchange_postcard(node: &Node, bob: Pid) -> io::Result<()> {
    // Resolve Bob through Alice's cached namespace and open the application
    // stream along the resulting route through Relay.
    let mut stream = node
        .dial(bob, ResolveConsistency::Cached)
        .await
        .map_err(io::Error::other)?;
    // Send the complete application-defined postcard on Alice's half.
    stream.send_mut().write_all(POSTCARD).await?;
    // Finishing Alice's sending half lets Bob read the postcard to EOF while
    // keeping Bob's acknowledgement half open.
    stream.send_mut().finish().await.map_err(io::Error::other)?;
    // Read Bob's reverse half through EOF, rejecting an oversized response.
    let acknowledgement = read_bounded(stream.recv_mut(), ACKNOWLEDGEMENT.len()).await?;
    // The exact bytes are the application-level success response for this demo.
    if acknowledgement != ACKNOWLEDGEMENT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Bob returned the wrong acknowledgement",
        ));
    }
    Ok(())
}

/// Verifies that many independent streams can share Alice and Bob's connection.
///
/// Alice opens 32 streams, sends a different one-byte number on each, and keeps
/// every stream alive. She then checks that Bob echoes each number on the same
/// stream.
async fn exchange_numbered_streams(node: &Node, bob: Pid) -> io::Result<()> {
    // Retain each stream together with the byte expected back from Bob.
    let mut streams = Vec::with_capacity(STREAM_COUNT);
    for index in 0..STREAM_COUNT {
        // The example's stream identifier is one byte. Fail explicitly if
        // STREAM_COUNT is ever increased beyond that representation.
        let number = u8::try_from(index).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream number does not fit in u8",
            )
        })?;
        // Each dial opens another stream to Bob while reusing the live QUIC
        // connection selected through the namespace.
        let mut stream = node
            .dial(bob, ResolveConsistency::Cached)
            .await
            .map_err(io::Error::other)?;
        stream.send_mut().write_all(&[number]).await?;
        // Finish only Alice's sending half. Bob can still return the echo on
        // the reverse half of this stream.
        stream.send_mut().finish().await.map_err(io::Error::other)?;
        streams.push((number, stream));
    }
    // Match every response to the stream on which its number was sent; stream
    // arrival order at Bob does not affect this check.
    for (number, mut stream) in streams {
        let echo = read_bounded(stream.recv_mut(), 1).await?;
        // An empty, oversized, or different response identifies the stream in
        // the error to make a failed process run diagnosable.
        if echo != [number] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Bob returned {echo:?} for stream {number}"),
            ));
        }
    }
    Ok(())
}

/// How often Relay takes recorded datagrams out of `ObservedSocket`.
///
/// `ObservedSocket` can hold at most 4,096 datagrams. Each drain counts them,
/// checks them for postcard plaintext, and clears that storage for more traffic.
/// Draining every 100 ms reduces the chance of filling it; any overflow marks
/// the final report incomplete.
const OBSERVATION_INTERVAL: Duration = Duration::from_millis(100);

/// Relay's accumulated view of every drained observation batch.
struct TrafficObservations {
    count: usize,
    plaintext_seen: bool,
    complete: bool,
}

impl TrafficObservations {
    /// Starts with an empty, complete record.
    fn new() -> Self {
        Self {
            count: 0,
            plaintext_seen: false,
            complete: true,
        }
    }

    /// Takes the socket's current batch and adds it to the accumulated result.
    fn drain(&mut self, socket: &ObservedSocket<VsockDatagramSocket>) {
        let snapshot = socket.take_observations();
        // One truncated batch makes the whole run incomplete.
        self.complete &= !snapshot.truncated;
        self.count += snapshot.datagrams.len();
        // Search raw encrypted datagrams for the postcard's exact literal bytes.
        self.plaintext_seen |= snapshot.datagrams.iter().any(|datagram| {
            datagram
                .bytes
                .windows(POSTCARD.len())
                .any(|window| window == POSTCARD)
        });
    }
}

/// A running Relay node and the traffic recorder attached to its carrier.
///
/// [`Relay::start`] creates the node and parent/listener links. [`Relay::serve`]
/// keeps this value alive until a stop signal, then shuts the node down and
/// turns the accumulated observations into [`RelayReport`].
pub(crate) struct Relay {
    /// Chrysalis node that maintains Relay's Alice parent link, Bob child link,
    /// namespace state, routing, and QUIC transport.
    node: Node,

    /// Recorder retained outside `Node` so `serve` can periodically drain the
    /// same carrier traffic that the node sends and receives.
    socket: Arc<ObservedSocket<VsockDatagramSocket>>,

    /// Relay's advertised `node:<pid>` address, captured before carrier
    /// ownership moved into the node.
    advertised: DatagramAddr,
}

impl Relay {
    /// Creates Relay, starts its parent link to Alice, and binds its child
    /// listeners.
    ///
    /// Returning means Relay's node and retrying parent manager are running; it
    /// does not require Alice's listener to be available yet. [`Self::serve`]
    /// owns the later serving and shutdown phase.
    pub(crate) fn start(
        identity: QuicIdentity,
        parent_pid: Pid,
        parent: VsockDatagramAddr,
        listen: Vec<VsockDatagramAddr>,
    ) -> io::Result<Self> {
        // Bind every address where children such as Bob may connect. Name the
        // logical carrier from Relay's certificate-derived PID.
        let carrier = carrier(&identity, &listen)?;
        // Save the logical `node:<pid>` address for the report before wrapping
        // and moving the carrier into Node.
        let advertised = carrier.local_addr().clone();
        // Record copies of Relay's carrier traffic while delegating every send
        // and receive unchanged to the real vsock socket.
        let socket = Arc::new(ObservedSocket::new(carrier));

        // Convert Alice's vsock-specific destination into the carrier-neutral
        // form used by the nameserver, then make it Relay's one parent path.
        let parent_address = DatagramAddr::try_from(&parent)?;
        let parent_endpoint = ParentEndpoint::new(parent_address);
        // Require the parent at that path to authenticate as Alice's PID.
        let parent_namespace = NamespaceConfig::try_new(parent_pid, vec![parent_endpoint])
            .map_err(io::Error::other)?;

        // Give Node a clone of the recorder. Relay retains the other Arc so
        // `serve` can drain observations from the same wrapper Node is using.
        let transport = TransportConfig::new(socket.clone(), identity);
        // Combine Relay's transport with the pinned Alice parent policy. This
        // makes Relay a child rather than a namespace root.
        let config = NodeConfig::new(transport).with_parent(parent_namespace);
        // Start Relay's transport, namespace service, child listener, and
        // retrying parent manager. The Alice connection may complete later.
        let node = Node::create(config).map_err(io::Error::other)?;
        Ok(Self {
            node,
            socket,
            advertised,
        })
    }

    /// Forwards traffic and records what Relay can observe until `stop` completes.
    ///
    /// Observation batches are drained while the node runs. After the stop
    /// signal, Relay shuts down, waits for its background work, drains one final
    /// batch containing teardown traffic, and returns the complete report.
    pub(crate) async fn serve<F>(self, stop: F) -> io::Result<RelayReport>
    where
        F: Future<Output = ()> + Send,
    {
        let Self {
            node,
            socket,
            advertised,
        } = self;
        let mut observations = TrafficObservations::new();
        // Delay missed ticks instead of running several drains back-to-back
        // after the runtime has been busy.
        let mut interval = tokio::time::interval(OBSERVATION_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // `stop` may borrow signal state and need not implement Unpin.
        tokio::pin!(stop);
        loop {
            tokio::select! {
                _ = &mut stop => break,
                _ = interval.tick() => observations.drain(&socket),
            }
        }
        // Stop forwarding and wait for every node-owned task before the final
        // drain, so the report includes traffic produced during teardown.
        node.shutdown();
        node.join().await;
        observations.drain(&socket);
        Ok(RelayReport {
            advertised,
            observed: observations.count,
            plaintext_seen: observations.plaintext_seen,
            complete: observations.complete,
        })
    }
}

/// A running Bob node and the Alice identity expected on application streams.
///
/// [`Bob::start`] creates Bob and starts his parent link to Relay.
/// [`Bob::serve`] accepts and answers streams until a stop signal, then shuts
/// the node down and returns [`BobReport`].
pub(crate) struct Bob {
    /// Chrysalis node that maintains Bob's Relay parent link, namespace state,
    /// routing, and end-to-end QUIC streams.
    node: Node,

    /// Certificate-derived PID every accepted application stream must report
    /// as its authenticated source.
    alice: Pid,

    /// Bob's advertised `node:<pid>` address, captured before carrier ownership
    /// moved into the node.
    advertised: DatagramAddr,
}

/// Application exchange expected on Bob's next accepted stream.
#[derive(Clone, Copy, Debug)]
enum IncomingExchange {
    /// Alice's postcard followed by Bob's acknowledgement.
    Postcard,
    /// One numbered byte followed by Bob echoing that byte.
    NumberedEcho,
}

impl Bob {
    /// Creates Bob and starts his retrying parent link to Relay.
    ///
    /// Bob has no listening address: he opens the outbound parent connection
    /// to `parent` and later accepts Alice's end-to-end streams through that
    /// link. Returning does not require Relay to be listening yet.
    pub(crate) fn start(
        identity: QuicIdentity,
        parent_pid: Pid,
        parent: VsockDatagramAddr,
        alice: Pid,
    ) -> io::Result<Self> {
        // An empty listener slice makes Bob outbound-only at the carrier layer.
        // ParentLink pins Relay's authenticated PID to the address Bob dials.
        let (node, advertised) = create_node(
            identity,
            &[],
            Some(ParentLink {
                pid: parent_pid,
                address: parent,
            }),
        )?;
        // Retain Alice's PID separately from the Relay parent policy: Relay is
        // the routing parent, while Alice is the authenticated application peer.
        Ok(Self {
            node,
            alice,
            advertised,
        })
    }

    /// Accepts and answers Alice's streams until `stop` completes.
    ///
    /// The first accepted stream carries the postcard; every later stream
    /// carries a one-byte number that Bob echoes. Bob records whether all
    /// streams authenticated Alice, then stops and waits for his node before
    /// returning the report.
    pub(crate) async fn serve<F>(self, stop: F) -> io::Result<BobReport>
    where
        F: Future<Output = ()> + Send,
    {
        let Self {
            node,
            alice,
            advertised,
        } = self;
        // `served == 0` distinguishes the postcard from the numbered streams.
        let mut served = 0;
        // Keep checking after a mismatch so the final report describes every
        // stream Bob served before shutdown.
        let mut all_from_alice = true;
        // Tests use a oneshot future; the CLI uses Ctrl-C or SIGTERM. Neither
        // future is required to implement Unpin.
        tokio::pin!(stop);
        // Save an application error instead of returning immediately so Bob's
        // node is always shut down and joined below.
        let result = loop {
            let exchange = if served == 0 {
                IncomingExchange::Postcard
            } else {
                IncomingExchange::NumberedEcho
            };
            tokio::select! {
                // Dropping `serve_one` may interrupt a stream in progress; node
                // shutdown below releases the remaining transport work.
                _ = &mut stop => break Ok(()),
                source = serve_one(&node, exchange) => {
                    match source {
                        Ok(source) => {
                            served += 1;
                            // IncomingStream::source is the PID authenticated by
                            // the end-to-end QUIC connection, not a claimed name.
                            all_from_alice &= source == alice;
                        }
                        Err(error) => break Err(error),
                    }
                }
            }
        };
        // Stop every Bob-owned task and wait for it before exposing the report
        // or propagating an application error.
        node.shutdown();
        node.join().await;
        result?;
        Ok(BobReport {
            advertised,
            served,
            all_from_alice,
        })
    }
}

/// Accepts one authenticated application stream and performs its exchange.
///
/// Returns the PID authenticated by the end-to-end QUIC connection so the
/// caller can verify that Alice opened every stream.
async fn serve_one(node: &Node, exchange: IncomingExchange) -> io::Result<Pid> {
    // Node::accept waits only for application streams addressed to Bob; parent
    // control traffic is handled by the node's link-local protocol.
    let mut incoming = node.accept().await.map_err(io::Error::other)?;
    // Capture the certificate-derived peer identity before borrowing the stream.
    let source = incoming.source();
    match exchange {
        IncomingExchange::Postcard => answer_postcard(&mut incoming).await?,
        IncomingExchange::NumberedEcho => echo_number(&mut incoming).await?,
    }
    // Finish Bob's sending half so Alice's bounded read reaches EOF. Bob has
    // already consumed Alice's sending half inside the selected exchange.
    incoming
        .stream_mut()
        .send_mut()
        .finish()
        .await
        .map_err(io::Error::other)?;
    Ok(source)
}

/// Reads and validates Alice's postcard, then writes Bob's acknowledgement.
async fn answer_postcard(incoming: &mut IncomingStream) -> io::Result<()> {
    let bytes = read_bounded(incoming.stream_mut().recv_mut(), POSTCARD.len()).await?;
    if bytes != POSTCARD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Alice sent the wrong postcard",
        ));
    }
    incoming
        .stream_mut()
        .send_mut()
        .write_all(ACKNOWLEDGEMENT)
        .await
}

/// Reads one numbered byte and writes the same byte back to Alice.
async fn echo_number(incoming: &mut IncomingStream) -> io::Result<()> {
    let bytes = read_bounded(incoming.stream_mut().recv_mut(), 1).await?;
    if bytes.len() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "numbered stream did not contain one byte",
        ));
    }
    incoming.stream_mut().send_mut().write_all(&bytes).await
}

/// Creates the stop future used by long-running Relay and Bob modes.
///
/// The future completes when the process receives either Ctrl-C (`SIGINT`) or
/// `SIGTERM`. Whichever arrives first starts the same orderly node shutdown.
///
/// # Errors
///
/// Returns an error when Tokio cannot install either Unix signal handler.
pub(crate) fn stop_signal() -> io::Result<impl Future<Output = ()>> {
    // Install both streams before returning so setup failures reach main
    // before the role starts its node and background tasks.
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    Ok(async move {
        // `select!` cancels the other wait after the first signal. The role
        // performs shutdown after this future returns.
        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigterm.recv() => {}
        }
    })
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use tokio::sync::oneshot;

    use super::*;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(1);

    fn test_dir(label: &str) -> PathBuf {
        let id = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
        let path = PathBuf::from(format!("/tmp/chr-{label}-{}-{id}", std::process::id()));
        std::fs::create_dir(&path).expect("create test directory");
        path
    }

    async fn stop(rx: oneshot::Receiver<()>) {
        let _ = rx.await;
    }

    fn node_address(pid: Pid) -> DatagramAddr {
        DatagramAddr::try_from(&VsockDatagramAddr::Node(PidPrefix::from(pid).to_string()))
            .expect("node address")
    }

    #[test]
    fn identity_files_are_private_and_never_overwritten() {
        let dir = test_dir("identities");
        let identities = write_identities(&dir).expect("write identities");
        assert_eq!(identities.len(), 3);
        let original = ["alice", "relay", "bob"].map(|name| {
            let path = dir.join(format!("{name}.pem"));
            let metadata = path.metadata().expect("identity metadata");
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
            let bundle = std::fs::read_to_string(path).expect("read identity");
            QuicIdentity::from_pem_bundle(&bundle, "localhost").expect("load identity");
            bundle
        });
        assert_eq!(
            write_identities(&dir)
                .expect_err("existing identities must not be overwritten")
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        for (name, expected) in ["alice", "relay", "bob"].into_iter().zip(original) {
            assert_eq!(
                std::fs::read_to_string(dir.join(format!("{name}.pem"))).expect("reread identity"),
                expected
            );
        }
        std::fs::remove_dir_all(dir).expect("remove test directory");
    }

    #[tokio::test]
    async fn three_roles_exchange_the_postcard_over_unix_streams() {
        let dir = test_dir("roles");
        let alice_path = dir.join("a");
        let relay_path = dir.join("r");
        let [alice_identity, relay_identity, bob_identity] =
            identity::mutually_trusted_identities::<3>();
        let alice_pid = alice_identity.pid();
        let relay_pid = relay_identity.pid();
        let bob_pid = bob_identity.pid();

        let bob = Bob::start(
            bob_identity,
            relay_pid,
            VsockDatagramAddr::Unix(relay_path.clone()),
            alice_pid,
        )
        .expect("start Bob");
        assert!(!relay_path.exists(), "Relay must not be listening yet");
        let (stop_bob, bob_stop) = oneshot::channel();
        let bob_task = tokio::spawn(bob.serve(stop(bob_stop)));

        let relay = Relay::start(
            relay_identity,
            alice_pid,
            VsockDatagramAddr::Unix(alice_path.clone()),
            vec![VsockDatagramAddr::Unix(relay_path)],
        )
        .expect("start Relay");
        assert!(!alice_path.exists(), "Alice must not be listening yet");
        let (stop_relay, relay_stop) = oneshot::channel();
        let relay_task = tokio::spawn(relay.serve(stop(relay_stop)));

        let alice = tokio::time::timeout(
            Duration::from_secs(60),
            run_alice(alice_identity, VsockDatagramAddr::Unix(alice_path), bob_pid),
        )
        .await
        .expect("Alice timed out")
        .expect("Alice failed");
        let _ = stop_bob.send(());
        let bob = tokio::time::timeout(Duration::from_secs(10), bob_task)
            .await
            .expect("Bob stop timed out")
            .expect("Bob task panicked")
            .expect("Bob failed");
        let _ = stop_relay.send(());
        let relay = tokio::time::timeout(Duration::from_secs(10), relay_task)
            .await
            .expect("Relay stop timed out")
            .expect("Relay task panicked")
            .expect("Relay failed");

        assert_eq!(alice.advertised, node_address(alice_pid));
        assert_eq!(bob.advertised, node_address(bob_pid));
        assert_eq!(relay.advertised, node_address(relay_pid));
        assert_eq!(bob.served, STREAM_COUNT + 1);
        assert!(bob.all_from_alice);
        assert!(relay.observed > 0);
        assert!(!relay.plaintext_seen);
        assert!(relay.complete);
        std::fs::remove_dir_all(dir).expect("remove test directory");
    }
}
