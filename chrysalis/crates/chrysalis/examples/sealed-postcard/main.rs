/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The sealed-postcard walkthrough and separate-process entry point.
//!
//! With no subcommand, this binary creates Alice, Relay, and Bob in one process
//! over an in-memory carrier. Alice sends Bob a postcard through Relay, receives
//! an acknowledgement, and checks 32 additional echo streams. A role
//! subcommand instead runs only Alice, Relay, or Bob, so separate invocations
//! can form the same process tree over real carrier sockets.
//!
//! The separate processes form this namespace tree:
//!
//! ```text
//! Relay (root; listens for Alice and Bob)
//! ├── Alice (connects to Relay)
//! └── Bob (connects to Relay)
//! ```
//!
//! Alice and Bob are the application endpoints. Their authenticated QUIC
//! streams pass through Relay, which forwards encrypted carrier datagrams but
//! does not terminate the application connection.
//!
//! This module owns command-line parsing, user-facing reports, and exit status.
//! [`roles`] owns the Chrysalis nodes and their application-stream behavior.
//!
//! ```text
//! main.rs
//! ├── parse command line
//! ├── no mode → run the original in-process walkthrough
//! └── role mode
//!     ├── identities → roles::write_identities
//!     ├── alice      → roles::run_alice
//!     ├── relay      → roles::Relay::start(...).serve(...)
//!     └── bob        → roles::Bob::start(...).serve(...)
//! ```

// Generates the ephemeral, mutually trusted identities used by both the
// in-process walkthrough and the separate role modes.
mod identity;
// Keep Relay's raw-datagram observation layer separate from the
// postcard application logic.
mod observed_socket;
// The role modes use VsockDatagramSocket, whose native-vsock implementation
// and public exports are Linux-only. The no-mode in-process walkthrough stays
// available on other platforms.
#[cfg(target_os = "linux")]
mod roles;

#[cfg(target_os = "linux")]
use std::env;
use std::io;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrysalis::DatagramSocket;
use chrysalis::InprocNetwork;
use chrysalis::NamespaceConfig;
use chrysalis::Node;
use chrysalis::NodeConfig;
use chrysalis::ParentEndpoint;
use chrysalis::Pid;
use chrysalis::PidPrefix;
// Role commands load their PEM bundle into a QuicIdentity before starting a
// node. Those commands are Linux-only because they use VsockDatagramSocket;
// the in-process walkthrough constructs its identities in `identity.rs`.
#[cfg(target_os = "linux")]
use chrysalis::QuicIdentity;
use chrysalis::RecvStream;
use chrysalis::Resolution;
use chrysalis::ResolveConsistency;
use chrysalis::TransportConfig;
use clap::Parser;
use clap::Subcommand;
use observed_socket::DatagramDirection;
use observed_socket::ObservedSocket;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

// Alice's arbitrary address within this in-memory network; bound
// endpoint IDs must be unique.
const ALICE_ENDPOINT: u64 = 1;
// Relay's arbitrary address within the same in-memory network; it
// must differ from Alice's endpoint ID.
const RELAY_ENDPOINT: u64 = 2;
// Bob's arbitrary address within the same in-memory network; it must
// differ from both Alice's and Relay's endpoint IDs.
const BOB_ENDPOINT: u64 = 3;
// Chrysalis streams carry opaque bytes; the postcard and
// acknowledgement formats are application conventions defined only by
// Alice and Bob.
const POSTCARD: &[u8] = b"Tie a yellow ribbon round the old oak tree.";
const ACKNOWLEDGEMENT: &[u8] = b"The porch light is on.";
// Number of numbered, bidirectional application streams Alice will open
// to Bob during the connection-reuse experiment.
const STREAM_COUNT: usize = 32;
// Maximum datagrams each endpoint can queue before applying
// backpressure.
const DATAGRAM_QUEUE_CAPACITY: usize = 1_024;

async fn read_bounded(recv: &mut RecvStream, limit: usize) -> io::Result<Vec<u8>> {
    let read_limit = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    let mut bytes = Vec::new();
    recv.take(read_limit).read_to_end(&mut bytes).await?;
    if bytes.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "stream exceeded application limit",
        ));
    }
    Ok(bytes)
}

/// Chooses between the in-process walkthrough and one separate-process role.
#[derive(Debug, Parser)]
#[command(name = "sealed-postcard")]
struct Cli {
    /// Optional role command. Absence selects the in-process walkthrough.
    #[command(subcommand)]
    role: Option<Command>,
}

/// A complete certificate-derived PID parsed at the command-line boundary.
#[derive(Clone, Copy, Debug)]
struct RolePid(Pid);

impl RolePid {
    /// Returns the validated complete PID.
    const fn pid(self) -> Pid {
        self.0
    }
}

impl FromStr for RolePid {
    type Err = io::Error;

    fn from_str(text: &str) -> io::Result<Self> {
        let prefix = PidPrefix::from_str(text)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        prefix
            .as_pid()
            .map(Self)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "PID must have 32 digits"))
    }
}

/// Identity setup and the three separate-process roles.
#[derive(Debug, Subcommand)]
enum Command {
    /// Write mutually trusted identity bundles for Alice, Relay, and Bob.
    Identities {
        /// Directory in which to create `alice.pem`, `relay.pem`, and `bob.pem`.
        #[arg(long)]
        out: PathBuf,
    },
    /// Run Alice until the postcard and echo-stream exchange completes.
    Alice {
        /// Relay's PID. Alice uses it to authenticate her parent connection.
        #[arg(long)]
        parent_pid: RolePid,
        /// Carrier destination for Alice's parent link to Relay. Locally this
        /// names Relay's Unix listener; in a VM, `native:2:5001` reaches Relay
        /// on the host.
        #[arg(long)]
        parent: String,
        /// Bob's certificate-derived PID from the `identities` output. Bob's
        /// parent link publishes this PID to Relay; Alice waits until she can
        /// resolve it through Relay, then opens an authenticated end-to-end
        /// stream to it.
        #[arg(long)]
        bob_pid: RolePid,
    },
    /// Run Relay, the namespace root, until Ctrl-C or SIGTERM.
    Relay {
        /// Text-form carrier addresses where children can connect, such as
        /// `unix:/tmp/sealed-postcard/r` or `native:4294967295:5001`.
        #[arg(long, required = true)]
        listen: Vec<String>,
    },
    /// Run Bob until Ctrl-C or SIGTERM.
    Bob {
        /// Relay's PID. Bob uses it to authenticate its parent connection.
        #[arg(long)]
        parent_pid: RolePid,
        /// Carrier destination for Bob's parent link to Relay. Locally this
        /// names Relay's Unix listener; in a VM, `native:2:5001` reaches Relay
        /// on the host.
        #[arg(long)]
        parent: String,
        /// Alice's PID. Bob checks that every application stream came from her.
        #[arg(long)]
        alice_pid: RolePid,
    },
}

#[tokio::main]
async fn main() {
    let result = match Cli::parse().role {
        Some(command) => run_role(command).await,
        None => {
            run().await;
            Ok(())
        }
    };
    if let Err(error) = result {
        eprintln!("sealed-postcard: {error}");
        process::exit(1);
    }
}

/// Rejects role modes when the Linux-only `VsockDatagramSocket` is unavailable.
///
/// The no-mode in-process walkthrough does not call this function and remains
/// available on other platforms.
#[cfg(not(target_os = "linux"))]
async fn run_role(_command: Command) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "sealed-postcard roles require Linux",
    ))
}

/// Dispatches one Linux role command.
#[cfg(target_os = "linux")]
async fn run_role(command: Command) -> io::Result<()> {
    match command {
        Command::Identities { out } => run_identities_mode(&out),
        Command::Alice {
            parent_pid,
            parent,
            bob_pid,
        } => run_alice_mode(parent_pid, parent, bob_pid).await,
        Command::Relay { listen } => run_relay_mode(listen).await,
        Command::Bob {
            parent_pid,
            parent,
            alice_pid,
        } => run_bob_mode(parent_pid, parent, alice_pid).await,
    }
}

/// Writes the three identity bundles and prints their PIDs.
#[cfg(target_os = "linux")]
fn run_identities_mode(out: &std::path::Path) -> io::Result<()> {
    for identity in roles::write_identities(out)? {
        println!("{} {}", identity.name, PidPrefix::from(identity.pid));
    }
    Ok(())
}

/// Runs Alice until the postcard and echo-stream exchange completes.
///
/// Alice connects to Relay at `parent`, requiring Relay's `parent_pid`.
#[cfg(target_os = "linux")]
async fn run_alice_mode(parent_pid: RolePid, parent: String, bob_pid: RolePid) -> io::Result<()> {
    let parent = parent.parse()?;
    let parent_pid = parent_pid.pid();
    let bob = bob_pid.pid();
    // `run_alice` owns Alice's complete lifecycle: it creates her parent link
    // to Relay, waits until she can resolve Bob's PID through Relay, opens an
    // authenticated stream to that PID, performs every application exchange,
    // and waits for Alice's background work to finish before returning.
    // It also returns Alice's advertised carrier address so the role test can
    // verify that the address is named after her PID. The CLI does not print it.
    let roles::AliceReport {
        advertised: _advertised,
    } = roles::run_alice(role_identity()?, parent_pid, parent, bob).await?;
    Ok(())
}

/// Runs the middle node between Alice and Bob.
///
/// Relay is the namespace root. It listens for Alice and Bob on each address in
/// `listen` and forwards their encrypted datagrams until Ctrl-C or SIGTERM, then
/// stops its background work and prints whether its complete traffic record
/// contained any postcard plaintext.
#[cfg(target_os = "linux")]
async fn run_relay_mode(listen: Vec<String>) -> io::Result<()> {
    // Relay may listen on more than one carrier endpoint. The local and VM
    // demos use Unix addresses here. Parse them as VsockDatagramAddr values;
    // binding accepts only the Unix and Native variants.
    let listen = listen
        .into_iter()
        .map(|address| address.parse())
        .collect::<io::Result<Vec<_>>>()?;
    // Install both signal handlers before starting Relay. If either handler
    // cannot be installed, no node or background task has been created yet.
    let stop = roles::stop_signal()?;

    // `start` binds Relay's listeners and creates the root node. `serve`
    // forwards traffic until Ctrl-C or SIGTERM, then stops and waits for
    // Relay's background work before returning the complete report.
    let roles::RelayReport {
        advertised: _advertised,
        observed,
        plaintext_seen,
        complete,
    } = roles::Relay::start(role_identity()?, listen)?
        .serve(stop)
        .await?;

    // Report how much encrypted carrier traffic Relay recorded, whether any
    // datagram exposed the postcard text, and whether the bounded log retained
    // every observation. The advertised address is returned for the role test;
    // Relay's CLI report does not include it.
    println!(
        "relay observed {observed} datagrams, postcard plaintext seen: {plaintext_seen}, \
         log complete: {complete}"
    );
    // Print the report even on failure, then use the exit status to make every
    // condition machine-checkable by the process and VM tests.
    if observed == 0 || plaintext_seen || !complete {
        return Err(io::Error::other("Relay's observation check failed"));
    }
    Ok(())
}

/// Runs Alice's application peer as Relay's child.
///
/// Bob connects to Relay at `parent`, requiring Relay's `parent_pid`. He accepts
/// the postcard and 32 echo streams, requiring every stream to authenticate as
/// Alice's `alice_pid`. On Ctrl-C or SIGTERM, he stops his background work and
/// reports the streams he served.
#[cfg(target_os = "linux")]
async fn run_bob_mode(parent_pid: RolePid, parent: String, alice_pid: RolePid) -> io::Result<()> {
    let parent = parent.parse()?;
    let parent_pid = parent_pid.pid();
    let alice = alice_pid.pid();
    // Install both signal handlers before starting Bob. If either handler
    // cannot be installed, no node or background task has been created yet.
    let stop = roles::stop_signal()?;

    // `start` creates Bob's authenticated parent link to Relay. `serve` accepts
    // Alice's streams until Ctrl-C or SIGTERM, then stops and waits for Bob's
    // background work before returning the complete report.
    let roles::BobReport {
        advertised: _advertised,
        served,
        all_from_alice,
    } = roles::Bob::start(role_identity()?, parent_pid, parent, alice)?
        .serve(stop)
        .await?;

    // One postcard stream plus 32 numbered streams must all authenticate as
    // Alice. The advertised address is returned for the role test; Bob's CLI
    // report does not include it.
    println!("bob served {served} streams, all from alice: {all_from_alice}");
    // Print the report even on failure, then make both checks part of the exit
    // status consumed by the process and VM tests.
    if served != STREAM_COUNT + 1 || !all_from_alice {
        return Err(io::Error::other("Bob's stream check failed"));
    }
    Ok(())
}

/// Loads the identity assigned to this role by the process launcher.
///
/// `SEALED_POSTCARD_IDENTITY` contains the PEM bundle text itself, not a file
/// path. The generated role certificates authenticate the name `localhost`.
#[cfg(target_os = "linux")]
fn role_identity() -> io::Result<QuicIdentity> {
    // Treat a missing or non-Unicode environment value as invalid command
    // input. `from_pem_bundle` reports malformed bundle contents separately.
    let bundle = env::var("SEALED_POSTCARD_IDENTITY").map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("SEALED_POSTCARD_IDENTITY is not set: {error}"),
        )
    })?;
    QuicIdentity::from_pem_bundle(&bundle, "localhost")
}

/// Runs Alice, Relay, and Bob together over one in-memory carrier.
///
/// This is the no-subcommand walkthrough. A role subcommand is dispatched
/// above and returns without entering this path.
async fn run() {
    // Use an in-memory datagram carrier so we can exercise Chrysalis
    // without configuring UDP.
    let network = InprocNetwork::new(
        NonZeroUsize::new(DATAGRAM_QUEUE_CAPACITY).expect("queue capacity is nonzero"),
    );

    // Generate one ephemeral test CA and three certificate/private-key
    // pairs signed by it. Each mutual-TLS config trusts that shared
    // issuer. QuicIdentity derives each 128-bit PID from the first 16
    // bytes of SHA-256 over its certificate.
    let [alice_identity, relay_identity, bob_identity] =
        identity::mutually_trusted_identities::<3>();
    // Save the PID before moving relay_identity into Node, so we can
    // verify that the resulting Node exposes the same identity.
    let certificate_pid = relay_identity.pid();

    // Bind Relay to endpoint 2 on the in-memory network, then wrap the
    // bound carrier in a shareable ObservedSocket. Relay's Node
    // components use the wrapper normally while it records each
    // successful raw ingress and egress datagram and delegates the
    // bytes unchanged.
    let relay_carrier = Arc::new(
        network
            .bind(RELAY_ENDPOINT)
            .expect("Relay's in-process endpoint should be unused"),
    );
    let relay_socket = Arc::new(ObservedSocket::new(relay_carrier));
    // Save Relay's underlying carrier address. Alice and Bob will use it
    // to contact their parent, and checkpoint 3 compares it with Alice's
    // locator for Bob. ObservedSocket delegates local_addr() to the
    // wrapped in-memory socket.
    let relay_address = relay_socket.local_addr().clone();
    // Combine Relay's carrier and identity, then start a root Chrysalis
    // node. No parent is configured, so Relay is the root of the
    // namespace.
    //
    // Pass Node an Arc clone of the observer so this function retains
    // a handle to the same observation log for inspection after the
    // postcard.
    let relay = Node::create(NodeConfig::new(TransportConfig::new(
        relay_socket.clone(),
        relay_identity,
    )))
    .expect("Relay should start with a valid identity and carrier");

    // Observe the PID exposed by Node after Relay's identity was
    // moved into it.
    let node_pid_matches_certificate = relay.pid() == certificate_pid;
    println!("sealed-postcard, checkpoint 1: Relay exists");
    println!("certificate-derived PID: {}", PidPrefix::from(relay.pid()));
    println!("node PID matches certificate: {node_pid_matches_certificate}");
    assert!(
        node_pid_matches_certificate,
        "Relay's node PID should match its certificate-derived PID"
    );

    // Bind Alice to endpoint 1 on the same in-memory network and make
    // the resulting socket shareable by Node's async components.
    let alice_socket = Arc::new(
        network
            .bind(ALICE_ENDPOINT)
            .expect("Alice's in-process endpoint should be unused"),
    );
    // Save Alice's carrier address; checkpoint 5 looks for Relay's
    // ingress from it.
    let alice_address = alice_socket.local_addr().clone();
    // Start Alice as Relay's child. Relay's PID identifies the parent
    // Alice must authenticate; Relay's carrier address tells Alice
    // where to contact it.
    let alice = Node::create(
        NodeConfig::new(TransportConfig::new(alice_socket, alice_identity)).with_parent(
            NamespaceConfig::try_new(
                relay.pid(),
                vec![ParentEndpoint::new(relay_address.clone())],
            )
            .expect("Relay should have a nonreserved PID and one endpoint"),
        ),
    )
    .expect("create Alice's node");

    // Alice's parent connection starts asynchronously. Resolve Relay
    // through that link to wait for it to become active, with a
    // timeout to prevent hanging.
    let relay_resolution = tokio::time::timeout(
        Duration::from_secs(5),
        alice.resolve(relay.pid(), ResolveConsistency::Refresh),
    )
    .await
    .expect("Alice should connect to Relay within five seconds")
    .expect("Alice should resolve Relay through her parent link");
    // Require a positive resolution and extract Relay's process
    // entry; a NotFound result means this checkpoint failed.
    let Resolution::Found {
        entry: relay_entry, ..
    } = relay_resolution
    else {
        panic!("Relay should be present in Alice's namespace");
    };

    // Wait for Alice's publication to reach Relay before consulting
    // Relay's local directory.
    wait_until_visible(&relay, alice.pid()).await;
    let alice_resolution = relay
        .resolve(alice.pid(), ResolveConsistency::Cached)
        .await
        .expect("Relay's local namespace lookup should succeed");
    // Require Relay's lookup to find Alice and retain Alice's
    // published entry.
    let Resolution::Found {
        entry: alice_entry, ..
    } = alice_resolution
    else {
        panic!("Alice should be present in Relay's namespace");
    };

    // Confirm that each lookup returned the PID of the process
    // requested.
    let alice_resolves_relay = relay_entry.pid == relay.pid();
    let relay_resolves_alice = alice_entry.pid == alice.pid();
    println!("sealed-postcard, checkpoint 2: Alice joined Relay");
    println!("Alice resolves Relay: {alice_resolves_relay}");
    println!("Relay resolves Alice: {relay_resolves_alice}");
    assert!(alice_resolves_relay, "Alice should resolve Relay's PID");
    assert!(relay_resolves_alice, "Relay should resolve Alice's PID");

    // Bind Bob to endpoint 3 on the same in-memory network and make
    // its socket shareable by the components of Bob's Node.
    let bob_socket = Arc::new(
        network
            .bind(BOB_ENDPOINT)
            .expect("Bob's in-process endpoint should be unused"),
    );
    // Save Bob's direct carrier address so we can compare it with the
    // contextual locators returned by namespace resolution.
    let bob_address = bob_socket.local_addr().clone();
    // Start Bob as Relay's child. Relay's PID identifies the parent
    // Bob must authenticate; Relay's carrier address tells Bob where
    // to contact it.
    let bob = Node::create(
        NodeConfig::new(TransportConfig::new(bob_socket, bob_identity)).with_parent(
            NamespaceConfig::try_new(
                relay.pid(),
                vec![ParentEndpoint::new(relay_address.clone())],
            )
            .expect("Relay should have a nonreserved PID and one endpoint"),
        ),
    )
    .expect("Bob should start with a valid identity and carrier");

    // Wait for Bob's publication to reach Relay.
    wait_until_visible(&relay, bob.pid()).await;

    // Resolve Bob from Relay's local directory; because Relay is
    // Bob's direct parent, the returned locator should be Bob's
    // actual address.
    let bob_from_relay = relay
        .resolve(bob.pid(), ResolveConsistency::Cached)
        .await
        .expect("Relay's local namespace lookup should succeed");
    // Require a positive result and retain Relay's contextual view of
    // Bob.
    let Resolution::Found {
        entry: relay_view, ..
    } = bob_from_relay
    else {
        panic!("Bob should be present in Relay's namespace");
    };

    // Resolve the same Bob PID from Alice. Bob is not in her own
    // directory, so the lookup goes through her parent link to Relay,
    // and the returned locator should point to Relay.
    let bob_from_alice = alice
        .resolve(bob.pid(), ResolveConsistency::Cached)
        .await
        .expect("Alice's lookup through Relay should succeed");
    // Require a positive result and retain Alice's contextual view of
    // Bob.
    let Resolution::Found {
        entry: alice_view, ..
    } = bob_from_alice
    else {
        panic!("Bob should be visible to Alice through Relay");
    };

    // Check that Relay's view contains Bob's direct carrier address.
    let relay_reaches_bob_directly = relay_view
        .locators
        .iter()
        .any(|locator| locator.address == bob_address);
    // Check that Alice's view uses Relay's address as the next hop to
    // Bob.
    let alice_reaches_bob_through_relay = alice_view
        .locators
        .iter()
        .any(|locator| locator.address == relay_address);
    let relay_resolves_bob = relay_view.pid == bob.pid();
    let alice_resolves_bob = alice_view.pid == bob.pid();

    // Display the same target PID with the different next hops seen
    // by Relay and Alice.
    println!("sealed-postcard, checkpoint 3: Bob joined Relay");
    println!(
        "resolve Bob from Relay: pid=Bob {}, next_hop=Bob {}",
        relay_resolves_bob, relay_reaches_bob_directly
    );
    println!(
        "resolve Bob from Alice: pid=Bob {}, next_hop=Relay {}",
        alice_resolves_bob, alice_reaches_bob_through_relay
    );
    assert!(relay_resolves_bob, "Relay should resolve Bob's PID");
    assert!(
        relay_reaches_bob_directly,
        "Relay should use Bob's direct carrier address"
    );
    assert!(alice_resolves_bob, "Alice should resolve Bob's PID");
    assert!(
        alice_reaches_bob_through_relay,
        "Alice should use Relay as the next hop to Bob"
    );

    // Define Bob's half of the round trip: accept one stream, record
    // its authenticated source, read the postcard, and send an
    // acknowledgement.
    let bob_receives = async {
        // Wait for Bob's next peer-initiated application stream.
        let mut incoming = bob
            .accept()
            .await
            .expect("Bob's application transport should remain open");
        // Record the source PID authenticated by the end-to-end QUIC
        // handshake.
        let authenticated_source = incoming.source();
        // Read until Alice finishes her sending half, rejecting a
        // payload larger than the expected postcard.
        let received_postcard = read_bounded(incoming.stream_mut().recv_mut(), POSTCARD.len())
            .await
            .expect("Bob should read Alice's postcard");
        // Write the complete acknowledgement on the Bob-to-Alice half
        // of the same bidirectional stream.
        incoming
            .stream_mut()
            .send_mut()
            .write_all(ACKNOWLEDGEMENT)
            .await
            .expect("Bob should send the acknowledgement");
        // Finish Bob's sending half so Alice can observe the end of
        // the acknowledgement with read_to_end().
        incoming
            .stream_mut()
            .send_mut()
            .finish()
            .await
            .expect("Bob should finish the acknowledgement");
        // Return Bob's observations so the main flow can verify them
        // after both sides of the round trip complete.
        (authenticated_source, received_postcard)
    };

    // Define Alice's half of the round trip: dial Bob by PID, send
    // the postcard, finish her sending half, and wait for Bob's
    // acknowledgement.
    let alice_sends = async {
        // Resolve Bob from Alice's namespace and open an
        // authenticated bidirectional stream; routing begins through
        // Relay, but Bob is the peer.
        let mut stream = alice
            .dial(bob.pid(), ResolveConsistency::Cached)
            .await
            .expect("Alice should dial Bob through the namespace");
        // Write the complete postcard on the Alice-to-Bob half of the
        // stream.
        stream
            .send_mut()
            .write_all(POSTCARD)
            .await
            .expect("Alice should send the postcard");
        // Finish Alice's sending half so Bob can observe the end of
        // the postcard while Alice keeps receiving on the reverse
        // half.
        stream
            .send_mut()
            .finish()
            .await
            .expect("Alice should finish the postcard");
        // Read Bob's reverse half through its end marker and return
        // the acknowledgement bytes as this future's result.
        read_bounded(stream.recv_mut(), ACKNOWLEDGEMENT.len())
            .await
            .expect("Alice should read Bob's acknowledgement")
    };

    // Drive Bob's and Alice's futures concurrently, fail if the round
    // trip hangs, and unpack the observations returned by both sides.
    let ((authenticated_source, received_postcard), acknowledgement) =
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(bob_receives, alice_sends)
        })
        .await
        .expect("the postcard round trip should complete within five seconds");
    let bob_authenticated_alice = authenticated_source == alice.pid();
    let postcard_arrived_intact = received_postcard == POSTCARD;
    let acknowledgement_arrived_intact = acknowledgement == ACKNOWLEDGEMENT;

    // Report the authenticated peer identity and verify both
    // application payloads after the round trip.
    println!("sealed-postcard, checkpoint 4: one postcard crossed Relay");
    println!("Bob authenticated source=Alice: {bob_authenticated_alice}");
    println!("Bob received postcard intact: {postcard_arrived_intact}");
    println!(
        "postcard contents: {}",
        String::from_utf8_lossy(&received_postcard)
    );
    println!("Alice received acknowledgement: {acknowledgement_arrived_intact}");
    assert!(
        bob_authenticated_alice,
        "Bob should authenticate Alice as the stream source"
    );
    assert!(
        postcard_arrived_intact,
        "Bob should receive Alice's postcard intact"
    );
    assert!(
        acknowledgement_arrived_intact,
        "Alice should receive Bob's acknowledgement intact"
    );

    // Snapshot Relay's raw datagram observations after the postcard
    // round trip; the log includes both control-plane and application
    // traffic.
    let relay_observations = relay_socket.take_observations();
    let relay_datagrams = relay_observations.datagrams;
    // Confirm that Relay observed at least one raw datagram whose
    // QUIC CID named Bob's PID as its routing target.
    let relay_saw_bob_target = relay_datagrams
        .iter()
        .any(|datagram| datagram.target == Some(bob.pid()));
    // Look for Bob-targeted ingress and egress observations containing
    // exactly the same raw datagram bytes. This illustrates the
    // current carrier's packet boundaries rather than defining a
    // transport invariant.
    let relay_forwarded_bob_datagram_unchanged = relay_datagrams.iter().any(|ingress| {
        ingress.direction == DatagramDirection::Ingress
            && ingress.target == Some(bob.pid())
            && ingress.peer == alice_address
            && relay_datagrams.iter().any(|egress| {
                egress.direction == DatagramDirection::Egress
                    && egress.target == Some(bob.pid())
                    && egress.peer == bob_address
                    && egress.bytes == ingress.bytes
            })
    });
    // Search every raw Relay datagram for the postcard's exact byte
    // sequence; absence is an illustration, not proof of
    // confidentiality.
    let postcard_visible_at_relay = relay_datagrams.iter().any(|datagram| {
        datagram
            .bytes
            .windows(POSTCARD.len())
            .any(|window| window == POSTCARD)
    });

    // Report Relay's visible routing metadata, one exact forwarding
    // match, and the illustrative absence of the literal postcard
    // bytes.
    println!("sealed-postcard, checkpoint 5: inspect Relay forwarding");
    println!(
        "Relay observation log remained complete: {}",
        !relay_observations.truncated
    );
    println!("Relay observed CID routing target=Bob: {relay_saw_bob_target}");
    println!(
        "observed Bob-targeted ingress forwarded byte-for-byte: \
         {relay_forwarded_bob_datagram_unchanged}"
    );
    println!(
        "postcard bytes present verbatim in Relay datagrams: {postcard_visible_at_relay} \
         (illustration only)"
    );
    assert!(
        !relay_observations.truncated,
        "Relay's bounded observation log should contain the complete demonstration"
    );
    assert!(
        relay_saw_bob_target,
        "Relay should observe a datagram routed toward Bob"
    );
    assert!(
        !postcard_visible_at_relay,
        "Relay should not observe the literal postcard in QUIC datagrams"
    );

    // Define—but do not yet run—Bob's half of the experiment: accept
    // the configured number of streams, check that each reports Alice
    // as its authenticated source, record its number, and echo that
    // number back.
    let bob_accepts_streams = async {
        // Accumulate whether every accepted stream reports Alice as
        // its source and the one-byte stream numbers Bob receives.
        // with_capacity reserves storage for every configured number
        // but leaves the vector empty.
        let mut every_source_is_alice = true;
        let mut received_stream_numbers = Vec::with_capacity(STREAM_COUNT);

        // Accept exactly STREAM_COUNT streams. Bob discards the loop
        // index because each stream identifies itself through its
        // payload, not its arrival order.
        for _ in 0..STREAM_COUNT {
            // Wait for Bob's next peer-initiated application
            // stream—not a new connection. incoming pairs that
            // bidirectional stream with the source PID authenticated
            // by its underlying QUIC connection.
            let mut incoming = bob
                .accept()
                .await
                .expect("Bob's application transport should remain open");
            // Compare this stream's connection-authenticated source
            // PID with Alice's PID. &= keeps the accumulator true
            // only if this and every earlier stream matched.
            every_source_is_alice &= incoming.source() == alice.pid();

            // Read Alice's sending half through its end marker,
            // allowing at most one byte because this experiment
            // encodes each stream number as a single u8.
            let payload = read_bounded(incoming.stream_mut().recv_mut(), 1)
                .await
                .expect("Bob should read one stream number");
            // Treat the payload as a stream number only when it
            // contains exactly one byte, then copy that byte into
            // Bob's observations for the later completeness check.
            if let [stream_number] = payload.as_slice() {
                received_stream_numbers.push(*stream_number);
            }

            // Echo the received payload on Bob's sending half of this
            // same bidirectional stream; write_all ensures the
            // complete byte is submitted.
            incoming
                .stream_mut()
                .send_mut()
                .write_all(&payload)
                .await
                .expect("Bob should echo the stream number");
            // Finish Bob's sending half so Alice can observe the end
            // of the echo. This finishes one stream direction, not
            // the pooled Alice–Bob connection.
            incoming
                .stream_mut()
                .send_mut()
                .finish()
                .await
                .expect("Bob should finish the stream-number echo");
        }

        // Return Bob's accumulated source check and received stream
        // numbers as this future's result; the join below will unpack
        // both values.
        (every_source_is_alice, received_stream_numbers)
    };

    // Define—but do not yet run—Alice's half: open the configured
    // number of streams to Bob, retain their handles, then verify
    // Bob’s echo on every stream.
    let alice_opens_streams = async {
        // Reserve space for each stream handle paired with its
        // expected echo byte. Keeping these pairs lets Alice validate
        // each response on its original stream.
        let mut streams = Vec::with_capacity(STREAM_COUNT);

        // Iterate over every configured index; each will identify one
        // newly opened bidirectional stream.
        for stream_index in 0..STREAM_COUNT {
            // Convert the usize loop index into the one-byte wire
            // representation. The checked conversion prevents silent
            // truncation if STREAM_COUNT grows too large.
            let stream_number = u8::try_from(stream_index)
                .unwrap_or_else(|_| panic!("{STREAM_COUNT} stream indices should fit in one byte"));
            // Resolve Bob using Alice's cached namespace view, then
            // open a new bidirectional application stream. The
            // transport reuses its live Bob connection instead of
            // creating a connection for every stream.
            let mut stream = alice
                .dial(bob.pid(), ResolveConsistency::Cached)
                .await
                .expect("Alice should open another stream to Bob");
            // Send this stream's one-byte number on the Alice-to-Bob
            // half. Chrysalis carries this application-defined byte
            // without interpreting it.
            stream
                .send_mut()
                .write_all(&[stream_number])
                .await
                .expect("Alice should send the stream number");
            // Finish Alice's sending half so Bob's read_to_end() can
            // complete, while leaving Alice's receiving half open for
            // Bob's echo.
            stream
                .send_mut()
                .finish()
                .await
                .expect("Alice should finish sending the stream number");
            // Retain the expected byte alongside its still-live
            // stream so Alice can match Bob's later echo to the
            // stream on which she sent it.
            streams.push((stream_number, stream));
        }

        // Begin with the claim that every echo matches; any empty or
        // incorrect echo encountered below will permanently change
        // this accumulator to false.
        let mut every_echo_matches = true;
        // After opening all configured streams, consume the stored pairs one
        // at a time. stream is mutable because Alice will read from
        // its receiving half.
        for (stream_number, mut stream) in streams {
            // Read Bob's reverse half through its end marker, again
            // allowing at most the single byte defined by this
            // experiment's echo format.
            let echo = read_bounded(stream.recv_mut(), 1)
                .await
                .expect("Alice should read Bob's stream-number echo");
            // Require the echo to contain exactly the expected byte,
            // and combine that result with every earlier stream's
            // result.
            every_echo_matches &= echo.as_slice() == [stream_number];
        }
        // Return the accumulated echo check as Alice's future result.
        every_echo_matches
    };

    // Poll Bob's and Alice's futures concurrently under one
    // five-second deadline. Unpack Bob's source check and mutable
    // number list alongside Alice's echo check; a timeout fails the
    // example instead of letting it hang.
    let ((every_source_is_alice, mut received_stream_numbers), every_echo_matches) =
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(bob_accepts_streams, alice_opens_streams)
        })
        .await
        .unwrap_or_else(|_| {
            panic!("{STREAM_COUNT} stream round trips should complete within five seconds")
        });

    // Normalize the streams' arrival order before comparison. Sorting
    // preserves duplicates, so a missing or repeated number will
    // still make the check fail.
    received_stream_numbers.sort_unstable();
    // Build the canonical expected vector for every configured stream,
    // converting each usize index into the same checked one-byte
    // representation Alice sent.
    let expected_stream_numbers = (0..STREAM_COUNT)
        .map(|stream_index| {
            u8::try_from(stream_index)
                .unwrap_or_else(|_| panic!("{STREAM_COUNT} stream indices should fit in one byte"))
        })
        .collect::<Vec<_>>();
    // Compare the sorted vectors exactly, proving Bob received every
    // number once with no missing, duplicate, malformed, or
    // unexpected value.
    let bob_received_every_stream = received_stream_numbers == expected_stream_numbers;
    // Record whether peer-specific pool entries are visible after all
    // the configured streams. These snapshots are not stable connection IDs.
    let pooled_connection_after = wait_until_pooled(&alice, &bob).await;

    // Report the end-to-end stream checks and the peer-specific pool
    // snapshot after the additional streams.
    println!("sealed-postcard, checkpoint 6: reuse one pooled connection");
    println!("Bob received every numbered stream: {bob_received_every_stream}");
    println!("Every numbered stream authenticated source=Alice: {every_source_is_alice}");
    println!("Alice received {STREAM_COUNT} matching echoes: {every_echo_matches}");
    println!(
        "pooled Alice-Bob connection visible after {STREAM_COUNT} streams: Alice={} Bob={}",
        pooled_connection_after.0, pooled_connection_after.1
    );
    assert!(
        bob_received_every_stream,
        "Bob should receive every numbered stream exactly once"
    );
    assert!(
        every_source_is_alice,
        "every numbered stream should authenticate Alice as its source"
    );
    assert!(
        every_echo_matches,
        "Alice should receive the matching echo on every numbered stream"
    );
    assert!(
        pooled_connection_after.0 && pooled_connection_after.1,
        "Alice and Bob should expose their pooled connection after the extra streams"
    );

    // Stop the children first, while Relay is still running to
    // withdraw each of them.
    bob.shutdown();
    bob.join().await;
    alice.shutdown();
    alice.join().await;
    // Request shutdown, then wait until every task owned by Relay, the
    // root, has finished.
    relay.shutdown();
    relay.join().await;
    println!("Bob, Alice, and Relay shut down cleanly");
}

// Wait until a PID appears in one node's local nameserver, failing
// instead of hanging forever if publication does not arrive.
async fn wait_until_visible(node: &Node, pid: Pid) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while node.nameserver().get(pid).await.is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("process should become visible within five seconds");
}

// Wait until both peers' connection statistics name each other,
// returning the last observation. The transport publishes these
// snapshots periodically, so they can lag behind completed streams.
async fn wait_until_pooled(alice: &Node, bob: &Node) -> (bool, bool) {
    let observe = || {
        (
            alice.transport().connection_stats(bob.pid()).is_some(),
            bob.transport().connection_stats(alice.pid()).is_some(),
        )
    };
    let wait = async {
        while observe() != (true, true) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(5), wait).await;
    observe()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[tokio::test]
    async fn sealed_postcard_completes_all_checkpoints() {
        tokio::time::timeout(Duration::from_secs(30), super::run())
            .await
            .expect("sealed-postcard should complete within thirty seconds");
    }
}
