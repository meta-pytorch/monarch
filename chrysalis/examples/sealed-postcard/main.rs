// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

mod identity;
// Keep Relay's raw-datagram observation layer separate from the
// postcard application logic.
mod observed_socket;

use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use chrysalis::DatagramSocket;
use chrysalis::InprocNetwork;
use chrysalis::NamespaceConfig;
use chrysalis::Node;
use chrysalis::NodeConfig;
use chrysalis::ParentEndpoint;
use chrysalis::Pid;
use chrysalis::Resolution;
use chrysalis::ResolveConsistency;
use chrysalis::TransportConfig;
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

async fn read_bounded(recv: &mut chrysalis::RecvStream, limit: usize) -> io::Result<Vec<u8>> {
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

#[tokio::main]
async fn main() {
    run().await;
}

async fn run() {
    // Use an in-memory datagram carrier so we can exercise Chrysalis
    // without configuring UDP. Endpoint 1 will be Alice's location in
    // this network.
    let network = InprocNetwork::new(
        NonZeroUsize::new(DATAGRAM_QUEUE_CAPACITY).expect("queue capacity is nonzero"),
    );
    // Bind Alice to endpoint 1 and make the resulting socket
    // shareable by Node's async components.
    let alice_socket = Arc::new(
        network
            .bind(ALICE_ENDPOINT)
            .expect("Alice's in-process endpoint should be unused"),
    );
    // Save Alice's carrier address before moving alice_socket into
    // Node; Relay will use this address to contact its parent.
    let alice_address = alice_socket.local_addr().clone();

    // Generate ephemeral certificate/private-key pairs for Alice,
    // Relay, and Bob, then build their mutual-TLS client/server
    // configs with all three certificates trusted. QuicIdentity
    // derives each 128-bit PID from the first 16 bytes of SHA-256
    // over its certificate.
    let mut identities = identity::mutually_trusted_identities(3).into_iter();
    // Assign the three generated, mutually trusted identities to
    // Alice, Relay, and Bob.
    let alice_identity = identities.next().expect("create Alice's identity");
    let relay_identity = identities.next().expect("create Relay's identity");
    let bob_identity = identities.next().expect("create Bob's identity");
    // Save the PID before moving alice_identity into Node, so we can
    // verify that the resulting Node exposes the same identity.
    let certificate_pid = alice_identity.pid();

    // Combine Alice's bound carrier socket and identity, then start a
    // root Chrysalis node. No parent is configured, so Alice is the
    // root of her own namespace.
    let alice = Node::create(NodeConfig::new(TransportConfig::new(
        alice_socket,
        alice_identity,
    )))
    .expect("create Alice's node");

    // Observe the PID exposed by Node after Alice's identity was
    // moved into it.
    let node_pid_matches_certificate = alice.pid() == certificate_pid;
    println!("sealed-postcard, checkpoint 1: Alice exists");
    println!("certificate-derived PID: {}", format_pid(alice.pid()));
    println!("node PID matches certificate: {node_pid_matches_certificate}");
    assert!(
        node_pid_matches_certificate,
        "Alice's node PID should match her certificate-derived PID"
    );

    // Bind Relay to endpoint 2 on the same in-memory network, then
    // wrap the bound carrier in a shareable ObservedSocket. Relay's
    // Node components use the wrapper normally while it records each
    // successful raw ingress and egress datagram and delegates the
    // bytes unchanged.
    let relay_carrier = Arc::new(
        network
            .bind(RELAY_ENDPOINT)
            .expect("Relay's in-process endpoint should be unused"),
    );
    let relay_socket = Arc::new(ObservedSocket::new(relay_carrier));
    // Save Relay's underlying carrier address. Bob will use it to contact
    // its parent, and checkpoint 3 compares it with Alice's locator for Bob.
    // ObservedSocket delegates local_addr() to the wrapped in-memory socket.
    let relay_address = relay_socket.local_addr().clone();
    // Start Relay as Alice's child. Alice's PID identifies the parent
    // Relay must authenticate; Alice's carrier address tells Relay
    // where to contact her.
    //
    // Pass Node an Arc clone of the observer so this function retains
    // a handle to the same observation log for inspection after the
    // postcard.
    let relay = Node::create(
        NodeConfig::new(TransportConfig::new(relay_socket.clone(), relay_identity)).with_parent(
            NamespaceConfig::try_new(
                alice.pid(),
                vec![ParentEndpoint::new(alice_address.clone())],
            )
            .expect("Alice should have a nonreserved PID and one endpoint"),
        ),
    )
    .expect("Relay should start with a valid identity and carrier");

    // Relay's parent connection starts asynchronously. Resolve Alice
    // through that link to wait for it to become active, with a
    // timeout to prevent hanging.
    let alice_resolution = tokio::time::timeout(
        Duration::from_secs(5),
        relay.resolve(alice.pid(), ResolveConsistency::Refresh),
    )
    .await
    .expect("Relay should connect to Alice within five seconds")
    .expect("Relay should resolve Alice through its parent link");
    // Require a positive resolution and extract Alice's process
    // entry; a NotFound result means this checkpoint failed.
    let Resolution::Found {
        entry: alice_entry, ..
    } = alice_resolution
    else {
        panic!("Alice should be present in Relay's namespace");
    };

    // Query Alice's local directory for Relay, confirming that Relay
    // published itself while establishing the parent link.
    let relay_resolution = alice
        .resolve(relay.pid(), ResolveConsistency::Cached)
        .await
        .expect("Alice's local namespace lookup should succeed");
    // Require Alice's lookup to find Relay and retain Relay's
    // published entry.
    let Resolution::Found {
        entry: relay_entry, ..
    } = relay_resolution
    else {
        panic!("Relay should be present in Alice's namespace");
    };

    // Confirm that each lookup returned the PID of the process
    // requested.
    let relay_resolves_alice = alice_entry.pid == alice.pid();
    let alice_resolves_relay = relay_entry.pid == relay.pid();
    println!("sealed-postcard, checkpoint 2: Relay joined Alice");
    println!("Relay resolves Alice: {relay_resolves_alice}");
    println!("Alice resolves Relay: {alice_resolves_relay}");
    assert!(relay_resolves_alice, "Relay should resolve Alice's PID");
    assert!(alice_resolves_relay, "Alice should resolve Relay's PID");

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

    // Wait for Bob's publication to reach Relay first and then
    // propagate recursively from Relay to Alice.
    wait_until_visible(&relay, bob.pid()).await;
    wait_until_visible(&alice, bob.pid()).await;

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

    // Resolve the same Bob PID from Alice's local directory; because
    // Bob is behind Relay, the returned locator should point to
    // Relay.
    let bob_from_alice = alice
        .resolve(bob.pid(), ResolveConsistency::Cached)
        .await
        .expect("Alice's local namespace lookup should succeed");
    // Require a positive result and retain Alice's contextual view of
    // Bob.
    let Resolution::Found {
        entry: alice_view, ..
    } = bob_from_alice
    else {
        panic!("Bob should be present in Alice's namespace");
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
    println!("sealed-postcard, checkpoint 3: Bob joined through Relay");
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
    let relay_datagrams = relay_socket.observations();
    // Confirm that Relay observed at least one raw datagram whose
    // QUIC CID named Bob's PID as its routing target.
    let relay_saw_bob_target = relay_datagrams
        .iter()
        .any(|datagram| datagram.target == Some(bob.pid()));
    // Find a Bob-targeted datagram received from Alice followed by an
    // outgoing datagram to Bob containing exactly the same raw bytes.
    let relay_forwarded_bob_datagram_unchanged =
        relay_datagrams
            .iter()
            .enumerate()
            .any(|(ingress_index, ingress)| {
                ingress.direction == DatagramDirection::Ingress
                    && ingress.target == Some(bob.pid())
                    && ingress.peer == alice_address
                    && relay_datagrams
                        .iter()
                        .skip(ingress_index + 1)
                        .any(|egress| {
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
        relay_saw_bob_target,
        "Relay should observe a datagram routed toward Bob"
    );
    assert!(
        relay_forwarded_bob_datagram_unchanged,
        "Relay should forward one Bob-targeted datagram unchanged"
    );
    assert!(
        !postcard_visible_at_relay,
        "Relay should not observe the literal postcard in QUIC datagrams"
    );

    // Define—but do not yet run—Bob's half of the experiment: accept
    // 32 streams, check that each reports Alice as its authenticated
    // source, record its number, and echo that number back.
    let bob_accepts_streams = async {
        // Accumulate whether every accepted stream reports Alice as
        // its source and the one-byte stream numbers Bob receives.
        // with_capacity reserves storage for 32 numbers but leaves
        // the vector empty.
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

    // Define—but do not yet run—Alice's half: open 32 numbered
    // streams to Bob, retain their handles, then verify Bob’s echo on
    // every stream.
    let alice_opens_streams = async {
        // Reserve space for each stream handle paired with its
        // expected echo byte. Keeping these pairs lets Alice validate
        // each response on its original stream.
        let mut streams = Vec::with_capacity(STREAM_COUNT);

        // Iterate over 0 through 31; each index will identify one
        // newly opened bidirectional stream.
        for stream_index in 0..STREAM_COUNT {
            // Convert the usize loop index into the one-byte wire
            // representation. The checked conversion prevents silent
            // truncation if STREAM_COUNT grows too large.
            let stream_number =
                u8::try_from(stream_index).expect("32 stream indices should fit in one byte");
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
        // After opening all 32 streams, consume the stored pairs one
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
        .expect("32 stream round trips should complete within five seconds");

    // Normalize the streams' arrival order before comparison. Sorting
    // preserves duplicates, so a missing or repeated number will
    // still make the check fail.
    received_stream_numbers.sort_unstable();
    // Build the canonical expected vector [0, 1, ..., 31], converting
    // each usize index into the same checked one-byte representation
    // Alice sent.
    let expected_stream_numbers = (0..STREAM_COUNT)
        .map(|stream_index| {
            u8::try_from(stream_index).expect("32 stream indices should fit in one byte")
        })
        .collect::<Vec<_>>();
    // Compare the sorted vectors exactly, proving Bob received every
    // number once with no missing, duplicate, malformed, or
    // unexpected value.
    let bob_received_every_stream = received_stream_numbers == expected_stream_numbers;
    // Record whether peer-specific pool entries are visible after all
    // 32 streams. These snapshots are not stable connection IDs.
    let pooled_connection_after = (
        alice.transport().connection_stats(bob.pid()).is_some(),
        bob.transport().connection_stats(alice.pid()).is_some(),
    );

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

    // Stop Bob first so Relay can withdraw Bob locally and propagate
    // that withdrawal upward to Alice while both ancestors are still
    // running.
    bob.shutdown();
    bob.join().await;
    // Stop Relay next so its parent link can close cleanly while
    // Alice is still running to receive the withdrawal.
    relay.shutdown();
    relay.join().await;
    // Request shutdown, then wait until every task owned by Alice has
    // finished.
    alice.shutdown();
    alice.join().await;
    println!("Bob, Relay, and Alice shut down cleanly");
}

// Render the 16-byte PID as 32 lowercase hexadecimal digits: two
// digits per byte.
fn format_pid(pid: Pid) -> String {
    pid.as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
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
