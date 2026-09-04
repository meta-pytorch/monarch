/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod counter_actor;
// Keep Echo's Tokio task, mailbox, and handle separate from Bob's
// network-facing Chrysalis dispatcher.
mod echo_actor;
// Declare identity.rs as this example's local identity module; it
// creates ephemeral QUIC identities signed by one shared test issuer.
mod identity;
// Keep our actor envelope separate from Chrysalis: this byte format
// belongs to the application, not the transport.
mod protocol;

use std::fmt::Write as _;
use std::io;
use std::mem::size_of;
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
use chrysalis::RecvStream;
use chrysalis::Resolution;
use chrysalis::ResolveConsistency;
use chrysalis::TransportConfig;
use counter_actor::CounterActorHandle;
use echo_actor::EchoActorHandle;
use protocol::ActorId;
use protocol::Envelope;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

const ALICE_ENDPOINT: u64 = 1;
const BOB_ENDPOINT: u64 = 2;
// Call Counter twice with our application-defined command so its
// responses reveal whether state survives between requests.
const COUNTER_CALLS: usize = 2;
const COUNTER_COMMAND: &[u8] = b"increment";
// Application-defined bytes used to exercise the Echo envelope
// locally; Chrysalis assigns them no meaning.
const ECHO_PAYLOAD: &[u8] = b"hello from Alice";
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

#[tokio::main]
async fn main() {
    // Build a direct two-node namespace: Alice is the root and Bob is
    // her child. Both nodes share one in-memory carrier and use
    // certificates signed by one shared test issuer.
    let network = InprocNetwork::new(
        NonZeroUsize::new(DATAGRAM_QUEUE_CAPACITY).expect("queue capacity is nonzero"),
    );
    let alice_socket = Arc::new(
        network
            .bind(ALICE_ENDPOINT)
            .expect("Alice's in-process endpoint should be unused"),
    );
    let alice_address = alice_socket.local_addr().clone();

    let [alice_identity, bob_identity] = identity::mutually_trusted_identities::<2>();

    let alice = Node::create(NodeConfig::new(TransportConfig::new(
        alice_socket,
        alice_identity,
    )))
    .expect("create Alice's node");

    let bob_socket = Arc::new(
        network
            .bind(BOB_ENDPOINT)
            .expect("Bob's in-process endpoint should be unused"),
    );
    let bob = Node::create(
        NodeConfig::new(TransportConfig::new(bob_socket, bob_identity)).with_parent(
            NamespaceConfig::try_new(alice.pid(), vec![ParentEndpoint::new(alice_address)])
                .expect("Alice should have a nonreserved PID and one endpoint"),
        ),
    )
    .expect("create Bob's node");

    let alice_from_bob = tokio::time::timeout(
        Duration::from_secs(5),
        bob.resolve(alice.pid(), ResolveConsistency::Refresh),
    )
    .await
    .expect("Bob should connect to Alice within five seconds")
    .expect("Bob should resolve Alice through his parent link");
    let Resolution::Found {
        entry: alice_entry, ..
    } = alice_from_bob
    else {
        panic!("Alice should be present in Bob's namespace");
    };

    let bob_from_alice = alice
        .resolve(bob.pid(), ResolveConsistency::Cached)
        .await
        .expect("Alice's local namespace lookup should succeed");
    let Resolution::Found {
        entry: bob_entry, ..
    } = bob_from_alice
    else {
        panic!("Bob should be present in Alice's namespace");
    };

    println!("tiny-actors, checkpoint 1: Alice and Bob exist");
    println!("Alice PID: {}", format_pid(alice.pid()));
    println!("Bob PID: {}", format_pid(bob.pid()));
    println!("Bob resolves Alice: {}", alice_entry.pid == alice.pid());
    println!("Alice resolves Bob: {}", bob_entry.pid == bob.pid());
    assert_eq!(
        alice_entry.pid,
        alice.pid(),
        "Bob should resolve Alice's PID"
    );
    assert_eq!(bob_entry.pid, bob.pid(), "Alice should resolve Bob's PID");

    // Exercise the application protocol locally before putting it on
    // a Chrysalis stream: construct an Echo envelope, encode it, then
    // parse it back.
    let envelope = Envelope::new(ActorId::Echo, ECHO_PAYLOAD);
    let encoded = envelope.encode();
    let decoded = Envelope::decode(&encoded).expect("decode Alice's actor envelope");

    // Show the actor's wire value and confirm that decoding recovered
    // both fields.
    println!("tiny-actors, checkpoint 2: define the application envelope");
    println!("wire actor byte: {}", encoded[0]);
    println!(
        "decoded actor is Echo: {}",
        decoded.actor() == ActorId::Echo
    );
    println!(
        "decoded payload: {}",
        String::from_utf8_lossy(decoded.payload())
    );
    assert_eq!(
        decoded.actor(),
        ActorId::Echo,
        "the decoded envelope should select Echo"
    );
    assert_eq!(
        decoded.payload(),
        ECHO_PAYLOAD,
        "the decoded envelope should preserve its payload"
    );

    // Start Echo's mailbox task before serving the request; Bob's
    // dispatcher uses the actor handle, while main retains the task
    // handle for graceful shutdown.
    let (echo_actor, echo_task) = EchoActorHandle::new();

    // Define Bob's network-facing dispatcher for one request: accept
    // a stream, decode its envelope, select the actor, and return
    // that actor's response.
    let bob_serves_echo = async {
        let mut incoming = bob
            .accept()
            .await
            .expect("Bob's application transport should remain open");
        let source = incoming.source();
        let request_bytes = read_bounded(incoming.stream_mut().recv_mut(), encoded.len())
            .await
            .expect("Bob should read Alice's actor envelope");
        let request = Envelope::decode(&request_bytes).expect("Bob should decode the envelope");
        let actor = request.actor();

        // Translate the envelope's application-level actor ID into a
        // local handle call, then await that actor's reply before
        // writing back to the Chrysalis stream.
        let response = match actor {
            ActorId::Echo => echo_actor.echo(request.payload()).await,
            ActorId::Counter => panic!("Echo request should target Echo"),
        };
        incoming
            .stream_mut()
            .send_mut()
            .write_all(&response)
            .await
            .expect("Bob should send the Echo response");
        incoming
            .stream_mut()
            .send_mut()
            .finish()
            .await
            .expect("Bob should finish the Echo response");
        (source, actor)
    };

    // Define Alice's call: dial Bob by process PID, send an envelope
    // naming Echo, finish the request half, then read Echo's reply on
    // the reverse half. Chrysalis routes to Bob; the actor ID is
    // interpreted only inside Bob.
    let alice_calls_echo = async {
        let mut stream = alice
            .dial(bob.pid(), ResolveConsistency::Cached)
            .await
            .expect("Alice should dial Bob by PID");
        stream
            .send_mut()
            .write_all(&encoded)
            .await
            .expect("Alice should send the actor envelope");
        stream
            .send_mut()
            .finish()
            .await
            .expect("Alice should finish the actor envelope");
        read_bounded(stream.recv_mut(), ECHO_PAYLOAD.len())
            .await
            .expect("Alice should read the Echo response")
    };

    // Drive Bob's server and Alice's client concurrently under one
    // deadline, then unpack Bob's source/dispatch observations and
    // Alice's returned bytes.
    let ((authenticated_source, dispatched_actor), echo_response) =
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(bob_serves_echo, alice_calls_echo)
        })
        .await
        .expect("the Echo request should complete within five seconds");

    println!("tiny-actors, checkpoint 3: Alice calls Bob's Echo actor");
    println!(
        "Bob authenticated source=Alice: {}",
        authenticated_source == alice.pid()
    );
    println!(
        "Bob dispatched actor=Echo: {}",
        dispatched_actor == ActorId::Echo
    );
    println!(
        "Alice received Echo payload intact: {}",
        echo_response == ECHO_PAYLOAD
    );
    println!("Echo response: {}", String::from_utf8_lossy(&echo_response));
    assert_eq!(
        authenticated_source,
        alice.pid(),
        "Bob should authenticate Alice as the stream source"
    );
    assert_eq!(
        dispatched_actor,
        ActorId::Echo,
        "Bob should dispatch the envelope to Echo"
    );
    assert_eq!(
        echo_response.as_slice(),
        ECHO_PAYLOAD,
        "Echo should return Alice's payload unchanged"
    );

    // Start Counter as a second actor task behind Bob's PID and
    // prepare the application envelope that selects it.
    let (counter_actor, counter_task) = CounterActorHandle::new();
    let counter_envelope = Envelope::new(ActorId::Counter, COUNTER_COMMAND).encode();

    // Serve both actors through one network-facing dispatcher:
    // Chrysalis reaches Bob by PID, then the envelope's ActorId
    // selects a mailbox.
    let bob_dispatches_actors = async {
        let mut every_source_is_alice = true;
        let mut dispatched_actors = Vec::with_capacity(1 + COUNTER_CALLS);
        let max_envelope_len = encoded.len().max(counter_envelope.len());

        // Each call arrives on a fresh stream addressed to the same
        // Bob PID: one for Echo and one for each Counter request.
        for _ in 0..(1 + COUNTER_CALLS) {
            let mut incoming = bob
                .accept()
                .await
                .expect("Bob's application transport should remain open");
            every_source_is_alice &= incoming.source() == alice.pid();
            let request_bytes = read_bounded(incoming.stream_mut().recv_mut(), max_envelope_len)
                .await
                .expect("Bob should read Alice's actor envelope");
            let request = Envelope::decode(&request_bytes).expect("Bob should decode the envelope");
            let actor = request.actor();
            // Dispatch locally after Chrysalis has delivered the
            // stream to Bob; each arm calls the handle for a
            // different Tokio actor task.
            let response = match actor {
                ActorId::Echo => echo_actor.echo(request.payload()).await,
                ActorId::Counter => {
                    // Counter accepts our "increment" command and
                    // encodes its new u32 value as four big-endian
                    // response bytes.
                    assert_eq!(
                        request.payload(),
                        COUNTER_COMMAND,
                        "Counter request should contain the Increment command"
                    );
                    counter_actor.increment().await.to_be_bytes().to_vec()
                }
            };
            dispatched_actors.push(actor);
            incoming
                .stream_mut()
                .send_mut()
                .write_all(&response)
                .await
                .expect("Bob should send the actor response");
            incoming
                .stream_mut()
                .send_mut()
                .finish()
                .await
                .expect("Bob should finish the actor response");
        }

        (every_source_is_alice, dispatched_actors)
    };

    // Address both actors through the same Bob PID; only the envelope
    // selects whether Bob dispatches to Echo or Counter.
    let alice_calls_actors = async {
        let echo_again = call_actor(&alice, bob.pid(), &encoded, ECHO_PAYLOAD.len()).await;
        let mut values = Vec::with_capacity(COUNTER_CALLS);
        // Call Counter twice in sequence; each response reports the
        // state after that request has been processed.
        for _ in 0..COUNTER_CALLS {
            let response = call_actor(&alice, bob.pid(), &counter_envelope, size_of::<u32>()).await;
            // Decode Counter's four application-defined response
            // bytes back into a u32.
            values.push(u32::from_be_bytes(
                response
                    .try_into()
                    .expect("Counter response should contain one u32"),
            ));
        }

        (echo_again, values)
    };

    let ((actor_sources_are_alice, dispatched_actors), (echo_again, counter_values)) =
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(bob_dispatches_actors, alice_calls_actors)
        })
        .await
        .expect("the actor requests should complete within five seconds");

    println!("tiny-actors, checkpoint 4: Echo remains available through its mailbox");
    println!(
        "second Echo call returned its payload intact: {}",
        echo_again == ECHO_PAYLOAD
    );
    assert_eq!(
        echo_again.as_slice(),
        ECHO_PAYLOAD,
        "a second Echo call should cross the mailbox and return unchanged"
    );

    println!("tiny-actors, checkpoint 5: Counter owns private state");
    println!("Bob authenticated all actor requests from Alice: {actor_sources_are_alice}");
    println!("Bob dispatched actors: {dispatched_actors:?}");
    println!(
        "one Bob PID hosted Echo and Counter: {}",
        dispatched_actors.as_slice() == [ActorId::Echo, ActorId::Counter, ActorId::Counter]
    );
    println!("Counter responses: {counter_values:?}");
    println!(
        "Counter preserved state across requests: {}",
        counter_values.as_slice() == [1, 2]
    );
    assert!(
        actor_sources_are_alice,
        "Bob should authenticate Alice as the source of every actor request"
    );
    assert_eq!(
        dispatched_actors.as_slice(),
        [ActorId::Echo, ActorId::Counter, ActorId::Counter],
        "one Bob PID should dispatch to Echo and Counter"
    );
    assert_eq!(
        counter_values.as_slice(),
        [1, 2],
        "Counter should retain state across separate Chrysalis streams"
    );

    // Drop the final mailbox sender, causing Echo's receive loop to
    // finish after draining queued messages, then await the task to
    // prove shutdown completed.
    drop(echo_actor);
    tokio::time::timeout(Duration::from_secs(5), echo_task)
        .await
        .expect("Echo actor should stop within five seconds")
        .expect("Echo actor task should not panic");
    println!("Echo actor stopped after its final handle was dropped");

    drop(counter_actor);
    tokio::time::timeout(Duration::from_secs(5), counter_task)
        .await
        .expect("Counter actor should stop within five seconds")
        .expect("Counter actor task should not panic");
    println!("Counter actor stopped after its final handle was dropped");

    bob.shutdown();
    bob.join().await;
    alice.shutdown();
    alice.join().await;
    println!("Alice and Bob shut down cleanly");
}

fn format_pid(pid: Pid) -> String {
    let bytes = pid.as_bytes();
    let mut formatted = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut formatted, "{byte:02x}").expect("writing to a String should not fail");
    }
    formatted
}

// Treat one Chrysalis stream as one actor call: dial the process PID,
// send an envelope, finish the request half, then read the response.
async fn call_actor(node: &Node, process: Pid, envelope: &[u8], response_limit: usize) -> Vec<u8> {
    let mut stream = node
        .dial(process, ResolveConsistency::Cached)
        .await
        .expect("dial actor's process by PID");
    stream
        .send_mut()
        .write_all(envelope)
        .await
        .expect("send the actor envelope");
    stream
        .send_mut()
        .finish()
        .await
        .expect("finish the actor envelope");
    // Bob's FIN delimits the response; response_limit bounds how many
    // bytes this application is willing to accept.
    read_bounded(stream.recv_mut(), response_limit)
        .await
        .expect("read the actor response")
}
