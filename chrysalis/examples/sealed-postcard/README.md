# Sealed Postcard

`sealed-postcard` is a small demonstration of the process mesh that Chrysalis
provides today.

Alice knows Bob's process ID, and her namespace-provided route to Bob goes
through Relay. Alice opens an end-to-end Chrysalis stream to Bob, sends a
postcard, and receives an acknowledgement. Relay forwards the encrypted QUIC
datagrams without terminating the application connection or understanding the
postcard.

## What is this program?

The program creates three Chrysalis nodes in a tree:

```text
Alice (namespace root)
└── Relay
    └── Bob
```

All three nodes run inside one Rust program. Their lowest-level datagram carrier
is an `InprocNetwork`, so datagrams move through in-memory channels rather than
UDP sockets or remote machines. Above that carrier, the example uses the real
Chrysalis machinery: certificate-derived PIDs, mutual TLS, the delegated process
namespace, PID routing, QUIC connections, and bidirectional streams.

This distinction is important: the example creates three logical Chrysalis
nodes and process identities, but not three OS processes or remote hosts. It
does exercise the Chrysalis layers that would connect them.

Each node receives an ephemeral certificate and private key signed by one shared test CA. The certificate determines its 128-bit PID, and each TLS configuration trusts that shared issuer. These identities exist only for this run.

## What does it do?

The program develops the mesh in six checkpoints.

1. **Create Alice.** Alice starts a root namespace and confirms that her node PID
   is the PID derived from her certificate.

2. **Attach Relay.** Relay joins Alice as a child. Their live control connection
   establishes reciprocal namespace visibility.

3. **Attach Bob behind Relay.** Bob joins Relay, and Relay republishes Bob upward
   to Alice. Resolution is contextual: Relay's next hop for Bob is Bob, while
   Alice's next hop for the same Bob PID is Relay.

4. **Send the sealed postcard.** Alice dials Bob by PID and writes an
   application-defined byte string to a bidirectional stream. Bob receives the
   source PID established by the end-to-end authenticated QUIC connection,
   reads the postcard, and returns an acknowledgement on the reverse half of
   the same stream.

5. **Observe Relay forwarding.** Relay's carrier is wrapped by `ObservedSocket`, which records a bounded set of raw ingress and egress observations without changing the datagrams. The program reports whether the current in-process carrier preserves an exact Bob-targeted packet boundary across Relay. Relay can read the destination PID from the QUIC connection ID, but the literal postcard does not appear in the complete observation log.

6. **Reuse the connection.** Alice opens 32 additional numbered bidirectional
   streams to Bob. Bob receives and echoes every number, while Alice and Bob
   each retain a peer-specific pooled application connection for one another.

The postcard and acknowledgement have no special meaning to Chrysalis. They are
an application protocol invented by this example. To Chrysalis, each is only an
opaque sequence of bytes.

## What does it show?

The example makes several Chrysalis properties concrete:

- A process is addressed by a certificate-derived PID, not by an application
  port or service name.
- A tree of parent links distributes process locations and rewrites the useful
  next hop for each observer.
- An intermediate node can route QUIC datagrams by destination PID without
  terminating the end-to-end application connection.
- The authenticated peer identity reaches the receiving application together
  with each accepted stream.
- Many independent bidirectional streams can reuse one live pooled application
  connection.
- Chrysalis supplies identity, discovery, routing, and opaque streams; the
  postcard format, acknowledgement, messaging semantics, and any future actor
  protocol belong above it.

It also exposes three different things that are easy to conflate:

- The **carrier** moves individual datagrams. Here it is an in-memory network;
  in another deployment it could be UDP or another carrier.
- A **QUIC connection** is the authenticated, encrypted relationship pooled
  between Alice and Bob.
- A **stream** is one independent bidirectional byte channel multiplexed over
  that connection.

## What does it not prove?

This is a teaching example, not a network benchmark or a security test.

- Because the carrier is in-process, it does not exercise real process or
  network boundaries, carrier failures, or failure detection.
- The missing plaintext marker in Relay's observations is illustrative, not a
  proof of confidentiality. That property comes from the end-to-end TLS/QUIC
  protocol, not from searching one run's datagrams.
- One application stream does not correspond to one datagram. QUIC may split,
  combine, retransmit, or otherwise packetize stream bytes.
- `connection_stats(peer)` returns a snapshot when that peer has a pooled
  connection. It is not a stable physical-connection ID and does not enumerate
  Relay's link-local control connections or routing state.
- The example does not add RPC, messages, mailboxes, actors, persistence, or an
  authorization policy. Those remain application or higher-layer concerns.

## Source files

- `main.rs` constructs the topology and performs the six checkpoints.
- `identity.rs` creates ephemeral test identities signed by one shared issuer.
- `observed_socket.rs` transparently records the raw datagrams crossing Relay's
  carrier.

From `fbcode/monarch`, run:

```sh
cargo run -p chrysalis --example sealed-postcard
```
