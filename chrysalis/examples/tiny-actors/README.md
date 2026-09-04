# Tiny Actors

`tiny-actors` is a small demonstration of the boundary between Chrysalis and an
actor system built on top of it.

Alice addresses Bob by his Chrysalis process ID and opens bidirectional streams
to him. Each request sends an application-defined envelope containing an actor
ID and payload. Chrysalis gets the stream to Bob and authenticates Alice as its
source; Bob's application decodes the envelope and dispatches the request to a
local Tokio actor.

## What is this program?

The program creates two Chrysalis nodes in a direct namespace:

```text
Alice (namespace root)
└── Bob
```

Inside Bob's application, outside the Chrysalis namespace, are two actors:

```text
Bob process
├── Echo actor
└── Counter actor
```

Alice and Bob run inside one Rust program. Their lowest-level datagram carrier
is an `InprocNetwork`, so datagrams move through in-memory channels rather than
UDP sockets or remote machines. Above that carrier, the example uses real
Chrysalis identities, namespace resolution, PID routing, authenticated QUIC
connections, and bidirectional streams.

Echo and Counter are ordinary Tokio tasks following the task-and-handle pattern
described in Alice Ryhl's
[Actors with Tokio](https://ryhl.io/blog/actors-with-tokio/). Each actor owns a
bounded mailbox receiver. Cloneable handles send request messages through the
mailbox and receive replies through one-shot channels.

## What does it do?

The program develops the example in five checkpoints.

1. **Create Alice and Bob.** Bob joins Alice as a child, and each process
   resolves the other's certificate-derived PID.

2. **Define an actor envelope.** The application assigns one-byte IDs to Echo
   and Counter, followed by actor-specific payload bytes. Chrysalis does not
   define or interpret this format.

3. **Call Echo over Chrysalis.** Alice dials Bob by PID and sends an envelope
   selecting Echo. Bob accepts the stream, observes Alice's authenticated source
   PID, decodes the envelope, and returns Echo's response.

4. **Call Echo again through its mailbox.** A second Chrysalis stream reaches the same sender-only handle. Echo's Tokio task receives the request from its bounded mailbox and replies through a one-shot channel.

5. **Host two actors behind one PID.** Bob runs both Echo and Counter. Alice
   reaches both through the same Bob PID, while the envelope's actor ID selects
   the local mailbox. Two sequential Counter calls return `1` and `2`, showing
   that the Counter task retains private state across separate Chrysalis
   streams.

Dropping each actor's final handle closes its mailbox. The actor's receive loop
then exits naturally, allowing the program to await both tasks before shutting
down Alice and Bob.

## What does it show?

- Chrysalis addresses and authenticates **processes**. The application adds
  actor identity and dispatch within a process.
- Chrysalis streams carry opaque bytes. The envelope, actor IDs, commands, and
  response encodings are application conventions.
- Many actors can live behind one process PID without requiring Chrysalis to
  know that actors exist.
- A network dispatcher can remain separate from actor behavior and state.
- An actor task can exclusively own mutable state while cloneable handles offer
  asynchronous access through bounded message passing.
- One Chrysalis stream can serve as one request-response exchange. Alice's
  request half contains an envelope, Bob's response half contains
  actor-specific bytes, and each sender finishes its half to delimit those
  bytes.

## What does it not show?

This is a teaching example, not an actor framework.

- Alice, Bob, Echo, and Counter all run in one OS process; the example does not
  exercise separate OS-process or host boundaries.
- The actor protocol has no versioning, structured errors, or general-purpose
  serialization.
- Bob's dispatcher serves a fixed number of requests sequentially. It does not
  demonstrate concurrent dispatch, supervision, actor discovery, persistence,
  migration, or failure recovery.
- Actor IDs are meaningful only inside Bob's application. Chrysalis resolves
  and routes to Bob's known PID; it does not discover or address the actors
  hosted behind it.

## Source files

- `main.rs` creates the Chrysalis nodes and connects network requests to actor
  handles.
- `protocol.rs` defines the application-owned actor envelope.
- `echo_actor.rs` implements a stateless actor using a bounded mailbox.
- `counter_actor.rs` implements a stateful actor using the same pattern.
- `identity.rs` creates ephemeral identities for Alice and Bob, signed by one shared test issuer.

From `fbcode/monarch`, run:

```sh
cargo run -p chrysalis --example tiny-actors
```
