# Sealed Postcard

`sealed-postcard` is a small demonstration of the process mesh that Chrysalis
provides today.

Alice knows Bob's process ID, and her namespace-provided route to Bob goes
through Relay. Alice opens an end-to-end Chrysalis stream to Bob, sends a
postcard, and receives an acknowledgement. Relay forwards the encrypted QUIC
datagrams without terminating the application connection or understanding the
postcard.

The example has a Rust implementation, which this page walks through, and a
Python one. [Other ways to run it](#other-ways-to-run-it) shows every way to run
either.

## What is this program?

The program creates three Chrysalis nodes in a tree:

```text
Relay (namespace root)
├── Alice
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

1. **Create Relay.** Relay starts a root namespace and confirms that its node PID
   is the PID derived from its certificate.

2. **Attach Alice.** Alice joins Relay as a child. Their live control connection
   establishes reciprocal namespace visibility.

3. **Attach Bob.** Bob also joins Relay. Resolution is contextual: Relay's next
   hop for Bob is Bob, while Alice, who resolves Bob through Relay, gets Relay as
   her next hop for the same Bob PID.

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
  Relay's link-local control connections or routing state. The transport
  refreshes these snapshots periodically, so a connection can carry streams
  before its snapshot appears; the example waits for both.
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

## Run the roles as separate processes

The same binary can run each node as its own operating-system process. These
role modes currently require Linux because they use `VsockDatagramSocket`,
whose native-vsock implementation is Linux-only. The no-mode walkthrough above
uses `InprocNetwork` and remains available on other platforms.

Relay is the namespace root and listens at `…/r`. Alice and Bob have no
listener; each connects to Relay.

```text
Relay (root, listens at …/r)
├── Alice (connects to …/r)
└── Bob (connects to …/r)
```

### Prepare the identities

Create a short run directory so its Unix socket paths fit the operating
system's limit. Generate one private identity bundle per role and save the
printed PIDs:

```sh
cd ~/fbsource/fbcode/monarch/chrysalis/crates/chrysalis/examples/sealed-postcard
sealed_postcard_dir=$(mktemp -d /tmp/sealed-postcard.XXXXXX)

cargo run --example sealed-postcard -- \
  identities --out "$sealed_postcard_dir/ids" \
  > "$sealed_postcard_dir/pids"

alice_pid=$(awk '$1 == "alice" { print $2 }' "$sealed_postcard_dir/pids")
relay_pid=$(awk '$1 == "relay" { print $2 }' "$sealed_postcard_dir/pids")
bob_pid=$(awk '$1 == "bob" { print $2 }' "$sealed_postcard_dir/pids")
```

Each bundle contains that role's certificate, the CA certificate all three
roles trust, and the role's private key. The files are readable only by their
owner and are never overwritten. A PID is the first 128 bits of the SHA-256 of
the role's certificate, printed as 32 hexadecimal digits. Role commands reject
a shorter PID.

The examples below use `/tmp/sealed-postcard.ab12cd` as the run directory and
these sample PIDs:

```text
alice 11111111111111111111111111111111
relay 22222222222222222222222222222222
bob 33333333333333333333333333333333
```

### Relay

```sh
SEALED_POSTCARD_IDENTITY=$(cat /tmp/sealed-postcard.ab12cd/ids/relay.pem) \
  cargo run --example sealed-postcard -- relay \
    --listen unix:/tmp/sealed-postcard.ab12cd/r
```

`SEALED_POSTCARD_IDENTITY` is the text of Relay's own bundle, not its path.
`--listen` is where Relay waits for Alice and Bob, so both use the same address
as `--parent`. Relay has no parent: it is the namespace root.

### Alice

```sh
SEALED_POSTCARD_IDENTITY=$(cat /tmp/sealed-postcard.ab12cd/ids/alice.pem) \
  cargo run --example sealed-postcard -- alice \
    --parent-pid 22222222222222222222222222222222 \
    --parent unix:/tmp/sealed-postcard.ab12cd/r \
    --bob-pid 33333333333333333333333333333333
```

`SEALED_POSTCARD_IDENTITY` is the text of Alice's own bundle. `--parent` is
Relay's `--listen` address. `--parent-pid` is Relay's PID from the `relay` line
of `pids`, and Alice accepts only a parent whose certificate matches it.
`--bob-pid` is Bob's PID from the `bob` line; Alice uses it to find and
authenticate Bob. Alice exits successfully after the postcard and 32 echo
streams complete.

### Bob

```sh
SEALED_POSTCARD_IDENTITY=$(cat /tmp/sealed-postcard.ab12cd/ids/bob.pem) \
  cargo run --example sealed-postcard -- bob \
    --parent-pid 22222222222222222222222222222222 \
    --parent unix:/tmp/sealed-postcard.ab12cd/r \
    --alice-pid 11111111111111111111111111111111
```

`SEALED_POSTCARD_IDENTITY` is the text of Bob's own bundle. `--parent` is
Relay's `--listen` address. `--parent-pid` is Relay's PID from `pids`, and Bob
accepts only a parent whose certificate matches it. `--alice-pid` is Alice's
PID; Bob checks that every application stream came from Alice.

The roles can start in any order. Children keep retrying their parent, and
Alice waits up to 120 seconds for Bob. Bob and Relay print reports when they
receive Ctrl-C or SIGTERM:

```text
bob served 33 streams, all from alice: true
relay observed 123 datagrams, postcard plaintext seen: false, log complete: true
```

They exit successfully only when those checks pass. A complete Relay report
means it recorded every datagram its socket sent or received, and none
contained the postcard's plaintext. Neither Alice nor Bob has a listening
socket, so everything between them crosses Relay. After stopping the roles, deleting the run
directory removes their private keys.

## Other ways to run it

| Run | Rust | Python |
|---|---|---|
| All three nodes in one process | this page | none; [`hello_world.py`](../../../../python/README.md) is the smaller in-process tutorial |
| Separate processes | [`separate-processes.md`](separate-processes.md) | [`python/README.md`](../../../../python/README.md#run-the-roles-as-separate-processes) |
| With Alice's child, Carol | none | [`python/README.md`](../../../../python/README.md#alices-child-carol) |
| Through two Relays joined over UDP | none | [`python/README.md`](../../../../python/README.md#two-relays-joined-over-udp) |
| Alice and Bob in Firecracker microVMs | [`firecracker.md`](firecracker.md) | none |
