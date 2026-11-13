# Chrysalis

Chrysalis is an embeddable process mesh. Every process has a globally unique,
authenticated PID, and any process can open cheap QUIC streams to another PID.
A hierarchical nameserver follows process-lifecycle and connectivity boundaries;
gateways aggregate their descendants and forward QUIC packets without inspecting
application streams.

The base crates provide transport, naming, routing, and opaque streams. Optional
link-local protocols can compose higher-level systems without adding them to the
core. `chrysalis-sqlite` uses this mechanism to replicate a cr-sqlite database
along the same parent-child topology.

See [DESIGN.md](DESIGN.md) for architectural contracts and
[IMPLEMENTATION.md](IMPLEMENTATION.md) for important implementation limitations
and planned optimizations.

## Meta mTLS

For multi-host tests inside Meta, select `--identity=meta` for every process.
The CLI uses the installed `/var/facebook/x509_identities/server.pem` to request
a fresh host-authenticated leaf from `certreq`, so every process receives a
distinct certificate, key, and PID. It trusts the Rootcanal bundle at
`/var/facebook/rootcanal/ca.pem`; `THRIFT_TLS_SRV_CA_PATH` overrides that path,
matching Hyperactor's MetaTLS setup. The CLI defaults to `--identity=ephemeral`
for local demonstrations.

```bash
# Root host
fbcode/monarch/chrysalis/bin/chrysalis \
  --identity=meta \
  --carrier 'udp://[<root-ipv6>]:5000' serve

# Another host
fbcode/monarch/chrysalis/bin/chrysalis \
  --identity=meta \
  --carrier 'udp://[<client-ipv6>]:0' \
  --cluster 'udp://[<root-ipv6>]:5000?authority=<root-pid>' ps
```

The Meta identity crate retains an explicit `load` API for applications that
need the stable installed host identity. It also supports delegated `certreq`
issuance with MAST CATs. The scale benchmark uses the delegated path to host many
independent nodes in one task.

The pure-Rust [`chrysalis-scale`](crates/chrysalis-scale/README.md) benchmark builds
and optionally launches MAST ablations at 1K, 10K, and 100K nodes. It measures
full namespace join followed by a one-byte framed echo on a new stream to every
child.

## Deployment resolvers

`--cluster` accepts deployment resolver URLs in addition to direct locators.
For a Chrysalis MAST deployment, the job name is sufficient:

```bash
fbcode/monarch/chrysalis/bin/chrysalis \
  --cluster 'mast://chrysalis_scale_meriksen_1000n_10t_...' \
  ps
```

The MAST resolver finds the root task, joins its IPv4 or IPv6 address on the
well-known port `26600`, binds a matching wildcard UDP carrier, and selects the
Meta identity provider. Explicit `--carrier` or `--identity` options override
the corresponding resolved values. Additional resolver schemes can implement
the same join, carrier, and identity contract without changing commands such as
`ps` or `cat`.

## SQLite shell

The bare `sqlite` command opens an ordinary in-memory SQLite shell:

```bash
fbcode/monarch/chrysalis/bin/chrysalis sqlite
```

The shell supports readline editing, persistent command history, multiline SQL,
`.tables`, `.schema`, and `.quit`. Press Ctrl-C to discard the current input and
Ctrl-D to exit. Use an explicit file to retain the database after exit:

```bash
fbcode/monarch/chrysalis/bin/chrysalis sqlite repl /tmp/chrysalis.db
```

The shell supports multiline SQL, `.tables`, `.schema`, and `.quit`. It accesses
an ordinary local SQLite database and does not join the process mesh; mesh
options such as `--cluster`, `--carrier`, and `--identity` are rejected for
SQLite commands.

For example:

```sql
CREATE TABLE items (
  id INTEGER PRIMARY KEY NOT NULL,
  value TEXT NOT NULL
);
INSERT INTO items VALUES (1, 'hello');
SELECT * FROM items;
```

Replicated applications use `chrysalis-sqlite` directly. They register trusted
table descriptors and explicitly capture row mutations in their application
transactions; arbitrary shell SQL is intentionally not intercepted or
replicated.

## Process streams

The CLI can also demonstrate the base process mesh without SQLite:

```bash
# Terminal 1
buck run fbcode//monarch/chrysalis/crates/chrysalis-cli -- serve
# prints: udp://127.0.0.1:<port>?authority=<root-pid>

# Terminal 2
buck run fbcode//monarch/chrysalis/crates/chrysalis-cli -- \
  --cluster 'udp://127.0.0.1:<port>?authority=<root-pid>' serve
# prints: udp://127.0.0.1:<child-port>?authority=<child-pid>

# Terminal 3
echo hello | buck run fbcode//monarch/chrysalis/crates/chrysalis-cli -- \
  cat '<child-pid-prefix>@udp://127.0.0.1:<port>?authority=<root-pid>'
```

`--cluster` also accepts an address without an authority, such as
`--cluster udp://127.0.0.1:<port>`. This discovers the authenticated parent PID on
the first successful nameserver handshake and pins it for subsequent reconnects.
Use the printed `address?authority=<pid>` form when the parent identity must be
pinned before dialing. `--join` remains as a deprecated alias for `--cluster`.

IPv6 socket addresses use brackets. For example, start a root on the IPv6
loopback address, then join it from another IPv6 carrier:

```bash
# Terminal 1; prints udp://[::1]:<port>?authority=<root-pid>
fbcode/monarch/chrysalis/bin/chrysalis --carrier 'udp://[::1]:0' serve

# Terminal 2
fbcode/monarch/chrysalis/bin/chrysalis \
  --carrier 'udp://[::1]:0' \
  ps 'udp://[::1]:<port>'
```

Every process in this example needs an IPv6 carrier. Without the second
`--carrier`, the joining process uses its default IPv4 socket and cannot send
datagrams to `[::1]`.

`cat` opens an opaque bidirectional QUIC stream. Connections are pooled, so
opening additional streams is cheap; applications can build request/response,
RPC, mailboxes, or actor messaging directly on top of them.

## Sealed postcard in Firecracker microVMs

The `sealed-postcard` example's separate-process roles also run across real
isolation boundaries. Alice sends an end-to-end encrypted postcard to Bob
through Relay, then checks 32 more echoed streams. Alice and Bob each run in
their own Firecracker microVM; Relay runs on the host and forwards their
datagrams without seeing the postcard's plaintext:

```text
  VM A                      host                       VM B
  Alice  <--- vsock --->    Relay    <--- vsock --->   Bob
         (Firecracker's Unix sockets on the host side)
```

The three roles are the unchanged Rust binary. A Python program on the host,
[`python/examples/sealed_postcard_microvms.py`](python/examples/sealed_postcard_microvms.py),
boots the two VMs, starts Relay, checks the outcome and releases every process
it started.

It needs a Linux devserver with read and write access to `/dev/kvm`; no root
access or host network setup. Buck builds the guest image, Firecracker, the
guest kernel and the Rust example. Run the automated test with:

```bash
buck test @fbcode//mode/dev-nosan \
  fbcode//monarch/chrysalis/python/tests:test_sealed_postcard_rust_microvms \
  -- --print-passing-details
```

With usable KVM it reports six passing tests and no skips: Alice exits
successfully, Bob reports `bob served 33 streams, all from alice: true`, and
Relay reports forwarded traffic, no postcard plaintext and a complete log.
Without KVM the microVM test is skipped and the five cleanup and interrupt
tests still run. A failed run prints whichever VM consoles and Relay log exist,
releases every process it started, and keeps its run directory if a process
couldn't be confirmed stopped.

[`firecracker.md`](crates/chrysalis/examples/sealed-postcard/firecracker.md)
runs the same example by hand in a tmux session, with each role's output in its
own pane. That walkthrough needs the host Python
`/usr/local/fbcode/platform010/bin/python3.12` and tmux. This example is
Buck-only: Cargo still runs the Rust example, but it doesn't build the image or
start a VM.
