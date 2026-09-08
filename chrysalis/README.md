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
fbcode/monarch/bin/chrysalis \
  --identity=meta \
  --carrier 'udp://[<root-ipv6>]:5000' serve

# Another host
fbcode/monarch/bin/chrysalis \
  --identity=meta \
  --carrier 'udp://[<client-ipv6>]:0' \
  --join '<root-pid>@udp://[<root-ipv6>]:5000' ps
```

The Meta identity crate retains an explicit `load` API for applications that
need the stable installed host identity. It also supports delegated `certreq`
issuance with MAST CATs. The scale benchmark uses the delegated path to host many
independent nodes in one task.

The pure-Rust [`chrysalis-scale`](../chrysalis-scale/README.md) benchmark builds
and optionally launches MAST ablations at 1K, 10K, and 100K nodes. It measures
full namespace join followed by a one-byte framed echo on a new stream to every
child.

## Deployment resolvers

`--join` accepts deployment resolver URLs in addition to direct join tokens.
For a Chrysalis MAST deployment, the job name is sufficient:

```bash
fbcode/monarch/bin/chrysalis \
  --join 'mast://chrysalis_scale_meriksen_1000n_10t_...' \
  ps
```

The MAST resolver finds the root task, joins its IPv4 or IPv6 address on the
well-known port `26600`, binds a matching wildcard UDP carrier, and selects the
Meta identity provider. Explicit `--carrier` or `--identity` options override
the corresponding resolved values. Additional resolver schemes can implement
the same join, carrier, and identity contract without changing commands such as
`ps`, `cat`, or `sqlite`.

## SQLite shell

The bare `sqlite` command opens an ordinary in-memory SQLite shell:

```bash
fbcode/monarch/bin/chrysalis sqlite
```

Pass the usual mesh options before entering the shell. For example, this creates
an in-memory replica attached to an existing root:

```bash
fbcode/monarch/bin/chrysalis \
  --identity=meta \
  --carrier 'udp://[::]:0' \
  --join 'udp://[<root-ipv6>]:26600' \
  sqlite
```

For a MAST deployment, the equivalent command needs only its job name:

```bash
fbcode/monarch/bin/chrysalis \
  --join 'mast://<job-name>' \
  sqlite
```

The shell and replication run in the same process. This is necessary because
the vendored cr-sqlite extension uses libSQL's extended loadable-extension ABI
and cannot be loaded safely into an arbitrary system `sqlite3` binary.

Use an explicit file to retain the local replica after exit:

```bash
fbcode/monarch/bin/chrysalis sqlite repl /tmp/chrysalis.db
```

The shell supports multiline SQL, `.tables`, `.schema`, and `.quit`:

```sql
CREATE TABLE items (
  id INTEGER PRIMARY KEY NOT NULL,
  value TEXT NOT NULL
);
SELECT crsql_as_crr('items');
```

New replicas receive CRR table definitions before their row changes, so a
joining database may start empty.

### Start the root

In the first terminal:

```bash
buck run fbcode//monarch/chrysalis-cli -- sqlite sync /tmp/root.db
```

The command prints its join token on standard output:

```text
32be1d32b140059f96786e3bbc79eaa1@udp://127.0.0.1:34778
```

It continues synchronizing until interrupted.

### Join the child

In the second terminal, substitute the root token:

```bash
buck run fbcode//monarch/chrysalis-cli -- \
  --join 32be1d32b140059f96786e3bbc79eaa1@udp://127.0.0.1:34778 \
  sqlite sync /tmp/child.db
```

Alternatively, attach an interactive child replica directly:

```bash
buck run fbcode//monarch/chrysalis-cli -- \
  --join 32be1d32b140059f96786e3bbc79eaa1@udp://127.0.0.1:34778 \
  sqlite repl /tmp/child.db
```

Replicated applications use `chrysalis-sqlite` directly. They register trusted
table descriptors and explicitly capture row mutations in their application
transactions; arbitrary shell SQL is intentionally not intercepted or
replicated.

## Process streams

The CLI can also demonstrate the base process mesh without SQLite:

```bash
# Terminal 1
buck run fbcode//monarch/chrysalis-cli -- serve
# prints: <root-pid>@udp://127.0.0.1:<port>

# Terminal 2
buck run fbcode//monarch/chrysalis-cli -- \
  --join <root-pid>@udp://127.0.0.1:<port> serve
# prints: <child-pid>@udp://127.0.0.1:<port>

# Terminal 3
echo hello | buck run fbcode//monarch/chrysalis-cli -- \
  --join <root-pid>@udp://127.0.0.1:<port> cat <child-pid>
```

`--join` also accepts an address without a PID, such as
`--join udp://127.0.0.1:<port>`. This discovers the authenticated parent PID on
the first successful nameserver handshake and pins it for subsequent reconnects.
Use the printed `<pid>@<address>` form when the parent identity must be pinned
before dialing.

IPv6 socket addresses use brackets. For example, start a root on the IPv6
loopback address, then join it from another IPv6 carrier:

```bash
# Terminal 1; prints <root-pid>@udp://[::1]:<port>
fbcode/monarch/bin/chrysalis --carrier 'udp://[::1]:0' serve

# Terminal 2
fbcode/monarch/bin/chrysalis \
  --carrier 'udp://[::1]:0' \
  --join 'udp://[::1]:<port>' ps
```

Every process in this example needs an IPv6 carrier. Without the second
`--carrier`, the joining process uses its default IPv4 socket and cannot send
datagrams to `[::1]`.

`cat` opens an opaque bidirectional QUIC stream. Connections are pooled, so
opening additional streams is cheap; applications can build request/response,
RPC, mailboxes, or actor messaging directly on top of them.
