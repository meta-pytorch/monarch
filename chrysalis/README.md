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
  --cluster 'udp://[<root-ipv6>]:5000?authority=<root-pid>' ps
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

`--cluster` accepts deployment resolver URLs in addition to direct locators.
For a Chrysalis MAST deployment, the job name is sufficient:

```bash
fbcode/monarch/bin/chrysalis \
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
fbcode/monarch/bin/chrysalis sqlite
```

The shell supports readline editing, persistent command history, multiline SQL,
`.tables`, `.schema`, and `.quit`. Press Ctrl-C to discard the current input and
Ctrl-D to exit. Use an explicit file to retain the database after exit:

```bash
fbcode/monarch/bin/chrysalis sqlite repl /tmp/chrysalis.db
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
buck run fbcode//monarch/chrysalis-cli -- serve
# prints: udp://127.0.0.1:<port>?authority=<root-pid>

# Terminal 2
buck run fbcode//monarch/chrysalis-cli -- \
  --cluster 'udp://127.0.0.1:<port>?authority=<root-pid>' serve
# prints: udp://127.0.0.1:<child-port>?authority=<child-pid>

# Terminal 3
echo hello | buck run fbcode//monarch/chrysalis-cli -- \
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
fbcode/monarch/bin/chrysalis --carrier 'udp://[::1]:0' serve

# Terminal 2
fbcode/monarch/bin/chrysalis \
  --carrier 'udp://[::1]:0' \
  ps 'udp://[::1]:<port>'
```

Every process in this example needs an IPv6 carrier. Without the second
`--carrier`, the joining process uses its default IPv4 socket and cannot send
datagrams to `[::1]`.

`cat` opens an opaque bidirectional QUIC stream. Connections are pooled, so
opening additional streams is cheap; applications can build request/response,
RPC, mailboxes, or actor messaging directly on top of them.
