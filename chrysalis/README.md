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
`ps`, `cat`, or `sqlite`.

## SQLite demo

The `chrysalis` CLI packages the matching cr-sqlite extension as a Buck resource.
No external extension build or path is required. The bare `sqlite` command opens
an in-memory replicated SQLite shell:

```bash
fbcode/monarch/bin/chrysalis sqlite
```

Pass the usual mesh options before entering the shell. For example, this creates
an in-memory replica attached to an existing root:

```bash
fbcode/monarch/bin/chrysalis \
  --identity=meta \
  --carrier 'udp://[::]:0' \
  --cluster 'udp://[<root-ipv6>]:26600' \
  sqlite
```

For a MAST deployment, the equivalent command needs only its job name:

```bash
fbcode/monarch/bin/chrysalis \
  --cluster 'mast://<job-name>' \
  sqlite
```

The shell and replication run in the same process. This is necessary because
the vendored cr-sqlite extension uses libSQL's extended loadable-extension ABI
and cannot be loaded safely into an arbitrary system `sqlite3` binary. The shell
supports readline editing, persistent command history, multiline SQL, `.tables`,
`.schema`, and `.quit`. Press Ctrl-C to discard the current input and Ctrl-D to
exit. When joining a parent, the CLI displays a progress indicator and waits for
the parent's complete schema and current changes before opening the shell.

Use an explicit file to retain the local replica after exit:

```bash
fbcode/monarch/bin/chrysalis sqlite repl /tmp/chrysalis.db
```

### Initialize a database

Create a table and convert it to a CRR from the shell. cr-sqlite requires
non-null columns to have defaults.

```sql
CREATE TABLE items (
  id INTEGER PRIMARY KEY NOT NULL,
  value TEXT NOT NULL DEFAULT ''
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
udp://127.0.0.1:34778?authority=32be1d32b140059f96786e3bbc79eaa1
```

It continues synchronizing until interrupted.

### Join the child

In the second terminal, substitute the root token:

```bash
buck run fbcode//monarch/chrysalis-cli -- \
  --cluster 'udp://127.0.0.1:34778?authority=32be1d32b140059f96786e3bbc79eaa1' \
  sqlite sync /tmp/child.db
```

Alternatively, attach an interactive child replica directly:

```bash
buck run fbcode//monarch/chrysalis-cli -- \
  --cluster 'udp://127.0.0.1:34778?authority=32be1d32b140059f96786e3bbc79eaa1' \
  sqlite repl /tmp/child.db
```

The child advertises its explicit cr-sqlite site subtree. The root advertises
the complement of that subtree. These scopes prevent changes from being reflected
back toward their origin while allowing gateways to aggregate further children.

### Write and query

In a third terminal, write through a separate connection to the root file:

```bash
buck run fbcode//monarch/chrysalis-cli -- sqlite query /tmp/root.db \
  "INSERT INTO items VALUES (1, 'hello from root')"
```

Then query the child:

```bash
buck run fbcode//monarch/chrysalis-cli -- sqlite query /tmp/child.db \
  "SELECT id, value FROM items ORDER BY id"
```

```text
id  value
1   hello from root
```

Replication is bidirectional:

```bash
buck run fbcode//monarch/chrysalis-cli -- sqlite query /tmp/child.db \
  "INSERT INTO items VALUES (2, 'hello from child')"

buck run fbcode//monarch/chrysalis-cli -- sqlite query /tmp/root.db \
  "SELECT id, value FROM items ORDER BY id"
```

```text
id  value
1   hello from root
2   hello from child
```

The synchronization process installs a SQLite update hook for writes through its
own connection and checks the database version every 250 milliseconds for writes
through other connections or processes. Replication frames large changes into
bounded chunks, applies all chunks in one destination transaction, and advances
the durable peer cursor only after the destination commits and acknowledges the
batch.

Press Control-C in either sync terminal to disconnect cleanly. The database files
remain ordinary local SQLite files and can still be queried with `sqlite query`.

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
