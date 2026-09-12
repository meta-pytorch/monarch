# Chrysalis SQLite replication

This crate uses the `cr-sqlite` CRR change feed as a replication payload over
Chrysalis link-local streams. The current implementation provides a framed,
bidirectional replication session, CRR schema propagation, durable per-peer
frontiers, dynamic site-scope advertisements, transactional application, and
conflict convergence. An ordered synchronization marker tells a receiver when
it has applied the peer's complete schema and current change frontier.

The extension source is already vendored at:

```text
third-party/libsql/main/src/libsql-sqlite3/ext/crr
```

Buck builds the Rust and C parts of the libSQL extension as one shared library:

```bash
buck build fbsource//third-party/libsql/main/src/libsql-sqlite3/ext/crr:crsql_bundle
```

No external `make`, Rustup toolchain, or environment variables are required.

This source snapshot uses libSQL's extended loadable-extension ABI and must be
loaded into the matching `libsql` host. It is not ABI-compatible with an
arbitrary upstream SQLite or `rusqlite` build.

## Findings

- The vendored CRR Rust bundle builds with the installed nightly after removing
  one obsolete, unused `concat_idents` feature gate.
- `crsql_as_crr` and `crsql_commit_alter` passed Rust string slices to C-string
  entry points. Calling their Rust implementations directly removes that
  undefined behavior and makes CRR conversion work on the current toolchain.
- A replication batch is the nine owned values exposed by `crsql_changes`,
  bounded by `crsql_db_version()`. Evaluating the peer's origin scope prevents
  changes from returning toward the side that originated them.
- Applying each wire chunk by inserting those values into `crsql_changes`
  inside one transaction preserves CRR conflict resolution while bounding
  transaction size while preserving convergence after concurrent updates.
- `chrysalis.crr.v3` is a reserved 128-bit link-local protocol ID. Replication
  follows the Node-managed parent and child link topology.

## Replication protocol

Each side sends the same records:

```text
Hello { site_id, scope: SiteScope }
Schema { table, create_sql, hash }
Scope { scope: SiteScope }
BeginBatch { batch_id }
BatchChunk { changes }
...
CommitBatch
Ack { batch_id }
Synchronized
```

`CREATE TABLE` and `crsql_as_crr` do not advance `crsql_db_version()` or emit
rows through `crsql_changes`. Version four therefore sends immutable CRR table
definitions alongside the change feed. Every session tracks the schema hashes
already sent to its peer and sends new definitions before any change batch that
references the table. The receiver creates the table, runs `crsql_as_crr`
locally, and verifies the resulting definition before applying dependent rows.

Sessions poll SQLite's schema version as well as its CRR database version. This
propagates newly initialized empty tables, which cannot be discovered through
the `table` field of a change row. That field remains a safety boundary: a
batch is not sent or applied unless its table schema is already established.
Reconnects resend the complete schema snapshot. Identical definitions are
idempotent; conflicting definitions terminate the session. Ordered schema
migrations and table removal are not supported yet.

After the sender has emitted every current schema and received acknowledgment
for every current change batch, it sends `Synchronized`. Ordered stream delivery
guarantees that the receiver has applied that snapshot before observing the
marker. Later schema, scope, or row updates clear the peer's synchronized state
until the next marker.

The sender allows one in-flight batch and partitions changes into bounded
frames. The receiver applies and commits each chunk independently, making
progress visible and yielding the database between chunks. `CommitBatch`
returns `Ack` after every preceding chunk has committed. If a session drops
before that acknowledgment, the sender replays the batch from its prior durable
frontier; CRR application makes already committed chunks idempotent. For each
peer database incarnation, the sender persists the greatest acknowledged
`db_version` independently for every origin site. A mixed-site batch records its
proposed frontier advances locally and commits all of them only after the final
ACK. A replacement peer database starts from an empty frontier. Scope changes
retain progress for previously synchronized origins, while newly eligible sites
start at version zero. Replay after a lost ACK is safe because CRR merge
semantics are idempotent.

Each frontier row also records whether its origin is currently eligible under
the link's effective scope. Reconciliation toggles that flag without discarding
the acknowledged version. The send query starts from eligible frontier rows and
joins into `crsql_changes` on `(site_id, db_version > synced_at)`, allowing the
CRR virtual table to apply both constraints instead of rescanning all changes.
New origins are inserted at version zero before selection.

Origins are recorded once in a durable append-only registry with monotonic
positions. Replica startup bootstraps that registry once from cr-sqlite; each
received chunk subsequently registers its origin IDs in the same transaction as
the CRR changes. An in-memory vector and membership set mirror the durable log.
Each peer persists the last registry position incorporated into its frontier,
so reconciliation consumes only the new suffix. Scope updates toggle only IDs
added to or removed from the explicit allow or block set. A scope-mode change is
the only operation that revisits every known origin.

`db_version` is local to the database serving the replication link. Applying a
remote winner preserves its origin `site_id`, but cr-sqlite assigns it a new
local `db_version` before that database republishes it. A scalar cursor would
therefore order one link correctly, but expanding a scope would require resetting
that cursor and replaying every origin to recover older, previously excluded
rows. The per-origin frontier preserves the same source-local ordering without
that replay.

`SiteScope` is either `Explicit(SiteSet)` or `ComplementOfPeer`. A child
advertises its finite subtree explicitly. Its parent advertises
`ComplementOfPeer`, meaning that the parent owns every origin outside the
child's own explicit advertisement. The complement is a predicate and is never
materialized as a global set.

The resulting send rules are:

```text
peer scope = Explicit(S):        send origins not in S
peer scope = ComplementOfPeer:   send origins in local explicit S
```

`ReplicationTopology` installs the CRR handler into `NodeConfig`. Node opens and
supervises one selected stream for every mutually supported link protocol. The
topology advertises its aggregated explicit subtree toward its parent and a
complement scope toward each child. Child joins, departures, and scope changes
automatically update the upstream publisher.

Applying a remote batch notifies every local session; applications call
`Replica::notify_changed` only when they need an explicit wakeup. `Replica::new`
installs the connection's SQLite update hook, replacing any previous hook, so
writes through that connection wake replication automatically. Sessions also
poll the database version every 250 milliseconds so writes through another
connection or process are eventually observed. `Replica::subscribe_peer_scopes`
provides coalescing membership notifications; `Replica::peer_scopes` returns the
complete current snapshot and removes a peer when its session ends.

## CLI

Running the bare `chrysalis sqlite` command opens an in-memory SQLite shell with
cr-sqlite replication active. Mesh options still apply:

```bash
chrysalis --identity=meta --carrier 'udp://[::]:0' \
  --cluster 'udp://[<root-ipv6>]:26600' sqlite
```

The shell runs in-process because this cr-sqlite snapshot uses libSQL's extended
extension ABI and is not compatible with an arbitrary system `sqlite3` binary.
Use `sqlite repl FILE` for a durable local replica:

```bash
chrysalis sqlite repl root.db
chrysalis --cluster 'udp://127.0.0.1:<port>?authority=<pid>' sqlite repl child.db
```

The `chrysalis` utility can also create and query a file-backed CRR database
non-interactively:

```bash
chrysalis sqlite query root.db "
  CREATE TABLE items (
    id INTEGER PRIMARY KEY NOT NULL,
    value TEXT NOT NULL DEFAULT ''
  );
  SELECT crsql_as_crr('items');
"
```

For synchronization without an interactive shell, start a root process and use
its printed token to join another database:

```bash
chrysalis sqlite sync root.db
# prints: udp://127.0.0.1:<port>?authority=<pid>

chrysalis --cluster 'udp://127.0.0.1:<port>?authority=<pid>' sqlite sync child.db
```

The child database may start empty. CRR table definitions are initialized from
the replication link before their row changes arrive.

Queries may run before, during, or after synchronization:

```bash
chrysalis sqlite query root.db "INSERT INTO items VALUES (1, 'hello')"
chrysalis sqlite query child.db "SELECT id, value FROM items ORDER BY id"
```

## Next step

The topology currently requires a rooted tree: one parent and any number of
children. Ordered schema migrations and explicit synchronization status remain
future work.
