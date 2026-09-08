# Local SQLite replication log

Chrysalis replication should use ordinary SQLite through the existing Rust
API. It must not patch SQLite, load native extensions, or execute schema SQL
received from another process.

This is row replication, not a CRDT. Each mutation replaces or deletes a full
row, and concurrent mutations use deterministic last-writer-wins resolution.
The design does not merge independently edited columns or preserve every
concurrent intent.

## Boundary

The application registers trusted local descriptors for the small set of
replicated tables. Each descriptor fixes the table's creation statement,
ordered columns, and primary key. Peers exchange only a digest of that
descriptor and reject incompatible schemas.

Application writes and replication metadata share one SQLite transaction:

1. the application updates its domain rows;
2. it explicitly captures the final rows, or their deleted keys;
3. Chrysalis advances a persistent logical clock and appends those mutations;
4. the transaction commits; and
5. replication sessions are notified.

This leaves direct SQL writes outside the replication API local until an
explicit reconciliation pass. It also avoids triggers, virtual tables,
generated C bindings, shared-library packaging, and a second database
implementation.

The database remains an ordinary SQLite file, so standard tools can inspect it.
They cannot act as replicated writers: a write made through `sqlite3`, a generic
SQLite library, or the Chrysalis SQLite shell does not append a mutation and
will propagate only after reconciliation. Applications that require immediate
replication must perform writes through the capture-and-commit API.

### Reconciling outside writes

An explicit reconciliation pass can recover the final state left by an outside
writer. In one SQLite write transaction, it compares every registered table
with Chrysalis's last resolved state. A new or changed row becomes a local
full-row upsert, and a missing row becomes a local delete. Chrysalis assigns
these mutations a new local logical version, updates the resolved metadata, and
commits them for the next replication session.

This makes `sqlite3` and other ordinary tools usable for occasional replicated
administration without putting Chrysalis in their write path. It is snapshot
reconciliation, not change capture: intermediate writes are lost, and drift is
treated as a new local write at reconciliation time. The pass is proportional
to the registered data unless a future implementation adds dirty-page or
application hints. It should therefore be explicit in the first cut, with a
background policy possible later.

### Writable-view alternative

SQLite views alone cannot intercept writes; they are read-only unless paired
with schema-specific `INSTEAD OF` triggers. We could hide each physical table
behind a writable view and generate triggers that update the backing table,
advance the logical clock, and append mutations. An external SQLite client
would then capture changes synchronously without knowing about replication,
while a Chrysalis process could replicate the resulting log asynchronously.

That model is feasible for a restricted schema, but it is not transparent.
Foreign keys and indexes belong to the hidden backing tables, schema tools see
views rather than ordinary tables, and every schema migration must regenerate
and validate replication SQL. It also makes triggers and generated DDL part of
the correctness boundary. The first cut keeps ordinary application tables and
uses explicit capture plus reconciliation; writable views remain a possible
compatibility mode rather than the core storage model.

## Durable state

The local engine owns normal `__chrysalis_*` tables for:

- a stable random 16-byte site ID and logical database version;
- immutable row mutations keyed by `(site ID, version, sequence)`;
- the winning `(version, site ID, sequence)` and resolved value digest for each
  logical row; and
- per-peer, per-origin acknowledged frontiers.

SQLite stores each application table's physical schema in `sqlite_schema`. The
trusted replication descriptor—its creation statement, ordered columns, and
primary key—is supplied locally by the application. Peers exchange only its
digest. We do not persist or accept a peer-provided schema as executable SQL.

Each mutation is a full-row upsert or a delete. Concurrent changes converge by
row-level last-writer-wins ordering on `(version, site ID, sequence)`. "Last"
means the greatest logical tuple, not the last mutation received. The version
is a Lamport-style logical clock, the site ID breaks ties between concurrent
sites, and the sequence orders mutations within one local transaction. Receipt
order therefore does not affect the winner. Applying a remote change advances
the local logical clock, preserves the origin tuple for relay, and ignores
duplicate log entries.

## Authority across the tree

The process and replication topologies are hierarchical and fixed. A possible
simplification is to make each receiving node the new authority: it would
resolve an incoming mutation and republish the result under its own site ID.
Then each link would track only adjacent authorities instead of every original
site.

That changes more than bookkeeping. Re-authoring discards end-to-end origin
identity, makes conflict ordering depend on the path and timing through the
tree, and complicates distinguishing a relayed value from a new local write.
Preserving the original origin gives split-horizon filtering and deterministic
resolution the same identity at every hop. Because we expect rows to be mostly
single-writer, either model should usually produce the same values. The first
cut preserves origins; we can reconsider hop-by-hop authority if per-origin
metadata becomes an operational cost.

## Scope

The first implementation keeps the existing Chrysalis stream topology,
split-horizon origin filtering, bounded batches, acknowledgments, and
synchronization markers. It deliberately does not implement column-level CRDT
merges, automatic interception of arbitrary SQL, schema migration, wire-level
snapshots, or log compaction.
