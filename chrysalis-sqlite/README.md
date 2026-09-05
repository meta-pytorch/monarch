# Chrysalis SQLite replication

`chrysalis-sqlite` replicates a small set of explicitly registered SQLite
tables over authenticated Chrysalis streams. It uses ordinary SQLite tables
and the existing `libsql` Rust API. It does not load an extension or modify the
SQLite build.

## Model

Applications provide a trusted local `TableSchema` for every replicated table.
The descriptor contains:

- the local `CREATE TABLE IF NOT EXISTS` statement;
- the ordered row columns;
- the ordered primary-key columns; and
- a deterministic compatibility hash over all three.

Peers exchange only `(table name, schema hash)`. A missing or mismatched local
schema closes the replication session. SQL received from a peer is never
executed.

Every application transaction explicitly captures each changed row before it
commits:

```rust
let mut transaction = replica.transaction().await?;
transaction
    .execute(
        "INSERT INTO items (id, value) VALUES (?1, ?2) \
         ON CONFLICT (id) DO UPDATE SET value = excluded.value",
        vec![Value::Integer(7), Value::Text("new".into())],
    )
    .await?;
transaction
    .capture_upsert("items", vec![Value::Integer(7)])
    .await?;
transaction.commit().await?;
```

`capture_upsert` reads the final row through the same SQLite transaction. The
application row update, durable mutation-log append, logical-clock increment,
and resolved-row metadata therefore commit atomically. `capture_delete`
represents a row removed by the application. The transaction owns its buffered
mutations, and consuming `commit` prevents the application from accidentally
committing one without the other.

## Conflict resolution

A change carries its origin's stable 16-byte site ID, logical database version,
and sequence within that version. The durable log preserves the original tuple
when a change is relayed.

Each row has one winner ordered by `(database version, site ID, sequence)`. Applying a
newer upsert replaces the complete row; applying a newer delete removes it.
Older changes remain in the log for forwarding but do not overwrite the current
row. This is deliberately row-level last-writer-wins rather than cr-sqlite's
column-level CRDT behavior.

The local logical clock advances past every accepted remote version, so a later
local write is ordered after all changes that replica has observed. Duplicate
wire changes are ignored by the log's `(site ID, database version, sequence)`
primary key.

## Replication

The `chrysalis.sql.v5` protocol retains the existing topology behavior:

- each link advertises its reachable origin-site scope;
- persisted per-peer frontiers prevent acknowledged changes from being resent;
- split-horizon filtering avoids returning changes toward their origin;
- batches preserve transaction-version boundaries and are acknowledged only
  after they commit; and
- a synchronization marker reports that no eligible changes remain.

All internal metadata is stored in normal `__chrysalis_*` SQLite tables. There
is no third-party source overlay, generated C binding, shared object, `dlopen`,
or extension-specific deployment artifact.

## Tradeoffs

The application must route replicated writes through `Replica::transaction`;
direct SQL writes are intentionally invisible to replication. This explicit
boundary keeps the implementation and trust model small, and makes transaction
atomicity obvious. The first version retains changes indefinitely. Snapshotting
and log compaction can be added independently once operational retention
requirements are known.
