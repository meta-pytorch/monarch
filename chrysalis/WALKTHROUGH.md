# Chrysalis Stack Walkthrough

This document presents Chrysalis in the order used by its review stack. Each
change introduces one complete layer of the final design. Later changes compose
earlier layers; they do not replace temporary implementations.

## 1. Process identity and connection IDs

Chrysalis begins with dependency-free wire types. A `Pid` names and
authenticates a process, while the fixed-width QUIC connection ID embeds the
destination PID for forwarding and reserves a process-local suffix for
demultiplexing.

## 2. Datagram sockets and carriers

The carrier layer moves atomic datagrams without interpreting QUIC. It provides
one socket contract across UDP, Unix datagrams, and in-process channels, plus a
socket set that selects a carrier from an opaque destination address.

## 3. Gated routing and switching

The router maps destination PIDs to next hops guarded by lifecycle gates. The
switch either terminates a datagram at a local PID or forwards it through the
router, preserving a connectionless and stream-oblivious middlebox.

## 4. Runtime-neutral stream transport

The completion core defines bounded commands, submissions, completions,
operation identifiers, ownership transfer, and wakeup contracts without
choosing an async runtime or QUIC implementation.

## 5. io_uring UDP packet engine

The io_uring engine owns stable receive and transmit slots on a dedicated
driver thread. It exposes batched UDP packet I/O, GSO, and GRO through the
runtime-neutral transport boundary.

## 6. Quiche packaging and overlay

Reindeer vendors quiche and overlays only the two source files that Chrysalis
changes. The overlay adds an application-selected Initial DCID and verifies IP
literals with IP subject alternative names; the vendored source remains
pristine.

## 7. QUIC endpoint and connection engine

The quiche endpoint owns all protocol state on one driver. It authenticates
certificate-derived PIDs, routes fixed CIDs directly, bounds server admission,
classifies packet errors, and manages connection shutdown and statistics.

## 8. Ordered and bounded QUIC streams

Each QUIC stream has ordered directional operation queues and explicit half
states. Completion credits bound retained resources end to end, runnable queues
avoid historical stream scans, cancellation returns caller buffers, and
terminal streams are reclaimed.

## 9. Tokio adapter

The Tokio adapter translates completion-driven handles into ergonomic async
connections and streams. It does not move protocol ownership into Tokio; the
same low-level driver remains usable from other runtimes and languages.

## 10. Carrier-neutral QUIC composition

The high-level transport composes routed datagram sockets, direct io_uring UDP,
pooled QUIC connections, and a link-local protocol mux. Application and control
streams share one physical carrier while retaining distinct routing identities.

## 11. Nameserver wire protocol

The nameserver protocol describes process publication, resolution,
enumeration, snapshots, deltas, acknowledgements, and errors. A bounded framed
codec gives every implementation the same deterministic wire contract.

## 12. Deterministic nameserver state

The nameserver state machine owns publications, cache entries, sequence
validation, and snapshots independently of network tasks. Tests can therefore
establish namespace behavior without timing or transport effects.

## 13. Nameserver sessions

Session drivers connect the deterministic state machine to framed streams.
They preserve request ordering, correlate responses, publish cache updates, and
tie all state learned from a peer to the lifetime of that link.

## 14. Hierarchical publication

A child link exports one complete process subtree under one lease. Parents
apply snapshots and ordered deltas, rewrite locators to their own next hop, and
republish the aggregate recursively.

## 15. Supervision and gated route projection

The nameserver manager supervises parent and child links and projects live
publications into router entries that reference one link gate. Expiring that
gate fences the entire subtree immediately while route cleanup proceeds later.

## 16. Node facade

`Node` is the composition boundary presented to embedders. It owns identity,
carriers, transport, nameserver state, parent linkage, and registered
link-local protocols while exposing a small serve, join, dial, accept, and
enumerate API.

## 17. Meta identity provider

The optional Meta identity crate obtains mTLS material and issues a distinct
leaf certificate per process. Core Chrysalis remains provider-neutral while
Meta deployments receive self-certifying PIDs without distributing secrets.

## 18. Hermetic cr-sqlite extension

The cr-sqlite extension is built from the vendored libSQL source with Buck and
loaded into the matching libSQL host. This keeps replicated SQLite optional and
outside the Chrysalis core dependency graph.

## 19. CRR schema and change replication

The SQLite layer discovers replicated schemas, initializes peer tables, chunks
change rows transactionally, and acknowledges only durable application. It
retains enough protocol state to restart without skipping unacknowledged data.

## 20. Topology-aware SQLite synchronization

Each adjacent Chrysalis link runs bidirectional SQLite synchronization. Durable
per-origin frontiers and incremental origin discovery prevent loops without
assuming that relayed sites share one scalar database version.

## 21. Resolver and CLI

Deployment resolvers turn addresses or `mast://` names into join settings. The
`chrysalis` utility then exposes serve, process listing, stream piping, and an
in-process replicated SQLite shell through the same public Node API.

## 22. Scale runner and MAST topology

The scale runner launches many real processes in flat or task-head topologies,
records process lineage, waits for namespace convergence, and exercises fresh
streams. Only task heads require UDP; local leaves use Unix datagrams.

## 23. Persistent experiments and bandwidth tests

Replicated SQLite tables distribute experiment requests and collect results.
Any process can claim work addressed to its PID, run echo or streaming-transfer
measurements after warmup, and publish status and timing for remote inspection.

## Companion: QUIC UDP roofline

The standalone roofline benchmark remains outside the Chrysalis stack. It
measures quiche and io_uring directly so transport decisions can be evaluated
without nameserver, routing, or Node overhead.
