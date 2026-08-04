# Delegated Heartbeating (QUIC scale-out)

Design proposal for offloading heartbeat cost from a high-fan-out parent to its
children, keeping the whole mechanism inside the QUIC subsystem. This is the
`IMPLEMENTATION.md` "TCP Scale-out" idea, adapted to QUIC.

> Status: design for review. Pseudocode is illustrative, not final. The goal is a
> normalized set of data structures so the eventual edit touches few places.

---

## 1. Goals & invariants

- **Ctx/Actors are unchanged in what they observe.** From the command loop's point
  of view an actor still has N child connections and still receives the same
  `Establish` / `PublishRoutes` / `Severed` notices. Only *how liveness is proven*
  changes. All new state and coroutines live in the QUIC transport.
- **Steady-state heartbeat cost is bounded per node.** A parent `B` actively
  heartbeats at most `MM_QUIC_MAX_DIRECT_CHILDREN` children; the rest are
  *delegated* to sibling children, who prove liveness on `B`'s behalf and report a
  running `+/-` diff of the connections they cover.
- **A delegated connection is quiet on the physical link.** No app heartbeat, and
  QUIC keep-alive / idle-timeout disabled (§9), so a delegated connection generates
  no periodic traffic. Liveness rides side channels between siblings instead.
- **Delegation is best-effort and self-healing.** If a delegate dies, or a child
  loses its delegate, the child falls straight back to heartbeating `B` directly,
  which re-runs the delegation decision.

---

## 2. Terminology & topology

Consider base actor `B` with children `C` (established) and `D` (new).

```
        B                     B  = parent, dialed both children (quic connect)
       / \                    C  = existing child, addressable (quic serve)
      C   D                   D  = new child, addressable (quic serve)
```

For one physical QUIC connection there are two heartbeat *roles*, chosen by
`ConnectionRef`:

| Role         | ConnectionRef       | Responsibility                                    |
|--------------|---------------------|---------------------------------------------------|
| **Parent**   | `ChildConnection`   | Detect the peer child's death; may *delegate* it. |
| **Child**    | `ParentConnection`  | Prove liveness to the parent; may *cover* siblings.|

(These match `ConnectionRef::role()` — a `ChildConnection` has `Role::Parent`.)
So on the `B–D` link, `B` runs a Parent coroutine and `D` a Child coroutine. The
delegate `C` is a Child toward `B` **and** additionally hosts *coverage watches*
for the siblings it has taken on.

**Guard (both must hold to delegate a link):** the parent *dialed* the connection
(`join`, "quic connect") and the child *served* it ("quic serve", so it has a
dial-able gateway tag). See §8.

---

## 3. Two identifiers: connection id vs. actor ident

The protocol uses each for what it is good at:

- **`ConnectionId` (`u64`)** — the identity of a parent→child connection, assigned
  ctx-globally by the **Parent side** at `spawn_connection` (a counter in the shared
  state). It names *which delegated link* a message concerns, independent of whether
  actor names are known yet. It appears in the delegation instruction (`B` tells `D`
  "you are connection X") and in coverage diffs (`C` reports "I cover X"). `B`
  correlates a coverage report to its child connection via a `ConnectionId → Parent
  task` index.
- **Actor ident** — the *addressing* key for side-channel beats/acks. A beat is sent
  to `gateway_tag(recipient_ident)` and delivered to the connection where that actor
  is the child (unique — an actor has one parent). See §6.2.

**Establish snooping** fills in the idents on both ends: the heartbeat coroutine
snoops the `Establish` frames that already flow through the writer (outgoing → local
ident) and reader (incoming → peer ident) — no new threading into ctx. From `ident =
name@tag`, `gateway_tag()` (suffix after the last `@`) is the dial address. Both the
child `D` and the delegate `C` must be named before delegation begins (the guard,
§8).

---

## 4. Coroutine topology (per connection)

Today `spawn_connection` starts a **writer** and a **reader**. Add a third: the
**heartbeat coroutine** (`heartbeat_task`). The reader/writer become dumb plumbing; the
heartbeat_task owns all liveness policy (interval + timeout + delegation).

```
        ┌────────────┐  WriterCtrl (SendBeat{payload})   ┌──────────┐
        │  heartbeat_task   │ ────────────────────────────────► │  writer  │──► stream
        │            │ ◄──── HeartbeatEvent::EstablishOut ────── │          │
        │  (Parent   │                                    └──────────┘
        │     or     │  HeartbeatEvent::{Beat, EstablishIn,      ┌──────────┐
        │   Child)   │ ◄─── ReaderClosed} ─────────────── │  reader  │◄── stream
        │            │                                    └──────────┘
        │            │ ──► loop_tx: Severed (on timeout)
        │            │ ◄──► Rc<RefCell<HeartbeatShared>>  (siblings, coverage indices)
        │            │ ──► loop_tx: SendSideChannel(msg)  (beats reuse the gateway
        │            │                                     side channel, §6.2)
        └────────────┘
```

Reader/writer changes:
- **writer**: delete the self-driven `heartbeat.tick()` arm. Instead select on a
  new `WriterCtrl` channel from heartbeat_task; on `SendBeat(payload)` write
  `WireFrame::Heartbeat(payload)`. On seeing an outgoing `Establish` command,
  forward `HeartbeatEvent::EstablishOut{local_ident}` to heartbeat_task. (Shutdown behavior
  unchanged.)
- **reader**: remove the heartbeat-timeout wrapper around the read — the *timeout
  moves to heartbeat_task*. Forwarding rules, designed so a message flood does **not**
  flood the heartbeat_task:
  - **Heartbeat frames** always forward as `HeartbeatEvent::FromChild` /
    `FromParent` (per the `Heartbeat` variant) — they are rare (one per interval)
    and carry the coverage diff / delegate instruction the heartbeat_task must
    process anyway.
  - **`Establish`** forwards to the loop as today and also sends
    `HeartbeatEvent::EstablishIn{peer_ident}` (one-shot, at setup).
  - **Ordinary message/route frames** forward to the loop as today, but notify the
    heartbeat_task only as a *throttled backstop*: the reader keeps a local
    `last_notify: Instant` (bumped by any event it sends, including real beats) and
    sends an *empty* beat — `FromChild{}` or `FromParent{None}` per the reader's own
    role — only when `now - last_notify >= 2 × HEARTBEAT_INTERVAL`. So while real
    heartbeats arrive on cadence, data frames cost nothing; if the peer stops
    heartbeating but traffic still flows, the pipe is still proven live at ≤ one
    event per two intervals.
  - **error/EOF** still emits `Severed` to the loop (hard close is transport-level,
    not heartbeat policy) and sends `HeartbeatEvent::ReaderClosed`.

  There is no separate liveness event: a backstop is just an empty beat, which
  refreshes the `Direct` deadline and is a no-op for coverage/delegation.

The heartbeat_task may **park** (no active timer, no beats) when a Child is fully
delegated or a Parent is fully covered; it is woken by `HeartbeatEvent`s and by shared
state notifications.

---

## 5. `quic_heartbeat.rs` data structures

```rust
type ConnectionId = u64;
type Handle = mpsc::UnboundedSender<HeartbeatEvent>;   // control channel to one heartbeat_task

/// One per QuicTransport (per context). Single-threaded → Rc<RefCell<>>.
pub(crate) struct HeartbeatShared {
    /// The next unused ConnectionId; bumped once per connection at spawn.
    next_conn_id: ConnectionId,

    /// For each local actor, the control handle of the heartbeat_task on that
    /// actor's parent connection (the Child-side task). Keyed by the actor's own
    /// ident. At most one entry per actor (an actor has one parent).
    children_by_ident: HashMap<Vec<u8>, Handle>,

    /// For each local parent→child connection, the control handle of its Parent-side
    /// heartbeat_task. Keyed by that connection's ConnectionId.
    parents_by_conn_id: HashMap<ConnectionId, Handle>,

    /// For each local actor that parents quic children, its delegate pool.
    parents: HashMap<Key, ParentPool>,
}

struct ParentPool {
    /// The ConnectionIds of this parent's children that are currently usable as a
    /// delegate (established + addressable + dialed + under coverage capacity).
    eligible: VecDeque<ConnectionId>,
    /// How many of this parent's children are monitored directly (not delegated).
    active_direct: usize,
}
```

Per-connection state each loop actually holds. There is **no stored `mode` object** —
a loop's "mode" is just which timeout it is currently awaiting (see the execution
model below), so only genuinely-persistent data lives here:

```rust
/// Held by the Parent-side loop (B, watching child D).
struct ParentLink {
    conn_id: ConnectionId,               // this connection's id (assigned at spawn)
    peer_ident: Option<Vec<u8>>,         // the child's ident, once snooped
    dialed: bool,                        // whether we dialed the child (join); see §8
    /// If this child is itself a delegate, the ConnectionIds it currently covers
    /// (from its cover_add/del diffs). On this link's failure, each is released via
    /// `Uncovered` to its own Parent loop.
    covers: HashSet<ConnectionId>,
}

/// Held by the Child-side loop (D, proving liveness to parent B). The same task
/// also owns this actor's coverage loops (its delegate-host duty as C).
struct ChildLink {
    own_ident: Option<Vec<u8>>,          // this actor's own ident, once snooped
    parent_ident: Option<Vec<u8>>,       // the parent's ident, once snooped
    /// Coverage changes not yet reported to the parent, produced by the coverage
    /// loops and drained onto the next FromChild beat.
    cover_add: Vec<ConnectionId>,
    cover_del: Vec<ConnectionId>,
}
```

**Execution model — one timeout per loop.** Every heartbeat loop owns exactly one
timeout ("did the thing I watch arrive in time?") and fires exactly one failure
event on lapse. Delegation moves *which loop owns the timeout* for a connection; it
is not a state machine.

- **Parent loop (B ⇢ D).** Directly watching: `select!` D's physical `FromChild`
  beats (reset the deadline) against the deadline (⇒ `sever`). On deciding to
  delegate, it emits the `Delegate` instruction and stops timing out — it now awaits
  a `Covered`/`Uncovered` event on its channel (a grace timeout guards the gap until
  the first `Covered`). `Covered` ⇒ parked (no timeout; the delegate owns it);
  `Uncovered` ⇒ re-arm the direct timeout (D will reconnect and beat directly).
- **Child loop (D ⇢ B).** Directly: beat B on the interval and time out on B's
  physical `FromParent` beats. On `Delegate{connection_id, sibling}`: beat the
  sibling `C` over the side channel and let the single timeout watch C's *acks*
  instead. Ack lapse ⇒ revert to beating B directly (B re-decides).
- **Coverage loop (C ⇢ X), one per covered connection.** Spawned by C's Child task
  when it first sees a side-channel beat for connection `X`; owns a single timeout
  on `X`'s beats. Each beat resets it and is acked; the first enqueues `cover_add(X)`
  and a lapse enqueues `cover_del(X)` and ends the loop. C's Child loop drains those
  onto its next `FromChild` beat to B — which is exactly what fires B's `Uncovered`
  for `X`.

Events into an heartbeat_task (a task is either a Parent or a Child, so it only ever
sees the events for its role):

```rust
enum HeartbeatEvent {
    // From the reader: an inbound physical heartbeat, split by direction. A task
    // receives whichever direction its peer sends (a Parent gets FromChild, a Child
    // gets FromParent). An empty one is the reader's liveness backstop.
    FromChild  { cover_add: Vec<ConnectionId>, cover_del: Vec<ConnectionId> },
    FromParent { delegate: Option<Delegate> },
    EstablishLocal { local_ident: Vec<u8> },   // our outgoing Establish (from the writer)
    EstablishPeer  { peer_ident: Vec<u8> },    // the peer's Establish (from the reader)
    ReaderClosed,                            // reader hit error/EOF

    // To a Parent-side task, when a sibling's coverage diff names this connection:
    Covered   { by: Vec<u8> },               // now covered by sibling `by` (its ident)
    Uncovered,                               // no longer covered

    // To a Child-side task, from an inbound side-channel beat/ack (§6.2):
    SideBeat  { from: Vec<u8>, conn_id: ConnectionId },  // a delegated child beating us
    SideAck   { from: Vec<u8>, conn_id: ConnectionId },  // our delegate acking our beat
}
```

---

## 6. Wire protocol changes

### 6.1 Physical heartbeat gains a directional body (`framing.rs`)

`WireFrame::Heartbeat` becomes non-unit, carrying a `Heartbeat` that is
discriminated by direction — a Child sends `FromChild`, a Parent sends `FromParent`:

```rust
WireFrame::Heartbeat(Heartbeat)

#[derive(Serialize, Deserialize)]
enum Heartbeat {
    // Child → parent: the running diff of connections this child covers for the
    // parent. Empty in both lists = a plain liveness beat (also the backstop).
    FromChild { cover_add: Vec<ConnectionId>, cover_del: Vec<ConnectionId> },
    // Parent → child: a delegation instruction, or None for a plain liveness beat.
    // None never revokes — a child reverts on its own (ack-timeout), a parent on
    // `Uncovered` — so the empty backstop beat is safe.
    FromParent { delegate: Option<Delegate> },
}

#[derive(Serialize, Deserialize)]
struct Delegate {
    connection_id: ConnectionId,   // "you are connection X" (what the child presents to C)
    sibling_ident: Vec<u8>,        // C — the child beats gateway_tag(C)
}
```

`read_frame`/`write_command` are updated so `Incoming::Heartbeat` carries the
`Heartbeat` (reader forwards it as `HeartbeatEvent::FromChild` / `FromParent` per
its variant). Heartbeats are still consumed by the transport and never surface to
ctx as a `ConnectionCommand`.

### 6.2 Side-channel beats **reuse the existing gateway side channel**

There is **no new stream, preamble, or dialer.** A beat/ack is just another
`SideChannelMessage` on the path that already exists (`send_to_gateway` →
`Preamble::SideChannel` writer → `Command::SideChannelDeliver` →
`deliver_side_channel`). Add one action variant (`connection.rs`) and the matching
`framing.rs` `SideChannelFrame`:

```rust
enum SideChannelAction {
    Send(SendPayload),
    UpdateRemoteMonitorState { .. },
    AckRemoteMonitor { .. },
    // + new: from = sender ident; conn_id = the delegated connection's id;
    //        ack = false beat / true ack
    Heartbeat { from: Vec<u8>, conn_id: ConnectionId, ack: bool },
}
```

- **Beat** `D→C`: `SideChannelMessage { gateway_for_actor: C_ident, Heartbeat { from: D_ident, conn_id: X, ack: false } }`.
- **Ack** `C→D`: `SideChannelMessage { gateway_for_actor: D_ident, Heartbeat { from: C_ident, conn_id: X, ack: true } }`.

`gateway_for_actor` is the recipient's ident, so the message dials
`gateway_tag(recipient)` and is delivered exactly like any cross-gateway message
(including the free `pending_side_channel` parking/replay if the recipient's route
isn't resolvable yet). At the recipient, `deliver_side_channel` routes the
`Heartbeat` action into the heartbeat subsystem instead of to an actor:
`self.quic.deliver_heartbeat(gateway_for_actor, from, conn_id, ack)`, which looks up
`children_by_ident[gateway_for_actor]` and sends `SideBeat{from, conn_id}` /
`SideAck{from, conn_id}` to that Child-side heartbeat_task — the one connection where
the addressed actor is the child. If it isn't registered yet, drop it; the next beat
retries.

---

## 7. Algorithms

### 7.1 Parent side (B, the `B–D` link)

One loop, one timeout. At spawn: `conn_id = shared.next_conn_id++`; register
`parents_by_conn_id[conn_id] -> self`. `EstablishIn{peer_ident}` records the child
ident and `active_direct += 1`.

```
# directly-watching D (owns D's liveness timeout):
loop select:
    FromChild{..}   -> deadline = now + TIMEOUT
                       if should_delegate() and dialed and peer_ident known:
                           C = pick_eligible_sibling()          # ident from ParentPool.eligible
                           if C:
                               next FromParent beat carries
                                   Delegate{ connection_id: conn_id, sibling_ident: C }
                               active_direct -= 1;  goto awaiting-coverage
    deadline        -> sever("quic heartbeat timeout"); done    # the single timeout fires
    ReaderClosed    -> sever; deregister; done

# awaiting-coverage (stopped timing out D; a grace timer guards the gap):
loop select:
    Covered{by}     -> goto delegated                           # a delegate owns D now
    FromChild{..}   -> active_direct += 1; goto directly-watching   # D fell back to us
    grace           -> active_direct += 1; goto directly-watching   # nobody took it; reclaim
    ReaderClosed    -> sever; done

# delegated (no timeout here — the delegate's coverage loop owns it):
loop select:
    Uncovered       -> active_direct += 1; deadline = now + TIMEOUT; goto directly-watching
    FromChild{..}   -> active_direct += 1; goto directly-watching   # D fell back to us
    ReaderClosed    -> sever; done
```

`should_delegate(P)` = `parents[P].active_direct > MAX_DIRECT_CHILDREN`.

`Covered`/`Uncovered` arrive from a *different* link: when `B`'s `B–C` Parent loop
parses `C`'s `cover_add:[X]` / `cover_del:[X]`, it looks up `parents_by_conn_id[X]`
and sends that loop `Covered{by=C_ident}` / `Uncovered`, and adds/removes `X` in
`B–C`'s `covers` set — so if `B–C` fails, every `X` it covered is released via
`Uncovered`.

### 7.2 Child side (D, the `B–D` link)

`beat(to_ident, conn_id, ack)` below = `loop_tx.send(SendSideChannel(
SideChannelMessage{ gateway_for_actor: to_ident, action: Heartbeat{ from: own_ident,
conn_id, ack } }))` — the existing gateway side channel (§6.2). `own_ident` is
learned from `EstablishOut` (then register `children_by_ident[own_ident] -> self`).

```
# direct — prove liveness to B (single timeout watches B's beats):
beat_timer = interval(INTERVAL); deadline = now + TIMEOUT
loop select:
    beat_timer               -> SendBeat(FromChild{ cover_add: drain, cover_del: drain })
    FromParent{delegate}     -> deadline = now + TIMEOUT
                                if Some(Delegate{connection_id, sibling_ident=C})
                                   and own_ident is addressable:
                                    goto delegated(connection_id, C)
                                # None = plain liveness beat, no-op
    deadline                 -> sever("parent heartbeat timeout"); done
    SideBeat{from,conn_id}   -> feed coverage loop (§7.3)       # delegate-host duty
    ReaderClosed             -> sever; done

# delegated(X, C) — prove liveness via the side channel to C (timeout watches C's acks):
beat_timer = interval(INTERVAL); ack_deadline = now + TIMEOUT
loop select:
    beat_timer               -> beat(C, X, ack=false)
    SideAck{conn_id=X}       -> ack_deadline = now + TIMEOUT
    ack_deadline             -> goto direct    # lost C; beat B directly, B re-decides (7.1)
    SideBeat{from,conn_id}   -> feed coverage loop (§7.3)       # still a delegate host
    ReaderClosed             -> sever; done
```

### 7.3 Coverage loop (C covering connection X on B's behalf)

C's Child task spawns one coverage loop per delegated connection it hosts, on the
first `SideBeat` for that `X`. Each owns a single timeout on `X`'s side-channel
beats; its `cover_add`/`cover_del` feed C's Child link (§7.2), which drains them onto
its next `FromChild` beat to B — that report is what fires B's `Covered`/`Uncovered`.

```
coverage_loop(X, from=D):
    cover_add.push(X)                        # first sight of X -> B learns C covers X
    deadline = now + TIMEOUT
    loop select:
        SideBeat{from=D, conn_id=X} -> deadline = now + TIMEOUT; beat(D, X, ack=true)
        deadline                    -> cover_del.push(X); done   # X lapsed -> fires B's Uncovered(X)
```

If `C`'s `C–B` link fails outright, ctx severs it as usual; the coverage loops die
with the process, and `B` releases everything `C` covered via the `covers`-set path
in §7.1 (no per-loop cleanup needed).

---

## 8. Guards

Delegation of link `B–D` is attempted only when **all** hold, else the link stays
`Direct` (today's behavior):

1. The Parent is the joiner of both the Child and the Delegate,
   which are both spawner. Ensuring the children are side-channel addressable.
   Remember it is possible for the relationship to be reversed (Child as joiner),
   we will not delegate here.
2. `B` knows `D`'s ident and the chosen sibling `C`'s ident (snooped Establish). This will
    progress to be true over time so eventually heartbeats will transition as more info is known.

`inproc`/`unix` links never enter this path (they have no quic heartbeat_task at all).

---

## 9. QUIC config & sending beats

- **Disable periodic QUIC traffic** so a delegated link is truly silent. In
  `load_tls` build a `quinn::TransportConfig` and apply to both `ServerConfig` and
  `ClientConfig`: `keep_alive_interval = None` (no PING keep-alives) and
  `max_idle_timeout = None` (QUIC must not reap an idle delegated connection —
  liveness is now our responsibility, not the transport's). Message-carrying links
  are unaffected; delegated links rely on the sibling fabric.
- **Sending beats reuses the gateway side channel (§6.2).** A heartbeat_task, on its
  timer, enqueues `Command::SendSideChannel(SideChannelMessage)` on `loop_tx`; the
  loop hands it to the existing `Ctx::send_to_gateway`, which reuses the cached
  per-gateway `Preamble::SideChannel` writer and the shared endpoint pool — no new
  connection, preamble, or dialer. This is a small, deliberate ctx seam (one
  `Command` arm + one `SideChannelAction::Heartbeat` dispatch arm, §10); the
  heartbeat *policy* still lives entirely in `quic_heartbeat.rs`, ctx only ferries
  the opaque side-channel message it already knows how to route.

---

## 10. Files & structures touched

| File                | Change                                                                       |
|---------------------|------------------------------------------------------------------------------|
| `quic_heartbeat.rs` | **new** — `HeartbeatShared`, `ParentPool`, `ParentLink`, `ChildLink`, `HeartbeatEvent`, the `heartbeat_task` loops (Parent / Child / coverage), `QuicTransport::deliver_heartbeat`, delegation policy. |
| `quic_transport.rs` | `QuicTransport` holds `Rc<RefCell<HeartbeatShared>>`; `spawn_connection` starts `heartbeat_task` + passes a `dialed` literal (true from `connector_task`, false from the serve-side `listener_task` arms) + shared handle; writer drops self-tick, gains `WriterCtrl`; reader drops timeout, forwards `HeartbeatEvent`s; `deliver_heartbeat` routes side-channel beats via `children_by_ident`; TLS `TransportConfig` (§9). *No new preamble/stream/dialer.* |
| `framing.rs`        | `WireFrame::Heartbeat(Heartbeat)` (was unit) with the `FromChild`/`FromParent` enum + `Delegate`; `SideChannelFrame::Heartbeat`; read/write updates. |
| `connection.rs`     | `SideChannelAction::Heartbeat { from, ack }` (new variant); no per-`Connection` state. |
| `ctx.rs`            | **minimal, deliberate:** `Command::SendSideChannel(msg)` → `send_to_gateway`; `deliver_side_channel` gains a `Heartbeat` arm → `quic.deliver_heartbeat(..)`. Routing/monitor/gateway logic unchanged. |

## 11. Tunables (env)

- `MM_QUIC_MAX_DIRECT_CHILDREN` — threshold above which new children are delegated.
- `MM_QUIC_MAX_COVERAGE_PER_SIBLING` — cap on how many coverage loops one sibling
  hosts (feeds `ParentPool.eligible`).
- `MM_QUIC_DELEGATE_GRACE_MS` — `AwaitingCoverage` grace before reverting.
- Existing `MM_QUIC_HEARTBEAT_INTERVAL_MS` / `_TIMEOUT_MS` reused for both physical
  and side-channel beats.

---

## 12. Open questions for review

2. **Multi-level delegation:** Not needed, we'd need to get up to 2**28 directly connected actors
    before heartbeating would need 3 levels.
4. **Ident ambiguity:** Actor idents are not reused. Actors have at most one parent.
    If we observe B -> C, then a new connection B -> ?, we know this new connection's actor
    will be a sibling of C even though we do not yet know its ident.
