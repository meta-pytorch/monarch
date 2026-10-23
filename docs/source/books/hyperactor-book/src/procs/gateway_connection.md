# Gateway attachment and outbound delivery

## Purpose and scope

Attaching a Gateway changes how its callers reach other procs. A successful
attachment must move previously obtained Gateway and Proc forwarders to the new
outbound route, after earlier outbound traffic reaches a terminal transport
outcome. Failure or cancellation must leave the current live route usable.

Here, **attach** means the production network operation `Gateway::serve_via`.
The `GatewayAttachExt::attach` in `gateway.rs` is a test-only helper, and
`HostMesh::attach` is a separate actor-level operation.

This is a general routing correctness fix. It does not discover service IDs,
change identity allocation, or depend on a discovery protocol. It applies to
callers that already know a concrete `ProcAddr` as well as callers that learn one
through either discovery alternative. It can land before or after discovery.

## Setup and session ownership

Each call to `duplex::dial` creates an independent session. Session A continues
carrying messages while fresh session B performs its attachment handshake. Only
after B's handshake succeeds does this Gateway begin the outbound cutover.

Peer registration precedes the attachment acknowledgment, so ordinary inbound
envelopes may arrive before that acknowledgment. They must not fail setup or be
lost. Buffer a bounded, ordered prefix and deliver each envelope exactly once.
Continue receiving while the old outbound route drains, so a full receive queue
does not prevent transport acknowledgment progress. On cancellation, route any
already accepted prefix instead of discarding transport-acknowledged messages.

The whole attach operation has a finite deadline. Its private duplex sender is
not published until setup and the old-route drain succeed. A scoped cancellation
owner cleans up failures and dropped futures without changing the general
channel's detach-on-drop convention. A successful attach transfers session
ownership to the returned serve handle; stopping or dropping it removes its
routing entry and stops its owned session. Existing independent inbound sessions
remain valid.

## Outbound route cutover

Model outbound egress as one shared routing object. Both the Gateway's fallback
delivery paths and handles returned by `Gateway::forwarder` / `Proc::forwarder`
consult that object on each send. An old public forwarder handle must not contain
a permanently captured underlying sender. The raw transport remains an internal
implementation detail. Independently constructed channels and other Gateways
are not owned or migrated by this Gateway.

The routing object has a stable state and a transition state. A transition is
serialized against other attaches on the same Gateway:

1. Establish the new session B and receive its attachment acknowledgment.
   Session A remains usable while this happens.
2. Gate new ordinary sends through this Gateway's default outbound route. Queue
   their envelopes and return handles. Sends that already selected A must finish
   posting before A's flush watermark is taken.
3. Drain A, including retries of its messages into replacement cached clients.
   This is a transport completion barrier, not an actor-handler completion barrier.
   Capture the drain targets when closing the gate, before waiting for selected
   posts. Include removed outbound routes that still have unsettled traffic;
   stopping a serve must not make those obligations disappear from the barrier.
4. Atomically publish B's outbound route and its advertised via location.
5. Release queued sends onto B without duplication, loss, or bypass through an
   old forwarder handle. Preserve submission order through the gate; concurrent
   callers have no stronger ordering than the existing sender contract.
6. Return the attachment handle.

Queued replay has a unique generation. A subsequent cutover can pause that
worker and capture a finite unposted prefix for the old route without waiting
for continuously arriving traffic to stop. It fences any post already selected
by that worker before posting the prefix. Cancellation restores only the
unposted remainder ahead of newer queued sends and also waits for the prior
selected post, so rollback cannot reorder that prefix.

Never hold a synchronous routing lock while awaiting, or while invoking an
arbitrary sender that can re-enter the Gateway. A plain `flush(); swap()` is
insufficient: the selection-to-post race must be fenced. Do not solve cutover by
waiting for application replies, stopping inbound processing, or recursively
flushing every peer Gateway. Preserve the existing local-proc and explicit peer
routing rules; the cutover affects the default outbound route.

The barrier's terminal outcomes are **wire acknowledgment or explicit delivery
failure**, consistent with `MailboxSender::flush`. It does not claim all earlier
messages succeeded, and a wire acknowledgment does not prove an actor handler has
run. Failed envelopes still follow their existing undeliverable path. Never
replay possibly delivered A traffic on B simply to migrate the connection.

`MailboxClient::flush` currently snapshots submitted counts, and stale-session
reposting can create another cached client before the first client's completion
is counted. A cache snapshot alone is not a sufficient router-wide drain. Track
pending outcomes across all generations, or provide an equivalent proved barrier
that covers replaced and retired clients as well as their retries. Keep ordinary
public flush watermarks finite under concurrent traffic; reserve quiescent drain
semantics for this fenced operation. The implementation uses `MailboxSender::drain`
for the fenced operation, with an aggregate tracker shared by a
DialMailboxRouter's clients. Ordinary `flush` retains its submission watermark.

If setup or drain fails, times out, or is cancelled before publication, leave the
existing routing configuration intact, tear down B, and release queued sends
through the still-current route. If another serve handle was removed meanwhile,
use the live fallback; do not resurrect its dead route. After publication,
cancellation has the semantics of dropping the returned attachment handle.
No cancellation may leave an indefinitely closed send gate or a hidden permanent
peer registration.

Serialize concurrent attaches and define cleanup by unique serve/registration
ownership. Preserve additive inbound reachability and existing out-of-order
serve-handle teardown: old inbound paths can remain valid while new default
outbound sends use B. Stopping B may deliberately restore an older live route.
The success guarantee lasts while B remains the selected live route; it does not
forbid an explicit later detach/fallback. Do not close an old duplex indiscriminately
if it is still an inbound route for existing references.

## Channel completion and lifecycle requirements

Both sides of duplex sessions must settle every queued/unacknowledged outbound
message on terminal exit, including messages still in the application queue.
Do not silently drop tracked completion sinks: that can strand a mailbox flush.
Use the simplex terminal-return behavior as the reference, including the
distinction between never-written messages and messages possibly delivered on an
earlier connection. Avoid broad changes to completion semantics unrelated to
this work, but cover the terminal paths used by attachment and cutover.

Keep acknowledgment flushing and shared mux shutdown intact. Cancelling one
attachment must not stop the peer listener or other sessions. Stopping the
listener must cancel and join active handshake sessions as well as normal serving.

A disconnected duplex acceptor waits for reconnection for at most
`MESSAGE_DELIVERY_TIMEOUT`. This bounds abandoned session and peer-registration
retention after abrupt connection loss. It is distinct from an idle connected
session, which remains valid even when it has not sent its first ordinary envelope.

## Required tests

Use deterministic barriers, oneshots, controlled senders, and paused time where
possible. Sleeps should be failure bounds, not the mechanism that orders races.
Use Unix/local/in-memory transports for most tests and loopback TCP for actual
Host restart/bind behavior. Run relevant existing mux/TLS/shutdown tests too.

| ID | Scenario and assertion |
| --- | --- |
| C1 | Hold A's completion/flush; attach must not publish B or finish until A is released. |
| C2 | Send through a forwarder obtained before attachment and a Proc's saved forwarder after success: both use B. |
| C3 | Pause a sender after selecting A but before posting; cutover must wait for that posting and its terminal completion. |
| C4 | Sends arriving during drain are queued; after success they use B once, preserving gate submission order; none escape through A. |
| C5 | New concurrent senders during queue release do not overtake the gated prefix, lose envelopes, deadlock, or starve cutover indefinitely. |
| C6 | Force stale-session replacement during A's drain; completion of the old client is insufficient while a retried envelope remains pending. |
| C7 | Terminal delivery failure on A settles the barrier and reports the original envelope, without replaying it on B. |
| C8 | Drain error/timeout/caller cancellation releases the gate, cleans B, and restores the current live route; queued sends retain their return handles. |
| C9 | Concurrent attaches serialize; a failed second attach does not remove the first route or another session's peer guard. |
| C10 | Remove A while a selected post is paused or while its completion is pending; B still drains that retired route, and rollback never revives it. Out-of-order handle stops preserve existing fallback semantics. |
| C11 | Retain old inbound refs after B becomes outbound: inbound reachability still works; local delivery and child/peer routes remain independent of the egress gate. |
| C12 | Check canonical destination, next hop, TTL, sequencing headers, and payload survive queuing/cutover unchanged apart from existing routing hops. |
| L1 | Pending duplex sends on both initiator and acceptor settle on cancel, peer close, and send failure; MailboxClient flush cannot hang. |
| L2 | Pending reply is sent before attachment rejection teardown; final acks and mux listener shutdown retain existing guarantees. |
| L3 | Abort before connect, while awaiting a reply, during drain, and after publication: no hidden peer, lost queue, leaked routing entry, or permanently blocked caller. |

The most important tests observe externally meaningful delivery and lifecycle
behavior, not just implementation state. A small amount of state inspection is
appropriate to coordinate deterministic race tests and prove resource cleanup.

## Review findings motivating the design

The relevant paths are Gateway routing, serve handles and peer guards;
MailboxSender, MailboxClient, Buffer, DialMailboxRouter and stale-session repost;
and duplex dial, accept, reconnect, acknowledgment, mux shutdown, and terminal
completion.

* Saved forwarders previously captured a sender and could bypass route changes.
* A route swap without a gate allowed sends selected before the swap to post
  after the old route's flush watermark.
* Router cache snapshots missed replacement clients created by stale-session
  retries. Ordinary flush still needs a finite boundary under concurrent traffic.
* Stopping an old route removed it from routing state while its deliveries could
  remain pending. A failed attach must use the live fallback, not restore a dead
  sender captured earlier.
* Duplex terminal paths could discard completion sinks without settling them,
  leaving a delivery barrier waiting forever.
* Setup could receive ordinary traffic before its acknowledgment, and dropped
  setup futures did not reliably cancel the detached duplex task.

These requirements explain the routing gate, generation-aware replay, separate
fenced drain, and explicit session ownership. The tests exercise their observable
ordering and cleanup guarantees; they do not assert that concurrency bugs have
been ruled out.
