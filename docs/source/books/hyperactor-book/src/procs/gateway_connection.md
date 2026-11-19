# Gateway attachment and outbound delivery

## Scope

`Gateway::serve_via` and `serve_via_with_key` attach a gateway to one upstream in
[the core-and-path routing model](stable_locations.md). Before publishing the
upstream, they settle earlier default-egress traffic. Forwarders obtained before
attachment follow the current route on subsequent sends.

The single-upstream rule remains in force: an active or connecting upstream
rejects another attach. Stop and join the current attachment before reattaching.
This operation does not provide live reparenting, change peer-slot replacement
semantics, or alter concrete process identities. Local delivery and downward
attachment paths retain their routing rules.

## Transition

1. Reserve the gateway's upstream slot and establish a private duplex session.
   The current outbound route remains usable during the handshake. An optional
   stable attachment key is passed through unchanged.
2. After acknowledgment, gate default-egress submissions. Wait for sends that
   selected the old sender to finish posting without holding a routing lock
   across arbitrary sender callbacks.
3. Drain the old sender and retired upstream senders. Include stale-session
   retries and clients evicted from the dial cache. Completion means wire
   acknowledgment or explicit delivery failure, not actor-handler completion.
4. Publish the upstream sender and its core/path location together. Release the
   queued prefix through the current route in submission order. Saved Gateway
   and Proc forwarders consult this shared state rather than retaining an old
   transport. Independent channels are outside this guarantee.

New submissions cannot overtake the queued prefix. Replay runs in bounded batches
and has a generation so a later attach can fence a remaining prefix after an
explicit detach. An already selected callback must finish before another replay
worker proceeds. No routing lock spans an asynchronous wait or sender callback.
Earlier messages are never replayed on the new route merely to migrate them;
possibly delivered messages retain normal transport failure semantics.

## Cancellation and inbound traffic

The handshake and drain share a bounded deadline. Failure, timeout, or cancellation
before publication stops the private duplex session, releases the upstream
reservation, and reopens egress through the current live fallback. It never
restores a captured route that has since stopped. Unposted messages retain their
return handles and precede newer gated submissions.

Peer registration precedes acknowledgment, so ordinary inbound envelopes may
arrive during setup. Buffer a bounded ordered prefix and keep receiving during
the drain to allow acknowledgment progress. On success, pass that prefix and the
remaining receiver through the attachment's endpoint ingress. On cancellation,
pass already accepted envelopes through that same ingress. This preserves local
endpoint-owner resolution as well as ordinary actor delivery; accepted envelopes
must not disappear just because setup was cancelled.

A successful attachment transfers session ownership to its serve handle. Stopping
or dropping that handle stops the owned session and removes its route. Existing
independent inbound sessions remain usable. A disconnected acceptor has a bounded
reconnection wait; a connected idle session remains valid.

## Completion and flush

Duplex terminal paths settle all queued and unacknowledged completion sinks,
including application-queue messages. A terminated session must not strand a
mailbox flush. Never-written and potentially delivered messages preserve their
normal failure distinction.

Ordinary flush keeps a finite submission boundary. Fenced attachment drain uses
`MailboxSender::drain`; the dial router tracks aggregate completion across cached
client generations so retries cannot escape the barrier. Gateway flush includes
the base dialer, current upstream and retained retired senders, plus local proc
muxers. It does not recursively flush peer gateways.

## Validation

Regression tests cover saved Gateway and Proc forwarders, selected-but-not-posted
sends, queue order and reentrant sends, cancellation and failed drain, stopped
routes, finite replay prefixes, terminal failure, stale-session retries, and
pending duplex completion on shutdown. Concurrent attachment is rejected while
the first operation drains; explicit stop/join permits reattachment.

The existing core/path routing and endpoint-discovery suites must also pass,
including stable attachment keys, replacement incarnations, terminal failures,
and ownership of outbound attachment endpoints. These guarantees concern default
egress cutover; they do not promise completion of application work or migration
of references when a deployment changes its topology.
