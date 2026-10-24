# Host identity discovery over duplex

A Host frontend address identifies an endpoint. Its concrete service `ProcId`,
such as `service<gVQHWd9h6Km>`, identifies one Host incarnation. A default Host
keeps that ID throughout its lifetime and allocates a fresh ID when restarted
at the same endpoint. Retained actor references continue to name the old
incarnation, even if its replacement creates actors with the same labels.

Clients knowing only the frontend discover its service proc on a private duplex
session:

```rust,ignore
use hyperactor_mesh::host::query_service_proc;

let service = query_service_proc(frontend).await?;
```

The resulting `ProcAddr` contains the concrete instance ID and normalized dial
location. Discovery registers no Gateway peer and changes no caller route.
A shared `service` singleton cannot distinguish physical Hosts or successive
incarnations. Discovering the ID lets the Host allocate it at startup without
requiring launchers to distribute it separately.

## One-shot protocol

The Host supplies its immutable service ID to its frontend before listening.
The advertisement belongs to that listener; other procs with the same label do
not affect the answer. The existing duplex first-message dispatch gains one case:

| First message | Behavior |
| --- | --- |
| `IdentifyService` | Reply once with the advertised concrete ID (or an explicit unavailable/malformed-request error), then close. No peer is registered. |
| `AttachRequest` | Existing Gateway attachment handshake and message serving. |
| Ordinary actor envelope | Existing inbound routing, including delivery of that first envelope. |

The identity response is appended to `AttachWire`, preserving the existing wire
variant ordering. Classification uses the request's type hash, so ordinary actor
payloads need no extra deserialization. No continuing control-message inspection
is added to ordinary messaging.

The querying client sends one request and expects one identity reply. Unexpected
replies, including actor envelopes, are errors: this session has no peer route
through which legitimate actor messages could arrive. Concrete instance IDs are
required; singleton IDs are rejected both when advertised and when received.

The query has a 30-second deadline. Its private client is cancelled on error,
timeout, or caller cancellation, and joined after receiving a reply. The server
waits for reply delivery before closing, bounded by the same deadline and listener
shutdown. Disconnected duplex sessions expire after the configured message-delivery
timeout so abandoned replies cannot retain sessions forever. Connected, idle
attachments retain their existing behavior. Completed connection tasks are reaped;
failed joins are logged both during normal serving and shutdown.

Clients needing Gateway attachment dial a separate session using `serve_via`.
This costs another connection setup but keeps discovery independent of attachment
state, inbound actor traffic, and outbound route publication. The discovery result
is an identity snapshot: a restart between discovery and attachment leaves the old
ID stale, never silently redirects it to the replacement. Combined Identify + Attach
can be added separately if avoiding the extra dial becomes necessary.

## Supplied IDs and Python bootstrap

`attach_to_workers(workers=["tcp://host:4444"], ...)` discovers the current service
incarnation before contacting its HostAgent. An explicit concrete
`service<uid>@channel-url` bypasses discovery, including for non-duplex transports.
Callers may still supply their own concrete IDs; reusing an ID across restarts
remains their responsibility. Slurm and Kubernetes identity allocation are unchanged.

Tests cover frontend-specific and concurrent queries, unchanged ordinary traffic,
identify followed by a separate attachment, invalid/unavailable replies, cancellation,
timeouts, session cleanup, and listener shutdown. Host and Python restart tests
confirm that a replacement is reachable under its new ID while stale references
fail. The generic Gateway attachment draining/migration fix is independent.
