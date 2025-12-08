# ProcOrDial Router

`ProcOrDial` is a specialized router that bridges the service proc and the `DialMailboxRouter`. It enables bidirectional routing in the `Host` by distinguishing between messages destined for the service proc versus those targeting spawned procs.

## Structure

```rust
struct ProcOrDial {
    proc: Proc,                      // The service proc
    dialer: DialMailboxRouter,       // Router for spawned procs
}
```

## Routing Logic

`ProcOrDial` implements `MailboxSender` with conditional routing based on the destination:

```rust
impl MailboxSender for ProcOrDial {
    fn post_unchecked(&self, envelope: MessageEnvelope, ...) {
        if envelope.dest() == self.proc.proc_id() {
            self.proc.post_unchecked(envelope, ...)  // → service proc
        } else {
            self.dialer.post_unchecked(envelope, ...) // → dialed procs
        }
    }
}
```

When a message arrives:
1. **Check destination**: Is it addressed to the service proc's `ProcId`?
2. **Route to service proc**: If yes, deliver directly to `proc`
3. **Route via dialer**: If no, forward to `DialMailboxRouter` which looks up the target proc and dials its backend address

## Integration with Host

Both the frontend and backend receivers in `Host` are served by `ProcOrDial`:

```rust
// Backend receiver (from spawned procs)
let _backend_handle = host.forwarder().serve(backend_rx);

// Frontend receiver (from external clients)
Some(self.forwarder().serve(self.frontend_rx.take()?))
```

This creates a unified routing layer where all incoming messages flow through the same router, regardless of their source.

## Bidirectional Routing

The `ProcOrDial` router enables complete bidirectional communication:

### Inbound Messages

**From external clients (frontend_rx)**:
- → `ProcOrDial`
- → service proc (if dest matches) OR spawned proc (via `DialMailboxRouter`)

**From spawned procs (backend_rx)**:
- → `ProcOrDial`
- → service proc (if dest matches) OR other spawned procs (via `DialMailboxRouter`)

### Outbound Messages

**From service proc**:
- Uses `DialMailboxRouter` as its forwarder
- → looks up proc by name in address book
- → dials backend address
- → delivers to target proc

This symmetric design allows procs to communicate freely - external clients can reach any proc, spawned procs can reach each other or the service proc, and the service proc can coordinate across all spawned procs.

## See Also

- [Host](host.md) - The container that uses `ProcOrDial`
- [Routers](../mailboxes/routers.md) - Details on `DialMailboxRouter`
- [MailboxSender](../mailboxes/mailbox_sender.md) - The trait `ProcOrDial` implements
