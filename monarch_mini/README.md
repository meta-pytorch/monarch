# Monarch Mini

*A miniature Monarch actor system in 16 C functions and 7.2 MB.*

Monarch was designed for _easy_ use. What if we instead designed for simplicity?
Monarch Mini is a prototype to answer this question. It lets us experiment with low-level
design choices and optimizations without having to thread them through Monarch's layers.

The API is inspired by ZeroMQ: define the smallest set of functionality needed for monitored actors.
It is just these 16 C functions:

```c
// ctx_t - context, owns the event loop, one per process
err_t ctx_create(ctx_t* out);
void ctx_destroy(ctx_t ctx);

// actor_t - an actor. The actor tree establishes message routing and acts as gateways.
err_t actor_create(ctx_t ctx, msg_part_t* ident, bool gateway, actor_t* out);
void actor_destroy(actor_t actor);

err_t actor_send(actor_t actor, msg_part_t receiver_ident, const msg_t* msg);

void actor_die(actor_t actor, msg_part_t reason);

err_t actor_serve(actor_t actor, const char* url, const connect_args_t* args);
err_t actor_join(actor_t actor, const char* url, const connect_args_t* args);

err_t actor_monitor(actor_t actor, msg_part_t to_monitor_ident, const msg_t* failure_prefix, uint64_t timeout_for_nonexistence, monitor_handle_t* out);
void monitor_handle_cancel(actor_t actor, monitor_handle_t handle);

err_t poller_create(ctx_t ctx, int* fd_out, poller_t* out);
void poller_destroy(poller_t poller);
err_t poller_subscribe(poller_t poller, size_t index, actor_t actor);
void poller_unsubscribe(poller_t poller, size_t index);

err_t poller_next(poller_t poller, size_t* index_out, msg_part_t* parts, size_t parts_cap, size_t* n_parts_out);

const char* last_error(void);
```

With simple Python bindings:

```python
import asyncio, minimonarch as mm
ba = mm.bytearray

async def main():
    parent = mm.Actor(b"parent")
    a, b = mm.Actor(b"a@[::1]:7001"), mm.Actor(b"b@[::1]:7002")
    a.serve("quic://[::1]:7001", "child"); b.serve("quic://[::1]:7002", "child")
    parent.join("quic://[::1]:7001", "parent", failure=[ba(b"LINKDOWN")])
    parent.join("quic://[::1]:7002", "parent", failure=[ba(b"LINKDOWN")])
    await a.next(); await b.next(); await parent.next(); await parent.next()   # 4 establishment hellos

    parent.send(b"a@[::1]:7001", [ba(b"ping")])                     # route a message parent -> a
    print(await a.next())                                           # [b"ping"]

    a.monitor(b"b@[::1]:7002", failure=[ba(b"DOWN")])               # sibling monitor: a has no link to b
    b.die(b"bye")
    # b's death reaches both actors linked to it:
    print(await parent.next())   # parent link failed: [b"LINKDOWN", b"b@[::1]:7002", b"bye"]
    print(await a.next())        # a's monitor fired:   [b"DOWN", b"b@[::1]:7002", b"actor died"]

asyncio.run(main())
```

Or Rust bindings:

```rust
use monarch_mini_rs::{Context, Part, Role};

#[tokio::main]
async fn main() -> Result<(), monarch_mini_rs::Error> {
    let ctx = Context::new()?;
    let parent = ctx.actor(Some(b"parent"), true)?;
    let a = ctx.actor(Some(b"a@[::1]:7001"), true)?;
    let b = ctx.actor(Some(b"b@[::1]:7002"), true)?;

    a.serve("quic://[::1]:7001", Role::Child, None, &[], &[])?;
    b.serve("quic://[::1]:7002", Role::Child, None, &[], &[])?;
    parent.join("quic://[::1]:7001", Role::Parent, None, &[], &[b"LINKDOWN"])?;
    parent.join("quic://[::1]:7002", Role::Parent, None, &[], &[b"LINKDOWN"])?;
    a.recv().await?; b.recv().await?; parent.recv().await?; parent.recv().await?;  // 4 establishment hellos

    parent.send(b"a@[::1]:7001", vec![Part::copy_from(b"ping")])?;  // route a message parent -> a
    println!("{:?}", a.recv().await?);                              // [b"ping"]

    a.monitor(b"b@[::1]:7002", &[b"DOWN"], 0)?;                      // sibling monitor: a has no link to b
    b.die(b"bye");
    // b's death reaches both actors linked to it:
    println!("{:?}", parent.recv().await?);  // parent link failed: [b"LINKDOWN", b"b@[::1]:7002", b"bye"]
    println!("{:?}", a.recv().await?);        // a's monitor fired:   [b"DOWN", b"b@[::1]:7002", b"actor died"]
    Ok(())
}
```

## Conceptual differences from Monarch

- Serve/join links represent physical parent/child relationships; children do not outlast parents.
- Despite manual parent/child links, any actor can message any other actor in the same tree.
- The connection links are always represented by parent/child actor pairs.

## Built on what we learned building Monarch

- Careful separation of user and message-system event loops.
- Immediate generation of actor ids.
- Join/serve direction is independent of parent/child and of who names whom.
- Gateways providing addressable entry points to a single machine.
- Monitoring as a key primitive for building supervision relationships.
- Offloading of heartbeating to enable scale-out of gateways.

## New experiments to make things more minimal

- No opinions about message serialization: messages are multi-part, with each part just being bytes.
- No opinion about the consumer's event loop: we just provide messages received, and a file descriptor to wait on.
- No ports: instead this is accomplished with headers.
- No replies: accomplished with headers. Things that reply normally take a prefix — the set of headers to prepend before sending the response. For instance, when a monitor fires it prepends the message with a prefix the user provided when the monitor was created.
- quic for scalable messaging: a root can have thousands of idle connections, and gateways can directly address for cheap routing.
- The connection between parent and child is the way faults are detected. Heartbeats are just the way the quic transport type implements connections.
- Scalable large fan-out implemented at the individual "sea-of-actors" layer. Tested up to 131k actors directly connected to root as a child.
  - Lets actors fail individually without having to make sure they do not interrupt the liveness of other directly connected actors.
  - Uses quic, and large fan-in actors delegate heartbeating to siblings at the connection transport level to implement it.
- Passes large messages between processes using shared-memory allocations to avoid copies.
- Monitors are implemented as subscriptions to the death of an actor, or the death of its gateway, so non-gateway actor deaths are reported immediately.

## Performance

Simple design means baseline linear performance is pretty good: with 131k direct quic connections to 4096 unique CPU machines, the root can send a message to everyone and get a response in 6 seconds from Python.
