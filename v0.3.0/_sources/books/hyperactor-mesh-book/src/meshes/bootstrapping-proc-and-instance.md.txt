##  §1 Proc and Instance (start from a Proc you control)

We begin in a process that can already run hyperactor code and create an instance:

```rust
let proc = Proc::direct(ChannelTransport::Unix.any(), "root".to_string())
    .await
    .unwrap();
let (instance, _handle) = proc.instance("client").unwrap();
```

This gives us a proc + mailbox we can use to tell the mesh to do things. That's two separate things happening, so let's spell them out.

### 1.1 What `Proc::direct(...)` actually does

`Proc::direct(...)` is the "stand up a proc right here and make it reachable on this channel address" helper.

Under the hood it does (cf. `Proc::direct` in the source):

1. **Start listening on a channel**
   It calls `channel::serve(addr)?;` — that binds to the transport/address you gave it (Unix, TCP, whatever) and gives you:
   - the actual bound address, and
   - a receiver for incoming messages.

2. **Name the proc as "direct"**
   It builds a `ProcId::Direct(bound_addr, name)`. That's just how this proc identifies itself to the rest of the world: "I am this channel, and my human-ish name is `root`."

3. **Create a proc with a dial-able forwarder**
   It does `Proc::new(proc_id, DialMailboxRouter::new().into_boxed())`. That "dial mailbox router" is the bit that lets this proc send to other procs later — it knows how to connect out.

4. **Hook the incoming channel into the proc**
   It calls `proc.clone().serve(rx);` so that anything that shows up on that channel gets demuxed into the proc's mailbox muxer.

**Result:** you now have a proc, running in this process, that can be reached on that channel. That's the minimum thing you need to start talking meshes.

**Why we start with this:** it's the simplest shape — "one OS process, one hyperactor proc, reachable on a real address."


## 1.2 What `proc.instance("client")` does

Once we have a proc (from `Proc::direct(...)` or otherwise), we usually want "a handle into that proc" that we can drive from plain Rust code. That's what

```rust
let (instance, handle) = proc.instance("client")?;
```

gives you.

Here's what that actually does, based on the `Proc::instance(...)` implementation.

1. **Allocates a root actor id on that proc**
   The proc keeps a map of root names. Calling `instance("client")` says: "reserve the name `client` on this proc and give me an `ActorId` for it."
   That becomes something like:

   ```text
   ActorId( <this-proc-id>, "client", pid=0 )
   ```

   Now you have an addressable actor identity inside the proc, even though no actor task is running yet.

2. **Builds an `Instance<()>` bound to that id**
   The runtime creates an `Instance<()>` for that actor id. An `Instance`:
   - has a mailbox bound into the proc's muxer
   - knows its `ActorId`
   - can open ports
   - can create children
   - can be turned into a real running actor later

   Because this is `proc.instance(...)` (not `proc.spawn(...)`), it does **not** start an actor loop. The instance is in the "client" shape.

3. **Returns both the instance and a handle**
   You get:
   - the `Instance<()>` so you can bind ports / make children / send
   - an `ActorHandle<()>` in case you want to observe/stop/share it

### Why we care for bootstrapping

In the bootstrapping flow we're describing, we often need "a proc-local, controllable actor endpoint" so we can:

- send messages to actors in the mesh (once hosts/procs are up)
- hand out ports that mesh actors can use to talk back to us

`proc.instance("…")` gives us exactly that: an addressable actor slot on this proc that we control from code.

Important: this instance is created in "client" / detached mode — there's no actor task running for it - so nothing will automatically read and handle incoming messages. If you open ports on this instance, mesh actors can send to them, but **you** (the calling code) are responsible for receiving/draining those messages.

`proc.instance("client")` is the cheapest way to get that: you get an actor-shaped participant inside the proc without having to define a whole actor type up front.

So this pattern:

```rust
let proc = Proc::direct(...).await?;
let (instance, _handle) = proc.instance("client")?;
```

means:

1. stand up a proc that other things can reach, **and**
2. inside it, create the first addressable participant that our bootstrap code can drive.
