# Routers

While a muxer dispatches messages to exact `ActorId` matches, a **router** generalizes this by routing messages to the *nearest matching prefix*. This enables hierarchical, prefix-based routing across clusters of actors—spanning local and remote processes.

Routers extend the ideas of the muxer with:
- Longest-prefix matching on structured `Reference` identifiers
- Dynamic routing to local and remote mailboxes
- Optional serialization and remote connection via `DialMailboxRouter`
- Fallback logic via `WeakMailboxRouter`

This page introduces:
- `MailboxRouter`: prefix-routing within a shared process
- `DialMailboxRouter`: remote routing with connection management
- `WeakMailboxRouter`: downgradeable reference for ephemeral routing

To support routing, hyperactor defines a universal reference type for hierarchical identifiers:
```rust
pub enum Reference {
    World(WorldId),
    Proc(ProcId),
    Actor(ActorId),
    Port(PortId),
}
```
A `Reference` encodes a path through the logical structure of the system-spanning from broad scopes like worlds and procs to fine-grained targets like actors or ports. It has a concrete string syntax (e.g., `world[0].actor[42]`) and can be parsed from user input or configuration via `FromStr`.

## Total Ordering and Prefix Routing

`Reference` implements a total order via a lexicographic comparison of its internal components:
```rust
impl PartialOrd for Reference {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Reference {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            // Ranked procs precede direct procs:
            self.world_id(),
            self.rank(),
            self.proc_id().and_then(ProcId::as_direct),
            self.actor_name(),
            self.pid(),
            self.port(),
        )
            .cmp(&(
                other.world_id(),
                other.rank(),
                other.proc_id().and_then(ProcId::as_direct),
                other.actor_name(),
                other.pid(),
                other.port(),
            ))
    }
}
```
This means that references are ordered by their position in the system hierarchy-starting with world, then rank (within the world), then direct proc address and name (for procs without ranks), then actor name, PID, and finally port. For example:
```text
world[0] < world[0].actor["trainer"] < world[0].actor["trainer"][5]
```

For Direct addressing (see [ProcId](../references/proc_id.md)), references use channel addresses instead of ranks:
```text
tcp:127.0.0.1:8080,service < tcp:127.0.0.1:8080,service,trainer[0]
```

Semantically, a `Reference` like `Proc(p)` is considered a prefix of any `Actor` or `Port` reference that shares the same proc (either by matching world and rank, or by matching channel address and proc name).

Because this order is total and consistent with prefix semantics, it enables efficient prefix-based routing using `BTreeMap<Reference, ...>`. When routing a message, the destination `ActorId` is converted into a `Reference`, and the router performs a longest-prefix match by locating the nearest entry that is a prefix of the destination.

### `MailboxRouter`

With this structure in place, we can now define the core router:
```rust
pub struct MailboxRouter {
    entries: Arc<RwLock<BTreeMap<Reference, Arc<dyn MailboxSender + Send + Sync>>>>,
}
```

A `MailboxRouter` maintains a thread-safe mapping from `Reference` prefixes to corresponding `MailboxSender`s. These entries form the routing table: each entry declares that messages targeting a reference in that subtree should be forwarded to the given sender.

When a message is routed, its destination `ActorId` is converted into a `Reference`. The router performs a longest-prefix match against the table to find the nearest registered handler.

### Binding and Downgrading

To register a new routing entry, the router provides a `bind` method:

```rust
impl MailboxRouter {
     pub fn bind(&self, dest: Reference, sender: impl MailboxSender + 'static) {
        let mut w = self.entries.write().unwrap();
        w.insert(dest, Arc::new(sender));
    }
}
```
Each call to `bind` inserts a new `Reference` → `MailboxSender` entry into the routing table. These entries act as prefixes: once inserted, they serve as candidates during longest-prefix matching at message delivery time.

In some cases, you may want to share or store a weak reference to the router-especially when integrating with structures that should not keep the routing table alive indefinitely. To support this, `MailboxRouter` can be downgraded to a `WeakMailboxRouter`:
```rust
impl MailboxRouter {
    pub fn downgrade(&self) -> WeakMailboxRouter {
        WeakMailboxRouter(Arc::downgrade(&self.entries))
    }
}
```
This enables ephemeral or optional routing logic—useful for circular dependencies, test scaffolding, or weakly held topology graphs.

The `WeakMailboxRouter` is a lightweight wrapper around a weak reference to the router’s internal state:
```rust
pub struct WeakMailboxRouter(
    Weak<RwLock<BTreeMap<Reference, Arc<dyn MailboxSender + Send + Sync>>>>,
);
```
A `WeakMailboxRouter` can be upgraded back into a strong `MailboxRouter` (if the underlying state is still alive) or used to fail gracefully when routing is unavailable.

### Routing via `MailboxSender`

To participate in message delivery, `MailboxRouter` implements the `MailboxSender` trait:
```rust
impl MailboxSender for MailboxRouter {
    fn post(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let sender = {
            let actor_id = envelope.dest().actor_id();
            match self
                .entries
                .read()
                .unwrap()
                .lower_bound(Excluded(&actor_id.clone().into()))
                .prev()
            {
                None => None,
                Some((key, sender)) if key.is_prefix_of(&actor_id.clone().into()) => {
                    Some(sender.clone())
                }
                Some(_) => None,
            }
        };

        match sender {
            None => envelope.undeliverable(
                DeliveryError::Unroutable(
                    "no destination found for actor in routing table".to_string(),
                ),
                return_handle,
            ),
            Some(sender) => sender.post(envelope, return_handle),
        }
    }
}
```
This implementation performs a longest-prefix match using the total order on `Reference`:
1. It converts the destination `ActorId` into a `Reference`.
2. It performs a descending prefix search using:
```rust
    entries.lower_bound(Excluded(&reference)).prev()
```
This locates the greatest key in the routing table that is strictly less than the destination.

3. It checks whether that key is a semantic prefix of the destination (via `is_prefix_of`).
4. If a match is found, the message is forwarded to the corresponding `MailboxSender`.
5. If no match is found, the message is marked as undeliverable, and returned using the provided `return_handle`.

### `WeakMailboxRouter`

A `WeakMailboxRouter` is a downgradeable, non-owning reference to a router's internal state. It allows optional or ephemeral routing participation-for example, when holding a fallback route without keeping the full routing table alive.
```rust
pub struct WeakMailboxRouter(
    Weak<RwLock<BTreeMap<Reference, Arc<dyn MailboxSender + Send + Sync>>>>,
);
```
To integrate into the routing system, `WeakMailboxRouter` also implements `MailboxSender`:
```rust
impl MailboxSender for WeakMailboxRouter {
    fn post(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        match self.upgrade() {
            Some(router) => router.post(envelope, return_handle),
            None => envelope.undeliverable(
                DeliveryError::BrokenLink("failed to upgrade WeakMailboxRouter".to_string()),
                return_handle,
            ),
        }
    }
}
```
If the router has already been dropped, `post` fails gracefully by returning the message to the sender with a `BrokenLink` error. This makes `WeakMailboxRouter` useful in dynamic topologies or teardown-sensitive control paths, where a full routing table may not be guaranteed to exist at the time of message delivery.

### `DialMailboxRouter`: Remote and Serializable Routing

While `MailboxRouter` supports prefix-based routing, it relies on explicitly registered `MailboxSender`s. In contrast, `DialMailboxRouter` enables **remote routing** through a dynamic address book and connection cache. It can forward messages to remote actors by establishing outbound connections on demand.
```rust
pub struct DialMailboxRouter {
    address_book: Arc<RwLock<BTreeMap<Reference, ChannelAddr>>>,
    sender_cache: Arc<DashMap<ChannelAddr, Arc<MailboxClient>>>,
    default: BoxedMailboxSender,
}
```
#### Address Book

The `address_book` maps `Reference` prefixes to `ChannelAddr`s representing remote destinations.

#### Sender Cache

The `sender_cache` holds active `MailboxClient` connections keyed by address. When a message arrives, the router looks up the target in the address book and either reuses an existing sender or dials a new one.

#### Default Route

If no matching reference is found, the message is forwarded to a `default` sender—useful as a catch-all route or failover handler.

This structure enables adaptive, connection-aware routing across distributed systems. Next, we’ll walk through constructing and populating a `DialMailboxRouter` using `new()` and `bind()`.

### Managing Routes: `bind` and `unbind`

To populate the router, use `bind` to associate a `Reference` with a `ChannelAddr`. This replaces any existing mapping for the same reference and evicts any cached sender tied to the old address:
```rust
router.bind(reference, remote_addr);
```
To remove entries, use `unbind`. It removes all mappings with the given prefix-effectively deleting a subtree of the address book. Corresponding cached senders are also evicted to prevent reuse of stale connections:
```rust
router.unbind(&reference_prefix);
```
This allows the router to adapt dynamically to process exits, topology changes, or application-level reconfiguration. The use of `is_prefix_of` during unbinding ensures that hierarchical references can be removed in bulk-e.g., removing a `Proc`-level entry will also remove all associated `Actor` routes.

### Lookup and Dialing

Once the router has been populated using `bind`, message delivery proceeds in two phases: **lookup** followed by **dialing**, if needed.

#### Address Lookup

When a message arrives, the router first attempts to locate a destination using `lookup_addr`. This method:

- Converts the message’s `ActorId` into a `Reference`
- Performs a longest-prefix search using `lower_bound(...).prev()` on the address book
- Applies `is_prefix_of` to confirm that the matched reference is semantically valid

This allows the router to resolve addresses at varying levels of granularity-e.g., by world, process, or actor.

#### Dialing

If a matching address is found and no cached connection exists, the router attempts to establish one using `channel::dial`. The resulting connection is wrapped in a `MailboxClient` and inserted into the sender cache for future use.

If the address is already cached, the router reuses the existing sender to avoid redundant connections.

The result of this lookup-and-dial process is an `Arc<MailboxClient>`—a runtime-capable `MailboxSender` for remote delivery.

We'll now see how this machinery is tied together in the `MailboxSender` implementation for `DialMailboxRouter`.

### Integration with `MailboxSender`

`DialMailboxRouter` implements the `MailboxSender` trait, enabling it to forward messages to remote actors by resolving and caching connections dynamically:

```rust
impl MailboxSender for DialMailboxRouter {
    fn post(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let Some(addr) = self.lookup_addr(envelope.dest().actor_id()) else {
            self.default.post(envelope, return_handle);
            return;
        };

        match self.dial(&addr, envelope.dest().actor_id()) {
            Err(err) => envelope.undeliverable(
                DeliveryError::Unroutable(format!("cannot dial destination: {err}")),
                return_handle,
            ),
            Ok(sender) => sender.post(envelope, return_handle),
        }
    }
}
```
Here’s what happens step by step:
1. Address lookup:
 - The destination `ActorId` is converted into a `Reference`.
 - The router searches for the nearest matching prefix in the address book.
 - If no match is found, the message is forwarded to the configured `default` sender.
2. Connection resolution:
 - If an address is found, the router attempts to `dial` or reuse a cached `MailboxClient`.
 - On error (e.g., failed dial), the message is returned to the sender with a `DeliveryError::Unroutable`.
3. Message forwarding:
 - If dialing succeeds, the resulting `sender` is used to post the message.

`DialMailboxRouter` performs prefix-based routing by resolving addresses at runtime and forwarding messages over dialed or cached connections, with fallback to a default sender.
