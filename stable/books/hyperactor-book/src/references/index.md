# References

This section documents the address system used throughout hyperactor to identify and communicate with distributed entities.

Hyperactor separates identity from reachability:

- `ProcId`, `ActorId`, and `PortId` are pure identities.
- `ProcAddr`, `ActorAddr`, and `PortAddr` pair those identities with a `Location`.
- `Addr` is the type-erased enum that can hold any of the three address forms.
- `ActorRef<A>`, `PortRef<M>`, and `OncePortRef<M>` wrap addresses with type information for public APIs.

The reference system is:

- **Uniform**: All references follow a shared syntax and structure.
- **Parsable**: References can be round-tripped from strings and manipulated programmatically.
- **Typed**: While `Addr` is typeless and dynamic, typed references like `ActorRef<A>` and `PortRef<M>` allow safe interaction in APIs.
- **Orderable**: References implement a total order, enabling prefix-based routing and sorted maps.

In this section, we'll cover:

- The [syntax](syntax.html) and string format of references
- The core reference types:

- [`ProcId`](proc_id.html)
- [`ActorId`](actor_id.html)
- [`PortId`](port_id.html)
- [`Addr`](reference.html), which unifies all address variants

- [Typed references](typed_refs.html) used in APIs: `ActorRef<A>`, `PortRef<M>`, and `OncePortRef<M>`