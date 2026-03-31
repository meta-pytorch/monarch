# References

This section documents the reference system used throughout hyperactor to identify and communicate with distributed entities.

References are lightweight, serializable identifiers for **worlds**, **procs**, **actors** **ports**, and **gangs**. They are the backbone of addressing and routing in the runtime. Whether you're sending a message, spawning an actor, or broadcasting to a group, references are how you name things.

The reference system is:

- **Uniform**: All references follow a shared syntax and structure.
- **Parsable**: References can be round-tripped from strings and manipulated programmatically.
- **Typed**: While the `Reference` enum is typeless and dynamic, typed references like `ActorRef<A>` and `PortRef<M>` allow safe interaction in APIs.
- **Orderable**: References implement a total order, enabling prefix-based routing and sorted maps.

In this section, we’ll cover:

- The [syntax](syntax.md) and string format of references
- The core reference types:
  - [`WorldId`](world_id.md)
  - [`ProcId`](proc_id.md)
  - [`ActorId`](actor_id.md)
  - [`PortId`](port_id.md)
  - [`GangId`](gang_id.md)
- The [Reference](reference.md), which unifies all reference variants
  - [Typed references](typed_refs.md) used in APIs: `ActorRef<A>`, `PortRef<M>`, and `OncePortRef<M>`
