# References

This section documents the reference system used throughout hyperactor to identify and communicate with distributed entities.

References are lightweight, serializable identifiers for **procs**, **actors**, and **ports**. They are the backbone of addressing and routing in the runtime. Whether you're sending a message, spawning an actor, or broadcasting to a group, references are how you name things.

The reference system is:

- **Uniform**: All references follow a shared syntax and structure.
- **Parsable**: References can be round-tripped from strings and manipulated programmatically.
- **Typed**: While the `Reference` enum is typeless and dynamic, typed references like `ActorRef<A>` and `PortRef<M>` allow safe interaction in APIs.
- **Orderable**: References implement a total order, enabling prefix-based routing and sorted maps.

In this section, we'll cover:

- The [syntax](syntax.html) and string format of references
- The core reference types:

- [`ProcId`](proc_id.html)
- [`ActorId`](actor_id.html)
- [`PortId`](port_id.html)
- The [Reference](reference.html), which unifies all reference variants

- [Typed references](typed_refs.html) used in APIs: `ActorRef<A>`, `PortRef<M>`, and `OncePortRef<M>`