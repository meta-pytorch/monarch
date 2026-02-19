# Macros

This section documents the macros provided by hyperactor for actor and message integration.

These macros support a complete message-passing workflow: from defining message enums and generating client APIs, to routing messages and exporting actors for dynamic or remote use.

- [`#[derive(Handler)]`](handler.md) — generate message handling and client traits for actor enums
- [`#[derive(HandleClient)]`](handle_client.md) — implement the generated client trait for `ActorHandle<T>`
- [`#[derive(RefClient)]`](ref_client.md) — implement the generated client trait for `ActorRef<T>`
- [`#[derive(Named)]`](named.md) — give a type a globally unique name and port for routing and reflection
- [`#[export]`](export.md) — make an actor remotely spawnable and routable by registering its type, handlers, and and optionally spawnable from outside the current runtime
- [`#[forward]`](forward.md) — route messages to a user-defined handler trait implementation
- [`behavior!`](behavior.md) - define a façade actor type that exposes only a curated set of messages, allowing you to hand out stable or restricted APIs without tying clients to the concrete actor


<a id="link-hyperactor_macros" href="https://meta-pytorch.org/monarch/rust-api/hyperactor_macros/index.html">**hyperactor_macros**</a><span id="desc-hyperactor_macros"> - See rust API link for all the macros including the ones above. </span>

## Macro Summary

- **`#[derive(Handler)]`**
  Generates handler and client traits for a message enum.

- **`#[derive(HandleClient)]`**
  Implements the client trait for `ActorHandle<T>`.

- **`#[derive(RefClient)]`**
  Implements the client trait for `ActorRef<T>`.

- **`#[derive(Named)]`**
  Registers the type with a globally unique name and port.

- **`#[export]`**
  Makes an actor spawnable and routable via inventory.

- **`#[forward]`**
  Forwards messages to a user-defined handler trait implementation.

- **`behavior!`**
  Defines a façade actor that exposes only a curated set of messages.
