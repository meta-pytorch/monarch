# §5 Bootstrapping from Python

So far we described the Rust side: there is a host, the host has a `HostAgent`, and we send `CreateOrUpdate<ProcSpec>` etc. That's the control plane.

Most users won't do that by hand — they'll write Python like this:

```python
import asyncio

from monarch._src.actor.host_mesh import this_host
from monarch._src.actor.proc_mesh import ProcMesh  # Optional, for typing
from monarch._src.actor.actor import Actor
from monarch._src.actor.endpoint import endpoint

class Counter(Actor):
  ...

def train_with_mesh():

    mesh = this_host().spawn_procs(per_host={"gpus": 2})
    counter = mesh.spawn("counter", Counter, 1)

   ...

```

## Getting a host in Python (`this_host()` → `this_proc()` → `context()`)

When you write code like:

```python
from monarch._src.actor.host_mesh import this_host

host = this_host()
```

there's a bootstrap under it. Here's what actually happens.

### 1. `this_host()` reads the host mesh off the current proc.

From monarch/\_src/actor/host_mesh.py:
```python
def this_host() -> "HostMesh":
    """
    The current machine.

    This is just shorthand for looking it up via the context
    """
    return this_proc().host_mesh
```
So: `this_host()` doesn't build a host. That means we have to look at `this_proc()`.

## 2. `this_proc()` pulls the proc mesh off the current context

From the same file:
```python
def this_proc() -> "ProcMesh":
    """
    The current singleton process that this specific actor is
    running on
    """
    return context().actor_instance.proc
```
So now we're down to the real root: `context()`. Everything hangs off of that.

### 3. `context()` — create (once) or return (later) the runtime context

From monarch/\_src/actor/actor_mesh.py:
```python
_context: contextvars.ContextVar[Context] = contextvars.ContextVar(
    "monarch.actor_mesh._context"
)
```
and:
```python
def context() -> Context:
    c = _context.get(None)
    if c is None:
        c = Context._root_client_context() # (1) ask Rust for a bare context
        _context.set(c)

        from monarch._src.actor.host_mesh import create_local_host_mesh
        from monarch._src.actor.proc_mesh import _get_controller_controller

        c.actor_instance.proc_mesh = _root_proc_mesh.get() # (2) give it a proc mesh
        _this_host_for_fake_in_process_host.get() # (3) make sure a host exists
        c.actor_instance._controller_controller = _get_controller_controller()[1]  # (4) wire control plane
    return c
```
So the logic is:
  1. First call: no context yet → build one.
  2. Later calls: return the same one from the ContextVar.

The interesting part is step (1) above — `Context._root_client_context()` — because that's where Python hands off to Rust.

### 4. What `Context._root_client_context()` does (Rust side)

The Rust in context.rs:
```rust
#[staticmethod]
fn _root_client_context(py: Python<'_>) -> PyResult<PyContext> {
    let _guard = runtime::get_tokio_runtime().enter();
    let instance: PyInstance = global_root_client().into();
    Ok(PyContext {
        instance: instance.into_pyobject(py)?.into(),
        rank: Extent::unity().point_of_rank(0).unwrap(),
    })
}
```
What matters is the call to `global_root_client()`. That function, on the Rust side, basically does this:
```rust
pub fn global_root_client() -> &'static Instance<()> {
    static GLOBAL_INSTANCE: OnceLock<(Instance<()>, ActorHandle<()>)> = OnceLock::new();
    &GLOBAL_INSTANCE.get_or_init(|| {
        // 1. Make a direct proc for the client to live in.
        let client_proc = Proc::direct(
            ChannelAddr::any(default_transport()),
            "mesh_root_client_proc".into(),
        ).unwrap();

        // 2. Start an actual actor instance in that proc, called "client".
        let (client, handle) = client_proc.instance("client").expect("root instance create");

        (client, handle)
    }).0
}
```
So when `_root_client_context()` runs, it is really:
1. Ensuring there is a single, global, direct-addressed proc called "`mesh_root_client_proc`".
2. Spawning a "client" actor in it.
3. Wrapping that actor as a Python `PyContext` and giving it rank 0.

Notice what it doesn't do: it does not attach a proc mesh or a host mesh. Those Python-only fields are still `None` at this point.

### 5. Python fills in the missing pieces

That's why, back in Python, right after calling the Rust function, we do three extra things:
```python
c.actor_instance.proc_mesh = _root_proc_mesh.get()
_this_host_for_fake_in_process_host.get()
c.actor_instance._controller_controller = _get_controller_controller()[1]
```

Here's what each does:

1.  `_root_proc_mesh: _Lazy["ProcMesh"] = _Lazy(_init_root_proc_mesh)`
Defined as:
```python
def _init_root_proc_mesh() -> "ProcMesh":
    from monarch._src.actor.host_mesh import fake_in_process_host

    return fake_in_process_host()._spawn_nonblocking(
        name="root_client_proc_mesh",
        per_host=Extent([], []),
        setup=None,
        _attach_controller_controller=False,
    )
```
So this:
- makes a fake in-process host,
- spawns one proc on it,
- that proc mesh is stored as `context().actor_instance.proc_mesh`. Later, when you call `this_proc()` (which reads `context().actor_instance.proc`), you're really just getting a slice of that stored `proc_mesh`.

2.  `_this_host_for_fake_in_process_host: _Lazy["HostMesh"] = _Lazy(...)`

This is the lazy "make me a host mesh" step. It spins up the local v1 host mesh using the same Rust path as the canonical bootstrap.

3. `_get_controller_controller()[1]`
And we stash the control-plane actor into `c.actor_instance._controller_controller` so later spawns have somewhere to go. We aren't going to unpack that here.

6. Now `this_proc()` / `this_host()` work

After that first `context()` run:
- `context().actor_instance.proc` is set → so `this_proc()` returns a real `ProcMesh`
- after the first `context()` run, the proc mesh you get (`context().actor_instance.proc`) was created from a host mesh, so it already carries a `host_mesh` reference — that’s why `this_host()` can just do `this_proc().host_mesh`.


So the original Python snippet:
```python
mesh = this_host().spawn_procs(per_host={"gpus": 2})
counter = mesh.spawn("counter", Counter, 1)
```
works because:
1. `this_host()` → got a `HostMesh` that Python created during `context()` bootstrap
2. `spawn_procs(...)` → asks that host mesh (which is powered by the Rust v1 host mesh) to create procs
3. `mesh.spawn(...)` → now that you have a `ProcMesh`, you can put actors on it
