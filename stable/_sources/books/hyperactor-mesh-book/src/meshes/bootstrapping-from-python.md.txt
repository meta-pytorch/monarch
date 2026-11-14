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
def context() -> Context:
    c = _context.get()
    if c is None:
        from monarch._src.actor.proc_mesh import _get_controller_controller

        c = _client_context.get()
        _set_context(c)
        _, c.actor_instance._controller_controller = _get_controller_controller()
    return c
```
`_context` is a context variable that holds the current actor's context. Outside any actor it is empty, so the first call takes the process's root client context from `_client_context`, a `_Lazy` that runs `_init_client_context` once, and then wires the control-plane actor into it.

### 4. `_init_client_context` bootstraps a real local host

Also from monarch/\_src/actor/actor_mesh.py:
```python
hy_host_mesh, hy_proc_mesh, hy_instance = bootstrap_host(
    default_bootstrap_cmd(), via=via, bind_addr=bind_addr
)

ctx = Context._from_instance(hy_instance)
token = _set_context(ctx)
try:
    py_host_mesh = HostMesh._from_rust(hy_host_mesh)
    py_proc_mesh = ProcMesh._from_rust(hy_proc_mesh, py_host_mesh)
    py_proc_mesh._start_root_logging()
finally:
    _reset_context(token)

ctx.actor_instance.proc_mesh = py_proc_mesh
```
`bootstrap_host` (monarch\_hyperactor/src/host\_mesh.rs) does the native work and returns three things:
1. a host mesh for this machine, served on the default transport, so the client process is itself a host;
2. the local proc mesh on that host, whose one proc is this process;
3. the root client actor instance, on that proc.

Python then wraps them. `HostMesh._from_rust` and `ProcMesh._from_rust` wrap meshes that already exist, instead of spawning new ones through `HostMesh.spawn_procs` and `ProcMesh.from_host_mesh`. `_start_root_logging` gives the root client's proc mesh the logging that `from_host_mesh` gives every other proc mesh: a forwarding client and a Python log level that starts at `INFO`. The temporary `_set_context(ctx)` lets these wrappers call `context()` without recursing into the bootstrap.

### 5. Now `this_proc()` / `this_host()` work

After the first `context()` call:
- `context().actor_instance.proc` is the root client's proc, so `this_proc()` returns a real `ProcMesh`;
- that `ProcMesh` carries the bootstrapped `HostMesh`, so `this_host()` is just `this_proc().host_mesh`.

So the original Python snippet:
```python
mesh = this_host().spawn_procs(per_host={"gpus": 2})
counter = mesh.spawn("counter", Counter, 1)
```
works because:
1. `this_host()` → the `HostMesh` that `bootstrap_host` created during `context()` bootstrap
2. `spawn_procs(...)` → asks that host mesh (which is powered by the Rust v1 host mesh) to create procs
3. `mesh.spawn(...)` → now that you have a `ProcMesh`, you can put actors on it
