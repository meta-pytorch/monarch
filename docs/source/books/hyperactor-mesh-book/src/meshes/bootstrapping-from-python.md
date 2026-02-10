# §5 Bootstrapping from Python

So far we described the Rust side: there is a host, the host has a `HostMeshAgent`, and we send `CreateOrUpdate<ProcSpec>` etc. That's the control plane.

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

From monarch/\_src/actor/v1/host_mesh.py:
```python
def this_host() -> "HostMesh":
    """
    The current machine.

    This is just shorthand for looking it up via the context
    """
    hm = this_proc().host_mesh
    assert isinstance(hm, HostMesh), f"expected v1 HostMesh, got v0 {hm}"
    return hm
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
    pm = context().actor_instance.proc
    assert isinstance(pm, ProcMesh), f"expected v1 ProcMesh, got {pm}"
    return pm
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
    c = _context.get()
    if c is None:
        from monarch._src.actor.proc_mesh import _get_controller_controller

        c = _client_context.get()  # (1) build the client context (Rust bootstrap)
        _set_context(c)
        _, c.actor_instance._controller_controller = _get_controller_controller()  # (2) wire control plane
    return c
```
So the logic is:
  1. First call: no context yet → build one via `_client_context.get()`.
  2. Later calls: return the same one from the ContextVar.

The interesting part is step (1) — `_client_context` is a `_Lazy` that calls `_init_client_context()`, which is where Python hands off to Rust.

### 4. What `_init_client_context()` does

From monarch/\_src/actor/actor_mesh.py:
```python
def _init_client_context() -> Context:
    from monarch._rust_bindings.monarch_hyperactor.host_mesh import bootstrap_host
    from monarch._src.actor.host_mesh import _bootstrap_cmd, HostMesh
    from monarch._src.actor.proc_mesh import ProcMesh

    hy_host_mesh, hy_proc_mesh, hy_instance = bootstrap_host(
        _bootstrap_cmd()
    ).block_on()

    ctx = Context._from_instance(cast(Instance, hy_instance))
    token = _set_context(ctx)
    try:
        py_host_mesh = HostMesh._from_rust(hy_host_mesh)
        py_proc_mesh = ProcMesh._from_rust(hy_proc_mesh, py_host_mesh)
    finally:
        _reset_context(token)

    ctx.actor_instance.proc_mesh = py_proc_mesh
    return ctx
```

The `bootstrap_host()` Rust function does everything in one call. On the Rust side, it is essentially:
1. Ensuring there is a single, global, direct-addressed proc for the client to live in.
2. Registering that proc in the global router so both direct and ranked messages can reach it.
3. Spawning a "client" actor instance in that proc.
4. Creating a local host mesh using `ProcessAllocator` (real OS processes) and a proc mesh on that host.
5. Returning all three to Python: `(hy_host_mesh, hy_proc_mesh, hy_instance)`.

Python then wraps these Rust objects in their Python counterparts (`HostMesh._from_rust`, `ProcMesh._from_rust`) and stores the proc mesh as `ctx.actor_instance.proc_mesh`.

### 5. Python fills in the missing pieces

After `_init_client_context()` runs, one more thing happens in `context()`:

```python
_, c.actor_instance._controller_controller = _get_controller_controller()
```

This stashes the control-plane actor into `c.actor_instance._controller_controller` so later spawns have somewhere to go. We aren't going to unpack that here.

### 6. Now `this_proc()` / `this_host()` work

After that first `context()` run:
- `context().actor_instance.proc` is set → so `this_proc()` returns a real `ProcMesh`
- the proc mesh was created from a host mesh, so it already carries a `host_mesh` reference — that's why `this_host()` can just do `this_proc().host_mesh`.


So the original Python snippet:
```python
mesh = this_host().spawn_procs(per_host={"gpus": 2})
counter = mesh.spawn("counter", Counter, 1)
```
works because:
1. `this_host()` → got a `HostMesh` that Python created during `context()` bootstrap
2. `spawn_procs(...)` → asks that host mesh (which is powered by the Rust v1 host mesh) to create procs
3. `mesh.spawn(...)` → now that you have a `ProcMesh`, you can put actors on it

# Python `create_local_host_mesh` and Rust bootstrap

This note shows that calling `create_local_host_mesh(...)` in Python ends up driving the same Rust v1 host/agent/bootstrap path we described for the canonical Rust example.

## 1. Python entry point

```python
def create_local_host_mesh(
    extent: Optional[Extent] = None, env: Optional[Dict[str, str]] = None
) -> "HostMesh":
    cmd, args, bootstrap_env = _get_bootstrap_args()
    if env is not None:
        bootstrap_env.update(env)

    return HostMesh.allocate_nonblocking(
        "local_host",
        extent if extent is not None else Extent([], []),
        ProcessAllocator(cmd, args, bootstrap_env),
        bootstrap_cmd=_bootstrap_cmd(),
    )
```

- `_get_bootstrap_args()` = "what command/env do we use to start a hyperactor proc?"
- we wrap that in a `ProcessAllocator(...)`
- we tell the Rust side to `allocate_nonblocking(...)` a v1 HostMesh using that allocator.

## 2. Hand-off to Rust

The Python classmethod does:

```python
await HyHostMesh.allocate_nonblocking(
    context().actor_instance._as_rust(),
    await alloc._hy_alloc,
    name,
    bootstrap_cmd,
)
```

It passes the allocation and (optionally) the bootstrap command straight to the Rust v1 `HostMesh::allocate(...)`, via the `PyHostMesh::allocate_nonblocking(...)` binding. That's the same Rust entry point the canonical bootstrap uses — just exposed to Python.

```rust
#[pymethods]
impl PyHostMesh {
    #[classmethod]
    fn allocate_nonblocking(
        _cls: &Bound<'_, PyType>,
        instance: &PyInstance,
        alloc: &mut PyAlloc,
        name: String,
        bootstrap_params: Option<PyBootstrapCommand>,
    ) -> PyResult<PyPythonTask> {
        let bootstrap_params =
            bootstrap_params.map_or_else(|| alloc.bootstrap_command.clone(), |b| Some(b.to_rust()));
        let alloc = match alloc.take() {
            Some(alloc) => alloc,
            None => {
                return Err(PyException::new_err(
                    "Alloc object already used".to_string(),
                ));
            }
        };
        let instance = instance.clone();
        PyPythonTask::new(async move {
            let mesh = instance_dispatch!(instance, async move |cx_instance| {
                HostMesh::allocate(cx_instance, alloc, &name, bootstrap_params).await
            })
            .map_err(|err| PyException::new_err(err.to_string()))?;
            Ok(Self::new_owned(mesh))
        })
    }
}
```
(This returns a Python task because all v1 Python bindings wrap Rust async in a small bridge. See Appendix: **Python async bridge (pytokio)**.)

`HostMesh::allocate(...)` is the entry point that stands up the host, creates its system proc, spawns the `HostMeshAgent`, and makes it reachable — it's the same path we used in the Rust canonical example.
