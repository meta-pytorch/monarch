# monarch.actor

The `monarch.actor` module provides the actor-based programming model for distributed computation. See [Getting Started](../generated/examples/getting_started.html) for an overview.

# Creating Actors

Actors are created on multidimensional meshes of processes that
are launched across hosts. HostMesh represents a mesh of hosts. ProcMesh is a mesh of processes.

*class*monarch.actor.HostMesh(*hy_host_mesh*, *region*, *stream_logs*, *is_fake_in_process*, *code_sync_proc_mesh*)[[source]](../_modules/monarch/_src/actor/host_mesh.html#HostMesh)

Bases: `MeshTrait`

HostMesh represents a collection of compute hosts that can be used to spawn
processes and actors.

Can be used as an async context manager, which will shut down the hosts on
exit:
```
host_mesh = job.state().hosts
async with host_mesh:

> # spawn proc meshes, actor meshes, etc.

# shutdown() is called automatically on exit.
```

If you don't want to shutdown the hosts, you don't need to use it as a
context manager.

spawn_procs(*per_host=None*, *bootstrap=None*, *name=None*, *proc_bind=None*, *bootstrap_command=None*)[[source]](../_modules/monarch/_src/actor/host_mesh.html#HostMesh.spawn_procs)

Spawn a ProcMesh onto this host mesh.

Parameters:

- **per_host** ([*Dict*](https://docs.python.org/3/library/typing.html#typing.Dict)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*,*[*int*](https://docs.python.org/3/library/functions.html#int)*]**|**None*) - shape of procs per host, e.g. `{"gpus": 4}`.
- **bootstrap** ([*Callable*](https://docs.python.org/3/library/typing.html#typing.Callable)*[**[**]**,**None**]**|*[*Callable*](https://docs.python.org/3/library/typing.html#typing.Callable)*[**[**]**,*[*Awaitable*](https://docs.python.org/3/library/typing.html#typing.Awaitable)*[**None**]**]**|**None*) - optional setup callable run on each proc.
- **name** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)*|**None*) - optional name for the proc mesh.
- **proc_bind** ([*list*](https://docs.python.org/3/library/stdtypes.html#list)*[*[*dict*](https://docs.python.org/3/library/stdtypes.html#dict)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*,*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]**]**|**None*) - optional per-process CPU/NUMA binding config.
Length must equal `math.prod(per_host.values())`.
Each dict maps binding keys (`cpunodebind`,
`membind`, `physcpubind`, `cpus`) to values.
- **bootstrap_command** (*BootstrapCommand**|*[*Callable*](https://docs.python.org/3/library/typing.html#typing.Callable)*[**[**Point**]**,**BootstrapCommand**]**|**None*) - optional BootstrapCommand or callable that
returns a BootstrapCommand for each coordinate. The callable
receives a `Point` (combined coordinate across host and
per_host dimensions). This allows full customization of the
bootstrap command per coordinate.

*property*region*: Region*

*property*stream_logs*: [bool](https://docs.python.org/3/library/functions.html#bool)*

with_python_executable(*python_executable*)[[source]](../_modules/monarch/_src/actor/host_mesh.html#HostMesh.with_python_executable)

Return a new HostMesh that will use the given Python executable when
spawning procs. Procs spawned from this mesh will also inherit this
Python executable when they call `this_host().spawn_procs(...)`.

Parameters:

**python_executable** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) - Path to the Python executable to use.

Returns:

A new HostMesh configured to use the specified Python executable.

Return type:

*HostMesh*

*property*is_fake_in_process*: [bool](https://docs.python.org/3/library/functions.html#bool)*

shutdown()[[source]](../_modules/monarch/_src/actor/host_mesh.html#HostMesh.shutdown)

Shutdown the host mesh and all of its processes. It will throw an exception
if this host mesh is a *reference* rather than *owned*, which can happen
if this HostMesh object was received from a remote actor or if it was
produced by slicing.
After shutting down, the hosts in this mesh will be unusable, and no new
HostMeshes will be able to connect to them.
If you want to stop everything on the host but keep them available for
new clients, use stop() instead.

This is run automatically on __aexit__ when used as an async context manager.

Returns:

A future that completes when the host mesh has been shut down.

Return type:

Future[None]

stop()[[source]](../_modules/monarch/_src/actor/host_mesh.html#HostMesh.stop)

Stop the host mesh, releasing all resources but keeping worker
processes alive for reconnection. A new HostMesh can be created that
points to the same hosts.

Like shutdown, this throws if the host mesh is a reference
rather than owned.

Returns:

A future that completes when the host mesh has been stopped.

Return type:

Future[None]

*async*sync_workspace(*workspace*, *conda=False*, *auto_reload=False*)[[source]](../_modules/monarch/_src/actor/host_mesh.html#HostMesh.sync_workspace)

Sync local code changes to the remote hosts.

Parameters:

- **workspace** (*Workspace*) - The workspace to sync.
- **conda** ([*bool*](https://docs.python.org/3/library/functions.html#bool)) - If True, also sync the currently activated conda env.
- **auto_reload** ([*bool*](https://docs.python.org/3/library/functions.html#bool)) - If True, automatically reload the workspace on changes.

*property*initialized*: Future[[Literal](https://docs.python.org/3/library/typing.html#typing.Literal)[True]]*

Future completes with 'True' when the HostMesh has initialized.
Because HostMesh are remote objects, there is no guarantee that the HostMesh is
still usable after this completes, only that at some point in the past it was usable.

*property*extent*: Extent*

flatten(*name*)

Returns a new device mesh with all dimensions flattened into a single dimension
with the given name.

Currently this supports only dense meshes: that is, all ranks must be contiguous
in the mesh.

rename(***kwargs*)

Returns a new device mesh with some of dimensions renamed.
Dimensions not mentioned are retained:

> new_mesh = mesh.rename(host='dp', gpu='tp')

size(*dim=None*)

Returns the number of elements (total) of the subset of mesh asked for.
If dims is None, returns the total number of devices in the mesh.

*property*sizes*: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int)]*

slice(***kwargs*)

Select along named dimensions. Integer values remove
dimensions, slice objects keep dimensions but restrict them.

Examples: mesh.slice(batch=3, gpu=slice(2, 6))

split(***kwargs*)

Returns a new device mesh with some dimensions of this mesh split.
For instance, this call splits the host dimension into dp and pp dimensions,
The size of 'pp' is specified and the dimension size is derived from it:

> new_mesh = mesh.split(host=('dp', 'pp'), gpu=('tp','cp'), pp=16, cp=2)

Dimensions not specified will remain unchanged.

*class*monarch.actor.ProcMesh(*hy_proc_mesh*, *host_mesh*, *region*, *root_region*, *_device_mesh=None*)[[source]](../_modules/monarch/_src/actor/proc_mesh.html#ProcMesh)

Bases: `MeshTrait`

A distributed mesh of processes for actor computation.

ProcMesh represents a collection of processes that can spawn and manage actors.
It provides the foundation for distributed actor systems by managing process
allocation, lifecycle, and communication across multiple hosts and devices.

The ProcMesh supports spawning actors, monitoring process health, logging
configuration, and code synchronization across distributed processes.

*property*initialized*: Future[[Literal](https://docs.python.org/3/library/typing.html#typing.Literal)[True]]*

Future completes with 'True' when the ProcMesh has initialized.
Because ProcMesh are remote objects, there is no guarantee that the ProcMesh is
still usable after this completes, only that at some point in the past it was usable.

*property*host_mesh*: HostMesh*

spawn(*name*, *Class*, **args*, ***kwargs*)[[source]](../_modules/monarch/_src/actor/proc_mesh.html#ProcMesh.spawn)

Spawn a T-typed actor mesh on the process mesh.

Args:
- name: The name of the actor.
- Class: The class of the actor to spawn.
- args: Positional arguments to pass to the actor's constructor.
- kwargs: Keyword arguments to pass to the actor's constructor.

Returns:
- The actor mesh reference typed as T.

Note:

The method returns immediately, initializing the underlying actor instances
asynchronously. Thus, return of this method does not guarantee the actor's
__init__ has be executed; but rather that __init__ will be executed before the
first call to the actor's endpoints.

Nonblocking enhances composition, permitting the user to easily pipeline mesh
creation, for exmaple to construct complex mesh object graphs without introducing
additional latency.

If __init__ fails, the actor will be stopped and a supervision event will
be raised.

*classmethod*from_host_mesh(*host_mesh*, *hy_proc_mesh*, *region*, *setup=None*, *_attach_controller_controller=True*)[[source]](../_modules/monarch/_src/actor/proc_mesh.html#ProcMesh.from_host_mesh)

to_table()[[source]](../_modules/monarch/_src/actor/proc_mesh.html#ProcMesh.to_table)

activate()[[source]](../_modules/monarch/_src/actor/proc_mesh.html#ProcMesh.activate)

Activate the device mesh. Operations done from insided this context manager will be
distributed tensor operations. Each operation will be excuted on each device in the mesh.

See [https://meta-pytorch.org/monarch/generated/examples/distributed_tensors.html](https://meta-pytorch.org/monarch/generated/examples/distributed_tensors.html) for more information

> with mesh.activate():
> 
> t = torch.rand(3, 4, device="cuda")

rank_tensor(*dim*)[[source]](../_modules/monarch/_src/actor/proc_mesh.html#ProcMesh.rank_tensor)

rank_tensors()[[source]](../_modules/monarch/_src/actor/proc_mesh.html#ProcMesh.rank_tensors)

stop(*reason='stopped by client'*)[[source]](../_modules/monarch/_src/actor/proc_mesh.html#ProcMesh.stop)

This will stop all processes (and actors) in the mesh and
release any resources associated with the mesh.

*property*extent*: Extent*

flatten(*name*)

Returns a new device mesh with all dimensions flattened into a single dimension
with the given name.

Currently this supports only dense meshes: that is, all ranks must be contiguous
in the mesh.

rename(***kwargs*)

Returns a new device mesh with some of dimensions renamed.
Dimensions not mentioned are retained:

> new_mesh = mesh.rename(host='dp', gpu='tp')

size(*dim=None*)

Returns the number of elements (total) of the subset of mesh asked for.
If dims is None, returns the total number of devices in the mesh.

*property*sizes*: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int)]*

slice(***kwargs*)

Select along named dimensions. Integer values remove
dimensions, slice objects keep dimensions but restrict them.

Examples: mesh.slice(batch=3, gpu=slice(2, 6))

split(***kwargs*)

Returns a new device mesh with some dimensions of this mesh split.
For instance, this call splits the host dimension into dp and pp dimensions,
The size of 'pp' is specified and the dimension size is derived from it:

> new_mesh = mesh.split(host=('dp', 'pp'), gpu=('tp','cp'), pp=16, cp=2)

Dimensions not specified will remain unchanged.

monarch.actor.get_or_spawn_controller(*name*, *Class*, **args*, ***kwargs*)[[source]](../_modules/monarch/_src/actor/proc_mesh.html#get_or_spawn_controller)

Creates a singleton actor (controller) indexed by name, or if it already exists, returns the
existing actor.

Parameters:

- **name** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) - The unique name of the actor, used as a key for retrieval.
- **Class** (*Type*) - The class of the actor to spawn. Must be a subclass of Actor.
- ***args** (*Any*) - Positional arguments to pass to the actor constructor.
- ****kwargs** (*Any*) - Keyword arguments to pass to the actor constructor.

Returns:

A Future that resolves to a reference to the actor.

Return type:

*Future*[*TActor*]

monarch.actor.this_host()[[source]](../_modules/monarch/_src/actor/host_mesh.html#this_host)

The current machine.

This is just shorthand for looking it up via the context

monarch.actor.this_proc()[[source]](../_modules/monarch/_src/actor/host_mesh.html#this_proc)

The current singleton process that this specific actor is
running on

monarch.actor.default_bootstrap_cmd()[[source]](../_modules/monarch/_src/actor/host_mesh.html#default_bootstrap_cmd)

Get the default bootstrap command for the current environment.

Returns a BootstrapCommand configured with the current Python executable
and environment. This can be used as a base for customization with
`with_env()` or by modifying its attributes directly.

Returns:

The default bootstrap command.

Return type:

BootstrapCommand

monarch.actor.hosts_from_config(*name*)[[source]](../_modules/monarch/_src/actor/host_mesh.html#hosts_from_config)

Get the host mesh 'name' from the monarch configuration for the project.

This config can be modified so that the same code can create meshes from scheduler sources,
and different sizes etc.

WARNING: This function is a standin so that our getting_started example code works. The real implementation
needs an RFC design.

monarch.actor.enable_transport(*transport*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#enable_transport)

Allow monarch to communicate with transport type 'transport'
This must be called before any other calls in the monarch API.
If it isn't called, we will implicitly call
monarch.enable_transport(ChannelTransport.Unix) on the first monarch call.

Currently only one transport type may be enabled at one time.
In the future we may allow multiple to be enabled.

Supported transport values:

- ChannelTransport enum: ChannelTransport.Unix, ChannelTransport.TcpWithHostname, etc.
- string short cuts for the ChannelTransport enum:

- "tcp": ChannelTransport.TcpWithHostname
- "ipc": ChannelTransport.Unix
- "metatls": ChannelTransport.MetaTlsWithIpV6
- "metatls-hostname": ChannelTransport.MetaTlsWithHostname
- "tls": ChannelTransport.Tls (uses configurable TLS certs)
- ZMQ-style URL format string for explicit address, e.g.:

- "[tcp://127.0.0.1:8080](tcp://127.0.0.1:8080)"

For Meta usage, use metatls-hostname

# Defining Actors

All actor classes subclass the Actor base object, which provides them mesh slicing API.
Each publicly exposed function of the actor is annotated with @endpoint:

*class*monarch.actor.Actor[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Actor)

Bases: `MeshTrait`

Base class for actors.

Subclass `Actor` to define an actor, and decorate the methods that form
its public API with `@endpoint`. Actors are spawned onto a `ProcMesh`
(for example with `this_proc().spawn(...)`), after which their endpoints
are invoked remotely through the messaging adverbs. Each actor processes its
messages sequentially and participates in the supervision tree.

*property*logger*: [Logger](https://docs.python.org/3/library/logging.html#logging.Logger)*

*property*initialized*: [Any](https://docs.python.org/3/library/typing.html#typing.Any)*

*property*extent*: Extent*

flatten(*name*)

Returns a new device mesh with all dimensions flattened into a single dimension
with the given name.

Currently this supports only dense meshes: that is, all ranks must be contiguous
in the mesh.

rename(***kwargs*)

Returns a new device mesh with some of dimensions renamed.
Dimensions not mentioned are retained:

> new_mesh = mesh.rename(host='dp', gpu='tp')

size(*dim=None*)

Returns the number of elements (total) of the subset of mesh asked for.
If dims is None, returns the total number of devices in the mesh.

*property*sizes*: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int)]*

slice(***kwargs*)

Select along named dimensions. Integer values remove
dimensions, slice objects keep dimensions but restrict them.

Examples: mesh.slice(batch=3, gpu=slice(2, 6))

split(***kwargs*)

Returns a new device mesh with some dimensions of this mesh split.
For instance, this call splits the host dimension into dp and pp dimensions,
The size of 'pp' is specified and the dimension size is derived from it:

> new_mesh = mesh.split(host=('dp', 'pp'), gpu=('tp','cp'), pp=16, cp=2)

Dimensions not specified will remain unchanged.

monarch.actor.endpoint(*method: [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[[Concatenate](https://docs.python.org/3/library/typing.html#typing.Concatenate)[[Any](https://docs.python.org/3/library/typing.html#typing.Any), P]], [Awaitable](https://docs.python.org/3/library/typing.html#typing.Awaitable)[R]]*, ***, *propagate: [None](https://docs.python.org/3/library/constants.html#None) | [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)['cached', 'inspect', 'mocked'] | [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[...], [Any](https://docs.python.org/3/library/typing.html#typing.Any)] = None*, *explicit_response_port: [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)[False] = False*, *instrument: [bool](https://docs.python.org/3/library/functions.html#bool) = True*) → EndpointProperty[P, R][[source]](../_modules/monarch/_src/actor/endpoint.html#endpoint)

monarch.actor.endpoint(*method: [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[[Concatenate](https://docs.python.org/3/library/typing.html#typing.Concatenate)[[Any](https://docs.python.org/3/library/typing.html#typing.Any), P]], R]*, ***, *propagate: [None](https://docs.python.org/3/library/constants.html#None) | [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)['cached', 'inspect', 'mocked'] | [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[...], [Any](https://docs.python.org/3/library/typing.html#typing.Any)] = None*, *explicit_response_port: [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)[False] = False*, *instrument: [bool](https://docs.python.org/3/library/functions.html#bool) = True*) → EndpointProperty[P, R]

monarch.actor.endpoint(***, *propagate: [None](https://docs.python.org/3/library/constants.html#None) | [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)['cached', 'inspect', 'mocked'] | [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[...], [Any](https://docs.python.org/3/library/typing.html#typing.Any)] = None*, *explicit_response_port: [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)[False] = False*, *instrument: [bool](https://docs.python.org/3/library/functions.html#bool) = True*) → EndpointIfy

monarch.actor.endpoint(*method: Callable[Concatenate[Any, 'Port[R]', P], Awaitable[[None](https://docs.python.org/3/library/constants.html#None)]]*, ***, *propagate: [None](https://docs.python.org/3/library/constants.html#None) | [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)['cached', 'inspect', 'mocked'] | [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[...], [Any](https://docs.python.org/3/library/typing.html#typing.Any)] = None*, *explicit_response_port: [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)[True]*, *instrument: [bool](https://docs.python.org/3/library/functions.html#bool) = True*) → EndpointProperty[P, R]

monarch.actor.endpoint(*method: Callable[Concatenate[Any, 'Port[R]', P], [None](https://docs.python.org/3/library/constants.html#None)]*, ***, *propagate: [None](https://docs.python.org/3/library/constants.html#None) | [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)['cached', 'inspect', 'mocked'] | [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[...], [Any](https://docs.python.org/3/library/typing.html#typing.Any)] = None*, *explicit_response_port: [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)[True]*, *instrument: [bool](https://docs.python.org/3/library/functions.html#bool) = True*) → EndpointProperty[P, R]

monarch.actor.endpoint(***, *propagate: [None](https://docs.python.org/3/library/constants.html#None) | [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)['cached', 'inspect', 'mocked'] | [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[...], [Any](https://docs.python.org/3/library/typing.html#typing.Any)] = None*, *explicit_response_port: [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)[True]*, *instrument: [bool](https://docs.python.org/3/library/functions.html#bool) = True*) → PortedEndpointIfy

Mark an `Actor` method as an endpoint callable from other actors.

An endpoint defines part of an actor's public API. Once the actor is
spawned onto a mesh, you invoke its endpoints through messaging adverbs
(`call`, `call_one`, `choose`, `stream`, `broadcast`, and
`rref`) rather than calling the method directly. Each adverb controls how
the message is delivered and how the response is returned. Endpoint methods
may be synchronous or `async`.

Apply it bare or with options:

```
class Counter(Actor):
 @endpoint
 def increment(self) -> int:
 ...

 @endpoint(explicit_response_port=True)
 async def stream_results(self, port: Port[int]) -> None:
 ...
```

Parameters:

- **method** - The actor method to wrap. Supplied automatically when the
decorator is applied without parentheses; leave it unset when
passing options.
- **propagate** - Controls how the tensor engine infers output tensor shapes
for `rref` without running the endpoint. Pass a callable that
takes the endpoint's arguments (excluding `self`) and returns
tensors of the shapes the endpoint would produce, or one of the
strings `"cached"`, `"inspect"`, or `"mocked"`. Defaults to
`None`, which matters only for endpoints used with distributed
tensors.
- **explicit_response_port** - When `True`, the endpoint receives a `Port`
as its first argument (after `self`) and is responsible for
sending its result through that port instead of returning a value.
This supports custom response protocols, such as sending several
responses or deferring one. The method's return annotation should
be `None`.
- **instrument** - When `True` (the default), wrap each invocation in a
tracing span named for the method.

Returns:

An `EndpointProperty` descriptor. Accessing it on a spawned actor
yields an `Endpoint` exposing the messaging adverbs.

monarch.actor.concurrent_endpoint(*method=None*, ***, *propagate=None*, *explicit_response_port=False*, *instrument=True*)[[source]](../_modules/monarch/_src/actor/concurrent.html#concurrent_endpoint)

Run one async actor endpoint as an `asyncio` background task.

The decorator accepts the same endpoint options as `@endpoint`. It
rewrites the endpoint to use an explicit response port, schedules the
original async body as a task, and returns immediately so that the actor can
handle another queued message.

If two messages from the same source actor call `@concurrent_endpoint`
methods on the same target actor, the first endpoint body starts before the
second. This is only a start-order guarantee: the first endpoint runs until
its first `await`, not to completion, before the second starts.

If you mix `@concurrent_endpoint` with normal `@endpoint` methods, a
normal endpoint that follows a concurrent endpoint may run before the
concurrent endpoint body has started.

Outstanding tasks created by this decorator are cancelled and awaited
before user `__cleanup__` runs. This is a cleanup-time guarantee only:
meshes owned by the actor may already have been stopped by the core actor
lifecycle before these tasks are cancelled. General pending tasks on the
actor's asyncio loop are cancelled later, after user `__cleanup__`
completes.

If the endpoint body raises an exception that it does not forward through
its response port, the actor fails with a supervision error, just as an
exception escaping an `@endpoint(explicit_response_port=True)` method
does. This follows the principle of no silent errors. To report an error to
the caller without failing the actor, forward it through the port (for
example, `port.exception(e)`); to recover, catch it inside the endpoint.

More complex protocols can still use `@endpoint(explicit_response_port=True)`
and manage response ports manually.

# Messaging Actor

Messaging is done through the "adverbs" defined for each endpoint

*class*monarch.actor.Endpoint(**args*, ***kwargs*)[[source]](../_modules/monarch/_src/actor/endpoint.html#Endpoint)

Bases: [`Protocol`](https://docs.python.org/3/library/typing.html#typing.Protocol)[`P`, `R`]

A callable endpoint on a spawned actor or actor mesh.

Accessing an `@endpoint` method on a spawned actor yields an `Endpoint`
rather than calling the method directly. Its methods are the messaging
adverbs that send a message to the actor(s) and control how responses are
returned: `call`, `call_one`, `choose`, `stream`, `broadcast`,
and `rref`.

call_one(**args*, ***kwargs*)[[source]](../_modules/monarch/_src/actor/endpoint.html#Endpoint.call_one)

call(**args*, ***kwargs*)[[source]](../_modules/monarch/_src/actor/endpoint.html#Endpoint.call)

stream(**args*, ***kwargs*)[[source]](../_modules/monarch/_src/actor/endpoint.html#Endpoint.stream)

Broadcasts to all actors and yields their responses as a stream.

This enables processing results from multiple actors incrementally as
they become available. Returns an iterator of Future values.

broadcast(**args*, ***kwargs*)[[source]](../_modules/monarch/_src/actor/endpoint.html#Endpoint.broadcast)

Fire-and-forget broadcast to all actors without waiting for actors to
acknowledge receipt.

In other words, the return of this method does not guarrantee the
delivery of the message.

rref(**args*, ***kwargs*)[[source]](../_modules/monarch/_src/actor/endpoint.html#Endpoint.rref)

*class*monarch.actor.Future(***, *coro*)[[source]](../_modules/monarch/_src/actor/future.html#Future)

Bases: [`Generic`](https://docs.python.org/3/library/typing.html#typing.Generic)[`R`]

The Future class wraps a PythonTask, which is a handle to a asyncio coroutine running on the Tokio event loop.
These coroutines do not use asyncio or asyncio.Future; instead, they are executed directly on the Tokio runtime.
The Future class provides both synchronous (.get()) and asynchronous APIs (await) for interacting with these tasks.

Parameters:

**coro** (*Coroutine**[**Any**,**Any**,**R**]**|**PythonTask**[**R**]*) - The coroutine or PythonTask representing
the asynchronous computation.

__init__(***, *coro*)[[source]](../_modules/monarch/_src/actor/future.html#Future.__init__)

get(*timeout=None*)[[source]](../_modules/monarch/_src/actor/future.html#Future.get)

Get the result of the Future.

Caveats:

This method is designed to be used in places where event loops are not available. Besides that, you should
avoid using this method if possible. Instead, use as_asyncio() (or await). This is because when Future.get() is called from
within an active event loop, it blocks synchronously and does not yield control. That may degrade performance
by preventing other tasks from running, and can potentially cause deadlocks if this future depends on them.

A timeout never consumes the Future: on TimeoutError the underlying task keeps running, so a later get()/await still observes its result.

examples:

This is not recommended because fut.get() blocks the event loop and might lead to issues explained above.
```
def inner_func(fut):

> result = fut.get()
> # ...

async def out_func(fut):

inner_func(fut)

```

This is okay because everything is running synchronously.
```
def inner_func(fut):

> result = fut.get()
> # ...

def main():

# ...
inner_func(fut)

```

as_asyncio()[[source]](../_modules/monarch/_src/actor/future.html#Future.as_asyncio)

Return a standard `asyncio.Future` that resolves when this Future
does.

Requires a running event loop; off a loop it raises `RuntimeError`
**without** consuming the underlying task (the Future stays unawaited, so
a later `get()` still drives it). Observation is non-consuming:
repeated `as_asyncio()`/`await` each return a fresh loop-local future
observing the same result.

result(*timeout=None*)[[source]](../_modules/monarch/_src/actor/future.html#Future.result)

exception(*timeout=None*)[[source]](../_modules/monarch/_src/actor/future.html#Future.exception)

*class*monarch.actor.ValueMesh(*shape*, *values*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#ValueMesh)

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

get(*rank*)

Get value by linear rank (0..num_ranks-1).

item(***kwargs*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#ValueMesh.item)

Get the value at the given coordinates.

Parameters:

**kwargs** ([*int*](https://docs.python.org/3/library/functions.html#int)) - Coordinates to get the value at.

Returns:

Value at the given coordinate.

Raises:

[**KeyError**](https://docs.python.org/3/library/exceptions.html#KeyError) - If invalid coordinates are provided.

Return type:

*R*

items()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#ValueMesh.items)

Generator that returns values for the provided coordinates.

Returns:

Values at all coordinates.

Return type:

[*Iterable*](https://docs.python.org/3/library/typing.html#typing.Iterable)[[*Tuple*](https://docs.python.org/3/library/typing.html#typing.Tuple)[*Point*, *R*]]

values()

Return the values in region/iteration order as a Python list.

*property*extent*: Extent*

flatten(*name*)

Returns a new device mesh with all dimensions flattened into a single dimension
with the given name.

Currently this supports only dense meshes: that is, all ranks must be contiguous
in the mesh.

*static*from_indexed(*shape*, *pairs*)

Build from (rank, value) pairs with last-write-wins semantics.

rename(***kwargs*)

Returns a new device mesh with some of dimensions renamed.
Dimensions not mentioned are retained:

> new_mesh = mesh.rename(host='dp', gpu='tp')

size(*dim=None*)

Returns the number of elements (total) of the subset of mesh asked for.
If dims is None, returns the total number of devices in the mesh.

*property*sizes*: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int)]*

slice(***kwargs*)

Select along named dimensions. Integer values remove
dimensions, slice objects keep dimensions but restrict them.

Examples: mesh.slice(batch=3, gpu=slice(2, 6))

split(***kwargs*)

Returns a new device mesh with some dimensions of this mesh split.
For instance, this call splits the host dimension into dp and pp dimensions,
The size of 'pp' is specified and the dimension size is derived from it:

> new_mesh = mesh.split(host=('dp', 'pp'), gpu=('tp','cp'), pp=16, cp=2)

Dimensions not specified will remain unchanged.

*class*monarch.actor.ActorError(*exception*, *message='A remote actor call has failed.'*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#ActorError)

Bases: [`Exception`](https://docs.python.org/3/library/exceptions.html#Exception)

Deterministic problem with the user's code.
For example, an OOM resulting in trying to allocate too much GPU memory, or violating
some invariant enforced by the various APIs.

__init__(*exception*, *message='A remote actor call has failed.'*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#ActorError.__init__)

add_note()

Exception.add_note(note) -
add a note to the exception

args

with_traceback()

Exception.with_traceback(tb) -
set self.__traceback__ to tb and return self.

*class*monarch.actor.Accumulator(*endpoint*, *identity*, *combine*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Accumulator)

Bases: [`Generic`](https://docs.python.org/3/library/typing.html#typing.Generic)[`P`, `R`, `A`]

Accumulate the result of a broadcast invocation of an endpoint
across a sliced mesh.

Usage:

```
>>> counter = Accumulator(Actor.increment, 0, lambda x, y: x + y)
```

__init__(*endpoint*, *identity*, *combine*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Accumulator.__init__)

Parameters:

- **endpoint** (*Endpoint**[**~P**,**R**]*) - Endpoint to accumulate the result of.
- **identity** (*A*) - Initial value of the accumulated value before the first combine invocation.
- **combine** ([*Callable*](https://docs.python.org/3/library/typing.html#typing.Callable)*[**[**A**,**R**]**,**A**]*) - Lambda invoked for combining the result of the endpoint with the accumulated value.

accumulate(**args*, ***kwargs*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Accumulator.accumulate)

Accumulate the result of the endpoint invocation.

Parameters:

- **args** (*~P*) - Arguments to pass to the endpoint.
- **kwargs** (*~P*) - Keyword arguments to pass to the endpoint.

Returns:

Future that resolves to the accumulated value.

Return type:

*Future*[*A*]

monarch.actor.send(*endpoint*, *args*, *kwargs*, *port=None*, *selection='all'*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#send)

> Fire-and-forget broadcast invocation of the endpoint across a given selection of the mesh.
> 
> 
> 
> 
> This sends the message to all actors but does not wait for any result. Use the port provided to
> send the response back to the caller.

Parameters:

- **endpoint** (*Endpoint**[**~P**,**R**]*) - Endpoint to invoke.
- **args** ([*Tuple*](https://docs.python.org/3/library/typing.html#typing.Tuple)*[*[*Any*](https://docs.python.org/3/library/typing.html#typing.Any)*,**...**]*) - Arguments to pass to the endpoint.
- **kwargs** ([*Dict*](https://docs.python.org/3/library/typing.html#typing.Dict)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*,*[*Any*](https://docs.python.org/3/library/typing.html#typing.Any)*]*) - Keyword arguments to pass to the endpoint.
- **port** (*Port**|**None*) - Handle to send the response to.
- **selection** ([*Literal*](https://docs.python.org/3/library/typing.html#typing.Literal)*[**'all'**,**'choose'**]*) - Selection query representing a subset of the mesh.

*class*monarch.actor.Channel[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Channel)

Bases: [`Generic`](https://docs.python.org/3/library/typing.html#typing.Generic)[`R`]

An advanced low level API for a communication channel used for message passing
between actors.

Provides static methods to create communication channels with port pairs
for sending and receiving messages of type R.

*static*open(*once=False*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Channel.open)

*static*open_ranked(*once=False*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Channel.open_ranked)

*class*monarch.actor.Port(*port_ref*, *instance*, *rank*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Port)

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

A port that sends messages to a remote receiver.
Wraps an EitherPortRef with the actor instance needed for sending.

send(*obj*)

send_message(*message*)

*async*resolve_and_send(*result*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Port.resolve_and_send)

exception(*e*)

return_undeliverable

*class*monarch.actor.PortReceiver(*mailbox*, *receiver*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#PortReceiver)

Bases: [`Generic`](https://docs.python.org/3/library/typing.html#typing.Generic)[`R`]

Receiver for messages sent through a communication channel.

Handles receiving R-typed objects sent from a corresponding Port.

recv()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#PortReceiver.recv)

ranked()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#PortReceiver.ranked)

monarch.actor.as_endpoint(*not_an_endpoint: [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[P], R]*, ***, *propagate: [None](https://docs.python.org/3/library/constants.html#None) | [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)['cached', 'inspect', 'mocked'] | [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[...], [Any](https://docs.python.org/3/library/typing.html#typing.Any)] = None*, *explicit_response_port: [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)[False] = False*) → Endpoint[P, R][[source]](../_modules/monarch/_src/actor/actor_mesh.html#as_endpoint)

monarch.actor.as_endpoint(*not_an_endpoint: Callable[Concatenate['PortProtocol[R]', P], [None](https://docs.python.org/3/library/constants.html#None)]*, ***, *propagate: [None](https://docs.python.org/3/library/constants.html#None) | [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)['cached', 'inspect', 'mocked'] | [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[...], [Any](https://docs.python.org/3/library/typing.html#typing.Any)] = None*, *explicit_response_port: [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)[True]*) → Endpoint[P, R]

Treat an actor method that is not an `@endpoint` as one.

Use this to call a plain method of a spawned actor through the messaging
adverbs when the method was not decorated with `@endpoint`, for example
`as_endpoint(actor.method).call(...)`. The options match those of
`endpoint`.

Parameters:

- **not_an_endpoint** - An unannotated method of a spawned actor.
- **propagate** - Tensor-shape propagation for `rref`; see `endpoint`.
- **explicit_response_port** - When `True`, the method receives a `Port`
as its first argument and sends its result through it; see
`endpoint`.

Returns:

An `Endpoint` exposing the messaging adverbs.

Raises:

[**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) - If `not_an_endpoint` is not a method of a spawned actor.

# Context API

Use these functions to look up what actor is running the currently executing code.

monarch.actor.current_actor_name()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#current_actor_name)

Return the actor id of the currently executing actor as a string.

monarch.actor.current_rank()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#current_rank)

Return the current message's position within its mesh as a `Point`.

monarch.actor.current_size()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#current_size)

Return the size of each mesh dimension for the current message.

The result maps each dimension label to the number of actors along it.

monarch.actor.context()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#context)

Return the `Context` for the currently executing actor.

Call this from within an endpoint to inspect the running actor and the
current message's position in the mesh. Outside an actor (on the client) it
returns the root client context.

monarch.actor.shutdown_context()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#shutdown_context)

Shutdown global actor context resources.

Idempotent: subsequent calls return an immediately-resolved future.
This is safe to call both explicitly and from atexit.

Returns:

A future that completes when shutdown is

finished. Call with .get() to wait for
completion.

Return type:

Future[None]

*class*monarch.actor.Context[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Context)

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

actor_instance

message_rank

*class*monarch.actor.Point(*rank*, *extent*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Point)

Bases: `Point`, [`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)

A coordinate within a mesh.

A `Point` maps each mesh dimension label (for example `"hosts"` or
`"gpus"`) to its integer index along that dimension. It behaves as a
read-only mapping, so `point["gpus"]` is the index along the `"gpus"`
dimension. `current_rank` and `Context.message_rank` return the
`Point` that locates the current actor within the mesh that received the
message.

extent

get(*k*[, *d*]) → D[k] if k in D, else d. d defaults to None.

items() → a set-like object providing a view on D's items

keys() → a set-like object providing a view on D's keys

rank

size(*label*)

values() → an object providing a view on D's values

*class*monarch.actor.Extent(*labels*, *sizes*)

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

keys()

nelements

region

# Supervision

Types used for error handling and supervision in actor meshes.

*class*monarch.actor.MeshFailure

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

mesh

mesh_name

report()

monarch.actor.unhandled_fault_hook(*failure*)[[source]](../_modules/monarch/_src/actor/supervision.html#unhandled_fault_hook)

When a supervision event is unhandled and is propagated back to the client,
this hook is called.
The default implementation is to exit the process with error code 1
after logging the event.
If this function raises any exception (including BaseException classes such
as SystemExit from sys.exit), this fault is considered unhandled.
Any normal return value will cause the fault to be dropped. Logs will be
written containing the failure message in either case.

To customize this behavior, overwrite this function in your client code like so:
```
import monarch.actor

def my_unhandled_fault_hook(failure: MeshFailure) -> None:

# log it, add metrics, etc.
print(f"Mesh failure was not handled: {failure}")
# To ignore this error, return any value (including None) without an exception.

monarch.actor.unhandled_fault_hook = my_unhandled_fault_hook
```

If the fault is unhandled, it exits the main thread by delivering a KeyboardInterrupt.
This is done because the Python Interpreter can only be finalized to run
destructors and atexit hooks from the main thread. So if you see a
"KeyboardInterrupt" happening that you didn't send, it's because there was
an unhandled fault.

# Telemetry

Utilities for tracing actor execution.

monarch.actor.traced(*fn: [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[_P], _R]*) → [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[_P], _R][[source]](../_modules/monarch/_src/actor/telemetry.html#traced)

monarch.actor.traced(***, *name: [str](https://docs.python.org/3/library/stdtypes.html#str)*) → [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[[Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[_P], _R]], [Callable](https://docs.python.org/3/library/typing.html#typing.Callable)[[_P], _R]]

Decorator that wraps a function in a telemetry span.

Works with both sync and async functions. The span is automatically
associated with the current actor context, if any. When no name is
provided, the function's `__name__` is used as the span name.

Usage:

```
@traced
async def do_work():
 ...

@traced(name="custom_name")
def compute():
 ...
```