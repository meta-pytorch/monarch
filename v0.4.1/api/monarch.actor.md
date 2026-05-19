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

spawn_procs(*per_host=None*, *bootstrap=None*, *name=None*, *proc_bind=None*)[[source]](../_modules/monarch/_src/actor/host_mesh.html#HostMesh.spawn_procs)

Spawn a ProcMesh onto this host mesh.

Parameters:

- **per_host** ([*Dict*](https://docs.python.org/3/library/typing.html#typing.Dict)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*,*[*int*](https://docs.python.org/3/library/functions.html#int)*]**|**None*) - shape of procs per host, e.g. `{"gpus": 4}`.
- **bootstrap** ([*Callable*](https://docs.python.org/3/library/typing.html#typing.Callable)*[**[**]**,**None**]**|*[*Callable*](https://docs.python.org/3/library/typing.html#typing.Callable)*[**[**]**,*[*Awaitable*](https://docs.python.org/3/library/typing.html#typing.Awaitable)*[**None**]**]**|**None*) - optional setup callable run on each proc.
- **name** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)*|**None*) - optional name for the proc mesh.
- **proc_bind** ([*list*](https://docs.python.org/3/library/stdtypes.html#list)*[*[*dict*](https://docs.python.org/3/library/stdtypes.html#dict)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*,*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]**]**|**None*) - optional per-process CPU/NUMA binding config.
Length must equal `math.prod(per_host.values())`.
Each dict maps binding keys (`cpunodebind`,
`membind`, `physcpubind`, `cpus`) to values.

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

# Defining Actors

All actor classes subclass the Actor base object, which provides them mesh slicing API.
Each publicly exposed function of the actor is annotated with @endpoint:

*class*monarch.actor.Actor[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Actor)

Bases: `MeshTrait`

*property*logger*: [Logger](https://docs.python.org/3/library/logging.html#logging.Logger)*[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Actor.logger)

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

monarch.actor.endpoint(*method: Callable[Concatenate[Any, P], Awaitable[R]]*, ***, *propagate: Propagator = None*, *explicit_response_port: Literal[False] = False*, *instrument: [bool](https://docs.python.org/3/library/functions.html#bool) = True*) → EndpointProperty[P, R][[source]](../_modules/monarch/_src/actor/endpoint.html#endpoint)

monarch.actor.endpoint(*method: Callable[Concatenate[Any, P], R]*, ***, *propagate: Propagator = None*, *explicit_response_port: Literal[False] = False*, *instrument: [bool](https://docs.python.org/3/library/functions.html#bool) = True*) → EndpointProperty[P, R]

monarch.actor.endpoint(***, *propagate: Propagator = None*, *explicit_response_port: Literal[False] = False*, *instrument: [bool](https://docs.python.org/3/library/functions.html#bool) = True*) → EndpointIfy

monarch.actor.endpoint(*method: Callable[Concatenate[Any, 'Port[R]', P], Awaitable[[None](https://docs.python.org/3/library/constants.html#None)]]*, ***, *propagate: Propagator = None*, *explicit_response_port: Literal[True]*, *instrument: [bool](https://docs.python.org/3/library/functions.html#bool) = True*) → EndpointProperty[P, R]

monarch.actor.endpoint(*method: Callable[Concatenate[Any, 'Port[R]', P], [None](https://docs.python.org/3/library/constants.html#None)]*, ***, *propagate: Propagator = None*, *explicit_response_port: Literal[True]*, *instrument: [bool](https://docs.python.org/3/library/functions.html#bool) = True*) → EndpointProperty[P, R]

monarch.actor.endpoint(***, *propagate: Propagator = None*, *explicit_response_port: Literal[True]*, *instrument: [bool](https://docs.python.org/3/library/functions.html#bool) = True*) → PortedEndpointIfy

# Messaging Actor

Messaging is done through the "adverbs" defined for each endpoint

*class*monarch.actor.Endpoint(**args*, ***kwargs*)[[source]](../_modules/monarch/_src/actor/endpoint.html#Endpoint)

Bases: [`Protocol`](https://docs.python.org/3/library/typing.html#typing.Protocol)[`P`, `R`]

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

add_note(*object*, */*)

Exception.add_note(note) -
add a note to the exception

args

with_traceback(*object*, */*)

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

exception(*e*)

return_undeliverable

*class*monarch.actor.PortReceiver(*mailbox*, *receiver*, *monitor=None*, *endpoint=None*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#PortReceiver)

Bases: [`Generic`](https://docs.python.org/3/library/typing.html#typing.Generic)[`R`]

Receiver for messages sent through a communication channel.

Handles receiving R-typed objects sent from a corresponding Port.
Asynchronously message reception with optional supervision
monitoring for error handling.

recv()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#PortReceiver.recv)

ranked()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#PortReceiver.ranked)

monarch.actor.as_endpoint(*not_an_endpoint: Callable[P, R]*, ***, *propagate: Propagator = None*, *explicit_response_port: Literal[False] = False*) → Endpoint[P, R][[source]](../_modules/monarch/_src/actor/actor_mesh.html#as_endpoint)

monarch.actor.as_endpoint(*not_an_endpoint: Callable[Concatenate['PortProtocol[R]', P], [None](https://docs.python.org/3/library/constants.html#None)]*, ***, *propagate: Propagator = None*, *explicit_response_port: Literal[True]*) → Endpoint[P, R]

# Context API

Use these functions to look up what actor is running the currently executing code.

monarch.actor.current_actor_name()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#current_actor_name)

monarch.actor.current_rank()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#current_rank)

monarch.actor.current_size()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#current_size)

monarch.actor.context()[[source]](../_modules/monarch/_src/actor/actor_mesh.html#context)

*class*monarch.actor.Context[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Context)

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

actor_instance

message_rank

*class*monarch.actor.Point(*rank*, *extent*)[[source]](../_modules/monarch/_src/actor/actor_mesh.html#Point)

Bases: `Point`, [`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)

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