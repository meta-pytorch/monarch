# monarch

These API functions define monarch's distributed tensor computation API. See [Distributed Tensors in Monarch](../generated/examples/distributed_tensors.html) for an overview.

*class*monarch.Tensor(*fake*, *mesh*, *stream*)[[source]](../_modules/monarch/common/tensor.html#Tensor)

Bases: `Referenceable`, `BaseTensor`

A distributed tensor for distributed computation across device meshes.

Tensor represents a distributed tensor that spans across multiple devices
in a device mesh. It provides the same interface as PyTorch tensors but
enables distributed operations and communication patterns.

stream*: Stream*

mesh*: DeviceMesh*

ref*: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None)*

drop()[[source]](../_modules/monarch/common/tensor.html#Tensor.drop)

*property*dropped

to_mesh(*mesh*, *stream=None*)[[source]](../_modules/monarch/common/tensor.html#Tensor.to_mesh)

Move data between one device mesh and another. Sizes of named dimensions must match.
If mesh has dimensions that self.mesh does not, it will broadcast to those dimensions.

broadcast:

t.slice_mesh(batch=0).to_mesh(t.mesh)

reduce_(*dims*, *reduction='sum'*, *scatter=False*, *mesh=None*)[[source]](../_modules/monarch/common/tensor.html#Tensor.reduce_)

reduce(*dims*, *reduction='sum'*, *scatter=False*, *mesh=None*, *_inplace=False*, *out=None*)[[source]](../_modules/monarch/common/tensor.html#Tensor.reduce)

Perform a reduction operation along dim, and move the data to mesh. If mesh=None, then mesh=self.mesh
'stack' (gather) will concat the values along dim, and produce a local result tensor with an addition outer dimension of len(dim).
If scatter=True, the local result tensor will be evenly split across dim.

allreduce:

t.reduce(dims='gpu', reduction='sum')

First reduces dim 'gpu' creating a local tensor with the 'gpu' dimension, then because output_mesh=input_mesh, and it still has dim 'gpu',
we broadcast the result reduced tensor to all members of gpu.

reducescatter:

t.reduce(dims='gpu', reduction='sum', scatter=True)

Same as above except that scatter=True introduces a new 'gpu' dimension that is the result of splitting the local tensor across 'gpu'

allgather:

t.reduce(dims='gpu', reduction='stack')

First reduces dim 'gpu' creating a bigger local tensor, then because output_mesh=input_mesh, and it still has dim 'gpu',
broadcasts the result concatenated tensor to all members of gpu.

alltoall:

t.reduce(dims='gpu', reduction='stack', scatter=True)

First reduces dim 'gpu' creating a bigger local tensor, then introduces a new 'gpu' dimension that is the result of splitting this
(bigger) tensor across 'gpu'. The result is the same dimension as the original tensor, but with each rank sending to all other ranks.

gather (to dim 0):

t.reduce(dims='gpu', reduction='stack', mesh=device_mesh(gpu=0))

First gathers dim 'gpu' and then places it on the first rank. t.mesh.gpu[0] doesn't have a 'gpu' dimension, but this is
ok because we eliminated the 'gpu' dim via reduction.

reduce:

t.reduce(dims='gpu', reduction='sum', mesh=device_mesh(gpu=0))

First reduces dim 'gpu' and then places it on the first rank. t.mesh.gpu[0] doesn't have a 'gpu' dimension, but this is
ok because we eliminated the 'gpu' dim via reduction.

Parameters:

- **dims** (*Dims**|*[*str*](https://docs.python.org/3/library/stdtypes.html#str)) - The dimensions along which to perform the reduction.
- **reduction** (*_valid_reduce*) - The type of reduction to perform. Defaults to "sum".
- **scatter** ([*bool*](https://docs.python.org/3/library/functions.html#bool)) - If True, the local result tensor will be evenly split across dimensions.
Defaults to False.
- **mesh** (*Optional**[**"DeviceMesh"**]**,**optional*) - The target mesh to move the data to.
If None, uses self.mesh. Defaults to None.
- **_inplace** ([*bool*](https://docs.python.org/3/library/functions.html#bool)) - If True, performs the operation in-place. Defaults to False.
Note that not all the reduction operations support in-place.
- **out** (*Optional**[**"Tensor"**]*) - The output tensor to store the result. If None, a new tensor
will be created on the stream where the reduce operation executes. Defaults to None.

Returns:

The result of the reduction operation.

Return type:

Tensor

slice_mesh(***kwargs*)[[source]](../_modules/monarch/common/tensor.html#Tensor.slice_mesh)

delete_ref(*ref*)[[source]](../_modules/monarch/common/tensor.html#Tensor.delete_ref)

*class*monarch.Stream(*name*, *_default=False*)[[source]](../_modules/monarch/common/stream.html#Stream)

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

__init__(*name*, *_default=False*)[[source]](../_modules/monarch/common/stream.html#Stream.__init__)

activate()[[source]](../_modules/monarch/common/stream.html#Stream.activate)

borrow(*t*, *mutable=False*)[[source]](../_modules/monarch/common/stream.html#Stream.borrow)

> borrowed_tensor, borrow = self.borrow(t)

Borrows tensor 't' for use on this stream.
The memory of t will stay alive until borrow.drop() is called, which will free t and
and any of its alises on stream self and will cause t.stream to wait on self at that point so
that the memory of t can be reused.

If mutable then self can write to the storage of t, but t.stream cannot read or write t until,
the borrow is returned (becomes free and a wait_for has been issued).

If not mutable both self and t.stream can read from t's storage but neither can write to it.

monarch.remote(*function: Callable[P, R]*, ***, *propagate: Propagator = None*) → Remote[P, R][[source]](../_modules/monarch/common/remote.html#remote)

monarch.remote(*function: [str](https://docs.python.org/3/library/stdtypes.html#str)*, ***, *propagate: Literal['mocked', 'cached', 'inspect'] | [None](https://docs.python.org/3/library/constants.html#None) = None*) → Remote

monarch.remote(*function: [str](https://docs.python.org/3/library/stdtypes.html#str)*, ***, *propagate: Callable[P, R]*) → Remote[P, R]

monarch.remote(***, *propagate: Propagator = None*) → RemoteIfy

monarch.coalescing()[[source]](../_modules/monarch/common/_coalescing.html#coalescing)

monarch.get_active_mesh()[[source]](../_modules/monarch/common/device_mesh.html#get_active_mesh)

monarch.no_mesh()

monarch.to_mesh(*tensors*, *mesh*, *stream=None*)[[source]](../_modules/monarch/common/device_mesh.html#to_mesh)

Move all tensors in tensors to the given mesh.

monarch.slice_mesh(*tensors*, ***kwargs*)[[source]](../_modules/monarch/common/device_mesh.html#slice_mesh)

Performs the slice_mesh operation for each tensor in tensors.

monarch.get_active_stream()[[source]](../_modules/monarch/common/stream.html#get_active_stream)

monarch.reduce(*tensors*, *dims*, *reduction='sum'*, *scatter=False*, *mesh=None*, *_inplace=False*)[[source]](../_modules/monarch/common/tensor.html#reduce)

Performs the tensor reduction operation for each tensor in tensors.
:param tensors: The pytree of input tensors to reduce.
:type tensors: pytree["Tensor"]
:param dims: The dimensions along which to perform the reduction.
:type dims: Dims | str
:param reduction: The type of reduction to perform. Defaults to "sum".
:type reduction: _valid_reduce
:param scatter: If True, the local result tensor will be evenly split across dimensions.

> Defaults to False.

Parameters:

- **mesh** (*Optional**[**"DeviceMesh"**]**,**optional*) - The target mesh to move the data to.
If None, uses self.mesh. Defaults to None.
- **_inplace** ([*bool*](https://docs.python.org/3/library/functions.html#bool)) - If True, performs the operation in-place. Defaults to False.
Note that not all the reduction operations support in-place.

monarch.reduce_(*tensors*, *dims*, *reduction='sum'*, *scatter=False*, *mesh=None*)[[source]](../_modules/monarch/common/tensor.html#reduce_)

monarch.call_on_shard_and_fetch(*remote*, **args*, *shard=None*, ***kwargs*)[[source]](../_modules/monarch/common/remote.html#call_on_shard_and_fetch)

monarch.fetch_shard(*obj*, *shard=None*, ***kwargs*)[[source]](../_modules/monarch/fetch.html#fetch_shard)

Retrieve the shard at coordinates of the current device mesh of each
tensor in obj. All tensors in obj will be fetched to the CPU device.

> obj - a pytree containing the tensors the fetch
> shard - a dictionary from mesh dimension name to coordinate of the shard
> 
> 
> 
> 
> > If None, this will fetch from coordinate 0 for all dimensions (useful after all_reduce/all_gather)
> 
> 
> 
> 
> preprocess - a
> **kwargs - additional keyword arguments are added as entries to the shard dictionary

monarch.inspect(*obj*, *shard=None*, ***kwargs*)[[source]](../_modules/monarch/fetch.html#inspect)

monarch.show(*obj*, *shard=None*, ***kwargs*)[[source]](../_modules/monarch/fetch.html#show)

monarch.grad_function(*fn*)[[source]](../_modules/monarch/gradient_generator.html#grad_function)

monarch.grad_generator(*roots=()*, *with_respect_to=()*, *grad_roots=()*)[[source]](../_modules/monarch/gradient_generator.html#grad_generator)

monarch.timer()

monarch.world_mesh(*ctx*, *hosts*, *gpu_per_host*, *_processes=None*)[[source]](../_modules/monarch/world_mesh.html#world_mesh)

monarch.function_resolvers()

Built-in mutable sequence.

If no argument is given, the constructor creates a new empty list.
The argument must be an iterable if specified.

# Types

*class*monarch.Extent(*labels*, *sizes*)

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

*static*from_bytes(*bytes*)

keys()

labels

nelements

region

sizes

*class*monarch.Shape(*labels*, *slice*)

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

at(*label*, *index*)

coordinates(*rank*)

extent

*static*from_bytes(*bytes*)

index(***kwargs*)

labels

ndslice

ranks()

region

select(*label*, *slice*)

*static*unity()

*class*monarch.Selection

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

all()

Selects all elements in the mesh -- use this to mean "route to
all nodes".

The '*' expression is automatically expanded to match the
dimensionality of the slice. For example, in a 3D slice, the
selection becomes *, *, *.

any()

Selects one element nondeterministically -- use this to mean
"route to a single random node".

The '?' expression is automatically expanded to match the
dimensionality of the slice. For example, in a 3D slice, the
selection becomes ?, ?, ?.

from_string()

Parses a selection expression from a string.

This allows you to construct a PySelection using the
selection algebra surface syntax, such as "(*, 0:4, ?)".

Raises:

- [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) - If the input string is not a valid selection
- **expression.** -

Example

PySelection.from_string("(*, 1:3, ?)") # subset of a mesh

monarch.NDSlice

alias of `Slice`

*class*monarch.OpaqueRef(*value=None*)[[source]](../_modules/monarch/common/opaque_ref.html#OpaqueRef)

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

OpaqueRef is a reference to an object that is only resolvable on the worker
This is used to pass objects from the controller to the worker across User Defined Functions

Example::
def init_udf_worker():

model = nn.Linear(3, 4)
model_ref = OpaqueRef(model)
return model_ref

def run_step_worker(model_ref: OpaqueRef):

model = model_ref.value
# do something with model (e.g. forward pass

# on Controller
model_ref = init_udf()
run_step(model_ref)

__init__(*value=None*)[[source]](../_modules/monarch/common/opaque_ref.html#OpaqueRef.__init__)

*property*value*: [Any](https://docs.python.org/3/library/typing.html#typing.Any)*

check_worker(*what*)[[source]](../_modules/monarch/common/opaque_ref.html#OpaqueRef.check_worker)

*class*monarch.Future(*client*)[[source]](../_modules/monarch/common/future.html#Future)

Bases: [`Generic`](https://docs.python.org/3/library/typing.html#typing.Generic)[`T`]

A future object representing the result of an asynchronous computation.

Future provides a way to access the result of a computation that may not
have completed yet. It allows for non-blocking execution and provides
methods to wait for completion and retrieve results.

Parameters:

**client** (*Client*) - The client connection for handling the future

__init__(*client*)[[source]](../_modules/monarch/common/future.html#Future.__init__)

result(*timeout=None*)[[source]](../_modules/monarch/common/future.html#Future.result)

done()[[source]](../_modules/monarch/common/future.html#Future.done)

exception(*timeout=None*)[[source]](../_modules/monarch/common/future.html#Future.exception)

add_callback(*callback*)[[source]](../_modules/monarch/common/future.html#Future.add_callback)

# Distributed Computing

*class*monarch.RemoteProcessGroup(*dims*, *device_mesh*)[[source]](../_modules/monarch/common/device_mesh.html#RemoteProcessGroup)

Bases: `Referenceable`

Client's view of a process group.

__init__(*dims*, *device_mesh*)[[source]](../_modules/monarch/common/device_mesh.html#RemoteProcessGroup.__init__)

ensure_split_comm_remotely(*stream*)[[source]](../_modules/monarch/common/device_mesh.html#RemoteProcessGroup.ensure_split_comm_remotely)

If we haven't already, send a message to the worker to split off a
communicator for this PG on the given stream.

delete_ref(*ref*)[[source]](../_modules/monarch/common/device_mesh.html#RemoteProcessGroup.delete_ref)

drop()[[source]](../_modules/monarch/common/device_mesh.html#RemoteProcessGroup.drop)

size()[[source]](../_modules/monarch/common/device_mesh.html#RemoteProcessGroup.size)

*property*dropped

# Simulation

*class*monarch.Simulator(*hosts*, *gpus*, ***, *simulate_mode=SimulatorBackendMode.SIMULATE*, *trace_mode=SimulatorTraceMode.STREAM_ONLY*, *upload_trace=False*, *trace_path='trace.json'*, *command_history_path='command_history.pkl'*, *group_workers=False*, *build_ir=False*)[[source]](../_modules/monarch/simulator/interface.html#Simulator)

Bases:

monarch.set_meta(*new_value*)[[source]](../_modules/monarch/simulator/config.html#set_meta)

Context manager that sets metadata for simulator tasks created within its scope.

Parameters:

**new_value** - The metadata value to associate with tasks created in this context.

Example:

```
with set_meta("training_phase"):
 # Tasks created here will have "training_phase" metadata
 ...
```

# Builtins

Builtins for Monarch is a set of remote function defintions for PyTorch functions and other utilities.