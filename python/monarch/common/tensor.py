# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import itertools
import re
import traceback
import typing
import warnings
from collections import defaultdict
from typing import (
    Any,
    Callable,
    cast,
    Dict,
    Iterable,
    List,
    Literal,
    NamedTuple,
    Optional,
    runtime_checkable,
    Sequence,
    TYPE_CHECKING,
    TypeVar,
    Union,
)

import torch
import torch._ops
from monarch.common._C import get_built_pytorch_version
from monarch.common.function import ResolvableFunctionFromPath
from torch._subclasses.fake_tensor import FakeTensor
from torch.utils._pytree import tree_map

from . import messages, stream
from .base_tensor import BaseTensor
from .borrows import StorageAliases

if TYPE_CHECKING:
    from monarch.common.device_mesh import DeviceMesh

from monarch._src.actor.shape import NDSlice

from .fake import fake_call
from .function import Propagator, ResolvableFunction
from .invocation import Invocation
from .messages import Dims
from .reference import Referenceable
from .stream import Stream
from .tree import flatten

_valid_reduce = Literal[
    "stack", "sum", "avg", "product", "min", "max", "band", "bor", "bxor"
]

T = TypeVar("T")


@runtime_checkable
class HasDeviceMesh(typing.Protocol):
    @property
    def _device_mesh(self) -> "DeviceMesh": ...


class DropLocation(NamedTuple):
    tensor_id: int
    traceback: List[traceback.FrameSummary]

    def __repr__(self) -> str:
        return f"tensor {self.tensor_id} is dropped at: \n" + "".join(
            traceback.format_list(self.traceback)
        )


class Tensor(Referenceable, BaseTensor):
    """A distributed tensor for distributed computation across device meshes.

    Tensor represents a distributed tensor that spans across multiple devices
    in a device mesh. It provides the same interface as PyTorch tensors but
    enables distributed operations and communication patterns.

    """

    # pyre-fixme[13]: Attribute `stream` is never initialized.
    stream: Stream
    # pyre-fixme[13]: Attribute `mesh` is never initialized.
    mesh: "DeviceMesh"
    ref: Optional[int]
    # pyre-fixme[13]: Attribute `_invocation` is never initialized.
    _invocation: Optional[Invocation]
    # pyre-fixme[13]: Attribute `_fake` is never initialized.
    _fake: torch.Tensor
    # pyre-fixme[13]: Attribute `_aliases` is never initialized.
    _aliases: StorageAliases
    # pyre-fixme[13]: Attribute `_on_first_use` is never initialized.
    _on_first_use: Optional[Callable]
    # pyre-fixme[13]: Attribute `_drop_location` is never initialized.
    _drop_location: Optional[DropLocation]
    # _seq represents the sequence number of the concrete invocation that
    # created this tensor, or the most recent invocation that mutated it.
    # Unlike the _invocation field, this will be set for both the rust and
    # python backends.
    # pyre-fixme[13]: Attribute `_seq` is never initialized.
    _seq: Optional[int]

    def __new__(cls, fake: torch.Tensor, mesh: "DeviceMesh", stream: "Stream"):
        # pyre-ignore[16]
        r = torch.Tensor._make_wrapper_subclass(
            cls,
            fake.size(),
            strides=fake.stride(),
            storage_offset=fake.storage_offset(),
            device=fake.device,  # This is the device of of either input tensor or first tensor of a list
            dtype=fake.dtype,
            layout=fake.layout,
            requires_grad=fake.requires_grad,
        )
        assert isinstance(fake, FakeTensor)
        r._fake = fake
        client = mesh.client
        r.ref = client.new_ref()
        r.mesh = mesh
        r.stream = stream

        storage = fake.untyped_storage()
        client = mesh.client
        if storage not in client.aliases:
            client.aliases[storage] = StorageAliases()
        r._aliases = client.aliases[storage]
        r._aliases.register(r)
        r._invocation = None
        r._on_first_use = None
        r._drop_location = None
        r._seq = None
        return r

    @classmethod
    def __torch_dispatch__(cls, func, types, args=(), kwargs=None):
        from monarch.common.remote import remote

        # device_mesh <-> tensor <-> remote are mututally recursive
        # we break the dependency to allow for separate files by
        # having device_mesh and tensor locally import the `remote`
        # entrypoint
        return remote(func, propagate=func)(*args, **kwargs)

    def __init__(
        self,
        fake: Optional[torch.Tensor] = None,
        mesh: Optional["DeviceMesh"] = None,
        stream: Optional[Stream] = None,
    ):
        pass

    def __repr__(self, *, tensor_contents=None):
        return f"monarch.Tensor(mesh={self.mesh}, stream={self.stream}, fake={repr(self._fake)})"

    def drop(self):
        if self.ref is None:
            return

        for alias in self._aliases.aliases:
            alias._drop_ref()

        # we should be in the tensors list as well
        assert self.ref is None

    @property
    def dropped(self):
        return self.ref is None

    def _drop_ref(self):
        if self.ref is None:
            return
        self.delete_ref(self.ref)
        self._drop_location = DropLocation(self.ref, traceback.extract_stack())
        self.ref = None

    @property
    def _access_permissions(self):
        return self._aliases.access

    def _use(self):
        if self._on_first_use:
            self._on_first_use(self)
            self._on_first_use = None

    def to_mesh(
        self,
        mesh: Union["DeviceMesh", "HasDeviceMesh"],
        stream: Optional["Stream"] = None,
    ):
        """
        Move data between one device mesh and another. Sizes of named dimensions must match.
        If mesh has dimensions that self.mesh does not, it will broadcast to those dimensions.


        broadcast:
            t.slice_mesh(batch=0).to_mesh(t.mesh)

        """
        if isinstance(mesh, HasDeviceMesh):
            mesh = mesh._device_mesh
        return MeshSliceTensor(self, self.mesh).to_mesh(mesh, stream)

    def reduce_(
        self,
        dims: Dims | str,
        reduction: _valid_reduce = "sum",
        scatter=False,
        mesh=None,
    ):
        return self.reduce(dims, reduction, scatter, mesh, _inplace=True)

    def reduce(
        self,
        dims: Dims | str,
        reduction: _valid_reduce = "sum",
        scatter: bool = False,
        mesh: Optional["DeviceMesh"] = None,
        _inplace: bool = False,
        out: Optional["Tensor"] = None,
    ):
        """
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


        Args:
            dims (Dims | str): The dimensions along which to perform the reduction.
            reduction (_valid_reduce): The type of reduction to perform. Defaults to "sum".
            scatter (bool): If True, the local result tensor will be evenly split across dimensions.
                Defaults to False.
            mesh (Optional["DeviceMesh"], optional): The target mesh to move the data to.
                If None, uses self.mesh. Defaults to None.
            _inplace (bool): If True, performs the operation in-place. Defaults to False.
                Note that not all the reduction operations support in-place.
            out (Optional["Tensor"]): The output tensor to store the result. If None, a new tensor
                will be created on the stream where the reduce operation executes. Defaults to None.

        Returns:
            Tensor: The result of the reduction operation.
        """
        if mesh is not None:
            raise NotImplementedError()
        if isinstance(dims, str):
            dims = (dims,)
        for d in dims:
            if d not in self.mesh.names:
                raise KeyError(f"dim {d} not found in {self.mesh}")
        if len(dims) == 0:
            dims = self.mesh.names
        if len(set(dims)) != len(dims):
            raise ValueError(f"reducing the same dimension twice: {dims}")
        if len(dims) > 1:
            if reduction == "stack" or scatter:
                raise ValueError(
                    f"reduction {reduction} or scatter = {scatter} is not valid for multiple dimensions"
                )
        if reduction not in _valid_reduce.__args__:
            raise ValueError(
                f"reduction {reduction} not supported, reductions are {_valid_reduce.__args__}"
            )

        if mesh is None:
            mesh = self.mesh

        ts: List[torch.Tensor] = [self]
        if out is not None:
            ts.append(out)
        with InputChecker(
            ts,
            lambda ts: (
                f"reduce({next(ts)}, {dims}, reduction={reduction}, out={next(ts, None)})"
            ),
        ) as checker:
            checker.check_no_requires_grad()
            checker.check_cuda()
            checker.check_mesh_stream_local(self.mesh, stream._active)
            checker.check_permission((out,) if out is not None else ())

        if _inplace:
            if out is not None:
                raise ValueError("`out` cannot be used with inplace reduce.")
            inplace_valid = (reduction == "gather" and scatter) or not scatter
            if not inplace_valid:
                raise ValueError(
                    f"reduction {reduction} is not valid for in-place operation because "
                    "the output size will not match the input size."
                )
            fake_output = self._fake
        else:
            N = (
                self.mesh.processes.sizes[self.mesh.names.index(dims[0])]
                if reduction == "stack" or scatter
                else -1
            )

            fake_output = fake_call(
                _fake_reduce, self._fake, self.mesh, N, reduction, scatter
            )
            if out is not None:
                if out.shape != fake_output.shape:
                    raise ValueError(
                        f"The given output shape, {out.shape}, is incorrect. "
                        f"Reduce expects the shape to be {fake_output.shape}."
                    )
                fake_output = out._fake

        r = Tensor(fake_output, self.mesh, self.stream)
        assert r.ref is not None
        self.mesh.define_remotely()
        defines = (r,) if out is None else (r, out)
        self.mesh.client.new_node(defines, (self,))
        self.mesh.client.backend_network_init()
        self.mesh.client.split_comm(dims, self.mesh, self.stream._to_ref(mesh.client))
        self.mesh._send(
            messages.Reduce(
                r,
                self,
                self._factory(),
                self.mesh,
                self.stream._to_ref(mesh.client),
                dims,
                reduction,
                scatter,
                _inplace,
                out,
            )
        )
        return r

    def slice_mesh(self, **kwargs: Union[int, slice]) -> "MeshSliceTensor":
        # technically a slice of a device mesh and a device mesh are not same thing
        # because a device mesh also has caches for doing collectives.
        # but this is an easy way to create a MeshSliceTensor until we optimize
        # how we represent mesh slices.
        slicing = self.mesh.slice(**kwargs)
        return MeshSliceTensor(self, slicing)

    def delete_ref(self, ref: int):
        mesh = self.mesh
        if not mesh.client.has_shutdown:
            self._aliases.unregister(self)
        mesh.client.delete_ref(mesh, ref)

    def _factory(self):
        return messages.TensorFactory.from_tensor(self._fake)


class MeshSliceTensor:
    def __init__(self, tensor: "Tensor", slicing: "DeviceMesh"):
        self.tensor = tensor
        self.slicing = slicing

    def to_mesh(
        self,
        mesh: Union["DeviceMesh", "HasDeviceMesh"],
        stream: Optional["Stream"] = None,
    ) -> "Tensor":
        if isinstance(mesh, HasDeviceMesh):
            mesh = mesh._device_mesh

        if stream is None:
            stream = self.tensor.stream

        with InputChecker(
            [self.tensor], lambda ts: f"{next(ts)}.to_mesh({mesh})"
        ) as checker:
            checker.check_no_requires_grad()
            checker.check_cuda()
            checker.check_permission(mutated_tensors=())

        sizes = []
        strides = []
        broadcast_dims = []
        for name, size in zip(mesh.names, mesh.processes.sizes):
            if name not in self.slicing.names:
                broadcast_dims.append(name)
                warnings.warn(
                    f"to_mesh is broadcasting along {name} dimension."
                    "This is implemented inefficiently and should only be used for initialization before it is fixed.",
                    stacklevel=2,
                )
                continue
            index = self.slicing.names.index(name)
            if self.slicing.processes.sizes[index] != size:
                raise ValueError(
                    f"dimension {name} of destination device_mesh has a different length than the source tensor"
                )
            sizes.append(size)
            strides.append(self.slicing.processes.strides[index])

        if len(sizes) != len(self.slicing.names):
            missing = set(self.slicing.names) - set(mesh.names)
            raise ValueError(f"destination mesh does not have dimensions {missing}")

        # Optimized algorithm where:
        # 1. We can represent submeshes as NDSlice(offet, sizes, strides) on rank.
        # 2. A message can be efficiently broadcast to List[NDSlice] ranks by a smart tree based algorithm that can
        #    figure out which subtrees need the message.
        # 3. The message itself will uses List[NDSlice] objects to express the send/recv set and so it is very small

        # so basically both the way the message is broadcast and its size will be compressed but the
        # send pattern and the meaning of the message will be the same as this ineffiecient form

        from_ranks = NDSlice(
            offset=self.slicing.processes.offset, sizes=sizes, strides=strides
        )
        # Ensure the destination mesh is defined remotely so it has a proper ref
        mesh.define_remotely()
        r = Tensor(fake_call(self.tensor._fake.clone), mesh, stream)
        assert r.ref is not None
        client = self.tensor.mesh.client
        from_stream_ref = self.tensor.stream._to_ref(client)
        to_stream_ref = stream._to_ref(client)
        client.backend_network_init()
        client.backend_network_point_to_point_init(from_stream_ref, to_stream_ref)
        client.new_node((r,), (self.tensor,))

        if broadcast_dims:
            mesh_sizes = mesh.sizes
            dim_sequences = [
                zip(itertools.repeat(dim), range(mesh_sizes[dim]))
                for dim in broadcast_dims
            ]
            destinations = [
                mesh.slice(**dict(dim_settings)).processes
                for dim_settings in itertools.product(*dim_sequences)
            ]
        else:
            destinations = [mesh.processes]

        for to_ranks in destinations:
            client.send(
                [from_ranks, to_ranks],
                messages.SendTensor(
                    r,
                    from_ranks,
                    to_ranks,
                    self.tensor,
                    self.tensor._factory(),
                    from_stream_ref,
                    to_stream_ref,
                ),
            )
        return r


def _fake_reduce(
    tensor, source_mesh: "DeviceMesh", group_size: int, reduction, scatter: bool
):
    if scatter:
        if tensor.ndim == 0 or tensor.size(0) != group_size:
            raise TypeError(
                f"When scattering results the outer most dimension of tensor with sizes ({list(tensor.size())} must match the size ({group_size})"
            )
        if reduction == "stack":
            # scatter removes a dimension of mesh size
            # but gather adds the dimension back
            return tensor
        return tensor.sum(dim=0)
    else:
        if reduction == "stack":
            return torch.empty(
                [group_size, *tensor.shape],
                dtype=tensor.dtype,
                device=tensor.device,
                layout=tensor.layout,
            )
        return tensor.add(tensor)


_explain = """\
LOCAL_TENSOR
This tensor is a local (non-distributed) tensor being used while a device_mesh is active.
If you want to do local tensor compute use `with no_mesh.activate():`

WRONG_MESH
This tensor is on a device mesh that is not the current device_mesh.
Use `with m.activate():` to switch the active mesh, or move the tensor to the correct device mesh with `to_mesh`/`on_mesh`.

WRONG_STREAM
This tensor is on a stream that is not the current active stream. Use with `stream.activate()` to switch streams, or
move the tensor to the correct stream with `.borrow`.

DROPPED
This tensor, or a view of it, was explicitly deleted with the t.drop() function and is no longer usable.

BORROWED
This tensor cannot be read because it is being used mutably in another stream.

MUTATING_BORROW
This tensor would be mutated by this operator but it is read only because it is being borrowed.

REQUIRES_GRAD
This tensor requires gradients but this operation does not work with autograd.

CROSS_DEVICE_REQUIRES_CUDA
Operations that send tensors across devices currently require CUDA tensors.
"""

explain = {}
for entry in _explain.split("\n\n"):
    lines = entry.split("\n")
    explain[lines[0]] = "".join(f"  {l}\n" for l in lines)


def handle_lift_fresh_dispatch(
    propagate, rfunction, args, kwargs, ambient_mesh, stream
):
    assert ambient_mesh is not None
    fake_result = fake_call(
        torch.zeros, args[0].shape, device=args[0].device, dtype=args[0].dtype
    )
    return fake_result, (), (), ambient_mesh


special_ops_handler = {"torch.ops.aten.lift_fresh.default": handle_lift_fresh_dispatch}


class _Symbol(NamedTuple):
    name: str

    def __repr__(self):
        return self.name


class InputChecker:
    @staticmethod
    def from_flat_args(func: Any, tensors: Sequence[torch.Tensor], unflatten: Callable):
        def format(tensor_values: Iterable[str]):
            args, kwargs = unflatten(tensor_values)
            actuals = ", ".join(
                itertools.chain(
                    map(repr, args),
                    (f"{key}={repr(value)}" for key, value in kwargs.items()),
                )
            )
            return f"{func}({actuals})"

        return InputChecker(tensors, format)

    def __init__(
        self, tensors: Sequence[torch.Tensor], format: Callable[[Iterable[Any]], str]
    ):
        self.tensors = tensors
        self.format = format
        self.errors: Dict[torch.Tensor, List[str]] = defaultdict(list)
        self.overall_errors = []
        # we set this here just so we have stream to report as the current
        # stream in errors where the stream does not matter.
        # If the stream matters for this call, we
        # get the right stream in `check_stream`.
        self.stream = stream._active
        self._mesh = None

    def check_mesh_stream_local(
        self, ambient_mesh: Optional["DeviceMesh"], stream: "Stream"
    ):
        self.stream = stream
        for t in self.tensors:
            if isinstance(t, Tensor):
                self._mesh = t.mesh
                break
        if self._mesh is None:
            self._mesh = ambient_mesh
        if self._mesh is None:
            self.overall_errors.append(
                "Remote functions require an active device mesh, use `with mesh.activate():`"
            )

        for t in self.tensors:
            if isinstance(t, Tensor):
                if t.mesh is not self._mesh:
                    self.errors[t].append(explain["WRONG_MESH"])
                if t.stream is not self.stream:
                    self.errors[t].append(explain["WRONG_STREAM"])
            else:
                self.errors[t].append(explain["LOCAL_TENSOR"])

    @property
    def mesh(self) -> "DeviceMesh":
        assert self._mesh is not None
        return self._mesh

    def raise_current_errors(self):
        if not self.errors and not self.overall_errors:
            return
        error_info: List[str] = [
            f"active_mesh = {self._mesh}\n",
            f"active_stream = {self.stream}\n",
            *self.overall_errors,
        ]
        error_names: Dict["Tensor", "str"] = {}
        for i, (t, errors) in enumerate(self.errors.items()):
            name = f"ERROR_{i}"
            error_names[t] = name
            error_info.append(f"{name}:\n")
            error_info.extend(errors)

        call = self.format(_Symbol(error_names.get(t, ".")) for t in self.tensors)
        msg = f"Incorrect arguments to monarch operation:\n\n  {call}\n\n{''.join(error_info)}"
        raise TypeError(msg)

    def _borrow_tracebacks(self, t: Tensor):
        lines = []
        for b in t._aliases.live_borrows:
            lines.append("  Traceback of borrow (most recent frame last):\n")
            lines.extend(f"  {line}\n" for line in b.traceback_string.split("\n"))
        return lines

    def check_permission(self, mutated_tensors: Sequence["Tensor"]):
        for t in self.tensors:
            if not isinstance(t, Tensor):
                continue
            if "r" not in t._access_permissions:
                errors = self.errors[t]
                errors.append(explain["BORROWED"])
                errors.extend(self._borrow_tracebacks(t))
            if t.dropped:
                self.errors[t].append(explain["DROPPED"])
                if t._drop_location:
                    self.errors[t].append(str(t._drop_location))

        for t in mutated_tensors:
            if "w" not in t._access_permissions:
                errors = self.errors[t]
                errors.append(explain["MUTATING_BORROW"])
                errors.extend(self._borrow_tracebacks(t))

    def check_no_requires_grad(self):
        for t in self.tensors:
            if torch.is_grad_enabled() and t.requires_grad:
                self.errors[t].append(explain["REQUIRES_GRAD"])

    def check_cuda(self):
        for t in self.tensors:
            if not t.is_cuda:
                self.errors[t].append(explain["CROSS_DEVICE_REQUIRES_CUDA"])

    def __enter__(self) -> "InputChecker":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            return
        self.raise_current_errors()


def dtensor_check(
    propagate: "Propagator",
    rfunc: "ResolvableFunction",
    args,
    kwargs,
    ambient_mesh: Optional["DeviceMesh"],
    stream: Stream,
):
    dtensors, unflatten = flatten((args, kwargs), lambda x: isinstance(x, torch.Tensor))
    with InputChecker.from_flat_args(rfunc, dtensors, unflatten) as checker:
        checker.check_mesh_stream_local(ambient_mesh, stream)

        # ensure tensors are correct enough to do propagation with them.
        checker.raise_current_errors()

        # the distinction is we only check permissions on the first level mutates
        # but have to record error-tracking dependency edges for all parent borrows.

        # future diff will change how we track this and then simplify this code.

        mutates = []
        fake_input_tensors = [d._fake for d in dtensors]
        before_versions = [f._version for f in fake_input_tensors]
        fake_args, fake_kwargs = unflatten(fake_input_tensors)
        result = propagate(args, kwargs, fake_args, fake_kwargs)
        for i in range(len(dtensors)):
            if before_versions[i] < fake_input_tensors[i]._version:
                mutates.extend(dtensors[i]._aliases.aliases)
        checker.check_permission(mutates)

    return result, dtensors, tuple(mutates), checker.mesh


def dtensor_dispatch(
    rfunction: ResolvableFunction,
    propagate: Propagator,
    args,
    kwargs,
    ambient_mesh: Optional["DeviceMesh"],
    stream: Stream,
):
    from .device_mesh import RemoteProcessGroup

    op_handler = dtensor_check
    if isinstance(rfunction, ResolvableFunctionFromPath):
        op_handler = special_ops_handler.get(rfunction.path, dtensor_check)

    fake_result, dtensors, mutates, device_mesh = op_handler(
        propagate, rfunction, args, kwargs, ambient_mesh, stream
    )
    assert device_mesh is not None

    fake_result_dtensors, unflatten_result = flatten(
        fake_result, lambda x: isinstance(x, torch.Tensor)
    )
    result_dtensors = tuple(
        Tensor(fake, device_mesh, stream) for fake in fake_result_dtensors
    )
    seq = device_mesh.client.new_node(result_dtensors + mutates, dtensors)
    assert all(t.ref is not None for t in result_dtensors)
    assert all(t.ref is not None for t in mutates)
    result = result_msg = unflatten_result(result_dtensors)
    if len(result_dtensors) == 0:
        result_msg = None

    # note the device mesh has to be defined regardles so the remote functions
    # can invoke device_mesh.rank("...")
    device_mesh.define_remotely()

    # if there's a process group anywhere in the args, kwargs we need to initialize the backend network
    # if it hasn't already been done.
    process_groups, _ = flatten(
        (args, kwargs), lambda x: isinstance(x, RemoteProcessGroup)
    )
    if len(process_groups) > 0:
        device_mesh.client.backend_network_init()
        for pg in process_groups:
            assert not pg.dropped
            pg.ensure_split_comm_remotely(stream._to_ref(device_mesh.client))

    device_mesh._send(
        messages.CallFunction(
            seq,
            result_msg,
            tuple(mutates),
            rfunction,
            args,
            kwargs,
            stream._to_ref(device_mesh.client),
            device_mesh,
            process_groups,
        )
    )
    # XXX - realistically this would be done on a non-python thread, keeping our messages up to date
    # but we can approximate it by checking for all ready meassages whenever we schedule new work
    while device_mesh.client.handle_next_message(0):
        pass
    return result


def reduce(
    tensors: T,
    dims: Dims | str,
    reduction: _valid_reduce = "sum",
    scatter: bool = False,
    mesh: Optional["DeviceMesh"] = None,
    _inplace: bool = False,
) -> T:
    """
    Performs the tensor reduction operation for each tensor in tensors.
    Args:
        tensors (pytree["Tensor"]): The pytree of input tensors to reduce.
        dims (Dims | str): The dimensions along which to perform the reduction.
        reduction (_valid_reduce): The type of reduction to perform. Defaults to "sum".
        scatter (bool): If True, the local result tensor will be evenly split across dimensions.
            Defaults to False.
        mesh (Optional["DeviceMesh"], optional): The target mesh to move the data to.
            If None, uses self.mesh. Defaults to None.
        _inplace (bool): If True, performs the operation in-place. Defaults to False.
            Note that not all the reduction operations support in-place.
    """

    def _reduce(tensor: "Tensor") -> "Tensor":
        return tensor.reduce(dims, reduction, scatter, mesh, _inplace)

    return tree_map(_reduce, tensors)


def reduce_(
    tensors: T,
    dims: Dims | str,
    reduction: _valid_reduce = "sum",
    scatter: bool = False,
    mesh: Optional["DeviceMesh"] = None,
) -> T:
    return reduce(tensors, dims, reduction, scatter, mesh, _inplace=True)


def check_torch_version() -> None:
    """
    Check if the runtime PyTorch version matches the build-time PyTorch version.

    This check is necessary because if the runtime PyTorch version is different from the build-time PyTorch version,
    it may cause unexpected behavior or errors, especially when using the monarch library to perform tensor operations.

    The check is performed by comparing the major, minor, and patch versions of the runtime PyTorch version
    with the build-time PyTorch version. If they do not match, a RuntimeError is raised.
    """
    # torch version as a string may contain optional suffixes like "+cu128", and
    # we can only compare the major/minor/patch version. Parse into a tuple of
    # integers.
    runtime_torch_version = torch.__version__.split(".")
    runtime_torch_major = int(runtime_torch_version[0])
    runtime_torch_minor = int(runtime_torch_version[1])
    # Exclude any suffix past the patch version.
    runtime_torch_patch = runtime_torch_version[2]
    if match := re.search("[0-9]+", runtime_torch_patch):
        runtime_torch_patch = int(match.group(0))
    else:
        raise RuntimeError(
            f"Could not parse patch version from torch version {torch.__version__}"
        )
    runtime_torch_version = (
        runtime_torch_major,
        runtime_torch_minor,
        runtime_torch_patch,
    )
    if runtime_torch_version != get_built_pytorch_version():
        raise RuntimeError(
            f"PyTorch version mismatch: runtime {runtime_torch_version} != build-time {get_built_pytorch_version()}. "
            "Make sure to download a version of monarch that matches your PyTorch version. "
            "This matters because you were using a feature that requires a matched libtorch.so symbol."
        )
