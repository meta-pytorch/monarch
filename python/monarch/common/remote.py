# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import functools
import logging
from logging import Logger
from typing import (
    Any,
    Callable,
    cast,
    Dict,
    Generic,
    Literal,
    Optional,
    overload,
    Protocol,
    Tuple,
    TYPE_CHECKING,
    TypeVar,
)

import monarch.common.messages as messages
import torch
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Shape
from monarch._src.actor.actor_mesh import Port
from monarch._src.actor.endpoint import Selection
from monarch._src.actor.future import Future
from monarch.common import _coalescing, device_mesh, stream
from monarch.common.future import Future as OldFuture

if TYPE_CHECKING:
    from monarch.common.client import Client

from monarch._src.actor.endpoint import Endpoint
from monarch.common.device_mesh import RemoteProcessGroup
from monarch.common.fake import fake_call
from monarch.common.function import (
    Propagator,
    resolvable_function,
    ResolvableFunction,
    ResolvableFunctionFromPath,
)
from monarch.common.function_caching import (
    hashable_tensor_flatten,
    tensor_placeholder,
    TensorGroup,
    TensorPlaceholder,
)
from monarch.common.messages import Dims
from monarch.common.tensor import dtensor_check, dtensor_dispatch, InputChecker
from monarch.common.tree import flatten, tree_map
from torch import autograd, distributed as dist
from typing_extensions import ParamSpec

logger: Logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")


class Remote(Generic[P, R], Endpoint[P, R]):
    def __init__(self, impl: Any, propagator_arg: Propagator):
        super().__init__(propagator_arg)
        self._remote_impl = impl

    def _call_name(self) -> Any:
        return self._remote_impl

    def _send(
        self,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        port: "Optional[Port]" = None,
        selection: Selection = "all",
    ) -> Extent:
        ambient_mesh = device_mesh._active
        propagator = self._fetch_propagate
        rfunction = self._maybe_resolvable
        # a None rfunction is an optimization for the identity function (lambda x: x)
        if rfunction is None:
            preprocess_message = None
            rfunction = ResolvableFunctionFromPath("ident")
        else:
            preprocess_message = rfunction
        _, dtensors, mutates, tensor_mesh = dtensor_check(
            propagator, rfunction, args, kwargs, ambient_mesh, stream._active
        )

        if ambient_mesh is None:
            raise ValueError(
                "Calling a 'remote' monarch function requires an active proc_mesh (`with proc_mesh.activate():`)"
            )

        if not ambient_mesh._is_subset_of(tensor_mesh):
            raise ValueError(
                f"The current mesh {ambient_mesh} is not a subset of the mesh on which the tensors being used are defined {tensor_mesh}"
            )

        client: "Client" = ambient_mesh.client
        if _coalescing.is_active(client):
            raise NotImplementedError("NYI: fetching results during a coalescing block")
        stream_ref = stream._active._to_ref(client)

        fut = (port, ambient_mesh._ndslice)

        ident = client.new_node(mutates, dtensors, cast("OldFuture", fut))

        client.send(
            ambient_mesh._ndslice,
            messages.SendValue(
                ident,
                None,
                mutates,
                preprocess_message,
                args,
                kwargs,
                stream_ref,
            ),
        )
        # we have to ask for status updates
        # from workers to be sure they have finished
        # enough work to count this future as finished,
        # and all potential errors have been reported
        client._request_status()
        return Extent(ambient_mesh._labels, ambient_mesh._ndslice.sizes)

    @property
    def _resolvable(self):
        return resolvable_function(self._remote_impl)

    @property
    def _maybe_resolvable(self):
        return None if self._remote_impl is None else self._resolvable

    def _rref(self, args, kwargs):
        return dtensor_dispatch(
            self._resolvable,
            self._propagate,
            args,
            kwargs,
            device_mesh._active,
            stream._active,
        )

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.rref(*args, **kwargs)


# This can't just be Callable because otherwise we are not
# allowed to use type arguments in the return value.
class RemoteIfy(Protocol):
    def __call__(self, function: Callable[P, R]) -> Remote[P, R]: ...


@overload
def remote(
    function: Callable[P, R], *, propagate: Propagator = None
) -> "Remote[P, R]": ...


@overload
def remote(
    function: str, *, propagate: Literal["mocked", "cached", "inspect"] | None = None
) -> "Remote": ...


@overload
def remote(function: str, *, propagate: Callable[P, R]) -> Remote[P, R]: ...


@overload
def remote(*, propagate: Propagator = None) -> RemoteIfy: ...  # type: ignore


# ignore because otherwise it claims that the actual implementation doesn't
# accept the above list of arguments


def remote(function: Any = None, *, propagate: Propagator = None) -> Any:
    if function is None:
        return functools.partial(remote, propagate=propagate)
    return Remote(function, propagate)


remote_identity = Remote(None, lambda x: x)


def call_on_shard_and_fetch(
    remote: Endpoint[P, R], *args, shard: Dict[str, int] | None = None, **kwargs
) -> Future[R]:
    # We have to flatten the tensors twice: first to discover
    # which mesh we are working on to shard it, and then again when doing the
    # dtensor_check in send. This complexity is a consequence of doing
    # implicit inference of the mesh from the tensors.
    dtensors, unflatten = flatten((args, kwargs), lambda x: isinstance(x, torch.Tensor))
    with InputChecker.from_flat_args(
        remote._call_name(), dtensors, unflatten
    ) as checker:
        checker.check_mesh_stream_local(device_mesh._active, stream._active)

        if not hasattr(checker.mesh.client, "_mesh_controller"):
            return cast(
                "Future[R]",
                _old_call_on_shard_and_fetch(
                    cast("Remote[P, R]", remote),
                    *args,
                    shard=shard,
                    **kwargs,
                ),
            )

        selected_slice = checker.mesh._process(shard)
        shard_mesh = checker.mesh._new_with_shape(Shape(["_"], selected_slice))
        with shard_mesh.activate():
            return remote.call_one(*args, **kwargs)


def _old_call_on_shard_and_fetch(
    remote_obj: Remote[P, R],
    /,
    *args: object,
    shard: dict[str, int] | None = None,
    **kwargs: object,
) -> OldFuture[R]:
    """
    Call `function` at the coordinates `shard` of the current device mesh, and retrieve the result as a Future.
        function - the remote function to call
        *args/**kwargs - arguments to the function
        shard - a dictionary from mesh dimension name to coordinate of the shard
                If None, this will fetch from coordinate 0 for all dimensions (useful after all_reduce/all_gather)
    """

    rfunction = remote_obj._maybe_resolvable
    propagator = remote_obj._fetch_propagate
    ambient_mesh = device_mesh._active

    if rfunction is None:
        preprocess_message = None
        rfunction = ResolvableFunctionFromPath("ident")
    else:
        preprocess_message = rfunction
    _, dtensors, mutates, mesh = dtensor_check(
        propagator, rfunction, args, kwargs, ambient_mesh, stream._active
    )

    client: "Client" = mesh.client
    if _coalescing.is_active(client):
        raise NotImplementedError("NYI: fetching results during a coalescing block")
    stream_ref = stream._active._to_ref(client)
    return client.fetch(
        mesh, stream_ref, shard, preprocess_message, args, kwargs, mutates, dtensors
    )


@remote
def _propagate(
    function: ResolvableFunction, args: Tuple[Any, ...], kwargs: Dict[str, Any]
):
    """
    RF preprocess function
    """
    fn = function.resolve()

    # XXX - in addition to the functional properties,
    # and info about if any of the input tensors got mutated.
    arg_tensors, _ = flatten((args, kwargs), lambda x: isinstance(x, torch.Tensor))
    input_group = TensorGroup(arg_tensors)
    result = fn(*args, **kwargs)
    result_tensors, unflatten_result = flatten(
        result, lambda x: isinstance(x, torch.Tensor)
    )

    output_group = TensorGroup(result_tensors, parent=input_group)

    the_result = unflatten_result([tensor_placeholder for _ in result_tensors])
    return (
        the_result,
        output_group.pattern,
    )


class DummyProcessGroup(dist.ProcessGroup):
    def __init__(self, dims: Dims, world_size: int):
        # pyre-ignore
        super().__init__(0, world_size)
        self.dims = dims
        self.world_size = world_size

    def allreduce(self, tensor, op=dist.ReduceOp.SUM, async_op=False):
        class DummyWork:
            def wait(self):
                return tensor

        return DummyWork()

    def _allgather_base(self, output_tensor, input_tensor, opts):
        class DummyWork:
            def wait(self):
                return output_tensor

        return DummyWork()

    def _reduce_scatter_base(self, output_tensor, input_tensor, opts):
        class DummyWork:
            def wait(self):
                return output_tensor

        return DummyWork()

    def __getstate__(self):
        return {"dims": self.dims, "world_size": self.world_size}

    def __setstate__(self, state):
        self.__init__(state["dims"], state["world_size"])


def _mock_pgs(x):
    if isinstance(x, autograd.function.FunctionCtx):
        for attr in dir(x):
            if not attr.startswith("__") and isinstance(attr, RemoteProcessGroup):
                setattr(x, attr, DummyProcessGroup(attr.dims, attr.size()))
        return x
    if isinstance(x, RemoteProcessGroup):
        return DummyProcessGroup(x.dims, x.size())
    return x


# for testing
_miss = 0
_hit = 0


def _cached_propagation(_cache, rfunction: ResolvableFunction, args, kwargs):
    tensors, shape_key = hashable_tensor_flatten(args, kwargs)
    # pyre-ignore
    inputs_group = TensorGroup([t._fake for t in tensors])
    requires_grads = tuple(t.requires_grad for t in tensors)
    key = (shape_key, inputs_group.pattern, requires_grads)

    global _miss, _hit
    if key not in _cache:
        _miss += 1
        args_no_pg, kwargs_no_pg = tree_map(_mock_pgs, (args, kwargs))
        result_with_placeholders, output_pattern = call_on_shard_and_fetch(
            _propagate, function=rfunction, args=args_no_pg, kwargs=kwargs_no_pg
        ).result()

        _, unflatten_result = flatten(
            result_with_placeholders, lambda x: isinstance(x, TensorPlaceholder)
        )
        _cache[key] = (unflatten_result, output_pattern)
    else:
        _hit += 1
    # return fresh fake result every time to avoid spurious aliasing
    unflatten_result, output_pattern = _cache[key]

    output_tensors = fake_call(output_pattern.empty, [inputs_group.tensors])
    return unflatten_result(output_tensors)
