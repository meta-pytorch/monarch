# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import math
from contextlib import nullcontext
from functools import partial
from types import CellType, FunctionType
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple, Union

import torch
import torch.autograd.graph
from monarch.common import device_mesh, stream
from monarch.common.tensor import Tensor
from monarch.common.tree import flatten
from monarch.gradient import GradientGenerator as _GradientGenerator
from torch._C._autograd import _get_sequence_nr  # @manual
from torch.autograd.graph import get_gradient_edge, GradientEdge

TensorOrEdge = Union[torch.Tensor, GradientEdge]


class Context(NamedTuple):
    device_mesh: "Optional[device_mesh.DeviceMesh]"
    stream: "stream.Stream"

    def enable(self):
        if device_mesh is None:
            activate_mesh = device_mesh.no_mesh.activate()
        elif self.device_mesh is not device_mesh._active:
            # XXX: something about activating device meshes from this object
            # doesn't work correctly and somehow inactivates the device mesh
            # if it is already enabled. This is a temporary workaround for
            # the demo.
            activate_mesh = self.device_mesh.activate()
        else:
            activate_mesh = nullcontext()
        with activate_mesh, self.stream.activate(), torch.no_grad():
            yield


_sequence_nr_to_context: Dict[int, Context] = {}
_sequence_nr_end = 0


def restore_context(t: Optional[Tensor], sn: Optional[int], last: bool):
    if sn is not None:
        _update_context_map(Context(device_mesh._active, stream._active))
        ctx = _sequence_nr_to_context.pop(sn) if last else _sequence_nr_to_context[sn]
        return ctx.enable()
    if t is not None:
        return Context(t.mesh, t.stream).enable()
    return Context(device_mesh._active, stream._active).enable()


def _update_context_map(ctx: Context):
    global _sequence_nr_end
    next_sequence_nr = _get_sequence_nr()
    for i in range(_sequence_nr_end, next_sequence_nr):
        _sequence_nr_to_context[i] = ctx
    _sequence_nr_end = _get_sequence_nr()


device_mesh._on_change.append(
    lambda old, mesh: _update_context_map(Context(old, stream._active))
)
stream._on_change.append(
    lambda old, stream: _update_context_map(Context(device_mesh._active, old))
)


def grad_generator(
    roots: Union[torch.Tensor, Sequence[TensorOrEdge]] = (),
    with_respect_to: Sequence[TensorOrEdge] = (),
    grad_roots: Sequence[Optional[torch.Tensor]] = (),
):
    if isinstance(roots, torch.Tensor):
        roots = [roots]
    return _GradientGenerator(
        list(roots), list(with_respect_to), list(grad_roots), restore_context
    )


def _gradient_edge(a: TensorOrEdge) -> GradientEdge:
    if isinstance(a, GradientEdge):
        return a
    return get_gradient_edge(a)


class GradGenerator:
    def __init__(self):
        self.roots: List[torch.autograd.graph.GradientEdge] = []
        self.with_respect_to: List[torch.autograd.graph.GradientEdge] = []
        self.grad_roots: List[Optional[torch.Tensor]] = []
        self.unflattens: List[Tuple[int, Any]] = []

    def grad(self, tree: Any):
        tensors, unflatten = flatten(tree, lambda x: isinstance(x, torch.Tensor))
        self.unflattens.append((len(tensors), unflatten))
        self.with_respect_to.extend(_gradient_edge(t) for t in tensors)

    def root(self, r: TensorOrEdge, grad: Optional[torch.Tensor] = None):
        self.roots.append(_gradient_edge(r))
        self.grad_roots.append(grad)

    def __iter__(self):
        gi = _GradientGenerator(
            self.roots,
            list(reversed(self.with_respect_to)),
            self.grad_roots,
            restore_context,
        )
        for n, unflatten in reversed(self.unflattens):
            yield unflatten(reversed([next(gi) for _ in range(n)]))


class GradFunction(torch.autograd.Function):
    @staticmethod
    def forward(ctx, fn, *args, **kwargs):
        result, backward_continuation = fn(*args, **kwargs)
        ctx.backward_continuation = backward_continuation
        values = []
        if backward_continuation.__closure__ is not None:
            for cell in backward_continuation.__closure__:
                values.append(cell.cell_contents)
                cell.cell_contents = None
        tensors, ctx.unflatten = flatten(values, lambda x: isinstance(x, torch.Tensor))
        ctx.save_for_backward(*tensors)
        return result

    @staticmethod
    def backward(ctx, *args, **kwargs):
        closure = tuple(CellType(v) for v in ctx.unflatten(ctx.saved_tensors))
        orig = ctx.backward_continuation
        fn = FunctionType(
            orig.__code__, orig.__globals__, orig.__name__, orig.__defaults__, closure
        )
        output = fn(*args, **kwargs)
        if isinstance(output, tuple):
            return None, *output
        else:
            return None, output


def grad_function(fn):
    return partial(GradFunction.apply, fn)


def gradient_execution_order(
    roots: Sequence[TensorOrEdge], with_respect_to: Sequence[Any]
) -> List[int]:
    """
    Returns the order in which the gradients for `with_respect_to` would become available
    if autograd were run on `roots`. This is the reverse order of each tensors
    first use in the gradient computation.
    """
    with_respect_to = [
        (g.node, g.output_nr) for g in map(_gradient_edge, with_respect_to)
    ]
    min_sequence_nr: Dict[Any, float] = {e: math.inf for e in with_respect_to}

    to_scan = [_gradient_edge(r).node for r in roots]
    scanned = set()
    while to_scan:
        node = to_scan.pop()
        if node in scanned:
            continue
        scanned.add(node)
        for key in node.next_functions:
            nnode = key[0]
            if nnode is None:
                continue
            to_scan.append(nnode)
            value = min_sequence_nr.get(key)
            if value is not None:
                # pyre-ignore
                min_sequence_nr[key] = min(node._sequence_nr(), value)

    return sorted(
        range(len(with_respect_to)),
        key=lambda i: min_sequence_nr[with_respect_to[i]],
        reverse=True,
    )
