# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import importlib
import logging
from contextlib import contextmanager
from typing import Dict, List, Optional, Type, Union

import torch
from monarch.common.process_group import SingleControllerProcessGroupWrapper
from monarch.common.remote import DummyProcessGroup, remote, RemoteProcessGroup
from torch import autograd
from torch.utils._pytree import tree_flatten, tree_unflatten

logger = logging.getLogger(__name__)


def _controller_autograd_function_forward(
    autograd_function_class: Type[autograd.Function],
):
    """
    Decorator for authoring a controller remote function wrapper that wraps an autograd.Function forward.
    Sets up the autograd.function.FunctionCtx() to send over the wire and sets up the original ctx
    with the ctx_tensors and ctx attributes.
    """

    def decorator(func):
        def wrapper(ctx, *args):
            # Need dummy context because cannot pickle autograd.FunctionBackward
            wire_ctx = autograd.function.FunctionCtx()
            # Track arg tensors that have requires_grad
            arg_tensors, _ = tree_flatten(args)
            wire_ctx.args_requires_grads = []
            for i, arg in enumerate(arg_tensors):
                if isinstance(arg, torch.Tensor) and arg.requires_grad:
                    wire_ctx.args_requires_grads.append(i)
            out, ctx_attrs, ctx_tensors = func(
                autograd_function_class.__module__,
                autograd_function_class.__name__,
                wire_ctx,
                *args,
            )
            if ctx is None:
                return out
            ctx.save_for_backward(*ctx_tensors)
            ctx.attr_names = ctx_attrs.keys()
            ctx.pg_names = []
            dim_to_remote_group = {}
            for arg in args:
                if isinstance(arg, RemoteProcessGroup):
                    dim_to_remote_group[arg.dims] = arg
            for name, v in ctx_attrs.items():
                if isinstance(v, DummyProcessGroup):
                    setattr(ctx, name, dim_to_remote_group[v.dims])
                    ctx.pg_names.append(name)
                else:
                    setattr(ctx, name, v)

            return out

        return wrapper

    return decorator


def _controller_autograd_function_backward(
    autograd_function_class: Type[autograd.Function],
):
    """
    Decorator for authoring a controller remote function wrapper that wraps an autograd.Function backward.
    Manually sets up wire_ctx with ctx tensors and attributes.
    """

    def decorator(func):
        def wrapper(ctx, *grad_outputs):
            # Manually set up wire_ctx with ctx tensors and attributes
            wire_ctx = autograd.function.FunctionCtx()
            # send over tensor references with ctx_tensors
            ctx_tensors = ctx.saved_tensors
            wire_ctx.save_for_backward(ctx_tensors)
            for name in ctx.attr_names:
                setattr(wire_ctx, name, getattr(ctx, name))
            process_groups = {name: getattr(ctx, name) for name in ctx.pg_names}

            return func(
                autograd_function_class.__module__,
                autograd_function_class.__name__,
                wire_ctx,
                ctx_tensors,
                # explicitly pass pg to worker
                process_groups,
                *grad_outputs,
            )

        return wrapper

    return decorator


@contextmanager
def manage_grads(list_of_tensors, indices):
    try:
        for i in indices:
            assert list_of_tensors[i].is_leaf, "can't have non-leaf tensors on worker"
            list_of_tensors[i].requires_grad = True
        yield list_of_tensors
    finally:
        for i in indices:
            list_of_tensors[i].requires_grad = False


def worker_autograd_function_forward(
    module_name: str,
    class_name: str,
    ctx: autograd.function.FunctionCtx,
    *args,
    **kwargs,
):
    # Capture initial state of ctx attributes
    before = set()
    before.add("to_save")
    for attr in dir(ctx):
        if not attr.startswith("_"):
            before.add(attr)

    # Set tensors that require grad from additional arg
    flatten_args, spec = tree_flatten(args)
    # pyre-ignore
    with manage_grads(flatten_args, ctx.args_requires_grads) as args_with_grad:
        args = tree_unflatten(args_with_grad, spec)

        # Call the original forward function
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)
        with torch.no_grad():
            out = class_.forward(ctx, *args, **kwargs)

        # Capture state of ctx attributes after the function call
        after = set()
        for attr in dir(ctx):
            if not attr.startswith("_"):
                after.add(attr)
        ctx_attrs = {attr: getattr(ctx, attr) for attr in after - before}
        ctx_attrs["ctx_requires_grads"] = []

        if not hasattr(ctx, "to_save"):
            to_save = []
        else:
            # pyre-ignore
            for idx, t in enumerate(ctx.to_save):
                # generally, workers should not have requires_grad set. Set to correct state after
                # but record requires_grad for next forward
                if isinstance(t, torch.Tensor) and t.requires_grad and t.is_leaf:
                    t.requires_grad = False
                    ctx_attrs["ctx_requires_grads"].append(idx)
            to_save = ctx.to_save
    return out, ctx_attrs, to_save


def worker_autograd_function_backward(
    module_name: str,
    class_name: str,
    ctx: autograd.function.FunctionCtx,
    ctx_tensors: List[torch.Tensor],
    process_groups: Dict[
        str, Union[SingleControllerProcessGroupWrapper, DummyProcessGroup]
    ],
    *grad_outputs: torch.Tensor,
):
    # set correct requires_grad state pre backward
    # pyre-ignore
    with manage_grads(ctx_tensors, ctx.ctx_requires_grads) as ctx_grad_tensors:
        # for i in ctx.ctx_requires_grads:
        #     ctx_tensors[i].requires_grad = True
        if ctx_grad_tensors:
            # pyre-ignore
            ctx.saved_tensors = ctx_grad_tensors
        for name, v in process_groups.items():
            setattr(ctx, name, v)
        # Call the original backward function
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)
        with torch.no_grad():
            out = class_.backward(ctx, *grad_outputs)
    return out


forward_remote_fn = remote(
    "monarch.cached_remote_function.worker_autograd_function_forward"
)

backward_remote_fn = remote(
    "monarch.cached_remote_function.worker_autograd_function_backward"
)


class RemoteAutogradFunction(autograd.Function):
    """
    New autograd.Function (custom forward/backward) that will run on the worker as a UDF RemoteFunction


    Example::
        my_remote_autograd_function = remote_autograd_function(my_custom_autograd_function)
    """

    @staticmethod
    def forward(ctx, *args):
        raise NotImplementedError()

    @staticmethod
    def backward(ctx, *grads):
        raise NotImplementedError()


def remote_autograd_function(
    target_class: Type[autograd.Function], name: Optional[str] = None
) -> Type[RemoteAutogradFunction]:
    """
    Returns a new autograd.Function (custom forward/backward) that will run on the worker as a UDF RemoteFunction
    Logic is done on the controller (e.g., Dtensors set up and saved for backward).
    The autograd.function.FunctionCtx() is sent over the wire to the worker.
    Special handling is done for ctx_tensors, requires_grad fo tensors and process groups.

    Args:
        target_class: autograd.Function class to be run remotely
        name: name of the new autograd.Function to be called on the worker
    """
    if issubclass(target_class, RemoteAutogradFunction):
        logging.warning(
            f"{target_class} is already a autograd.Function UDF! You are likely monkey-patching too many times"
        )
        return target_class
    assert issubclass(target_class, autograd.Function), (
        f"{target_class} is not a torch.autograd.Function!"
    )
    if name is None:
        name = f"Remote_{target_class.__name__}"

    return type(
        name,
        (RemoteAutogradFunction,),
        {
            "forward": staticmethod(
                _controller_autograd_function_forward(target_class)(forward_remote_fn)
            ),
            "backward": staticmethod(
                _controller_autograd_function_backward(target_class)(backward_remote_fn)
            ),
        },
    )
