# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from typing import List

import torch
from monarch.common.function_caching import TensorGroup, TensorGroupPattern
from monarch.common.opaque_ref import OpaqueRef
from monarch.common.remote import call_on_shard_and_fetch, remote
from monarch.common.tensor_factory import TensorFactory
from monarch.common.tree import flatten
from monarch.opaque_object import _fresh_opaque_ref, OpaqueObject
from torch.autograd.graph import get_gradient_edge


def _get_parameters_shape(module: OpaqueRef) -> TensorGroupPattern:
    the_module: torch.nn.Module = module.value
    group = TensorGroup(list(the_module.parameters()))
    return group.pattern


def _get_parameters(module: OpaqueRef) -> List[torch.Tensor]:
    # XXX - we do not want worker tensors refs to have requires grad on,
    # because then any compute will create a backward graph
    # which will never get used.
    # This should be enforced at the worker level, but I think we are
    # hijacking the requires_grad bit to communicate information in
    # the autograd controller wrapper. We need to to use a different
    # side-channel to do that.
    return [p.detach() for p in module.value.parameters()]


def _remote_forward(require_grads: List[bool], module: OpaqueRef, args, kwargs):
    # forward on the worker

    # the parameter tensors inside the module will be require_grad_(True),
    # but the input worker tensors, like all worker state, do not have
    # autograd recording on. We have to turn it on inside just this function
    # to do an autograd pass.

    # parameters has to match what _get_parameters returns for this to work.
    parameters = list(module.value.parameters())
    all_inputs, unflatten_inputs = flatten(
        (args, kwargs, parameters), lambda x: isinstance(x, torch.Tensor)
    )
    # set requires grad on inputs. We skip the parameters because they
    # will already have requires grad set, and we can't detach them
    # here otherwise grad won't flow to them.
    for i in range(len(all_inputs) - len(parameters)):
        if require_grads[i]:
            all_inputs[i] = all_inputs[i].detach().requires_grad_(True)

    # we have to create this just in case the module doesn't actually
    # _use_ the parameter, in which case we have to create the zero.
    # we can't really tell apriori if it will be used or not.
    input_factories = [TensorFactory.from_tensor(t) for t in all_inputs]

    # we have to be careful to save just the autograph graph edges and not
    # the input/output tensors. Otherwise we might keep them longer then they
    # are truly needed.
    all_inputs_require_grad_edges = [
        get_gradient_edge(input) for input, rg in zip(all_inputs, require_grads) if rg
    ]

    args, kwargs, _ = unflatten_inputs(all_inputs)

    # the real module gets called here.
    result = module.value(*args, **kwargs)

    all_outputs_requires_grad, unflatten_outputs = flatten(
        result, lambda x: isinstance(x, torch.Tensor) and x.requires_grad
    )

    all_output_edges = [
        get_gradient_edge(output) for output in all_outputs_requires_grad
    ]

    # this backward closure keeps the state around to invoke backward
    # and is held as the OpaqueRef we return to the controller.
    def backward(all_grad_outputs: List[torch.Tensor]):
        # careful, do not capture any input/output tensors.
        # they might not be required for gradient, and will waste memory.
        with torch.no_grad():
            grad_inputs = torch.autograd.grad(
                inputs=all_inputs_require_grad_edges,
                outputs=all_output_edges,
                grad_outputs=all_grad_outputs,
                allow_unused=True,
            )
        grad_inputs_iter = iter(grad_inputs)
        all_grad_inputs = [
            next(grad_inputs_iter) if rg else None for rg in require_grads
        ]
        for i, rg in enumerate(require_grads):
            # if the grad turned out unused we have to make a zero tensor here
            # because the controller is expecting tensors not None.
            if rg and all_grad_inputs[i] is None:
                all_grad_inputs[i] = input_factories[i].zeros()
        return all_grad_inputs

    # detach outputs, because worker tensors do not keep gradient state
    # the only gradient state on the worker is localized to the backward closure.
    result = unflatten_outputs(t.detach() for t in all_outputs_requires_grad)
    return OpaqueRef(backward), result


def _remote_backward(backward_closure: OpaqueRef, all_grad_outputs: List[torch.Tensor]):
    # this is just a small trampoline that calls the closure that forward defined.
    return backward_closure.value(all_grad_outputs)


class OpaqueModule:
    """
    Provides an _unsafe_ wrapper around a stateful module object that lives on a remote mesh.

        linear = OpaqueModule("torch.nn.Linear", 3, 3, device="cuda")
        output = linear(input, propagate=lambda self, x: x.clone())
        r = output.sum()
        with torch.no_grad():
            r.backward()

    It supports:

    * Accessing parameters of the module on the controller via m.parameters(), which will
    use remote functions to figure out the shape of parameters and get a reference to them.
    * invoking the forward of module by providing inputs and a manual shape propagation function.
        m(input, propagate=lambda self, x: x.clone())
      Trying to do a cached function in this situation is very tricky because of the boundaries
      between autograd/noautograd so it is not implemented yet.
    * calcuating gradients through the module invocation as if this module was a normal controller module.

    In the future we should consider whether we want this to actually be a subclass of torch.nn.Module,
    such that it could have hooks, and other features. If we do this, we need to implement most of
    the existing torch.nn.Module API so that it behaves in the expected way.

    """

    def __init__(self, *args, **kwargs):
        self._object = OpaqueObject(*args, **kwargs)
        # pyrefly: ignore [bad-assignment]
        self._parameters: List[torch.Tensor] = None

    def parameters(self):
        if self._parameters is None:
            tensor_group_pattern = call_on_shard_and_fetch(
                # pyrefly: ignore [bad-argument-type]
                remote(_get_parameters_shape),
                self._object,
            ).result()
            # pyrefly: ignore [bad-assignment]
            self._parameters = [
                p.requires_grad_(True)
                for p in remote(
                    _get_parameters,
                    propagate=lambda self: tensor_group_pattern.empty([]),
                )(self._object)
            ]

        return self._parameters

    def call_method(self, *args, **kwargs):
        return self._object.call_method(*args, **kwargs)

    def __call__(self, *args, propagator, **kwargs):
        parameters = self.parameters()
        # torch.autograd.Function only supports flat lists of input/output tensors
        # so we have to do a bunch of flattenting unflattening to call it
        all_inputs, unflatten_inputs = flatten(
            (args, kwargs, parameters), lambda x: isinstance(x, torch.Tensor)
        )

        # the worker will need to understand which gradients to calculate,
        # which we pass in as a flag array here.
        requires_grad = [t.requires_grad for t in all_inputs]
        if not sum(requires_grad):
            # early exit if we do not have gradients (including toward the parameters)
            return self._object.call_method("__call__", propagator, *args, **kwargs)

        # these will be used to describe the shape of gradients to the inputs,
        # so we cannot use TensorGroup to recover alias information. Having
        # gradient tensors that alias each other coming out of one of this functions
        # will break things.
        input_factories = [TensorFactory.from_tensor(i) for i in all_inputs]

        unflatten_outputs = None
        backward_ctx = None

        # we use this autograd function to define how to hook up the gradient
        # calculated on the worker to the gradient graph _on the client_.

        # This code runs entirely on the client.
        class F(torch.autograd.Function):
            @staticmethod
            def forward(ctx, *all_inputs):
                nonlocal backward_ctx, unflatten_outputs
                args, kwargs, parameters = unflatten_inputs(all_inputs)
                # this remote call invokes the forward pass on the worker.
                # notice it returns the (non-gradient recording result) of the
                # forward pass, and a backward_ctx opaque ref that we will
                # call in the backward pass to flow controller gradients
                # through the worker saved autograd state. Holding
                # backward_ctx alive on the worker is what keeps
                # the worker autograd state alive. We should check there is
                # no funny business with class lifetimes.
                backward_ctx, result = remote(
                    _remote_forward,
                    propagate=lambda requires_grad, obj, args, kwargs: (
                        _fresh_opaque_ref(),
                        propagator(obj, *args, **kwargs),
                    ),
                )(requires_grad, self._object, args, kwargs)

                flat_outputs, unflatten_outputs = flatten(
                    result, lambda x: isinstance(x, torch.Tensor)
                )
                return (*flat_outputs,)

            @staticmethod
            def backward(ctx, *all_grad_outputs):
                # this instructs the worker to propgate output grads back to our input
                # grads, all_grad_inputs has to match all_inputs of forward.
                all_grad_inputs = remote(
                    _remote_backward,
                    propagate=lambda _ctx, _all_grad_outputs: tuple(
                        f.empty() if rg else None
                        for f, rg in zip(input_factories, requires_grad)
                    ),
                    # pyrefly: ignore [bad-argument-type]
                )(backward_ctx, all_grad_outputs)
                return all_grad_inputs

        # apply unwraps the gradient tensors and inserts our custom block.
        flat_outputs = F.apply(*all_inputs)
        # pyrefly: ignore [not-callable]
        result = unflatten_outputs(flat_outputs)
        return result
