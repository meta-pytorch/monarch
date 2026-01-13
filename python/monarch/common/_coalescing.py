# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import functools
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
)

import torch
from monarch.common import messages
from monarch.common.fake import fake_call
from monarch.common.function_caching import (
    hashable_tensor_flatten,
    TensorGroup,
    TensorGroupPattern,
)
from monarch.common.tensor import InputChecker, Tensor
from monarch.common.tree import flatten

if TYPE_CHECKING:
    from monarch.common.client import Recorder
    from monarch.common.recording import Recording

    from .client import Client

_coalescing = None


class CoalescingState:
    def __init__(self, recording=False):
        self.controller: Optional["Client"] = None
        self.recorder: Optional["Recorder"] = None
        self.recording = recording

    def set_controller(self, controller: "Client"):
        if self.controller is None:
            self.controller = controller
            controller.flush_deletes(False)
        if self.controller is not controller:
            raise ValueError(
                "using multiple controllers in the same coalescing block is not supported"
            )

    @contextmanager
    def activate(self) -> Generator[None, Any, Any]:
        global _coalescing
        assert _coalescing is None
        finished = False
        try:
            _coalescing = self
            yield
            finished = True
        finally:
            ctrl = self.controller
            if ctrl is not None:
                if finished:
                    ctrl.flush_deletes()
                self.recorder = ctrl.reset_recorder()
                if not finished:
                    self.recorder.abandon()
            _coalescing = None


@contextmanager
def coalescing() -> Generator[None, Any, Any]:
    global _coalescing
    if _coalescing is not None:
        yield
        return

    state = CoalescingState()
    with state.activate():
        yield

    if state.recorder is not None:
        assert state.controller is not None
        state.recorder.run_once(state.controller)


def _record_and_define(
    fn: Callable, args: Tuple[Any, ...], kwargs: Dict[str, Any]
) -> "CacheEntry":
    input_tensors, unflatten_input = flatten(
        (args, kwargs), lambda x: isinstance(x, Tensor)
    )

    with InputChecker.from_flat_args(
        "compile", input_tensors, unflatten_input
    ) as checker:
        checker.check_no_requires_grad()

    for a in input_tensors:
        assert a._seq is not None

    state = CoalescingState(recording=True)
    with state.activate():
        formal_tensors = []
        for i, input in enumerate(input_tensors):
            state.set_controller(input.mesh.client)
            t = Tensor(input._fake, input.mesh, input.stream)
            input.mesh._send(
                messages.RecordingFormal(t, i, t.stream._to_ref(input.mesh.client))
            )
            formal_tensors.append(t)
        formal_args, formal_kwargs = unflatten_input(formal_tensors)
        recorded_result = fn(*formal_args, **formal_kwargs)
        output_tensors, unflatten_result = flatten(
            recorded_result, lambda x: isinstance(x, Tensor)
        )
        with InputChecker(
            output_tensors,
            lambda ts: f"{unflatten_result(ts)} = compiled_function(...)",
        ) as checker:
            checker.check_no_requires_grad()
        for i, output in enumerate(output_tensors):
            state.set_controller(output.mesh.client)
            output.mesh._send(
                messages.RecordingResult(
                    output, i, output.stream._to_ref(output.mesh.client)
                )
            )

    recorder = state.recorder
    if recorder is None:
        # no input tensors or output tensors, so just cache the result
        return CacheEntry(
            TensorGroup([]),
            TensorGroupPattern(()),
            lambda args, kwargs: recorded_result,
            None,
        )

    controller = state.controller
    assert controller is not None
    recorder.add((), output_tensors, [])
    recording = recorder.define_recording(
        controller, len(output_tensors), len(input_tensors)
    )

    fake_uses = [r._fake for r in recording.uses]
    captures_group = TensorGroup(fake_uses)
    inputs_group = TensorGroup([i._fake for i in input_tensors], parent=captures_group)

    outputs_group = TensorGroup([o._fake for o in output_tensors], parent=inputs_group)
    outputs_pattern = outputs_group.pattern

    def run(args, kwargs):
        actuals, _ = flatten((args, kwargs), lambda x: isinstance(x, Tensor))
        for a in actuals:
            assert a._seq is not None

        fake_result_tensors = fake_call(
            outputs_pattern.empty, [fake_uses, [a._fake for a in actuals]]
        )

        # recording.run does permissions checks on all the tensors.
        # if those checks fail then the tensors here will have been created
        # but not defined, causes spurious delete messages.
        # To avoid this, we pass a generator rather than a list
        # and only create the tensors in run
        result_tensors_generator = (
            Tensor(f, o.mesh, o.stream)
            for f, o in zip(fake_result_tensors, output_tensors)
        )
        return unflatten_result(recording.run(result_tensors_generator, actuals))

    return CacheEntry(captures_group, inputs_group.pattern, run, recording)


@dataclass
class CacheEntry:
    captures_group: TensorGroup
    inputs_pattern: TensorGroupPattern
    run: Callable[[Tuple[Any, ...], Dict[str, Any]], Any]
    to_verify: Optional["Recording"]

    def matches(self, input_tensors: List[torch.Tensor]) -> bool:
        # if an input aliases a captured tensor, then we have
        # to check that all future inputs alias the _same exact_
        # captured tensor. These are additional checks after
        # matching on the pattern of aliasing for just the inputs because
        # we do not what the captures would be without first matching the inputs without the captures.
        inputs_group = TensorGroup(input_tensors, parent=self.captures_group)
        return self.inputs_pattern == inputs_group.pattern


def compile(fn=None, verify=True):
    """
    Wraps `fn` such that it records and later replays a single message to workers
    to instruct them to run the entire contents of this function. Since the function invocation
    is much smaller than the original set of messages and since we do not re-execute the python inside
    the function after recording, this has substantially lower latency.

    While eventually `compile` will be backed by `torch.compile`'s dynamo executor, it currently
    works as a simple tracer with the following rules for when it chooses to trace vs when
    it will reuse an existing trace.

    A new trace is created whenever:

    * The _values_ of a non-tensor argument to fn have not been seen before.
    * The _metadata_ of a tensor arguments has not been seen before. Metadata includes the sizes, strides,
      dtype, devices, layout, device meshes, streams, and pattern of aliasing of the arguments
      with respect to other arguments and any values the trace captures.

    A new trace will not be created in these following situations that are known to be **unsafe**:

    * A value that is not an argument to the function but is used by the function (e.g. a global),
      changes in a way that would affect what messages are being sent.
    * A tensor that is not an argument to the function changes metadata, or gets reassigned to
      a new tensor in Python.


    The trace is allowed to use tensors that are referenced in the body but not listed as arguments,
    such as globals or closure-captured locals as long as these values are not modified in the
    the ways that are listed as unsafe above. When switched to a torch.compile backed version,
    these safety caveats will be improved.

    Compilation currently does not work if the inputs or outputs to the function have `requires_grad=True`,
    because we will not generate a correctly backwards pass graph. However, captured tensors
    are allowed to be requires_grad=True, and gradient calculation (forward+backward)
    can run entirely within the function.

    Can be used as a wrapper:
        wrapped = compile(my_function, verify=False)

    Or as a decorator:

        @compile
        def my_function(...):
            ...

        @compile(verify=False)
        def my_function(...):
            ...

    Args:

        fn (callable): the function to be wrapped. (Default: None, in which case we return a single argument,
            function that can be used as a decorator)
        verify (bool): To guard as much as possible against the above unsafe situations,
            if `verify=True`, the first time we would reuse a trace, we additionally do another
            recording and check the second recording matches the original recording, and report
            where they diverge. (Default: True)


    Returns:
        If fn=None, it returns a function that can be used as a decorator on a function to
        be wrapped. Otherwise, it returns the wrapped function itself.

    """
    if fn is None:
        return lambda fn: compile(fn, verify)
    cache: Dict[Any, Recording] = defaultdict(list)

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        global _coalescing
        if _coalescing:
            return fn(*args, **kwargs)

        tensors, shape_key = hashable_tensor_flatten(args, kwargs)
        input_group = TensorGroup([t._fake for t in tensors])
        props = tuple((t.mesh, t.stream, t.requires_grad) for t in tensors)
        key = (shape_key, input_group.pattern, props)
        for entry in cache[key]:
            if entry.matches(input_group.tensors):
                if entry.to_verify is not None:
                    entry.to_verify.client.recorder.verify_against(entry.to_verify)
                    _record_and_define(fn, args, kwargs)
                    entry.to_verify = None
                return entry.run(args, kwargs)

        entry = _record_and_define(fn, args, kwargs)
        if not verify:
            entry.to_verify = None
        cache[key].append(entry)
        return entry.run(args, kwargs)

    return wrapper


def is_active(controller: "Client"):
    if _coalescing is None:
        return False
    _coalescing.set_controller(controller)
    return True


def is_recording(controller: "Client"):
    return is_active(controller) and _coalescing.recording
