# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import logging
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, TYPE_CHECKING

import torch.fx
from monarch.common.messages import DependentOnError
from monarch.common.tree import tree_map
from torch.fx.proxy import GraphAppendingTracer

from .lines import Lines

if TYPE_CHECKING:
    from .worker import Cell, Stream

logger = logging.getLogger(__name__)


class Symbol:
    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return self.name


@dataclass
class ErrorContext:
    ident: Optional[int]
    fallback_resume_offset: int


class _BlockTracer(GraphAppendingTracer):
    def __init__(self, ctx: "ErrorContext", graph: torch.fx.Graph):
        super().__init__(graph)
        self._error_context = ctx

    def create_node(self, *args, **kwargs):
        n = super().create_node(*args, **kwargs)
        n.context = self._error_context
        return n


class CompiledBlock:
    def __init__(self):
        self.graphs: Dict["Stream", torch.fx.Graph] = defaultdict(
            lambda: torch.fx.Graph()
        )
        self.used_formals: Set[int] = set()
        self.used_results: Set[int] = set()
        self.results: Dict[torch.fx.Node, int] = {}
        self.fallback: Dict["Stream", List[Callable]] = defaultdict(list)
        self.recording_stream: Optional["Stream"] = None
        self.defined_borrows = {}  # dict not set to preserve order
        self.defined_cells: Dict["Cell", int] = {}  # dict not set to preserve order
        self.mutated_cells: Dict["Cell", "Stream"] = {}
        self.current_context = ErrorContext(None, 0)
        self.impls: Dict["Stream", Callable] = {}

    def call_function(self, *args, **kwargs):
        n = self.recording_graph.call_function(*args, **kwargs)
        n.context = self.current_context
        return n

    def define_formal(self, stream: "Stream", argument_index: int):
        self.used_formals.add(argument_index)
        n = self.graphs[stream].call_module("formal", (argument_index,))
        n.context = self.current_context
        return n

    def define_result(self, node: torch.fx.Node, output_index: int):
        self.used_results.add(output_index)
        self.results[node] = output_index

    def input_cell(self, cell: "Cell"):
        n = self.recording_graph.call_module("input_cell", (cell,))
        n.context = self.current_context
        return n

    def proxy(self, n: torch.fx.Node):
        return torch.fx.Proxy(n, _BlockTracer(self.current_context, n.graph))

    def mutates(self, results: Sequence["Cell"]):
        for r in results:
            if r not in self.defined_cells:
                assert self.recording_stream is not None
                self.mutated_cells[r] = self.recording_stream

    @property
    def recording_graph(self):
        # pyrefly: ignore [bad-index]
        return self.graphs[self.recording_stream]

    @contextmanager
    def record_to(self, stream: "Stream"):
        orig, self.recording_stream = self.recording_stream, stream
        ctx = ErrorContext(None, len(self.fallback[stream]))
        orig_context, self.current_context = self.current_context, ctx
        try:
            yield
        finally:
            self.recording_stream = orig
            self.current_context = orig_context

    def emit_stream(self, stream: "Stream"):
        # Generated function looks like this:

        # def fn(actuals: List["Cell"], outputs: List["Cell"]):
        #  a, b, c, d, e, f, g, e = EXTERNAL # global variable bound to all the values we just want to bind to this code
        #  try:
        #     a = cell0.get()
        #     b = actuals[0].get()
        #     r = a + b
        #     outputs[2].set(r)
        #     t = r + r
        #     r2 = r + t
        #     outputs[4].set(r2)
        #
        #  except Exception as e:
        #    # error recovery, fallback to
        #    # interpreter code that can handle some values failing
        #    return fallback(locals())

        graph: torch.fx.Graph = self.graphs[stream]
        fallback_functions = self.fallback[stream]

        external: List[Any] = []
        external_names: List[str] = []
        external_id_to_name: Dict[int, str] = {}

        def arg_map(x):
            if isinstance(x, torch.fx.Node):
                return x
            elif id(x) in external_id_to_name:
                return external_id_to_name[id(x)]
            else:
                candidate = getattr(x, "__name__", "external")
                sym = Symbol(graph._graph_namespace.create_name(candidate, None))
                external_names.append(sym.name)
                external.append(x)
                # pyrefly: ignore [unsupported-operation]
                external_id_to_name[id(x)] = sym
                return sym

        lines = Lines()
        body = Lines()

        def fallback(results, exc):
            lineno = exc.__traceback__.tb_lineno
            error_context: ErrorContext = lines.get_context(lineno)
            # report new errors and set
            # defined identifiers for currently failing
            # op to DependentOnError
            if not isinstance(exc, DependentOnError):
                if error_context is None or error_context.ident is None:
                    raise exc
                exc = stream.report_error(
                    # pyrefly: ignore [bad-argument-type]
                    stream.current_recording,
                    error_context.ident,
                    exc,
                )

            # set exceptionson all the values this stream was responsible  for.
            # this is the explicitly passed cell outputs, and all
            # the cells we mutated.
            for c, s in self.mutated_cells.items():
                if s is stream:
                    c.set(exc)
            for r, i in self.results.items():
                if r.graph is graph:
                    results[i].set(exc)

            # some ops we have to run despite errors such as
            # borrows, collectives, send_tensor
            # we run these universally here.
            # Note that all of these are ok loading from cells with dependent
            # on error status.
            inst_range = range(
                error_context.fallback_resume_offset, len(fallback_functions)
            )
            for inst in inst_range:
                fallback_functions[inst]()

        fallback_sym = arg_map(fallback)

        # figure out the last use of each node that isn't
        # live out, so that we appropriatelly `del` the variable.
        seen = {r for r in self.results.keys() if r.graph is graph}
        last_uses = defaultdict(list)
        for node in reversed(graph.nodes):
            for n in node.all_input_nodes:
                if n not in seen:
                    last_uses[node].append(n)
                    seen.add(n)

        # generate the repeat body
        for node in graph.nodes:
            if node.op == "call_module":
                if node.target == "input_cell":
                    # each input goes into the prologue where we issue a load from the
                    # cell it came from.
                    (cell_obj,) = node.args
                    cell = arg_map(cell_obj)
                    with body.context(node.context):
                        body.emit(f"    {node.name} = {cell}.get()")
                elif node.target == "formal":
                    with body.context(node.context):
                        (i,) = node.args
                        body.emit(f"    {node.name} = actuals[{i}].get()")
            else:
                assert node.op == "call_function"
                fn = arg_map(node.target)
                args, kwargs = tree_map(arg_map, (node.args, node.kwargs))
                all = [
                    *(repr(a) for a in args),
                    *(f"{k}={repr(v)}" for k, v in kwargs.items()),
                ]
                assign = ""
                if node in seen:
                    assign = f"{node.name} = "
                with body.context(node.context):
                    body.emit(f"    {assign}{fn}({', '.join(all)})")
                    # some inputs to this node may no longer be used in the body
                    # of the loop. We explicitly del them so their lifetime
                    # is not longer than it was originally without compilation.
                    to_delete = [repr(d) for d in last_uses[node]]
                    if to_delete:
                        body.emit(f"    del {', '.join(to_delete)}")
        for r, i in self.results.items():
            if r.graph is not graph:
                continue
            body.emit(f"    results[{i}].set({r})")

        lines.emit("def impl(results, actuals):")
        lines.emit(f"  {', '.join(external_names)} = EXTERNAL")
        lines.emit("  _exception = None")
        lines.emit("  try:")
        lines.emit_lines(body)
        lines.emit("  except Exception as e:")
        lines.emit("    _exception = e")
        # we do not call `fallback` inside of the exception block because we
        # do not want future exceptions to have the stack trace of e attached.
        lines.emit(
            f"  if _exception is not None: return {fallback_sym}(results, _exception)"
        )

        gbls = {"EXTERNAL": external}
        text = lines.text()
        logger.debug(f"Compiled\n{text}")
        exec(lines.text(), gbls)
        return gbls["impl"]

    def emit(self):
        self.impls = {stream: self.emit_stream(stream) for stream in self.graphs.keys()}

        # fallback functions for borrows/reduce/send read directly from these cells
        # we need to make sure they are set to errors so that they work correctly.
        # it always gets an error value
        err = DependentOnError(-1)
        for cell in self.defined_cells:
            cell.set(err)

        self.defined_cells.clear()
