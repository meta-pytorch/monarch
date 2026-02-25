# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import itertools
import traceback
import warnings
from dataclasses import dataclass
from typing import List, NamedTuple, Optional, Sequence

import torch
from monarch._src.actor.shape import NDSlice
from monarch.common import messages
from monarch.simulator.ir import IRGraph
from monarch.simulator.tensor import DTensorRef
from monarch.simulator.utils import clean_name, file_path_with_iter
from torch.utils._pytree import tree_map


@dataclass
class Command:
    timestamp: int
    # Either "send" or "recvready" now.
    backend_command: str
    # "send" arguments
    ranks: Optional[List[NDSlice]] = None
    msg: Optional[NamedTuple] = None
    # "recvready" arguments
    timeout: Optional[float] = None


class CommandHistory:
    """
    A class to record commands sent to the SimulatorBackend. The class can be
    later be used for replaying the recorded commands.

    Args:
        maxlen (int): The maximum number of commands to record. Defaults to 10_000_000.
    """

    def __init__(
        self,
        world_size: int,
        *,
        maxlen: int = 10_000_000,
        file_path: str = "command_history.pt",
    ) -> None:
        self.world_size = world_size
        self.maxlen = maxlen
        self.commands: List[Command] = []
        self.warn_once: bool = False
        self.file_path = file_path

    def __del__(self):
        DTensorRef.created.clear()

    def record(
        self,
        now: int,
        backend_command: str,
        command_id: int,
        traceback: Sequence[traceback.FrameSummary] = (),
        ranks: Optional[List[NDSlice]] = None,
        msg: Optional[NamedTuple] = None,
        timeout: Optional[float] = None,
        ir: Optional[IRGraph] = None,
    ) -> Command:
        command = self.convert_command(
            now, backend_command, command_id, traceback, ranks, msg, timeout, ir
        )
        if len(self.commands) < self.maxlen:
            self.commands.append(command)
        elif not self.warn_once:
            warnings.warn(
                (
                    f"CommandHistory's maxlen is {self.maxlen}, and we already "
                    " execeed the limit. The rest commands will not be recorded."
                ),
                stacklevel=2,
            )
            self.warn_once = True
        return command

    @staticmethod
    def convert_command(
        now: int,
        backend_command: str,
        command_id: int,
        traceback: Sequence[traceback.FrameSummary] = (),
        ranks: Optional[List[NDSlice]] = None,
        msg: Optional[NamedTuple] = None,
        timeout: Optional[float] = None,
        ir: Optional[IRGraph] = None,
    ) -> Command:
        msg = CommandHistory._convert_command(msg)

        if ir:
            if isinstance(msg, messages.CommandGroup):
                for i, command in enumerate(msg.commands):
                    CommandHistory._maybe_insert_ir(
                        ir, command_id + i + 1, traceback, ranks, command
                    )  # i starts from 0, so command_id + i + 1
            else:
                CommandHistory._maybe_insert_ir(ir, command_id, traceback, ranks, msg)
        return Command(
            timestamp=now,
            backend_command=backend_command,
            ranks=ranks,
            msg=msg,
            timeout=timeout,
        )

    @staticmethod
    def convert_msg(msg):
        def _convert_arg(v):
            if isinstance(v, torch.Tensor):
                return DTensorRef.from_ref(v)
            return v

        name = type(msg).__name__
        match name:
            case "CallFunction":
                args, kwargs, mutates, result = tree_map(
                    _convert_arg, (msg.args, msg.kwargs, msg.mutates, msg.result)
                )
                msg = msg._replace(
                    args=args, kwargs=kwargs, mutates=mutates, result=result
                )
            case "SendTensor":
                msg = msg._replace(
                    tensor=DTensorRef.from_ref(msg.tensor),
                    result=DTensorRef.from_ref(msg.result),
                )
            case "Reduce":
                msg = msg._replace(
                    local_tensor=DTensorRef.from_ref(msg.local_tensor),
                    result=DTensorRef.from_ref(msg.result),
                )
            case "BorrowCreate":
                msg = msg._replace(
                    result=DTensorRef.from_ref(msg.result),
                    tensor=DTensorRef.from_ref(msg.tensor),
                )

        return msg

    @staticmethod
    def _convert_command(msg):
        if isinstance(msg, messages.CommandGroup):
            for idx, command in enumerate(msg.commands):
                msg.commands[idx] = CommandHistory.convert_msg(command)
            return msg
        else:
            return CommandHistory.convert_msg(msg)

    # TODO: Add function to simplify repeated modifications to ir
    @staticmethod
    def _maybe_insert_ir(
        ir: IRGraph,
        command_id: int,
        tb: Sequence[traceback.FrameSummary] = (),
        ranks: Optional[List[NDSlice]] = None,
        msg: Optional[NamedTuple] = None,
    ) -> None:
        # Process tensor results and update IR
        def _process_tensor_results(
            result,
            worker_rank,
            stream_name,
            command_id,
            mutate=False,
            borrow_src_tensor_ref=None,
        ):
            if result is not None:
                results_list = result if isinstance(result, list) else [result]
                for item in results_list:
                    # Handle tuples recursively - some operations return tuples.
                    if isinstance(item, tuple):
                        for sub_item in item:
                            _process_tensor_results(
                                sub_item,
                                worker_rank,
                                stream_name,
                                command_id,
                                mutate=mutate,
                                borrow_src_tensor_ref=borrow_src_tensor_ref,
                            )
                    # Only process items that have _fake attribute (i.e., are tensor references)
                    elif hasattr(item, "_fake"):
                        fake = item._fake
                        # Extract mesh reference from DTensorRef (captured at creation time)
                        mesh_ref = getattr(item, "_mesh_ref", None)

                        ir.update_tensor(
                            item._storage_id,
                            item.ref,
                            fake.dtype,
                            tuple(fake.shape),
                            worker_rank,
                            stream_name,
                            command_id,
                            mutate=mutate,
                            borrow_src_tensor_ref=borrow_src_tensor_ref,
                            tensor_size=item._size,
                            mesh_ref=mesh_ref,
                        )
                    # Skip non-tensor items (like borrow handles, strings, etc.)
                    # These don't need IR tracking

        assert msg is not None
        stream_name = src_stream_name = dst_stream_name = ""
        flattened_ranks = list(itertools.chain.from_iterable(ranks or []))
        command_type = ""
        devices = []
        control_dependencies = []
        dag_item_type = type(msg).__name__
        result = getattr(msg, "result", None)
        for worker_rank in flattened_ranks:
            match dag_item_type:
                case "CallFunction":
                    stream_name = getattr(msg, "stream", None).name
                    command_type = (
                        f"CallFunction: {clean_name(str(getattr(msg, 'function', '')))}"
                    )
                    devices = [worker_rank]
                    msg_args = getattr(msg, "args", None)
                    if msg_args is not None:
                        for arg in msg_args:
                            if isinstance(arg, DTensorRef):
                                _process_tensor_results(
                                    arg, worker_rank, stream_name, command_id
                                )
                    msg_mutates = getattr(msg, "mutates", None)
                    if msg_mutates is not None:
                        for mutate_src in msg_mutates:
                            if isinstance(mutate_src, DTensorRef) or (
                                isinstance(mutate_src, list)
                                and all(isinstance(m, DTensorRef) for m in mutate_src)
                            ):
                                mutates_list = (
                                    mutate_src
                                    if isinstance(mutate_src, list)
                                    else [mutate_src]
                                )
                                _process_tensor_results(
                                    mutates_list,
                                    worker_rank,
                                    stream_name,
                                    command_id,
                                    mutate=True,
                                )
                    _process_tensor_results(
                        result,
                        worker_rank,
                        stream_name,
                        command_id,
                    )

                case "Reduce":
                    stream_name = getattr(msg, "stream", None).name
                    reduction = getattr(msg, "reduction", None)
                    scatter = getattr(msg, "scatter", False)
                    if reduction == "stack":
                        if scatter:
                            reduce_type = "all_to_all"
                        else:
                            reduce_type = "all_gather"
                    else:
                        if scatter:
                            reduce_type = "all_reduce"
                        else:
                            reduce_type = "reduce_scatter"
                    command_type = f"Reduce: {reduce_type}: {result.ref}"  # use result.ref as unique Reduce id
                    devices = flattened_ranks
                    _process_tensor_results(
                        result, worker_rank, stream_name, command_id
                    )
                case "BorrowCreate":
                    borrow_id = getattr(msg, "borrow", None)
                    borrow_src_tensor_ref = getattr(msg, "tensor", None).ref
                    stream_name = src_stream_name = getattr(
                        msg, "from_stream", None
                    ).name
                    dst_stream_name = getattr(msg, "to_stream", None).name

                    command_type = f"BorrowCreate: {borrow_id}"
                    devices = [worker_rank]
                    ir.add_borrow(
                        borrow_id,
                        worker_rank,
                        src_stream_name,
                        dst_stream_name,
                        command_id,
                    )
                    _process_tensor_results(
                        result,
                        worker_rank,
                        dst_stream_name,
                        command_id,
                        borrow_src_tensor_ref=borrow_src_tensor_ref,
                    )
                case "BorrowFirstUse":
                    borrow_id = getattr(msg, "borrow", None)
                    stream_name = ir._control.borrows_info[borrow_id].dst_stream_name
                    command_type = f"BorrowFirstUse: {borrow_id}"
                    devices = [worker_rank]
                    control_dependencies = [
                        ir._control.borrows_info[borrow_id].create_id
                    ]
                    ir._control.borrows_info[borrow_id].firstuse_id = command_id
                case "BorrowLastUse":
                    borrow_id = getattr(msg, "borrow", None)
                    stream_name = src_stream_name = ir._control.borrows_info[
                        borrow_id
                    ].dst_stream_name
                    dst_stream_name = ir._control.borrows_info[
                        borrow_id
                    ].src_stream_name
                    command_type = f"BorrowLastUse: {borrow_id}"
                    devices = [worker_rank]
                    ir._control.borrows_info[borrow_id].lastuse_id = command_id
                case "BorrowDrop":
                    borrow_id = getattr(msg, "borrow", None)
                    stream_name = ir._control.borrows_info[borrow_id].src_stream_name
                    command_type = f"BorrowDrop: {borrow_id}"
                    devices = [worker_rank]
                    control_dependencies = [
                        ir._control.borrows_info[borrow_id].lastuse_id
                    ]
                    ir._control.borrows_info[borrow_id].drop_id = command_id

            if dag_item_type in [
                "CallFunction",
                "Reduce",
                "BorrowCreate",
                "BorrowFirstUse",
                "BorrowLastUse",
                "BorrowDrop",
            ]:
                ir.insert_node(
                    worker_rank,
                    stream_name,
                    command_id,
                    command_type,
                    devices,
                    control_dependencies,
                    tb,
                )

        assert ranks is not None
        if dag_item_type == "SendTensor" and len(ranks) == 2:
            src_flattened_ranks = list(
                itertools.chain.from_iterable([ranks[0]])
            )  # for SendTensor, ranks[0] == source ranks
            dst_flattened_ranks = list(
                itertools.chain.from_iterable([ranks[1]])
            )  # for SendTensor, ranks[1] == destination ranks

            src_stream_name = getattr(msg, "from_stream", None).name
            dst_stream_name = getattr(msg, "to_stream", None).name

            # Create sets of (rank, stream) pairs for source and destination ranks
            src_rank_stream_pairs = {
                (rank, src_stream_name) for rank in src_flattened_ranks
            }
            dst_rank_stream_pairs = {
                (rank, dst_stream_name) for rank in dst_flattened_ranks
            }
            rank_stream_pairs = (
                src_rank_stream_pairs | dst_rank_stream_pairs
            )  # find the union of the two sets
            command_type = f"SendTensor: {result.ref if result else None}"
            devices = flattened_ranks
            control_dependencies = flattened_ranks
            for rank, stream_name in rank_stream_pairs:
                ir.insert_node(
                    rank,
                    stream_name,
                    command_id,
                    command_type,
                    devices,
                    control_dependencies,
                    tb,
                )
            src_tensor = getattr(msg, "tensor", None)
            if src_tensor is not None:
                src_tensors_list = (
                    src_tensor if isinstance(src_tensor, list) else [src_tensor]
                )
                for src_t in src_tensors_list:
                    for rank, src_stream_name in src_rank_stream_pairs:
                        _process_tensor_results(
                            src_t, rank, src_stream_name, command_id
                        )
            if result is not None:
                results_list = result if isinstance(result, list) else [result]
                for res in results_list:
                    ir.add_sendtensor(
                        res.ref,
                        src_flattened_ranks,
                        src_stream_name,
                        dst_flattened_ranks,
                        dst_stream_name,
                        tuple(res._fake.size()),
                    )

                    for rank, dst_stream_name in dst_rank_stream_pairs:
                        _process_tensor_results(res, rank, dst_stream_name, command_id)

        if dag_item_type == "DeleteRefs":
            refs = getattr(msg, "refs", None)
            for ref in refs:
                stream_name = ir._data.tensorref_to_stream[ref]
                # Do not call _insert_node() since we do not need DeleteRefs for the control DAG
                ir.delete_tensor(ref, flattened_ranks, stream_name, command_id, tb)
        if dag_item_type == "Exit":
            ir.convert_devices_to_meshes()

    def step(self, iter_count: int, dump: bool = False) -> None:
        if dump:
            self.dump(file_path_with_iter(self.file_path, iter_count))

        self.commands.clear()

    def dump(self, file_path: str) -> None:
        with open(file_path, "wb") as f:
            torch.save({"world_size": self.world_size, "commands": self.commands}, f)

    @classmethod
    def load(cls, filename: str) -> "CommandHistory":
        with open(filename, "rb") as f:
            states = torch.load(f, weights_only=False)
            self = cls(states["world_size"])
            self.commands = states["commands"]

        return self
