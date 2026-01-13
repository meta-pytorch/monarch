# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from __future__ import annotations

import copy
import csv
import itertools
import re
from collections import defaultdict
from enum import Enum
from typing import Callable, Dict, List, NamedTuple, Optional, Tuple


# We reuse the IR definition and optimizations from FairInternal/XLFormers' implementation of pipeline parallelism,
# originally found in core/parallelism/pipeline_parallel/schedule_ir.py.
# TODO: Investigate how to adapt this code for reuse after further integration
class _ComputationType(Enum):
    # TODO(whc) rename to _ActType?
    FORWARD = 1
    BACKWARD = 2
    WEIGHT = 3
    UNSHARD = 4
    RESHARD = 5
    SEND_F = 6
    RECV_F = 7
    SEND_B = 8
    RECV_B = 9
    SEND_F_RECV_B = 10
    SEND_B_RECV_F = 11
    # TODO- probably want to reconsider naming backward_input 'B' and having 'FULL_BACKWARD'.
    # instead, B = full backward, Bx, Bw are the partials?
    FULL_BACKWARD = 12

    def __str__(self):
        str_map = {
            _ComputationType.FORWARD: "F",
            _ComputationType.BACKWARD: "B",
            _ComputationType.WEIGHT: "W",
            _ComputationType.UNSHARD: "UNSHARD",
            _ComputationType.RESHARD: "RESHARD",
            _ComputationType.SEND_F: "SEND_F",
            _ComputationType.RECV_F: "RECV_F",
            _ComputationType.SEND_B: "SEND_B",
            _ComputationType.RECV_B: "RECV_B",
            _ComputationType.SEND_F_RECV_B: "SEND_F_RECV_B",
            _ComputationType.SEND_B_RECV_F: "SEND_B_RECV_F",
            _ComputationType.FULL_BACKWARD: "BW",
        }
        return str_map[self]

    @staticmethod
    def from_str(action):
        if action == "F":
            return _ComputationType.FORWARD
        elif action == "B":
            return _ComputationType.BACKWARD
        elif action == "W":
            return _ComputationType.WEIGHT
        elif action == "UNSHARD":
            return _ComputationType.UNSHARD
        elif action == "RESHARD":
            return _ComputationType.RESHARD
        elif action == "SEND_F":
            return _ComputationType.SEND_F
        elif action == "RECV_F":
            return _ComputationType.RECV_F
        elif action == "SEND_B":
            return _ComputationType.SEND_B
        elif action == "RECV_B":
            return _ComputationType.RECV_B
        elif action == "SEND_F_RECV_B":
            return _ComputationType.SEND_F_RECV_B
        elif action == "SEND_B_RECV_F":
            return _ComputationType.SEND_B_RECV_F
        elif action == "BW":
            return _ComputationType.FULL_BACKWARD
        else:
            raise RuntimeError(f"Invalid computation type {action}")


FORWARD = _ComputationType.FORWARD
BACKWARD = _ComputationType.BACKWARD
WEIGHT = _ComputationType.WEIGHT
UNSHARD = _ComputationType.UNSHARD
RESHARD = _ComputationType.RESHARD
SEND_F = _ComputationType.SEND_F
RECV_F = _ComputationType.RECV_F
SEND_B = _ComputationType.SEND_B
RECV_B = _ComputationType.RECV_B
SEND_F_RECV_B = _ComputationType.SEND_F_RECV_B
SEND_B_RECV_F = _ComputationType.SEND_B_RECV_F
FULL_BACKWARD = _ComputationType.FULL_BACKWARD

# Convenience shorthand for compute actions only since they are used in 'simple schedule format'
F = FORWARD
B = BACKWARD
W = WEIGHT
BW = FULL_BACKWARD

# Helper to parse an action string like 1F0 into a tuple of (stage_index, computation_type, microbatch_index)
_action_regex = re.compile(
    r"(\d+)(F|BW|B|W|UNSHARD|RESHARD|SEND_F|RECV_F|SEND_B|RECV_B){0,1}(\d*)(_(\d*)(RECV_B|RECV_F)(\d)){0,1}"
)


class _Action(NamedTuple):
    stage_index: int
    computation_type: _ComputationType
    microbatch_index: Optional[int] = None
    # Used only for batched comms, for the second comm
    other_stage_index: Optional[int] = None
    other_microbatch_index: Optional[int] = None
    # Indicates whether to call the post-backward reduce-scatter for W/BW actions.
    require_reduce_scatter: Optional[bool] = False

    def __repr__(self):
        repr = str(self.stage_index)
        if self.computation_type == SEND_B_RECV_F:
            assert self.microbatch_index is not None, (
                "SEND_B_RECV_F requires microbatch_index"
            )
            assert self.other_stage_index is not None, (
                "SEND_B_RECV_F requires other_stage_index"
            )
            assert self.other_microbatch_index is not None, (
                "SEND_B_RECV_F requires other_microbatch_index"
            )
            repr += str(SEND_B) + str(self.microbatch_index)
            repr += "_" + str(self.other_stage_index)
            repr += str(RECV_F) + str(self.other_microbatch_index)
        elif self.computation_type == SEND_F_RECV_B:
            assert self.microbatch_index is not None, (
                "SEND_F_RECV_B requires microbatch_index"
            )
            assert self.other_stage_index is not None, (
                "SEND_F_RECV_B requires other_stage_index"
            )
            assert self.other_microbatch_index is not None, (
                "SEND_F_RECV_B requires other_microbatch_index"
            )
            repr += str(SEND_F) + str(self.microbatch_index)
            repr += "_" + str(self.other_stage_index)
            repr += str(RECV_B) + str(self.other_microbatch_index)
        else:
            repr += str(self.computation_type)
            if self.microbatch_index is not None:
                repr += str(self.microbatch_index)
            require_reduce_scatter = (
                hasattr(self, "require_reduce_scatter") and self.require_reduce_scatter
            )
            if require_reduce_scatter and self.computation_type in [
                WEIGHT,
                FULL_BACKWARD,
            ]:
                repr += "_rs"
        return repr

    @staticmethod
    def from_str(str):
        """
        Reverse of __repr__

        String should be formatted as [stage][action type][(microbatch)]
            e.g. `2F0`, `1UNSHARD`, `3SEND_F1`
        """
        if match := _action_regex.match(str):
            # the _ is for the combined group that captures the whole second action
            (
                stage_index,
                computation_type,
                microbatch_index,
                _,
                other_stage_index,
                other_computation_type,
                other_microbatch_index,
            ) = match.groups()
            if other_computation_type is not None:
                assert (
                    other_stage_index is not None and other_microbatch_index is not None
                )
                return _Action(
                    int(stage_index),
                    _ComputationType.from_str(
                        f"{computation_type}_{other_computation_type}"
                    ),
                    int(microbatch_index) if len(microbatch_index) else None,
                    int(other_stage_index),
                    int(other_microbatch_index),
                )
            return _Action(
                int(stage_index),
                _ComputationType.from_str(computation_type),
                int(microbatch_index) if len(microbatch_index) else None,
            )
        elif str == "" or str.isspace():
            return None
        raise RuntimeError(
            f"Invalid action string: {str}, should be formatted as [stage][action type][(microbatch)] e.g. 2F0"
        )

    def get_pair_commu_action(self) -> Optional[_Action]:
        """
        Returns the corresponding communication action another rank.
        """
        if self.computation_type not in [RECV_F, RECV_B, SEND_F, SEND_B]:
            return None
        stage_id = self.stage_index
        op = self.computation_type
        microbatch_id = self.microbatch_index
        if op == RECV_F:
            other_stage = stage_id - 1
            other_op = SEND_F
        elif op == RECV_B:
            other_stage = stage_id + 1
            other_op = SEND_B
        elif op == SEND_F:
            other_stage = stage_id + 1
            other_op = RECV_F
        else:
            assert op == SEND_B
            other_stage = stage_id - 1
            other_op = RECV_B
        return _Action(other_stage, other_op, microbatch_id)


def _format_pipeline_order(pipeline_order: Dict[int, List[Optional[_Action]]]) -> str:
    """
    Formats the pipeline order in a timestep (row) x rank (column) grid of actions
    and returns the formatted string
    """
    # Replace None with ""
    for rank in pipeline_order:
        for i in range(len(pipeline_order[rank])):
            if pipeline_order[rank][i] is None:
                # TODO make a real 'None action' that prints as empty string and make mypy happy
                pipeline_order[rank][i] = ""  # type: ignore[call-overload]
    # Calculate the maximum number of steps across all ranks
    num_steps = max(len(actions) for actions in pipeline_order.values())
    step_labels = [
        "Step " + str(i).zfill(len(str(num_steps - 1))) for i in range(num_steps)
    ]
    # Sorting the dictionary by keys and retrieving values in that order
    rank_actions = [
        pipeline_order.get(key, [""] * num_steps) for key in sorted(pipeline_order)
    ]
    # Transpose the list of lists (rows to columns)
    transposed_actions = list(itertools.zip_longest(*rank_actions, fillvalue=""))
    # Generate column labels for ranks
    num_ranks = len(pipeline_order)
    rank_labels = ["Rank " + str(i) for i in range(num_ranks)]
    # Calculate the maximum length of each column, considering labels
    max_lengths = [
        max(len(str(item)) if item is not None else 0 for item in col)
        for col in zip(step_labels, *transposed_actions)
    ]
    # Format the header row with rank labels
    header_row = " " * (len(step_labels[0]) + 2) + " ".join(
        f"{label:<{max_lengths[i]}}" for i, label in enumerate(rank_labels)
    )
    # Format each row with its corresponding label
    formatted_rows = [
        f"{label}: "
        + " ".join(f"{str(item):<{max_lengths[i]}}" for i, item in enumerate(row))
        for label, row in zip(step_labels, transposed_actions)
    ]
    # Join the rows into a single string
    formatted_table = header_row + "\n" + "\n".join(formatted_rows) + "\n"
    return formatted_table


def _add_send_recv(
    compute_actions: Dict[int, List[_Action]],
    stage_to_rank: Callable[[int], int],
    num_stages: int,
    batch_send_recv: bool = False,
) -> Dict[int, List[_Action]]:
    comm_actions: Dict[int, List[_Action]] = {rank: [] for rank in compute_actions}

    def _has_comms(action: _Action) -> bool:
        if action.computation_type == F:
            return action.stage_index != num_stages - 1 and stage_to_rank(
                action.stage_index + 1
            ) != stage_to_rank(action.stage_index)
        elif action.computation_type in (B, BW):
            return action.stage_index != 0 and stage_to_rank(
                action.stage_index - 1
            ) != stage_to_rank(action.stage_index)
        return False

    def _get_comms(action: _Action) -> Tuple[_Action, _Action]:
        assert _has_comms(action), f"{action} is not a valid comm action"
        stage_idx = action.stage_index
        ctype = action.computation_type
        mb_idx = action.microbatch_index
        send = _Action(stage_idx, SEND_F if ctype == F else SEND_B, mb_idx)
        recv_stage_idx = stage_idx + 1 if ctype == F else stage_idx - 1
        recv = _Action(recv_stage_idx, RECV_F if ctype == F else RECV_B, mb_idx)
        return send, recv

    def _peer_rank(action: _Action) -> int:
        # TODO asserts for invalid stage ids (RECV_F for stage 0)
        if action.computation_type == SEND_F:
            return stage_to_rank(action.stage_index + 1)
        elif action.computation_type == SEND_B:
            return stage_to_rank(action.stage_index - 1)
        elif action.computation_type == RECV_F:
            return stage_to_rank(action.stage_index - 1)
        elif action.computation_type == RECV_B:
            return stage_to_rank(action.stage_index + 1)
        else:
            raise ValueError("unsupported action for peer rank")

    def _ready_to_schedule(
        action: Optional[_Action], prev_actions: List[_Action]
    ) -> bool:
        """We don't put our own recv ops in the schedule, we let a sender on another rank put our recv ops in place.
        This helps ensure a sane (non-hanging) ordering of sends and recvs.
        But it also means we might not be able to schedule our next compute action yet.
        """
        if action is None:
            return True
        elif action.computation_type == F and not action.stage_index == 0:
            for p in prev_actions:
                if (
                    p.computation_type == RECV_F
                    and p.stage_index == action.stage_index
                    and p.microbatch_index == action.microbatch_index
                ):
                    return True
                elif (
                    p.computation_type == SEND_B_RECV_F
                    and p.other_stage_index == action.stage_index
                    and p.other_microbatch_index == action.microbatch_index
                ):
                    return True
                elif (
                    p.computation_type == FORWARD
                    and p.stage_index == action.stage_index - 1
                    and p.microbatch_index == action.microbatch_index
                ):
                    return True
            return False
        elif (
            action.computation_type in (B, BW)
            and not action.stage_index == num_stages - 1
        ):
            for p in prev_actions:
                if (
                    p.computation_type == RECV_B
                    and p.stage_index == action.stage_index
                    and p.microbatch_index == action.microbatch_index
                ):
                    return True
                elif (
                    p.computation_type == SEND_F_RECV_B
                    and p.other_stage_index == action.stage_index
                    and p.other_microbatch_index == action.microbatch_index
                ):
                    return True
                elif (
                    p.computation_type in (B, BW)
                    and p.stage_index == action.stage_index + 1
                    and p.microbatch_index == action.microbatch_index
                ):
                    return True
            return False
        else:
            return True

    while compute_actions:
        progress = False
        # go in order of ranks even if dict keys aren't ordered
        new_comms: Dict[int, defaultdict[int, list]] = {
            rank: defaultdict(list) for rank in sorted(compute_actions)
        }
        for rank in sorted(compute_actions):
            if rank not in compute_actions:
                continue

            assert len(compute_actions[rank]) > 0
            action = compute_actions[rank][0]
            if not _ready_to_schedule(action, comm_actions[rank]):
                continue

            if action is not None:
                comm_actions[rank].append(action)
                if _has_comms(action):
                    send, recv = _get_comms(action)
                    # TODO we can avoid send/recv if the 2 stages are on the same rank.
                    # should we avoid that in the runtime or here?
                    new_comms[rank][_peer_rank(send)].append(send)
                    new_comms[stage_to_rank(recv.stage_index)][rank].append(recv)

            compute_actions[rank].pop(0)
            if len(compute_actions[rank]) == 0:
                del compute_actions[rank]
            progress = True

        if not progress:
            print("WIP comms schedule:\n", _format_pipeline_order(comm_actions))  # type: ignore[arg-type]
            print("remaining compute actions:\n", compute_actions)
        assert progress, "Malformed compute schedule, can't schedule sends/recvs"

        # comm batching needs to be done carefully to avoid reordering comms and causing a hang
        # algorithm:
        # Process sends/recvs in pairs.  Processing means consuming from 'new_comms' and adding the final schedule
        # processing batches is done the same way except 4 ops at a time are consumed and 2 are written
        # rules:
        # 1- if we batch ops for one rank, we also batch matching ops for another rank
        # 2- when we create a batch, we append the batches to both ranks' schedules at the same time
        # 3- we remove individual sends/recvs from 'new_comms' when we consume them in a batch
        # 4- append individual (unbatchable) sends/recvs
        for rank in new_comms:
            for peer in new_comms[rank]:
                if rank == peer:
                    continue
                # we batch and process all the operations between rank and peer.
                # this should symmetrically consume all actions from new_comms[rank][peer] and new_comms[peer][rank]
                ops = new_comms[rank][peer]
                peer_ops = new_comms[peer][rank]
                if len(ops) == 0:
                    assert len(peer_ops) == 0, (
                        f"ops was empty but peer_ops was not, {peer_ops}"
                    )

                batched_ops = list(ops)
                batched_peer_ops = list(peer_ops)
                # TODO - refactor so that it is not necessary to consume/clear ops/peer_ops
                ops.clear()
                peer_ops.clear()
                comm_actions[rank].extend(batched_ops)
                comm_actions[peer].extend(batched_peer_ops)

    # # Run extra optimizations to adjust send/recv scheduling.
    # optimized_comm_actions = _optimize_communication_ops(
    #     comm_actions,
    # )
    return comm_actions


def _simulate_comms_compute(
    pipeline_order, stage_to_rank: Callable[[int], int], num_stages: int
):
    pipeline_order = {
        rank: [a for a in pipeline_order[rank] if a is not None]
        for rank in sorted(pipeline_order)
    }
    schedule: Dict[int, List[_Action | None]] = {
        rank: [] for rank in sorted(pipeline_order)
    }

    def _prev_ops(stage_idx):
        rank = stage_to_rank(stage_idx)
        ops = copy.deepcopy(schedule[rank])
        if len(pipeline_order[rank]):
            # batched comm ops may need to be jointly scheduled (e.g. send_f_recv_b depends on and is a dep of send_b_recv_f)
            # assuming we iterate in sorted rank order, peeking at the next unscheduled action for later ranks should unblock us
            ops.append(pipeline_order[rank][0])

        return ops

    def _ready_to_schedule(action: Optional[_Action]) -> bool:
        if action is None:
            return True

        stage_idx = action.stage_index
        if action.computation_type == F:
            if action.stage_index == 0:
                return True
            for p in _prev_ops(stage_idx):
                if p is None:
                    continue
                elif (
                    p.computation_type == F
                    and p.stage_index + 1 == action.stage_index
                    and p.microbatch_index == action.microbatch_index
                ):
                    return True
                elif (
                    p.computation_type == RECV_F
                    and p.stage_index == action.stage_index
                    and p.microbatch_index == action.microbatch_index
                ):
                    return True
                elif (
                    p.computation_type == SEND_B_RECV_F
                    and p.other_stage_index == action.stage_index
                    and p.other_microbatch_index == action.microbatch_index
                ):
                    return True
            return False
        elif action.computation_type in (B, BW):
            if action.stage_index == num_stages - 1:
                return True

            for p in _prev_ops(stage_idx):
                if p is None:
                    continue
                elif (
                    p.computation_type == RECV_B
                    and p.stage_index == action.stage_index
                    and p.microbatch_index == action.microbatch_index
                ):
                    return True
                elif (
                    p.computation_type == SEND_F_RECV_B
                    and p.other_stage_index == action.stage_index
                    and p.other_microbatch_index == action.microbatch_index
                ):
                    return True
                elif (
                    p.computation_type in (B, BW)
                    and p.stage_index - 1 == action.stage_index
                    and p.microbatch_index == action.microbatch_index
                ):
                    return True
            return False
        elif action.computation_type == W:
            return True
        elif action.computation_type == SEND_F:
            expected_f = _Action(action.stage_index, F, action.microbatch_index)
            return expected_f in _prev_ops(stage_idx)
        elif action.computation_type == RECV_F:
            peer_stage_idx = stage_idx - 1
            expected_send = _Action(peer_stage_idx, SEND_F, action.microbatch_index)
            return expected_send in _prev_ops(peer_stage_idx)
        elif action.computation_type == SEND_B:
            expected_b = _Action(action.stage_index, B, action.microbatch_index)
            expected_bw = _Action(action.stage_index, BW, action.microbatch_index)
            return expected_b in _prev_ops(stage_idx) or expected_bw in _prev_ops(
                stage_idx
            )
        elif action.computation_type == RECV_B:
            peer_stage_idx = stage_idx + 1
            expected_send = _Action(peer_stage_idx, SEND_B, action.microbatch_index)
            return expected_send in _prev_ops(peer_stage_idx)
        elif action.computation_type == SEND_F_RECV_B:
            # though the stage_index may not be the same between the SEND and the RECV, the rank must be
            peer_stage_idx = stage_idx + 1
            for p in _prev_ops(peer_stage_idx):
                if p is None:
                    continue
                elif (
                    p.computation_type == SEND_B_RECV_F
                    and action.other_stage_index is not None
                    and p.stage_index == action.other_stage_index + 1
                    and p.other_stage_index is not None
                    and p.other_stage_index == action.stage_index + 1
                    and p.microbatch_index == action.other_microbatch_index
                    and p.other_microbatch_index == action.microbatch_index
                ):
                    return True
            return False
        elif action.computation_type == SEND_B_RECV_F:
            # though the stage_index may not be the same between the SEND and the RECV, the rank must be
            peer_stage_idx = action.stage_index - 1
            for p in _prev_ops(peer_stage_idx):
                # if p is not None and str(p) == "0SEND_F14-16RECV_B0":
                # breakpoint()
                if p is None:
                    continue
                elif (
                    p.computation_type == SEND_F_RECV_B
                    and p.stage_index + 1 == action.other_stage_index
                    and p.other_stage_index + 1 == action.stage_index
                    and p.microbatch_index == action.other_microbatch_index
                    and p.other_microbatch_index == action.microbatch_index
                ):
                    return True
            return False

        else:
            raise ValueError(f"Unsupported action type {action}")

    while pipeline_order:
        progress = False
        for rank in sorted(pipeline_order):
            if len(pipeline_order[rank]) == 0:
                continue

            action = pipeline_order[rank][0]
            if _ready_to_schedule(action):
                if action is not None:
                    schedule[rank].append(action)
                pipeline_order[rank].pop(0)
                progress = True
            else:
                schedule[rank].append(None)

        for i in sorted(pipeline_order, reverse=True):
            if len(pipeline_order[i]) == 0:
                del pipeline_order[i]

        # hacky, but do a second pass to replace any 'none' at this timestep with a real action, if it got unblocked
        # by one of the later ranks
        for rank in sorted(pipeline_order):
            if len(pipeline_order[rank]) == 0:
                continue

            if schedule[rank][-1] is not None:
                continue

            action = pipeline_order[rank][0]
            if _ready_to_schedule(action):
                if action is not None:
                    schedule[rank][-1] = action
                pipeline_order[rank].pop(0)

        for i in sorted(pipeline_order, reverse=True):
            if len(pipeline_order[i]) == 0:
                del pipeline_order[i]

        if not progress:
            print("WIP comms schedule:\n", _format_pipeline_order(schedule))
            for rank in pipeline_order:
                print(f"{rank=} next action= {pipeline_order[rank][0]}")
            raise ValueError("Schedule is not progressing")

    return schedule


def _dump_chrometrace(schedule, filename):
    events = []
    for rank in sorted(schedule):
        for timestep, action in enumerate(schedule[rank]):
            if action is None:
                continue
            events.append(
                {
                    "name": str(action),
                    "cat": (
                        "computation"
                        if action.computation_type in (F, B, W)
                        else "communication"
                    ),
                    "ph": "X",
                    "pid": rank,
                    "tid": rank,
                    "ts": timestep,
                    "dur": 1,
                }
            )
    import json

    with open(filename, "w") as f:
        json.dump({"traceEvents": events}, f)


def _dump_csv(pipeline_order_with_comms, filename: str):
    """Dump a CSV representation of the compute + comms schedule into a file with the provided filename."""
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        for rank in pipeline_order_with_comms:
            writer.writerow(pipeline_order_with_comms[rank])


def _merge_bw(
    compute_actions: List[Optional[_Action]],
) -> List[_Action]:
    """Given a basic schedule involving only compute actions (F,B,W), merge adjacent B and W ops into BW ops.

    BW refers to running the whole backward (not separating grad_input and grad_weight), which can be more efficient
    in some cases.
    """
    merged_actions = []
    while compute_actions:
        action = compute_actions.pop(0)
        if action is None:
            continue

        while len(compute_actions) and (next_action := compute_actions[0]) is None:
            # remove any None actions between 'action' and 'next_action'
            compute_actions.pop(0)

        if (
            action.computation_type == B
            and next_action is not None
            and next_action.computation_type == W
            and action.stage_index == next_action.stage_index
            and action.microbatch_index == next_action.microbatch_index
        ):
            merged_actions.append(
                _Action(action.stage_index, BW, action.microbatch_index)
            )
            compute_actions.pop(0)
        else:
            merged_actions.append(action)
    return merged_actions
