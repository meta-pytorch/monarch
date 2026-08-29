# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import math
from functools import cache
from logging import getLogger
from timeit import default_timer as timer

from .schedule_ir import (
    _Action,
    _add_send_recv,
    _ComputationType,
    _dump_csv,
    _format_pipeline_order,
    _merge_bw,
    BACKWARD,
    FORWARD,
    FULL_BACKWARD,
)

logger = getLogger()


def get_stage_str(model_chunk_index, training_stage, mb_index):
    ctype = _ComputationType.from_str(training_stage)
    return str(_Action(model_chunk_index, ctype, mb_index))


def get_dora_schedule(
    num_model_chunks,
    pipeline_parallel_size,
    num_round,
    num_microbatch_per_round,
    zero_bubble,
    total_num_microbatches,
    num_microbatches,
    dfs=False,
    prefetch_weight_latency=1.0,
    enable_weight_sharding_in_pp=False,
    enable_wgrad_sharding_in_pp=False,
):
    start_time = timer()
    num_warmup_microbatches_list = []
    num_1f1b_microbatches_list = []
    num_additional_1b1w_list = []
    for pipeline_parallel_rank in range(pipeline_parallel_size):
        num_warmup_microbatches = 0
        # The number of microbatches that last pipeline stage run before 1f1b.
        num_warmup_microbatches += (num_model_chunks - 1) * num_microbatch_per_round
        # From last PP stage up, each rank will be 2 more than the previous one.
        num_warmup_microbatches += (
            pipeline_parallel_size - pipeline_parallel_rank - 1
        ) * 2
        num_warmup_microbatches = min(num_warmup_microbatches, total_num_microbatches)
        # The number of 1f1b for zero bubble schedule
        if num_microbatches == pipeline_parallel_size:
            num_1f1b_microbatches = pipeline_parallel_rank
        else:
            num_1f1b_microbatches = 2 * pipeline_parallel_rank
        num_additional_1b1w = max(
            int(math.ceil((pipeline_parallel_size - 4) / 2)) - pipeline_parallel_rank,
            0,
        )
        if dfs:
            num_1f1b_microbatches = 0
            num_additional_1b1w = 0

        num_warmup_microbatches_list.append(num_warmup_microbatches)
        num_1f1b_microbatches_list.append(num_1f1b_microbatches)
        num_additional_1b1w_list.append(num_additional_1b1w)
    schedules = []

    def get_last_pp_rank(i):
        return (i - 1) % pipeline_parallel_size, i - 1 < 0

    def get_next_pp_rank(i):
        return (i + 1) % pipeline_parallel_size, i + 1 >= pipeline_parallel_size

    for pipeline_parallel_rank in range(pipeline_parallel_size):
        s = []
        fwd_mb_index_list = [0 for i in range(num_model_chunks)]
        bwd_mb_index_list = [0 for i in range(num_model_chunks)]
        fwd_model_chunk_index = 0
        bwd_model_chunk_index = num_model_chunks - 1
        weight_store = []
        num_warmup_microbatches = num_warmup_microbatches_list[pipeline_parallel_rank]
        num_1f1b_microbatches = num_1f1b_microbatches_list[pipeline_parallel_rank]
        num_additional_1b1w = num_additional_1b1w_list[pipeline_parallel_rank]
        fwd_mb_index = fwd_mb_index_list[fwd_model_chunk_index]
        bwd_mb_index = bwd_mb_index_list[bwd_model_chunk_index]
        fill_1b1w = False
        for _ in range(num_warmup_microbatches):  # warm up fwd
            fwd_mb_index = fwd_mb_index_list[fwd_model_chunk_index]
            bwd_mb_index = bwd_mb_index_list[bwd_model_chunk_index]
            tmp = get_stage_str(fwd_model_chunk_index, "F", fwd_mb_index)
            s.append(tmp)
            fwd_mb_index_list[fwd_model_chunk_index] += 1
            if fwd_mb_index_list[fwd_model_chunk_index] % num_microbatch_per_round == 0:
                if fwd_model_chunk_index < num_model_chunks - 1:
                    fwd_model_chunk_index += 1
                else:
                    fwd_model_chunk_index = 0
        for i in range(
            total_num_microbatches - num_warmup_microbatches
        ):  # 1f1b and 1f1b1w
            if (
                fwd_model_chunk_index == 1 and not fill_1b1w
            ):  # additional 1b1w to fill before fwd
                fill_1b1w = True
                for _ in range(num_additional_1b1w):
                    bwd_mb_index = bwd_mb_index_list[bwd_model_chunk_index]
                    tmp = get_stage_str(bwd_model_chunk_index, "B", bwd_mb_index)
                    s.append(tmp)
                    tmp = get_stage_str(bwd_model_chunk_index, "W", bwd_mb_index)
                    s.append(tmp)
                    bwd_mb_index_list[bwd_model_chunk_index] += 1
                    if (
                        bwd_mb_index_list[bwd_model_chunk_index]
                        % num_microbatch_per_round
                        == 0
                    ):
                        if bwd_model_chunk_index > 0:
                            bwd_model_chunk_index -= 1
                        else:
                            bwd_model_chunk_index = num_model_chunks - 1
            fwd_mb_index = fwd_mb_index_list[fwd_model_chunk_index]
            bwd_mb_index = bwd_mb_index_list[bwd_model_chunk_index]
            tmp = get_stage_str(fwd_model_chunk_index, "F", fwd_mb_index)
            s.append(tmp)
            fwd_mb_index_list[fwd_model_chunk_index] += 1
            if fwd_mb_index_list[fwd_model_chunk_index] % num_microbatch_per_round == 0:
                if fwd_model_chunk_index < num_model_chunks - 1:
                    fwd_model_chunk_index += 1
                else:
                    fwd_model_chunk_index = 0
            tmp = get_stage_str(
                bwd_model_chunk_index, "B" if zero_bubble else "BW", bwd_mb_index
            )
            s.append(tmp)
            tmp = get_stage_str(bwd_model_chunk_index, "W", bwd_mb_index)
            if zero_bubble and i < num_1f1b_microbatches:
                weight_store.append(tmp)
            else:
                s.append(tmp)
            bwd_mb_index_list[bwd_model_chunk_index] += 1
            if bwd_mb_index_list[bwd_model_chunk_index] % num_microbatch_per_round == 0:
                if bwd_model_chunk_index > 0:
                    bwd_model_chunk_index -= 1
                else:
                    bwd_model_chunk_index = num_model_chunks - 1
        num_cooldown = (
            num_warmup_microbatches - num_additional_1b1w
            if fill_1b1w
            else num_warmup_microbatches
        )
        for _ in range(num_cooldown):  # cooldown bwd
            fwd_mb_index = fwd_mb_index_list[fwd_model_chunk_index]
            bwd_mb_index = bwd_mb_index_list[bwd_model_chunk_index]
            tmp = get_stage_str(bwd_model_chunk_index, "B", bwd_mb_index)
            s.append(tmp)
            tmp = get_stage_str(bwd_model_chunk_index, "W", bwd_mb_index)
            s.append(tmp)
            bwd_mb_index_list[bwd_model_chunk_index] += 1
            if bwd_mb_index_list[bwd_model_chunk_index] % num_microbatch_per_round == 0:
                if bwd_model_chunk_index > 0:
                    bwd_model_chunk_index -= 1
                else:
                    bwd_model_chunk_index = num_model_chunks - 1
        if len(weight_store) > 0:
            s += weight_store
        schedules.append(s)

    compute_schedules = {}
    for rank in range(pipeline_parallel_size):
        compute_schedules[rank] = []
        for action_str in schedules[rank]:
            action = _Action.from_str(action_str)
            stage_index = action.stage_index * pipeline_parallel_size + rank
            action = _Action(
                stage_index, action.computation_type, action.microbatch_index
            )
            compute_schedules[rank].append(action)

    lowered_comm_schedule = compute_schedules
    for rank in lowered_comm_schedule:
        lowered_comm_schedule[rank] = _merge_bw(lowered_comm_schedule[rank])

    dump_scheduler_ir = True
    if dump_scheduler_ir:
        compute_str = _format_pipeline_order(lowered_comm_schedule)
        with open("lowered_compute.log", "w") as logf:
            logf.write(compute_str)
        _dump_csv(compute_schedules, "lowered_compute.csv")

    lowered_comm_schedule = _add_send_recv(
        lowered_comm_schedule,
        stage_to_rank=lambda chunk_index: chunk_index % pipeline_parallel_size,
        num_stages=num_model_chunks * pipeline_parallel_size,
    )

    comms_str = _format_pipeline_order(lowered_comm_schedule)
    if dump_scheduler_ir:
        with open("lowered_comms.log", "w") as logf:
            logf.write(comms_str)
        _dump_csv(lowered_comm_schedule, "lowered_compute_with_send_recv.csv")
    logger.debug("---------- lowered IR\n%s----------", comms_str)

    if not enable_weight_sharding_in_pp and not enable_wgrad_sharding_in_pp:
        return lowered_comm_schedule

    generation_time = timer() - start_time
    logger.info(f"schedule generation took {generation_time:.6f} seconds")

    return lowered_comm_schedule


# TODO - replace bfs / dfs functions below with new IR generators
ir_schedules = {
    # "dora": get_dora_schedule,
    "dora-dfs": lambda *args, **kwargs: get_dora_schedule(*args, **kwargs, dfs=True),
    # "zbv": get_zbv_schedule,
    # "zbw": get_zbw_schedule,
}

is_zero_bubble = {
    # "dora": True,
    "dora-dfs": True,
    # "zbv": True,
    # "zbw": True,
}


@cache
def generate_schedule(name: str, *args, **kwargs):
    assert name in ir_schedules, f"{name} is not a supported schedule type"
    schedules = ir_schedules[name](*args, **kwargs)
    stage_to_rank = {}
    for rank, schedule_actions_rank in schedules.items():
        for action in schedule_actions_rank:
            comp_type = action.computation_type
            stage_idx = action.stage_index
            if comp_type == FORWARD:
                stage_to_rank[stage_idx] = rank
            if comp_type in (BACKWARD, FULL_BACKWARD):
                stage_to_rank[stage_idx] = rank
    return schedules, stage_to_rank
