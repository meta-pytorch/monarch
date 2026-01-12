# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from __future__ import annotations

import copy
import importlib
import sys
from itertools import chain
from logging import getLogger
from typing import Callable, Dict, List, Optional

import torch
import torch.nn as nn
import torch.optim as optim
from monarch import fetch_shard, OpaqueRef, remote, Stream, Tensor
from monarch.common.device_mesh import DeviceMesh, no_mesh
from monarch.opaque_module import OpaqueModule

from .schedule_ir import (
    _Action,
    _format_pipeline_order,
    B,
    BW,
    F,
    RECV_B,
    RECV_F,
    SEND_B,
    SEND_B_RECV_F,
    SEND_F,
    SEND_F_RECV_B,
    W,
)
from .scheduler import generate_schedule

logger = getLogger()


run_forward_udf = remote(
    "monarch.parallel.pipelining.runtime.run_forward_impl",
    propagate=lambda stage, input_tensor, model_chunk_id, microbatch_id: input_tensor,
)


def run_forward_impl(
    stage: nn.Module | OpaqueRef,
    input_tensor: torch.Tensor,
    model_chunk_id: int,
    microbatch_id: int,
) -> torch.Tensor:
    """
    Run the forward function for one model chunk.

    Args:
        stage: The current stage of the model.
        input_tensor: The input tensor for the forward pass.
        buffers: Buffers used during the forward pass.
        model_chunk_id: Identifier for the model chunk.
        microbatch_id: Identifier for the microbatch.

    Returns:
        The output tensor after the forward pass.
    """
    if isinstance(stage, OpaqueRef):
        worker_stage = stage.value
    else:
        assert isinstance(stage, nn.Module)
        worker_stage = stage
    input_tensor.requires_grad_(True)
    with torch.enable_grad():
        output = worker_stage(
            input_tensor=input_tensor,
        )
        return output


def _run_backward_udf(
    input_tensor: torch.Tensor,
    output_tensor: torch.Tensor,
    output_tensor_grad: Optional[torch.Tensor],
    y: torch.Tensor,
    loss_layer: OpaqueRef,
    loss_list: OpaqueRef,
    model_chunk_id: int,
    microbatch_id: int,
    num_microbatches: int,
    is_last_stage: bool,
    is_last_microbatch: bool,
) -> Optional[torch.Tensor]:
    return input_tensor


run_backward_udf = remote(
    "monarch.parallel.pipelining.runtime.run_backward_impl", propagate=_run_backward_udf
)


def run_backward_impl(
    input_tensor: torch.Tensor,
    output_tensor: torch.Tensor,
    output_tensor_grad: Optional[torch.Tensor],
    y: torch.Tensor,
    loss_layer: nn.Module | OpaqueRef,
    loss_list: List[torch.Tensor] | OpaqueRef,
    model_chunk_id: int,
    microbatch_id: int,
    num_microbatches: int,
    is_last_stage: bool,
    is_last_microbatch: bool,
) -> Optional[torch.Tensor]:
    """
    Run the backward function for one model chunk.

    Args:
        input_tensor: The input tensor for the backward pass.
        output_tensor: The output tensor from the forward pass.
        output_tensor_grad: The gradient of the output tensor.
        y: The target tensor.
        loss_layer: The loss layer used to compute the loss.
        loss_list: A list to store the computed loss values.
        model_chunk_id: Identifier for the model chunk.
        microbatch_id: Identifier for the microbatch.
        num_microbatches: Total number of microbatches.
        is_last_stage: Flag indicating if this is the last stage.
        is_last_microbatch: Flag indicating if this is the last microbatch.

    Returns:
        The gradient of the input tensor if it requires gradient, otherwise None.
    """
    input_tensor.requires_grad_(True)
    if is_last_stage:
        if isinstance(loss_layer, OpaqueRef):
            worker_loss_layer = loss_layer.value
        else:
            worker_loss_layer = loss_layer
        with torch.enable_grad():
            loss = worker_loss_layer(output_tensor, y).mean() / num_microbatches
            worker_loss_list = (
                loss_list.value if isinstance(loss_list, OpaqueRef) else loss_list
            )
            worker_loss_list.append(loss)
            if is_last_microbatch:
                all_loss = torch.stack(worker_loss_list).sum()
                worker_loss_list.clear()
                worker_loss_list.append(all_loss)

            output = loss

    if input_tensor is not None and input_tensor.requires_grad:
        input_tensor.retain_grad()
    if output_tensor_grad is None:
        assert is_last_stage
        output.backward(retain_graph=True)
    else:
        torch.autograd.backward(
            output_tensor, grad_tensors=output_tensor_grad, retain_graph=True
        )
    if input_tensor is not None and input_tensor.requires_grad:
        return input_tensor.grad
    return None


# Get the parameter with the given name from a module.
get_parameter_udf = remote(
    "monarch.parallel.pipelining.runtime.get_parameter",
    propagate=lambda module, param_name, param_shape: torch.randn(param_shape),
)


def get_parameter(
    module_ref: nn.Module | OpaqueRef,
    param_name: str,
    param_shape: tuple,
):
    """
    Retrieves a parameter from a PyTorch module.
    Args:
        module (nn.Module): The PyTorch module to retrieve the parameter from.
        param_name (str): The name of the parameter to retrieve.
    Returns:
        torch.Tensor: The retrieved parameter as a tensor.
    Raises:
        AttributeError: If the parameter does not exist in the module.
    """

    if isinstance(module_ref, OpaqueRef):
        module = module_ref.value
    else:
        module = module_ref
    for name, param in module.named_parameters():
        if name == param_name:
            return param
    raise AttributeError(
        f"Module '{module.__class__.__name__}' has no attribute '{param_name}'"
    )
    return param


# Retrieves the loss for the batch.
get_loss_udf = remote(
    "monarch.parallel.pipelining.runtime.get_loss_impl",
    propagate=lambda loss_list: torch.tensor(0.0),
)


def get_loss_impl(loss_list):
    """
    Get the loss for the batch.

    Args:
        loss_list: A list containing loss values.

    Returns:
        The first loss value from the list.
    """
    if isinstance(loss_list, OpaqueRef):
        worker_loss_list = loss_list.value
    else:
        worker_loss_list = loss_list
    loss = worker_loss_list[0]
    worker_loss_list.clear()
    return loss


class PipelineParallelism:
    """
    Utility class for generating schedule actions based on a pipeline parallelism schedule.
    This class is not a core Monarch primitive, but rather a helper utility to simplify the
    process of creating and executing pipeline parallelism schedules.
    It reuses the similar abstraction as PyTorch pipelining API
    (https://github.com/pytorch/pytorch/blob/3cbc8c54fd37eb590e2a9206aecf3ab568b3e63c/torch/distributed/pipelining/_IR.py#L1200)

    This class handles the following functionality of pipeline parallelism:
    1. Initialization: Takes a list of modules as model pipeline stages and a
        list of meshes as devices to execute pipeline stages. Initializes the
        model stages on the meshes in the initialize() function.
    2. Pipeline IR Schedule Generation: Generates a pipeline parallelism
        schedule according to the user-selected algorithm in the
        generate_pipeline_ir_schedule() function.
    3. Schedule Dispatch: Dispatches actions from the pipeline parallelism
        schedule to all stages in the dispatch_pp_schedule() function.
    4. Action Execution: Executes individual actions on a pipeline stage in the run_action() function.
    """

    def __init__(
        self,
        meshes: List[DeviceMesh],
        stages: List[List[nn.Module | OpaqueRef | OpaqueModule]],
        compute_stream: Stream,
        p2p_stream: Stream,
        schedule: str = "dora-dfs",
        batch_size: int = 4,
        microbatch_size: int = 1,
        loss_fn: Optional[nn.Module] = None,
        loss_list=None,
    ):
        self.stages = stages
        self.meshes = meshes
        self.schedule = schedule
        self.batch_size = batch_size
        self.microbatch_size = microbatch_size
        self.compute_stream = compute_stream
        self.p2p_stream = p2p_stream or Stream("p2p_stream")

        # TODO(dongli): clean up buffer eagerly to save memory.
        self.input_tensors = {}
        self.output_tensors = {}
        self.output_tensor_grads = {}
        self.input_tensor_grads = {}
        self.fwd_send_handles = {}
        self.bwd_send_handles = {}
        self.input_tensors_borrowed = {}

        self.num_microbatches = self.batch_size // self.microbatch_size
        self.num_model_chunks = len(self.stages)
        self.pipeline_parallel_size = len(self.meshes)
        self.loss_list = loss_list

        for i in range(self.num_microbatches):
            self.output_tensor_grads[(self.num_model_chunks - 1, i)] = None

        with self.meshes[-1].activate():
            self.loss_layer = loss_fn

        self.all_rank_actions, self.stage_to_rank_map = (
            self.generate_pipeline_ir_schedule(
                schedule_name=self.schedule,
                total_num_model_chunks=len(self.stages),
                pipeline_parallel_size=len(self.meshes),
                batch_size=self.batch_size,
                microbatch_size=self.microbatch_size,
            )
        )
        logger.info(
            f"PipelineParallelism: pp_ir_schedule:\n{_format_pipeline_order(self.all_rank_actions)} \n{self.stage_to_rank_map=}"
        )

    def stage_to_rank(self, stage_idx):
        return self.stage_to_rank_map[stage_idx]

    def initialize(
        self,
    ):
        pp_stages = self.stages
        assert len(pp_stages) == len(self.meshes)
        for stage_idx, stage in enumerate(pp_stages):
            for module in stage:
                state_dict = module.state_dict()
                for k, v in state_dict.items():
                    if isinstance(v, Tensor):
                        state_dict[k] = v.to_mesh(self.meshes[stage_idx])
                module.load_state_dict(state_dict, assign=True)

    def copy_params_to_new_model(
        self,
        ref_model: List[nn.Module],
    ):
        pp_stages = self.stages
        assert len(pp_stages) == len(self.meshes)
        for stage_idx, stage in enumerate(pp_stages):
            assert len(stage) == 1
            module = stage[0]
            ref_module = ref_model[stage_idx]
            ref_model_state_dict = ref_module.state_dict()

            src_params = {}
            ref_params_shape = {}
            with self.meshes[0].activate():
                for ref_name, ref_param in ref_model_state_dict.items():
                    ref_params_shape[ref_name] = ref_param.shape
            with self.meshes[stage_idx].activate():
                for ref_name, _ in ref_model_state_dict.items():
                    ref_param_shape = ref_params_shape[ref_name]
                    if isinstance(module, OpaqueRef):
                        param = get_parameter_udf(module, ref_name, ref_param_shape)
                    elif isinstance(module, OpaqueModule):
                        # TODO: implment named_parameters() for OpaqueModule
                        param = get_parameter_udf(
                            module._object, ref_name, ref_param_shape
                        )
                    elif isinstance(module, nn.Module):
                        param = get_parameter(module, ref_name, ref_param_shape)
                    else:
                        raise ValueError(f"Unknown module type: {module}")

                    param_local = fetch_shard(param).result()
                    with no_mesh.activate():
                        src_params[ref_name] = param_local.detach().cpu().numpy()
            for (
                name,
                _,
            ) in ref_model_state_dict.items():
                param_value = src_params[name]
                with self.meshes[0].activate():
                    new_param = torch.tensor(param_value)
                ref_model_state_dict[name] = new_param

            ref_module.load_state_dict(ref_model_state_dict, assign=True)

    def configure_optimizers(self, config, config_fn):
        optimizers = []

        for stage in self.stages:
            params = list(chain(*[list(m.parameters()) for m in stage]))
            optimizers.append(
                config_fn(
                    config.weight_decay,
                    config.learning_rate,
                    (config.beta1, config.beta2),
                    config.device_type,
                    config.optimizer,
                    params,
                )
            )

        return optimizers

    def generate_pipeline_ir_schedule(
        self,
        schedule_name,
        total_num_model_chunks,
        pipeline_parallel_size,
        batch_size,
        microbatch_size,
    ):
        assert batch_size % microbatch_size == 0, (
            "Batch size should be divisible by microbatch size."
        )
        num_microbatches = batch_size // microbatch_size
        num_round = max(num_microbatches // pipeline_parallel_size, 1)
        assert num_microbatches % num_round == 0, (
            "Number of microbatches should be divisible by number of pipeline rounds."
        )
        num_microbatch_per_round = num_microbatches // num_round

        num_model_chunks = total_num_model_chunks // pipeline_parallel_size
        total_num_microbatches = num_microbatches * num_model_chunks
        zero_bubble = True
        all_rank_actions, stage_to_rank = generate_schedule(
            schedule_name,
            num_model_chunks,
            pipeline_parallel_size,
            num_round,
            num_microbatch_per_round,
            zero_bubble,
            total_num_microbatches,
            num_microbatches,
        )
        return all_rank_actions, stage_to_rank

    def split_inputs_outputs(self, x, y):
        microbatch_x = x.split(self.microbatch_size, dim=0)
        for i, _x in enumerate(microbatch_x):
            _x = _x.to_mesh(self.meshes[0])
            self.input_tensors[(0, i)] = _x

        y = y.to_mesh(self.meshes[-1])
        with self.meshes[-1].activate():
            microbatch_y = y.split(self.microbatch_size, dim=0)
        return microbatch_x, microbatch_y

    def run(self, x, y):
        self.loss = None
        microbatch_x, microbatch_y = self.split_inputs_outputs(x, y)
        self.dispatch_pp_schedule(
            pipeline_order=self.all_rank_actions,
            stage_to_rank=self.stage_to_rank,
            num_stages=len(self.stages),
            microbatch_x=microbatch_x,
            microbatch_y=microbatch_y,
        )
        self.loss = (
            get_loss_udf(self.loss_list)
            if isinstance(self.loss_list, OpaqueRef)
            else get_loss_impl(self.loss_list)
        )
        return self.loss
        return self.loss

    def dispatch_pp_schedule(
        self,
        pipeline_order,
        stage_to_rank: Callable[[int], int],
        num_stages: int,
        microbatch_x,
        microbatch_y,
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
                peer_stage_idx = action.stage_index - 1
                for p in _prev_ops(peer_stage_idx):
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
            is_progressing = False
            for rank in sorted(pipeline_order):
                if len(pipeline_order[rank]) == 0:
                    continue

                action = pipeline_order[rank][0]
                if _ready_to_schedule(action):
                    if action is not None:
                        schedule[rank].append(action)
                        self.run_action(action, microbatch_x, microbatch_y)
                    pipeline_order[rank].pop(0)
                    is_progressing = True
                else:
                    schedule[rank].append(None)

            for i in sorted(pipeline_order, reverse=True):
                if len(pipeline_order[i]) == 0:
                    del pipeline_order[i]

            if not is_progressing:
                logger.error("WIP comms schedule:\n", _format_pipeline_order(schedule))
                for rank in pipeline_order:
                    print(f"{rank=} next action= {pipeline_order[rank][0]}")
                raise ValueError("Schedule is not progressing")
        return schedule

    def run_action(
        self,
        action,
        microbatch_x,
        microbatch_y,
    ):
        logger.info(f"running --------> {action=}")
        comp_type = action.computation_type
        mb_index: int = (
            action.microbatch_index if action.microbatch_index is not None else -1
        )
        stage_idx = action.stage_index
        model_chunk_id = stage_idx
        microbatch_id = mb_index

        pipeline_parallel_rank = self.stage_to_rank(stage_idx)

        num_model_chunks = self.num_model_chunks

        is_last_stage = stage_idx == num_model_chunks - 1
        is_last_microbatch = microbatch_id == self.num_microbatches - 1
        try:
            with torch.profiler.record_function(
                f"r{pipeline_parallel_rank}/{model_chunk_id}_{comp_type}_{microbatch_id}"
            ):
                match str(comp_type):
                    case "SEND_F":
                        output_tensor = (
                            self.output_tensors[(model_chunk_id, microbatch_id)]
                            .clone()
                            .detach()
                        )
                        other_rank = self.stage_to_rank(stage_idx + 1)
                        borrow_output_tensor, borrow = self.p2p_stream.borrow(
                            output_tensor
                        )
                        self.fwd_send_handles[(model_chunk_id + 1, microbatch_id)] = (
                            borrow
                        )
                        with self.p2p_stream.activate():
                            self.input_tensors[(model_chunk_id + 1, microbatch_id)] = (
                                borrow_output_tensor.to_mesh(self.meshes[other_rank])
                            )
                    case "SEND_B":
                        other_rank = self.stage_to_rank(stage_idx - 1)
                        input_tensor_grad = self.input_tensor_grads[
                            (model_chunk_id, microbatch_id)
                        ]
                        borrow_input_tensor_grad, borrow = self.p2p_stream.borrow(
                            input_tensor_grad
                        )
                        self.bwd_send_handles[(model_chunk_id - 1, microbatch_id)] = (
                            borrow
                        )
                        with self.p2p_stream.activate():
                            self.output_tensor_grads[
                                (model_chunk_id - 1, microbatch_id)
                            ] = borrow_input_tensor_grad.to_mesh(
                                self.meshes[other_rank]
                            )
                        if model_chunk_id > 0:
                            (
                                input_tensor,
                                borrow,
                            ) = self.input_tensors_borrowed[
                                (model_chunk_id, microbatch_id)
                            ]
                            borrow.drop()
                    case "RECV_F":
                        assert (model_chunk_id, microbatch_id) in self.input_tensors
                        borrow = self.fwd_send_handles[(model_chunk_id, microbatch_id)]
                        borrow.drop()
                    case "RECV_B":
                        assert (
                            model_chunk_id,
                            microbatch_id,
                        ) in self.output_tensor_grads
                        borrow = self.bwd_send_handles[(model_chunk_id, microbatch_id)]
                        borrow.drop()
                    case "F":
                        with self.meshes[stage_idx].activate():
                            stage = self.stages[model_chunk_id][0]
                            input_tensor = self.input_tensors[
                                (model_chunk_id, microbatch_id)
                            ]
                            if model_chunk_id > 0:
                                input_tensor_borrowed, borrow = (
                                    self.compute_stream.borrow(input_tensor)
                                )
                                self.input_tensors_borrowed[
                                    (model_chunk_id, microbatch_id)
                                ] = (
                                    input_tensor_borrowed,
                                    borrow,
                                )
                            else:
                                input_tensor_borrowed = input_tensor
                            if isinstance(stage, OpaqueRef) or isinstance(
                                stage, OpaqueModule
                            ):
                                fwd_func = run_forward_udf
                            else:
                                fwd_func = run_forward_impl

                            output_tensor = fwd_func(
                                stage._object
                                if isinstance(stage, OpaqueModule)
                                else stage,
                                input_tensor_borrowed,
                                model_chunk_id=model_chunk_id,
                                microbatch_id=microbatch_id,
                            )
                            self.output_tensors[(model_chunk_id, microbatch_id)] = (
                                output_tensor
                            )
                    case "BW":
                        with self.meshes[stage_idx].activate():
                            stage = self.stages[model_chunk_id][0]
                            if model_chunk_id > 0:
                                (
                                    input_tensor,
                                    borrow,
                                ) = self.input_tensors_borrowed[
                                    (model_chunk_id, microbatch_id)
                                ]
                            else:
                                input_tensor = self.input_tensors[
                                    (model_chunk_id, microbatch_id)
                                ]
                                borrow = None

                            output_tensor = self.output_tensors[
                                (model_chunk_id, microbatch_id)
                            ]
                            output_tensor_grad = self.output_tensor_grads[
                                (model_chunk_id, microbatch_id)
                            ]
                            if output_tensor_grad is not None:
                                borrow_output_tensor_grad, output_tensor_grad_borrow = (
                                    self.compute_stream.borrow(output_tensor_grad)
                                )
                            else:
                                borrow_output_tensor_grad = None
                            if isinstance(self.loss_list, OpaqueRef):
                                bwd_func = run_backward_udf
                            else:
                                bwd_func = run_backward_impl
                            input_tensor_grad = bwd_func(
                                input_tensor=input_tensor,
                                output_tensor=output_tensor,
                                output_tensor_grad=borrow_output_tensor_grad,
                                y=microbatch_y[microbatch_id]
                                if is_last_stage
                                else None,
                                loss_layer=self.loss_layer if is_last_stage else None,
                                loss_list=self.loss_list if is_last_stage else None,
                                model_chunk_id=model_chunk_id,
                                microbatch_id=microbatch_id,
                                num_microbatches=self.num_microbatches,
                                is_last_stage=is_last_stage,
                                is_last_microbatch=is_last_microbatch,
                            )
                            self.input_tensor_grads[(model_chunk_id, microbatch_id)] = (
                                input_tensor_grad
                            )
                            if output_tensor_grad is not None:
                                output_tensor_grad_borrow.drop()
                    case _:
                        raise ValueError(f"{action=} is unknown or unsupported")

        except Exception as e:
            logger.exception(
                "_PipelineScheduleRuntime caught exception at step when running action %s. error %s Full Schedule:",
                action,
                e,
            )


def add_sys_path_impl(new_directory):
    if new_directory not in sys.path:
        sys.path.append(new_directory)


def build_module_chunk(module_name_or_path, *args, **kwargs):
    """
    Builds a module chunk for pipeline parallelism.

    Args:
        input_dim (int): The number of input features.
        output_dim (int): The number of output features.
        hidden_dim (int, optional): The number of neurons in the hidden layer. Defaults to 128.

    Returns:
        torch.nn.Module: The module chunk.
    """
    module_path, class_name = module_name_or_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    module_class = getattr(module, class_name)

    model_chunk = module_class(*args, **kwargs)
    model_chunk.train()
    model_chunk.to("cuda")
    return OpaqueRef(model_chunk)


def build_loss_list():
    loss_list = []
    return OpaqueRef(loss_list)


def build_pp_loss_layer():
    loss = nn.MSELoss()
    return OpaqueRef(loss)


def build_optimizer_chunk(model_chunk, lr):
    """
    Builds an optimizer chunk for pipeline parallelism.

    Args:
        model_chunk (torch.nn.Module): The module chunk.

    Returns:
        torch.optim.Optimizer: The optimizer chunk.
    """
    optimizer_chunk = optim.SGD(model_chunk.value.parameters(), lr=lr)
    return OpaqueRef(optimizer_chunk)


def optimizer_zero_grad(optimizer_chunk):
    """
    Zeros the gradients of the optimizer chunk.

    Args:
        optimizer_chunk (torch.optim.Optimizer): The optimizer chunk.
    """
    optimizer_chunk.value.zero_grad()


def optimizer_step(optimizer_chunk):
    """
    Performs a step of the optimizer chunk.

    Args:
        optimizer_chunk (torch.optim.Optimizer): The optimizer chunk.
    """
    optimizer_chunk.value.step()
