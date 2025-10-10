# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import sys
import unittest
from typing import Dict, List

import cloudpickle

import torch
from monarch._rust_bindings.monarch_hyperactor.alloc import AllocConstraints, AllocSpec
from monarch._src.actor.allocator import LocalAllocator
from monarch._src.actor.host_mesh import create_local_host_mesh, fake_in_process_host
from monarch._src.actor.proc_mesh import proc_mesh as proc_mesh_v0, ProcMesh
from monarch._src.actor.v1 import enabled as v1_enabled
from monarch.actor import Actor, endpoint


class CudaInitTestActor(Actor):
    """Actor that initializes CUDA and checks environment variables"""

    def __init__(self) -> None:
        self.env_vars_before_init: Dict[str, str] = {}
        self.cuda_initialized: bool = False

    @endpoint
    async def init_cuda_and_check_env(self, env_var_names: List[str]) -> Dict[str, str]:
        """
        Check environment variables before initializing CUDA
        Returns the values of the environment variables
        """
        for var_name in env_var_names:
            self.env_vars_before_init[var_name] = os.environ.get(var_name, "NOT_SET")

        if torch.cuda.is_available():
            torch.cuda.init()
            self.cuda_initialized = True

        return self.env_vars_before_init

    @endpoint
    async def is_cuda_initialized(self) -> bool:
        """Return whether CUDA was initialized"""
        return self.cuda_initialized


class TestEnvBeforeCuda(unittest.IsolatedAsyncioTestCase):
    """Test that the env vars are setup before cuda init"""

    @classmethod
    def setUpClass(cls) -> None:
        cloudpickle.register_pickle_by_value(sys.modules[CudaInitTestActor.__module__])

    @classmethod
    def tearDownClass(cls) -> None:
        cloudpickle.unregister_pickle_by_value(
            sys.modules[CudaInitTestActor.__module__]
        )

    async def test_lambda_sets_env_vars_before_cuda_init(self) -> None:
        """Test that environment variables are set by lambda before CUDA initialization"""
        cuda_env_vars: Dict[str, str] = {
            "CUDA_VISIBLE_DEVICES": "0",
            "CUDA_CACHE_PATH": "/tmp/cuda_cache_test",
            "CUDA_LAUNCH_BLOCKING": "1",
        }

        def setup_cuda_env() -> None:
            for name, value in cuda_env_vars.items():
                os.environ[name] = value

        if not v1_enabled:
            spec = AllocSpec(AllocConstraints(), gpus=1, hosts=1)
            allocator = LocalAllocator()
            alloc = allocator.allocate(spec)

            proc_mesh = ProcMesh.from_alloc(alloc, setup=setup_cuda_env)
        else:
            proc_mesh = fake_in_process_host().spawn_procs(bootstrap=setup_cuda_env)

        try:
            actor = proc_mesh.spawn("cuda_init", CudaInitTestActor)

            env_vars = await actor.init_cuda_and_check_env.call_one(
                list(cuda_env_vars.keys())
            )

            await actor.is_cuda_initialized.call_one()

            for name, expected_value in cuda_env_vars.items():
                self.assertEqual(
                    env_vars.get(name),
                    expected_value,
                    f"Environment variable {name} was not set correctly before CUDA initialization",
                )

        finally:
            await proc_mesh.stop()

    async def test_proc_mesh_with_lambda_env(self) -> None:
        """Test that proc_mesh function works with lambda for env parameter"""
        cuda_env_vars: Dict[str, str] = {
            "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
            "CUDA_MODULE_LOADING": "LAZY",
            "CUDA_DEVICE_MAX_CONNECTIONS": "1",
        }

        def setup_cuda_env() -> None:
            for name, value in cuda_env_vars.items():
                os.environ[name] = value

        if not v1_enabled:
            proc_mesh_instance = proc_mesh_v0(gpus=1, hosts=1, setup=setup_cuda_env)
        else:
            proc_mesh_instance = create_local_host_mesh().spawn_procs(
                bootstrap=setup_cuda_env
            )

        try:
            actor = proc_mesh_instance.spawn("cuda_init", CudaInitTestActor)

            env_vars = await actor.init_cuda_and_check_env.call_one(
                list(cuda_env_vars.keys())
            )
            for name, expected_value in cuda_env_vars.items():
                self.assertEqual(
                    env_vars.get(name),
                    expected_value,
                    f"Environment variable {name} was not set correctly before CUDA initialization",
                )

        finally:
            await proc_mesh_instance.stop()

    async def test_proc_mesh_with_dictionary_env(self) -> None:
        """Test that proc_mesh function works with dictionary for env parameter"""
        cuda_env_vars: Dict[str, str] = {
            "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
            "CUDA_MODULE_LOADING": "LAZY",
            "CUDA_DEVICE_MAX_CONNECTIONS": "1",
        }

        if not v1_enabled:
            proc_mesh_instance = proc_mesh_v0(gpus=1, hosts=1, env=cuda_env_vars)
        else:
            proc_mesh_instance = create_local_host_mesh(env=cuda_env_vars).spawn_procs()

        try:
            actor = proc_mesh_instance.spawn("cuda_init", CudaInitTestActor)
            env_vars = await actor.init_cuda_and_check_env.call_one(
                list(cuda_env_vars.keys())
            )

            self.assertEqual(
                env_vars.get("CUDA_DEVICE_ORDER"),
                "PCI_BUS_ID",
            )
            self.assertEqual(
                env_vars.get("CUDA_MODULE_LOADING"),
                "LAZY",
            )
            self.assertEqual(
                env_vars.get("CUDA_DEVICE_MAX_CONNECTIONS"),
                "1",
            )

        finally:
            await proc_mesh_instance.stop()
