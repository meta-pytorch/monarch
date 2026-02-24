# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import pytest
import torch
from monarch import fetch_shard
from monarch._testing import TestingContext
from monarch.builtins.random import (
    get_rng_state_all_cuda_remote,
    get_rng_state_remote,
    manual_seed_all_cuda_remote,
    manual_seed_cuda_remote,
    random_seed_remote,
    seed_remote,
    set_manual_seed_remote,
    set_rng_state_all_cuda_remote,
    set_rng_state_remote,
)
from monarch.common.device_mesh import no_mesh


@pytest.mark.timeout(120)
class TestRandomFunctions:
    local = None

    @classmethod
    def setup_class(cls):
        cls.local = TestingContext().__enter__()

    @classmethod
    def teardown_class(cls):
        if cls.local is not None:
            cls.local.__exit__(None, None, None)

    @classmethod
    def local_device_mesh(cls, num_hosts, gpu_per_host, activate=True):
        return cls.local.local_device_mesh(
            num_hosts,
            gpu_per_host,
            activate,
        )

    def test_set_manual_seed_remote(self):
        with self.local_device_mesh(1, 1) as device_mesh:
            with device_mesh.activate():
                set_manual_seed_remote(12345)
                t1 = torch.rand(5, 5)

                set_manual_seed_remote(12345)
                t2 = torch.rand(5, 5)

                set_manual_seed_remote(12346)
                t3 = torch.rand(5, 5)

                # t1 == t2 (same seed), t1 != t3 (different seed)
                result = fetch_shard((t1, t2, t3)).result()
                with no_mesh.activate():
                    assert torch.equal(result[0], result[1])
                    assert not torch.equal(result[0], result[2])

    def test_set_manual_seed_remote_with_process_idx(self):
        with self.local_device_mesh(1, 1) as device_mesh:
            with device_mesh.activate():
                set_manual_seed_remote(12345, process_idx=0)
                t1 = torch.rand(5, 5)

                set_manual_seed_remote(12345, process_idx=1)
                t2 = torch.rand(5, 5)

                result = fetch_shard((t1, t2)).result()
                with no_mesh.activate():
                    assert not torch.equal(result[0], result[1])

    def test_get_rng_state(self):
        with self.local_device_mesh(1, 1) as device_mesh:
            with device_mesh.activate():
                state1 = get_rng_state_remote()
                state2 = get_rng_state_remote()

                # generate a random tensor to change the state
                _ = torch.rand(5, 5)

                state3 = get_rng_state_remote()

                result = fetch_shard((state1, state2, state3)).result()
                with no_mesh.activate():
                    assert torch.equal(result[0], result[1])
                    assert not torch.equal(result[0], result[2])

    def test_set_rng_state(self):
        with self.local_device_mesh(1, 1) as device_mesh:
            with device_mesh.activate():
                # save the initial RNG state
                state = get_rng_state_remote()

                t1 = torch.rand(3, 3)
                t2 = torch.rand(3, 3)

                # restore the saved RNG state
                set_rng_state_remote(state)
                t3 = torch.rand(3, 3)

                # t1 == t3 (same state), t1 != t2 (different state)
                result = fetch_shard((t1, t2, t3)).result()
                with no_mesh.activate():
                    assert not torch.equal(result[0], result[1])
                    assert torch.equal(result[0], result[2])

    # seed and random.seed seem to be the same function.
    def test_random_seed(self):
        with self.local_device_mesh(1, 1) as device_mesh:
            with device_mesh.activate():
                random_seed_remote()
                t1 = torch.rand(5, 5)

                random_seed_remote()
                t2 = torch.rand(5, 5)

                seed_remote()
                t3 = torch.rand(5, 5)

                result = fetch_shard((t1, t2, t3)).result()
                with no_mesh.activate():
                    assert not torch.equal(result[0], result[1])
                    assert not torch.equal(result[1], result[2])

    def test_get_rng_state_all_cuda(self):
        NUM_GPUS = 1
        with self.local_device_mesh(1, NUM_GPUS) as device_mesh:
            with device_mesh.activate():
                states = get_rng_state_all_cuda_remote()

                result = fetch_shard(states).result()
                with no_mesh.activate():
                    assert isinstance(result, list)
                    assert len(result) == NUM_GPUS

    def test_set_rng_state_all_cuda(self):
        with self.local_device_mesh(1, 1) as device_mesh:
            with device_mesh.activate():
                # save the initial RNG states
                states = get_rng_state_all_cuda_remote()
                t1 = torch.rand(3, 3, device="cuda")

                # restore the saved RNG states
                set_rng_state_all_cuda_remote(states)
                t2 = torch.rand(3, 3, device="cuda")

                # t1 == t2 (same state)
                result = fetch_shard((t1, t2)).result()
                with no_mesh.activate():
                    assert torch.equal(result[0], result[1])

    def test_cuda_manual_seed(self):
        with self.local_device_mesh(1, 1) as device_mesh:
            with device_mesh.activate():
                self._cuda_seed_test(manual_seed_cuda_remote)

    def test_cuda_manual_seed_all(self):
        with self.local_device_mesh(1, 1) as device_mesh:
            with device_mesh.activate():
                self._cuda_seed_test(manual_seed_all_cuda_remote)

    def _cuda_seed_test(self, seed_func):
        seed_func(12345)
        t1 = torch.rand(5, 5, device="cuda")

        seed_func(12345)
        t2 = torch.rand(5, 5, device="cuda")

        seed_func(54321)
        t3 = torch.rand(5, 5, device="cuda")

        # t1 = t2 (same seed), t1 != t3 (different seed)
        result = fetch_shard((t1, t2, t3)).result()
        with no_mesh.activate():
            assert torch.equal(result[0], result[1])
            assert not torch.equal(result[0], result[2])
