# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from unittest import TestCase

import pytest

import torch
from monarch._rust_bindings.monarch_extension import client

from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._src.actor.v1 import enabled as v1_enabled
from pyre_extensions import none_throws

pytestmark: pytest.MarkDecorator = pytest.mark.skipif(
    not v1_enabled, reason="no dep on v0/v1, so only run on v1"
)


class TestClient(TestCase):
    def test_simple_with_error_response(self) -> None:
        err = client.Error.new_for_unit_test(
            7,
            8,
            ActorId(world_name="test", rank=0, actor_name="actor"),
            "test error",
        )
        resp = client.WorkerResponse.new_for_unit_test(
            seq=10,
            response=err,
        )
        self.assertTrue(resp.is_exception())
        exc = none_throws(resp.exception())
        assert isinstance(exc, client.Error)

        self.assertEqual(exc.backtrace, "test error")
        self.assertEqual(resp.result(), None)
        self.assertEqual(resp.seq, 10)

    def test_simple_with_result_response(self) -> None:
        resp = client.WorkerResponse.new_for_unit_test(
            seq=11,
            response={"test": 1},
        )
        self.assertFalse(resp.is_exception())
        self.assertEqual(resp.exception(), None)
        self.assertEqual(resp.result(), {"test": 1})
        self.assertEqual(resp.seq, 11)

    def test_tensor(self) -> None:
        tensor = torch.rand(3)
        resp = client.WorkerResponse.new_for_unit_test(
            seq=11,
            response={"result": tensor},
        )
        self.assertTrue(torch.equal(resp.result()["result"], tensor))
