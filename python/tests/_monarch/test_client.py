# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

# pyre-strict

from unittest import TestCase

import torch

from monarch._monarch import client, hyperactor
from pyre_extensions import none_throws


class TestClient(TestCase):
    def test_simple_with_error_response(self) -> None:
        err = client.Error.new_for_unit_test(
            7,
            8,
            hyperactor.ActorId(world_name="test", rank=0, actor_name="actor"),
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
