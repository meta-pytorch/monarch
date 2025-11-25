# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import math
from typing import cast
from unittest import TestCase

import cloudpickle

from monarch._rust_bindings.monarch_extension import tensor_worker
from monarch._rust_bindings.monarch_hyperactor import shape
from pyre_extensions import none_throws


def is_nan(val: int) -> bool:
    return math.isnan(val)


class MockReferencable:
    def __init__(self, ref: int) -> None:
        self.ref = ref

    def __monarch_ref__(self) -> int:
        return self.ref


class TestWorker(TestCase):
    def test_backend_network_init(self) -> None:
        msg = tensor_worker.BackendNetworkInit()
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))

    def test_backend_network_init_point_to_point(self) -> None:
        msg = tensor_worker.BackendNetworkPointToPointInit(
            from_stream=tensor_worker.StreamRef(id=1),
            to_stream=tensor_worker.StreamRef(id=2),
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.from_stream.id, 1)
        self.assertEqual(msg.to_stream.id, 2)

    def test_call_function(self) -> None:
        import torch

        msg = tensor_worker.CallFunction(
            seq=10,
            results=[tensor_worker.Ref(id=2), None],
            mutates=[],
            function=tensor_worker.FunctionPath(path="torch.ops.aten.ones.default"),
            args=([2, 3],),
            kwargs={"device": "cpu", "pin_memory": False},
            stream=tensor_worker.StreamRef(id=1),
            remote_process_groups=[],
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.seq, 10)
        self.assertEqual(msg.results, [tensor_worker.Ref(id=2), None])
        self.assertEqual(msg.mutates, [])
        self.assertEqual(
            cast(tensor_worker.FunctionPath, msg.function).path,
            "torch.ops.aten.ones.default",
        )
        self.assertEqual(msg.args, ([2, 3],))
        self.assertEqual(
            msg.kwargs, {"device": torch.device("cpu"), "pin_memory": False}
        )
        self.assertIsInstance(msg.kwargs["pin_memory"], bool)
        # we cannot use isinstance to assert bool vs int
        self.assertTrue(msg.kwargs["pin_memory"] is False)
        self.assertEqual(msg.stream, tensor_worker.StreamRef(id=1))

    def test_call_function_live_function(self) -> None:
        msg = tensor_worker.CallFunction(
            seq=10,
            results=[],
            mutates=[],
            function=tensor_worker.Cloudpickle(bytes=cloudpickle.dumps(is_nan)),
            args=(),
            kwargs={},
            stream=tensor_worker.StreamRef(id=1),
            remote_process_groups=[],
        )
        self.assertFalse(msg.function.resolve()(4))

    def test_call_function_referencable_args(self) -> None:
        msg = tensor_worker.CallFunction(
            seq=10,
            results=[],
            mutates=[],
            function=tensor_worker.FunctionPath(
                path="torch.ops.aten._foreach_add.Tensor"
            ),
            args=([MockReferencable(1), MockReferencable(2)],),
            kwargs={
                "other": MockReferencable(3),
            },
            stream=tensor_worker.StreamRef(id=1),
            remote_process_groups=[],
        )
        self.assertEqual(
            msg.args, ([tensor_worker.Ref(id=1), tensor_worker.Ref(id=2)],)
        )
        self.assertEqual(msg.kwargs, {"other": tensor_worker.Ref(id=3)})

    def test_create_stream(self) -> None:
        msg = tensor_worker.CreateStream(
            id=tensor_worker.StreamRef(id=10),
            stream_creation=tensor_worker.StreamCreationMode.CreateNewStream,
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.id.id, 10)
        self.assertEqual(
            msg.stream_creation, tensor_worker.StreamCreationMode.CreateNewStream
        )

    def test_create_device_mesh(self) -> None:
        msg = tensor_worker.CreateDeviceMesh(
            result=tensor_worker.Ref(id=10),
            names=("x", "y"),
            ranks=shape.Slice(offset=0, sizes=[2, 3], strides=[3, 1]),
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.result.id, 10)
        self.assertEqual(msg.names, ["x", "y"])
        self.assertEqual(msg.ranks.ndim, 2)

    def test_create_remote_process_group(self) -> None:
        msg = tensor_worker.CreateRemoteProcessGroup(
            result=tensor_worker.Ref(id=10),
            device_mesh=tensor_worker.Ref(id=12),
            dims=("x", "y"),
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.result.id, 10)
        self.assertEqual(msg.device_mesh.id, 12)
        self.assertEqual(msg.dims, ["x", "y"])

    def test_borrow_create(self) -> None:
        msg = tensor_worker.BorrowCreate(
            result=tensor_worker.Ref(id=10),
            borrow=23,
            tensor=tensor_worker.Ref(id=20),
            from_stream=tensor_worker.StreamRef(id=1),
            to_stream=tensor_worker.StreamRef(id=2),
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.result.id, 10)
        self.assertEqual(msg.borrow, 23)
        self.assertEqual(msg.tensor.id, 20)
        self.assertEqual(msg.from_stream.id, 1)
        self.assertEqual(msg.to_stream.id, 2)

    def test_borrow_first_use(self) -> None:
        msg = tensor_worker.BorrowFirstUse(
            borrow=23,
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.borrow, 23)

    def test_borrow_last_use(self) -> None:
        msg = tensor_worker.BorrowLastUse(
            borrow=23,
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.borrow, 23)

    def test_borrow_drop(self) -> None:
        msg = tensor_worker.BorrowDrop(
            borrow=23,
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.borrow, 23)

    def test_delete_refs(self) -> None:
        msg = tensor_worker.DeleteRefs(
            refs=[tensor_worker.Ref(id=1), tensor_worker.Ref(id=2)]
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.refs, [tensor_worker.Ref(id=1), tensor_worker.Ref(id=2)])

    def test_request_status(self) -> None:
        msg = tensor_worker.RequestStatus(seq=10, controller=False)
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.seq, 10)

    def test_reduce(self) -> None:
        import torch

        msg = tensor_worker.Reduce(
            result=tensor_worker.Ref(id=10),
            tensor=tensor_worker.Ref(id=20),
            factory=tensor_worker.TensorFactory(
                size=(2, 3),
                dtype=torch.bfloat16,
                device=torch.device("cpu"),
                layout=torch.sparse_csr,
            ),
            mesh=tensor_worker.Ref(id=30),
            dims=("x",),
            stream=tensor_worker.StreamRef(id=1),
            scatter=False,
            in_place=False,
            reduction=tensor_worker.ReductionType.Stack,
            out=None,
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.result.id, 10)
        self.assertEqual(msg.tensor.id, 20)
        self.assertEqual(msg.factory.size, (2, 3))
        self.assertEqual(msg.factory.dtype, torch.bfloat16)
        self.assertEqual(msg.factory.device, str(torch.device("cpu")))
        self.assertEqual(msg.factory.layout, torch.sparse_csr)
        self.assertEqual(msg.mesh.id, 30)
        self.assertEqual(msg.dims, ["x"])
        self.assertEqual(msg.stream.id, 1)
        self.assertEqual(msg.scatter, False)
        self.assertEqual(msg.in_place, False)
        self.assertEqual(msg.reduction, tensor_worker.ReductionType.Stack)
        self.assertIsNone(msg.out)

        msg = tensor_worker.Reduce(
            result=tensor_worker.Ref(id=10),
            tensor=tensor_worker.Ref(id=20),
            factory=tensor_worker.TensorFactory(
                size=(2, 3),
                dtype=torch.bfloat16,
                device=torch.device("cpu"),
                layout=torch.sparse_csr,
            ),
            mesh=tensor_worker.Ref(id=30),
            dims=("x",),
            stream=tensor_worker.StreamRef(id=1),
            scatter=False,
            in_place=False,
            reduction=tensor_worker.ReductionType.Stack,
            out=tensor_worker.Ref(id=40),
        )
        self.assertEqual(msg.out.id, 40)

    def test_send_tensor(self) -> None:
        import torch

        msg = tensor_worker.SendTensor(
            tensor=tensor_worker.Ref(id=10),
            from_stream=tensor_worker.StreamRef(id=1),
            to_stream=tensor_worker.StreamRef(id=2),
            from_ranks=shape.Slice(offset=0, sizes=[2, 3], strides=[3, 1]),
            to_ranks=shape.Slice(offset=0, sizes=[3, 4, 5], strides=[20, 5, 1]),
            result=tensor_worker.Ref(id=2),
            factory=tensor_worker.TensorFactory(
                size=(2, 5),
                dtype=torch.float32,
                device=torch.device("cuda"),
                layout=torch.strided,
            ),
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.tensor.id, 10)
        self.assertEqual(msg.from_stream.id, 1)
        self.assertEqual(msg.to_stream.id, 2)
        self.assertEqual(msg.from_ranks.ndim, 2)
        self.assertEqual(msg.to_ranks.ndim, 3)
        self.assertEqual(msg.result.id, 2)
        self.assertEqual(msg.factory.size, (2, 5))
        self.assertEqual(msg.factory.dtype, torch.float32)

    def test_create_pipe(self) -> None:
        msg = tensor_worker.CreatePipe(
            result=tensor_worker.Ref(id=10),
            key="some_key",
            function=tensor_worker.FunctionPath(path="builtins.range"),
            max_messages=1,
            mesh=tensor_worker.Ref(id=20),
            args=(1, 10),
            kwargs={},
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.result.id, 10)
        self.assertEqual(msg.key, "some_key")
        self.assertEqual(
            cast(tensor_worker.FunctionPath, msg.function).path, "builtins.range"
        )
        self.assertEqual(msg.mesh.id, 20)
        self.assertEqual(msg.args, (1, 10))
        self.assertEqual(msg.kwargs, {})

    def test_send_value(self) -> None:
        msg = tensor_worker.SendValue(
            seq=100,
            destination=tensor_worker.Ref(id=10),
            mutates=[],
            function=None,
            args=(500,),
            kwargs={},
            stream=tensor_worker.StreamRef(id=1),
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.seq, 100)
        self.assertEqual(none_throws(msg.destination).id, 10)
        self.assertEqual(msg.mutates, [])
        self.assertEqual(msg.function, None)
        self.assertEqual(msg.args, (500,))
        self.assertEqual(msg.kwargs, {})
        self.assertEqual(msg.stream.id, 1)

    def test_pipe_recv(self) -> None:
        msg = tensor_worker.PipeRecv(
            seq=101,
            results=[tensor_worker.Ref(id=10), tensor_worker.Ref(id=11)],
            pipe=tensor_worker.Ref(id=1),
            stream=tensor_worker.StreamRef(id=2),
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(msg.seq, 101)
        self.assertEqual(
            msg.results, [tensor_worker.Ref(id=10), tensor_worker.Ref(id=11)]
        )
        self.assertEqual(msg.pipe.id, 1)
        self.assertEqual(msg.stream.id, 2)

    def test_exit(self) -> None:
        msg = tensor_worker.Exit(error_reason=None)
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))

    def test_command_group(self) -> None:
        msg = tensor_worker.CommandGroup(
            commands=[
                tensor_worker.CallFunction(
                    seq=10,
                    results=[tensor_worker.Ref(id=2), None],
                    mutates=[],
                    function=tensor_worker.FunctionPath(
                        path="torch.ops.aten.ones.default"
                    ),
                    args=([2, 3],),
                    kwargs={"device": "cpu"},
                    stream=tensor_worker.StreamRef(id=1),
                    remote_process_groups=[],
                ),
                tensor_worker.Exit(error_reason=None),
            ]
        )
        self.assertTrue(isinstance(msg, tensor_worker.WorkerMessage))
        self.assertEqual(len(msg.commands), 2)
        msg0 = msg.commands[0]
        assert isinstance(msg0, tensor_worker.CallFunction)
        self.assertEqual(msg0.seq, 10)
        self.assertEqual(msg0.results, [tensor_worker.Ref(id=2), None])
        self.assertEqual(
            cast(tensor_worker.FunctionPath, msg0.function).path,
            "torch.ops.aten.ones.default",
        )
        self.assertEqual(msg0.args, ([2, 3],))
        self.assertTrue(isinstance(msg.commands[1], tensor_worker.Exit))
