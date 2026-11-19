# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import threading
from concurrent.futures import ThreadPoolExecutor
from unittest import main, TestCase

import torch
from monarch.gradient import GradientGenerator
from monarch.gradient_generator import gradient_execution_order


class TestGradIter(TestCase):
    def checkEqual(self, r, r2):
        self.assertEqual(len(r), len(r2))
        for i, i2 in zip(r, r2):
            self.assertTrue((i is None and i2 is None) or torch.allclose(i, i2))

    def test_simple(self):
        t = torch.rand(2, requires_grad=True)
        t2 = torch.rand(2, requires_grad=True)

        _ = t + t2
        a, b = torch.std_mean(t + t2)

        r2 = torch.autograd.grad([a, b], [t2, t], retain_graph=True)
        # pyrefly: ignore [missing-argument]
        r = list(GradientGenerator([a, b], [t2, t]))
        print(a, b)
        print(a.grad_fn, b.grad_fn)

        print(r)
        self.checkEqual(r, r2)

    def test_failed_reinitialization_preserves_generator(self) -> None:
        t = torch.ones(2, requires_grad=True)
        root = (t * 2).sum()

        def restore_context(*args):
            yield

        def fail_context(*args):
            raise ValueError("context restoration failed")

        generator = GradientGenerator([root], [t], [], restore_context)
        with self.assertRaisesRegex(ValueError, "context restoration failed"):
            generator.__init__([root], [t], [], fail_context)

        self.checkEqual(list(generator), [torch.full_like(t, 2)])

    def test_concurrent_calls_during_callback(self) -> None:
        t = torch.ones(2, requires_grad=True)
        root = (t * 2).sum()
        entered = threading.Event()
        release = threading.Event()
        generator = None

        def restore_context(*args):
            if generator is not None:
                entered.set()
                if not release.wait(timeout=10):
                    raise TimeoutError("callback was not released")
            yield

        generator = GradientGenerator([root], [t], [], restore_context)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(next, generator)
            try:
                self.assertTrue(entered.wait(timeout=10))
                with self.assertRaisesRegex(RuntimeError, "already in use"):
                    iter(generator)
                with self.assertRaisesRegex(RuntimeError, "already in use"):
                    next(generator)
                with self.assertRaisesRegex(RuntimeError, "already in use"):
                    generator.__init__([root], [t], [], restore_context)
            finally:
                release.set()
            self.checkEqual([future.result(timeout=10)], [torch.full_like(t, 2)])

        self.assertEqual(list(generator), [])

    def test_pipeline_like(self):
        t = torch.rand(3, 3, requires_grad=True)

        w1 = torch.rand(3, 2, requires_grad=True)
        w2 = torch.rand(3, 2, requires_grad=True)
        w3 = torch.rand(3, 2, requires_grad=True)

        u = torch.rand(3, 2, requires_grad=True)

        _ = u * u

        w4 = torch.rand(2, 3, requires_grad=True)
        w5 = torch.rand(2, 3, requires_grad=True)
        w6 = torch.rand(2, 3, requires_grad=True)

        from torch.nn.functional import relu

        a = relu(t @ (w1 @ w4))
        a = relu(a @ (w2 @ w5))
        a = relu(a @ (w3 @ w6))

        std, mean = torch.std_mean(a)
        loss = std + std

        cgrads = torch.autograd.grad(
            [loss], [t, w3, w6, u, w2, w5], allow_unused=True, retain_graph=True
        )
        # pyrefly: ignore [missing-argument]
        gi = GradientGenerator([loss], [t, w3, w6, u, w2, w5])
        grads = [*gi]
        self.checkEqual(grads, cgrads)

    def test_tree(self):
        t = torch.rand(3, 3, requires_grad=True)

        t2 = t + t
        t3 = t * t
        t4 = t / t
        t5 = t - t

        t6 = t2 * t3
        t7 = t4 * t5
        t8 = t2 * t4
        t9 = t3 * t5
        t10 = t6 + t7 + t8 + t9

        t11 = t10.sum()

        cgrads = torch.autograd.grad([t11], [t2, t], retain_graph=True)
        # pyrefly: ignore [missing-argument]
        gi = GradientGenerator([t11], [t2, t])
        grads = [*gi]
        self.checkEqual(grads, cgrads)

    def test_broadcast(self):
        t = torch.rand(3, 3, requires_grad=True)
        t2 = torch.rand(3, requires_grad=True)
        t3 = t2 / t2

        r = (t * t3).sum()
        cgrads = torch.autograd.grad([r], [t, t2], retain_graph=True)
        # pyrefly: ignore [missing-argument]
        gi = GradientGenerator([r], [t, t2])
        grads = [*gi]
        self.checkEqual(grads, cgrads)

    def test_grad_order(self):
        t = torch.rand(3, 3, requires_grad=True)
        w1 = torch.rand(3, 3, requires_grad=True)
        w2 = torch.rand(3, 3, requires_grad=True)
        w3 = torch.rand(3, 3, requires_grad=True)

        u = torch.rand(3, 2, requires_grad=True)
        _ = u * u
        from torch.nn.functional import relu

        a = relu(t @ w1)
        a = relu(a @ w2)
        a = relu(a @ w3)

        std, mean = torch.std_mean(a)
        loss = std + std

        order = gradient_execution_order([loss], [w2, w3, w1, a])
        self.assertEqual(order, [3, 1, 0, 2])


if __name__ == "__main__":
    main()
