# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import math
import random
from unittest import TestCase

import pytest
from monarch._rust_bindings.monarch_hyperactor.selection import Selection
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Point, Shape, Slice


class TestNdslice(TestCase):
    def test_slice(self) -> None:
        s = Slice(offset=0, sizes=[2, 3], strides=[3, 1])
        for i in range(4):
            self.assertEqual(s[i], i)
        # Test IntoIter
        current = 0
        for index in s:
            self.assertEqual(index, current)
            current += 1
        s = Slice(offset=0, sizes=[3, 4, 5], strides=[20, 5, 1])
        self.assertEqual(s[3 * 4 + 1], 13)
        s = Slice(offset=0, sizes=[2, 2, 2], strides=[4, 32, 1])
        self.assertEqual(s[2], 32)
        self.assertEqual(s[1], 1)

    def test_slice_iter(self) -> None:
        s = Slice(offset=0, sizes=[2, 3], strides=[3, 1])
        self.assertTrue(list(s) == list(range(6)))
        s = Slice(offset=10, sizes=[10, 2], strides=[10, 5])
        self.assertTrue(list(s) == list(range(10, 106, 5)))
        self.assertTrue(list(s) == [s[i] for i in range(len(s))])

    def test_slice_coordinates(self) -> None:
        s = Slice(offset=0, sizes=[2, 3], strides=[3, 1])
        self.assertEqual(s.coordinates(0), [0, 0])
        self.assertEqual(s.coordinates(3), [1, 0])
        with self.assertRaises(ValueError):
            s.coordinates(6)

        s = Slice(offset=10, sizes=[2, 3], strides=[3, 1])
        with self.assertRaises(ValueError):
            s.coordinates(6)

        self.assertEqual(s.coordinates(10), [0, 0])
        self.assertEqual(s.coordinates(13), [1, 0])

    def test_slice_index(self) -> None:
        s = Slice(offset=0, sizes=[2, 3], strides=[3, 1])
        self.assertEqual(s.index(3), 3)
        with self.assertRaises(ValueError):
            s.index(14)
        s = Slice(offset=0, sizes=[2, 2], strides=[4, 2])
        self.assertEqual(s.index(2), 1)

    def test_slice_from_list(self) -> None:
        self.assertEqual(Slice.from_list([]), [])
        slices = Slice.from_list([1, 2, 3])
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0].offset, 1)
        self.assertEqual(slices[0].sizes, [3])
        self.assertEqual(slices[0].strides, [1])
        slices = Slice.from_list([1, 3, 5])
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0].offset, 1)
        self.assertEqual(slices[0].sizes, [3])
        self.assertEqual(slices[0].strides, [2])
        slices = Slice.from_list([1, 2, 4, 5, 6])
        self.assertEqual(len(slices), 2)
        self.assertEqual(slices[0].offset, 1)
        self.assertEqual(slices[0].sizes, [2])
        self.assertEqual(slices[0].strides, [1])
        self.assertEqual(slices[1].offset, 4)
        self.assertEqual(slices[1].sizes, [3])
        self.assertEqual(slices[1].strides, [1])
        slices = Slice.from_list(list(range(100)))
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0].offset, 0)
        self.assertEqual(slices[0].sizes, [100])
        self.assertEqual(slices[0].strides, [1])
        slices = Slice.from_list([1, 2, 3, 10, 11, 12])
        self.assertEqual(len(slices), 2)
        self.assertEqual(slices[0].offset, 1)
        self.assertEqual(slices[0].sizes, [3])
        self.assertEqual(slices[0].strides, [1])
        self.assertEqual(slices[1].offset, 10)
        self.assertEqual(slices[1].sizes, [3])
        self.assertEqual(slices[1].strides, [1])

    def test_ndslice(self) -> None:
        def check(s: Slice) -> None:
            elems = list(s)
            # print("checking", s, elems)
            all_ = set(elems)
            assert len(all_) == len(elems), "wrong checks for valid strides"

            for i, e in enumerate(s):
                assert s[i] == e, "index broken"
                # print(i, ":", e, s.index(e))
                assert s.index(e) == i, "inverse broken"

            N = math.prod(s.sizes)
            try:
                s[N]
                raise RuntimeError("index broken, too many elements")
            except IndexError:
                pass

            try:
                s[-1]
                raise RuntimeError("index broken, allows negative index")
            except TypeError:
                pass

            if not all_:
                return

            small = min(all_)
            large = max(all_)
            for i in range(small, large + 1):
                try:
                    if i not in s:
                        s.index(i)
                        raise RuntimeError("index broken, extra elements")
                except ValueError:
                    pass

        check(Slice(offset=0, sizes=[3, 4], strides=[4, 1]))
        check(Slice(offset=0, sizes=[3, 4], strides=[1, 4]))
        check(Slice(offset=1, sizes=[3], strides=[2]))
        check(Slice(offset=1, sizes=[3, 3], strides=[12, 2]))
        check(Slice(offset=0, sizes=[4, 3, 8], strides=[48, 16, 2]))
        check(Slice(offset=24, sizes=[4, 3, 8], strides=[48 * 2, 16, 2]))
        check(Slice(offset=37, sizes=[], strides=[]))

        def fuzz(gen: random.Random) -> Slice:
            ndim = gen.choices([1, 2, 3, 4], [1, 2, 3, 4])[0]
            sizes = [gen.randrange(1, 10) for _ in range(ndim)]
            strides = []
            run = 1
            for j in range(ndim):
                run *= gen.randrange(1 if j == 0 else 2, 4)
                strides.append(run)

            order = list(range(ndim))
            gen.shuffle(order)
            try:
                return Slice(
                    offset=gen.randrange(10000),
                    sizes=[sizes[o] for o in order],
                    strides=[strides[o] for o in order],
                )
            except ValueError:
                # rejection sample, because some things are not in bound
                return fuzz(gen)

        gen = random.Random(0)
        for _ in range(1000):
            s = fuzz(gen)
            check(s)

    def test_pickle(self) -> None:
        import pickle

        s = Slice(offset=10, sizes=[2, 3], strides=[3, 1])
        pickled = pickle.dumps(s)
        unpickled = pickle.loads(pickled)
        self.assertEqual(s, unpickled)

    def test_slice_repr(self) -> None:
        s = Slice(offset=0, sizes=[2, 3], strides=[3, 1])
        self.assertEqual(str(s), "Slice { offset: 0, sizes: [2, 3], strides: [3, 1] }")
        self.assertEqual(repr(s), "Slice { offset: 0, sizes: [2, 3], strides: [3, 1] }")


class TestShape(TestCase):
    def test_shape_repr(self) -> None:
        s = Slice(offset=0, sizes=[2, 3], strides=[3, 1])
        shape = Shape(["label0", "label1"], s)
        self.assertEqual(str(shape), "{label0=2,label1=3}")
        self.assertEqual(
            repr(shape),
            'Shape { labels: ["label0", "label1"], slice: Slice { offset: 0, sizes: [2, 3], strides: [3, 1] } }',
        )


class TestPoint(TestCase):
    def test_point_str_simple(self) -> None:
        """Test __str__ method for Point with simple 2D shape."""

        shape = Extent(["label0", "label1"], [3, 4])

        # Test different ranks and their string representations
        point_0 = Point(0, shape)
        self.assertEqual(str(point_0), "{'label0': 0/3, 'label1': 0/4}")

        point_3 = Point(3, shape)
        self.assertEqual(str(point_3), "{'label0': 0/3, 'label1': 3/4}")

        point_11 = Point(11, shape)
        self.assertEqual(str(point_11), "{'label0': 2/3, 'label1': 3/4}")


class TestSelection(TestCase):
    def test_constants(self) -> None:
        self.assertEqual(repr(Selection.any()), "Any(True)")
        self.assertEqual(repr(Selection.all()), "All(True)")

    def test_parse(self) -> None:
        sel = Selection.from_string("(*, 1:3) & (0, *)")
        self.assertIsInstance(sel, Selection)
        self.assertEqual(
            repr(sel),
            "Intersection(All(Range(Range(1, Some(3), 1), True)), Range(Range(0, Some(1), 1), All(True)))",
        )

    def test_parse_invalid(self) -> None:
        with self.assertRaises(ValueError):
            Selection.from_string("this is not valid selection syntax")
