# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import math
from dataclasses import dataclass
from typing import Iterator, List, Sequence


@dataclass
class Interval:
    start: int
    end: int
    value: bytes


@dataclass
class IntervalIterator:
    next_interval: Interval
    rest: Iterator[Interval]


@dataclass
class Values:
    """
    Compression Scheme for value mesh aggregation
    """

    sorted_ranks: List[Interval]
    start: int  # smallest rank in values
    end: int  # largest rank in values + 1
    nelements: int  # num of total ranks covered by ranks

    @staticmethod
    def lift(rank: int, payload: bytes):
        start = rank
        end = rank + 1
        return Values([Interval(start, end, payload)], start, end, 1)

    @staticmethod
    def merge(inputs: Sequence["Values"]):
        # construct a single Values that represents all the inputs merged together
        # (1) each elements of the new payloads should be unique. Use a hash table during merge to see if we
        # we already have the payload.
        # (2) assume that there are no overlapping ranks in the inputs, but verify the integrety of this assumption
        # during the merge
        # (3) The ranks in the intervals are sorted, so the algorithm should use a merge-sort style merge of intervals
        #    processing all the inputs objects at once. It should probably be tracking an iterator for each thing being
        #    merged and looking for the min start each time.
        # (4) if the next start is equal to the last end and they have the same payload, the interval should merge.
        #    For instance, if everything has the same value, we should end up with a single entry for sorted_ranks that
        #    covers the whole range.

        if not inputs:
            return Values([], 0, 0, 0)

        # Create iterators for each input's sorted_ranks using real iterators
        iterators = []
        for values_obj in inputs:
            interval_iter = iter(values_obj.sorted_ranks)
            try:
                next_interval = next(interval_iter)
                iterators.append(
                    IntervalIterator(
                        next_interval=next_interval,
                        rest=interval_iter,
                    )
                )
            except StopIteration:
                pass

        merged_intervals = []
        min_start = math.inf
        max_end = -math.inf
        total_elements = 0

        while iterators:
            # Find the iterator with the minimum start rank
            min_iter = min(iterators, key=lambda x: x.next_interval.start)
            current_interval = min_iter.next_interval

            # Update global bounds
            min_start = min(min_start, current_interval.start)
            max_end = max(max_end, current_interval.end)
            total_elements += current_interval.end - current_interval.start

            # Check for interval merging (requirement 4)
            if (
                merged_intervals
                and merged_intervals[-1].end == current_interval.start
                and merged_intervals[-1].value == current_interval.value
            ):
                # Merge with previous interval
                merged_intervals[-1] = Interval(
                    merged_intervals[-1].start,
                    current_interval.end,
                    merged_intervals[-1].value,
                )
            else:
                # Check for overlapping ranks (requirement 2)
                if (
                    merged_intervals
                    and merged_intervals[-1].end > current_interval.start
                ):
                    raise ValueError(
                        f"Overlapping ranks detected: previous interval ends at {merged_intervals[-1].end}, current starts at {current_interval.start}"
                    )
                merged_intervals.append(current_interval)

            # Advance the iterator
            try:
                min_iter.next_interval = next(min_iter.rest)
            except StopIteration:
                iterators.remove(min_iter)

        # Handle empty case
        if not merged_intervals:
            min_start = max_end = 0

        return Values(
            sorted_ranks=merged_intervals,
            start=int(min_start),
            end=int(max_end),
            nelements=total_elements,
        )

    def expand(self) -> List[bytes]:
        """
        Expand the compressed Values back to a list of bytes for each rank.
        Returns a list where index i contains the payload for rank self.start + i.
        """
        if not self.dense:
            raise ValueError("Values must be dense to expand to an array")

        result = []
        for interval in self.sorted_ranks:
            for _ in range(interval.end - interval.start):
                result.append(interval.value)
        return result

    @property
    def dense(self):
        return self.end - self.start == self.nelements


# pytest-style tests
def test_merge_contiguous_sequence():
    """Test merging a contiguous sequence of singleton values."""
    # Create singleton values for ranks 0-9 with the same payload
    payload = b"same_value"
    inputs = []
    for i in range(10):
        inputs.append(Values.lift(i, payload))

    # Merge them
    merged = Values.merge(inputs)

    # Should result in a single interval covering the whole range
    assert len(merged.sorted_ranks) == 1
    assert merged.sorted_ranks[0].start == 0
    assert merged.sorted_ranks[0].end == 10
    assert merged.start == 0
    assert merged.end == 10
    assert merged.nelements == 10

    # Verify expansion
    expanded = merged.expand()
    assert len(expanded) == 10
    assert all(val == payload for val in expanded)


def test_merge_randomized_order():
    """Test merging the same sequence but in randomized order."""
    import random

    # Create singleton values for ranks 0-9 with the same payload
    payload = b"same_value"
    inputs = []
    for i in range(10):
        inputs.append(Values.lift(i, payload))

    # Randomize the order
    random.shuffle(inputs)

    # Merge them
    merged = Values.merge(inputs)

    # Should still result in a single interval covering the whole range
    assert len(merged.sorted_ranks) == 1
    assert merged.sorted_ranks[0].start == 0
    assert merged.sorted_ranks[0].end == 10
    assert merged.start == 0
    assert merged.end == 10
    assert merged.nelements == 10

    # Verify expansion
    expanded = merged.expand()
    assert len(expanded) == 10
    assert all(val == payload for val in expanded)


def test_merge_two_unique_values_with_single_different_rank():
    """Test merging with two unique values where one rank has a different value."""
    payload_a = b"value_a"
    payload_b = b"value_b"

    # Create mostly payload_a with one payload_b at rank 5
    inputs = []
    for i in range(10):
        if i == 5:
            inputs.append(Values.lift(i, payload_b))
        else:
            inputs.append(Values.lift(i, payload_a))

    # Merge them
    merged = Values.merge(inputs)

    # Should result in 3 intervals: [0,5) with payload_a, [5,6) with payload_b, [6,10) with payload_a
    assert len(merged.sorted_ranks) == 3
    assert merged.start == 0
    assert merged.end == 10
    assert merged.nelements == 10

    # Check intervals
    assert merged.sorted_ranks[0].start == 0
    assert merged.sorted_ranks[0].end == 5
    assert merged.sorted_ranks[0].value == payload_a

    assert merged.sorted_ranks[1].start == 5
    assert merged.sorted_ranks[1].end == 6
    assert merged.sorted_ranks[1].value == payload_b

    assert merged.sorted_ranks[2].start == 6
    assert merged.sorted_ranks[2].end == 10
    assert merged.sorted_ranks[2].value == payload_a

    # Verify expansion
    expanded = merged.expand()
    assert len(expanded) == 10
    for i in range(10):
        if i == 5:
            assert expanded[i] == payload_b
        else:
            assert expanded[i] == payload_a


def test_merge_empty_inputs():
    """Test merging empty inputs."""
    merged = Values.merge([])
    assert len(merged.sorted_ranks) == 0
    assert merged.start == 0
    assert merged.end == 0
    assert merged.nelements == 0


def test_merge_overlapping_ranks_raises_error():
    """Test that overlapping ranks raise an error."""
    # Create two Values with overlapping ranks
    val1 = Values([Interval(0, 5, b"a")], 0, 5, 5)
    val2 = Values([Interval(3, 8, b"b")], 3, 8, 5)

    try:
        Values.merge([val1, val2])
        assert False, "Expected ValueError for overlapping ranks"
    except ValueError as e:
        assert "Overlapping ranks detected" in str(e)
