# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""CPU-only coverage for the same-node replica ready/consumed sequence bank."""

import os

from wbridge.backend.router import _LocalReplFlagBank, WBEndpoint


class _FakeEvent:
    def __init__(self):
        self.records = 0

    def record(self):
        self.records += 1


def test_local_repl_flag_bank_is_bidirectionally_visible_and_unlinked():
    owner = _LocalReplFlagBank.create(3)
    path = owner.path
    peer = _LocalReplFlagBank.open(path)
    try:
        assert peer.ready() == 0
        owner.publish_ready(11)
        assert peer.ready() == 11

        assert owner.consumed(2) == 0
        peer.publish_consumed(2, 9)
        assert owner.consumed(2) == 9
    finally:
        peer.close()
        owner.close()
    assert not os.path.exists(path)


def test_local_repl_flag_bank_channels_are_independent():
    owner = _LocalReplFlagBank.create(2, channels=3)
    path = owner.path
    peer = _LocalReplFlagBank.open(path, slots=2, channels=3)
    try:
        owner.publish_ready(7, 1)
        owner.publish_ready(9, 2)
        assert [peer.ready(channel) for channel in range(3)] == [0, 7, 9]

        peer.publish_consumed(0, 11, 1)
        peer.publish_consumed(1, 13, 2)
        assert owner.consumed(0, 1) == 11
        assert owner.consumed(1, 2) == 13
        assert owner.consumed(0, 2) == 0
    finally:
        peer.close()
        owner.close()
    assert not os.path.exists(path)


def test_topology_grecv_parities_use_independent_ready_and_consumed_channels():
    owner_bank = _LocalReplFlagBank.create(1, channels=3)
    reader_bank = _LocalReplFlagBank.open(owner_bank.path, slots=1, channels=3)
    owner = WBEndpoint.__new__(WBEndpoint)
    reader = WBEndpoint.__new__(WBEndpoint)
    source, owner_rank, reader_rank = 16, 8, 9
    channels = {(source, 0): 1, (source, 1): 2}
    events = {(source, slot): {reader_rank: _FakeEvent()} for slot in range(2)}
    owner._repl_local_flags = owner_bank
    owner._topo_local_slot_channel = channels
    owner._topo_slot_ready_event = events
    owner._repl_flag_slot_of = {reader_rank: 0}
    reader._repl_peer_local_flags = {owner_rank: reader_bank}
    reader._topo_peer_slot_channel = {owner_rank: channels}
    reader._repl_peer_slot_of_me = {owner_rank: 0}
    try:
        owner._publish_topo_slot_ready(source, 0, (reader_rank,), 11)
        assert reader._topo_slot_ready_reached(owner_rank, source, 0, 11)
        assert not reader._topo_slot_ready_reached(owner_rank, source, 1, 12)
        assert events[(source, 0)][reader_rank].records == 1
        assert events[(source, 1)][reader_rank].records == 0

        owner._publish_topo_slot_ready(source, 1, (reader_rank,), 12)
        assert reader._topo_slot_ready_reached(owner_rank, source, 1, 12)
        assert events[(source, 1)][reader_rank].records == 1

        reader._write_topo_slot_cons_flag(owner_rank, source, 1, 12)
        assert owner._topo_slot_cons_flag_reached(reader_rank, source, 1, 12)
        assert not owner._topo_slot_cons_flag_reached(reader_rank, source, 0, 11)
        reader._write_topo_slot_cons_flag(owner_rank, source, 0, 11)
        assert owner._topo_slot_cons_flag_reached(reader_rank, source, 0, 11)
    finally:
        reader_bank.close()
        owner_bank.close()
