# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Check whether consolidation drops any tensor from the global round packing (→ never transferred → nan)."""

import os

import torch  # noqa: F401
from analyze_consolidate import build_all_specs
from loadspec_replay import load_records
from wbridge.backend.router import WeightRouter

recs = load_records(os.environ["SPECS"])
all_specs, ws, dts = build_all_specs(recs)


def rounds_cover(cons):
    if cons:
        os.environ["WBRIDGE_DEDUP_PAIR_BYTES"] = str(20 * 1024 * 1024)
    else:
        os.environ["WBRIDGE_DEDUP_PAIR_BYTES"] = "0"
    r = WeightRouter(rank=0, sender_ws=ws, all_specs=all_specs, dtype_spec=dict(dts))
    in_rounds = set()
    for names in r.global_rounds:
        in_rounds |= set(names)
    recv_names = set()
    for s in r.recv_specs_full:
        recv_names |= set(s.entries.keys())
    missing = recv_names - in_rounds
    return len(r.global_rounds), len(in_rounds), len(recv_names), sorted(missing)


for c in (0, 1):
    nr, nin, nall, miss = rounds_cover(c)
    print(
        f"CONS={c}: rounds={nr} names_in_rounds={nin} recv_names={nall} MISSING_FROM_ROUNDS={len(miss)}"
    )
    if miss:
        print("   e.g.:", miss[:12])
