#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Unit tests for the multi-host RDMA benchmark's driver.
"""

import argparse

import benchmark_driver as bd
import pytest


def _parse(*flags: str, batch: bool = True, command: str = bd.RUN_COMMAND):
    """The namespace the CLI would build for ``flags`` plus a subcommand."""
    parser = argparse.ArgumentParser()
    bd.add_benchmark_args(parser, batch=batch)
    return parser.parse_args([*flags, command])


def _config(*flags: str, **kwargs) -> bd.BenchConfig:
    return bd.config_from_args(_parse(*flags, **kwargs))


def test_the_defaults_describe_a_two_host_gpu_run() -> None:
    cfg = _config()

    assert cfg.pattern == "p2p"
    assert cfg.num_hosts == 2
    assert cfg.procs_per_host == 8
    assert cfg.lane_pairing == "same"
    assert cfg.transport == "ibverbs"
    assert cfg.payload_size_mb == 1024
    assert cfg.concurrent_ops == 1
    assert cfg.runs == 3
    assert (cfg.source_on_gpu, cfg.dest_on_gpu) == (True, True)
    assert cfg.verify == "sampled"
    assert cfg.command == bd.RUN_COMMAND
    assert cfg.local_only is False
    assert cfg.cached_path is None
    assert cfg.teardown_policy == bd.TEARDOWN_ALWAYS


def test_every_shape_flag_reaches_the_config() -> None:
    cfg = _config(
        "--pattern",
        "all-to-all",
        "--num-hosts",
        "8",
        "--procs-per-host",
        "4",
        "--lane-pairing",
        "shifted",
        "--lane-shift",
        "3",
    )

    assert (cfg.pattern, cfg.num_hosts, cfg.procs_per_host) == ("all-to-all", 8, 4)
    assert (cfg.lane_pairing, cfg.lane_shift) == ("shifted", 3)


@pytest.mark.parametrize(
    ("flags", "source_on_gpu", "dest_on_gpu"),
    [
        ((), True, True),
        (("--cpu",), False, False),
        (("--gpu",), True, True),
        (("--cpu", "--dest-device", "gpu"), False, True),
        (("--gpu", "--dest-device", "cpu"), True, False),
        (("--source-device", "cpu"), False, True),
        (("--gpu", "--source-device", "cpu"), False, True),
        (("--cpu", "--source-device", "gpu"), True, False),
    ],
)
def test_each_sides_memory_kind_can_be_set_on_its_own(
    flags, source_on_gpu, dest_on_gpu
) -> None:
    """``--gpu``/``--cpu`` set both sides; either side then overrides it."""
    cfg = _config(*flags)

    assert (cfg.source_on_gpu, cfg.dest_on_gpu) == (source_on_gpu, dest_on_gpu)
    assert (cfg.source_label, cfg.dest_label) == (
        "gpu" if source_on_gpu else "cpu",
        "gpu" if dest_on_gpu else "cpu",
    )


def test_gpu_and_cpu_cannot_both_be_given() -> None:
    with pytest.raises(SystemExit):
        _parse("--gpu", "--cpu")


def test_a_payload_is_counted_in_decimal_megabytes() -> None:
    assert _config("--payload-size-mb", "1024").payload_bytes == 1024 * 1000**2
    assert _config("--payload-size-mb", "0.5").payload_bytes == 500_000


def test_a_run_is_its_ramp_plus_its_warm_iterations() -> None:
    cfg = _config("--warmup-iters-per-run", "4", "--warm-iters-per-run", "10")

    assert cfg.iterations_per_run == 14


def test_local_only_provisions_nothing() -> None:
    parser = argparse.ArgumentParser()
    bd.add_benchmark_args(parser)
    local = bd.config_from_args(
        parser.parse_args(["--num-hosts", "8", "run", "--local-only"])
    )

    assert local.num_hosts == 8
    assert local.local_only is True
    assert local.job_hosts == 0, "no job at all, however many hosts take part"


@pytest.mark.parametrize(
    "flag",
    ["--warmup-iters-per-run", "--warm-iters-per-run", "--runs"],
)
def test_a_run_shape_that_reports_nothing_is_refused(flag) -> None:
    """Each of these at zero leaves a phase with no samples, or spends the cold
    iteration on a warm slot, so the CLI rejects it before provisioning."""
    with pytest.raises(ValueError, match=flag):
        _config(flag, "0")

    assert _config(flag, "1") is not None


def test_run_batch_is_offered_only_to_schedulers_that_have_it() -> None:
    assert _config(command=bd.BATCH_COMMAND).command == bd.BATCH_COMMAND

    with pytest.raises(SystemExit):
        _parse(batch=False, command=bd.BATCH_COMMAND)


def test_run_batch_gets_the_run_only_flags_defaulted() -> None:
    """They live on the ``run`` subparser, so the namespace lacks them entirely
    and the config has to supply the same values ``run`` would have."""
    cfg = _config(command=bd.BATCH_COMMAND)

    assert cfg.local_only is False
    assert cfg.cached_path is None
    assert cfg.teardown_policy == bd.TEARDOWN_ALWAYS
