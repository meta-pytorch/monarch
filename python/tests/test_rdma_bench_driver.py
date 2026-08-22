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
import csv

import bench_peer
import bench_stats as bs
import bench_topology as bt
import benchmark_driver as bd
import pytest
from monarch._rust_bindings.monarch_hyperactor.shape import Shape, Slice
from monarch.actor import ValueMesh


def _value_mesh(values, *, hosts: int | None = None, lanes: int = 1) -> ValueMesh:
    """A real ``ValueMesh`` over ``hosts`` x ``lanes``, in rank order."""
    labels = ["lanes"] if hosts is None else ["hosts", "lanes"]
    sizes = [lanes] if hosts is None else [hosts, lanes]
    return ValueMesh(Shape(labels, Slice.new_row_major(sizes)), list(values))


class _FakeEndpoint:
    """One actor endpoint, recording its casts and replying from ``replies``."""

    def __init__(self, replies) -> None:
        self._replies = replies
        self.casts: list[tuple] = []

    async def call(self, *args, **kwargs) -> ValueMesh:
        self.casts.append(args)
        return (
            self._replies(*args, **kwargs) if callable(self._replies) else self._replies
        )


class _FakePeers:
    """Stands in for the peer actor mesh."""

    def __init__(self, **endpoints: _FakeEndpoint) -> None:
        for name, endpoint in endpoints.items():
            setattr(self, name, endpoint)


class _FakeProcs:
    def __init__(self, peers=None) -> None:
        self.peers = peers
        self.spawned: list[tuple] = []
        self.stopped = False

    def spawn(self, name, actor_class):
        self.spawned.append((name, actor_class))
        return self.peers

    async def stop(self) -> None:
        self.stopped = True


class _FakeHost:
    """A host mesh that hands out one proc mesh and remembers the request."""

    def __init__(self, procs: _FakeProcs, hosts: int = 1) -> None:
        self.procs = procs
        self.per_host = None
        self._hosts = hosts

    def size(self) -> int:
        return self._hosts

    def spawn_procs(self, per_host):
        self.per_host = per_host
        return self.procs


class _FakeJob:
    def __init__(self, mesh_name: str, host: _FakeHost) -> None:
        self._state = type("State", (), {mesh_name: host})()
        self.cached_paths: list = []
        self.killed = False

    def state(self, cached_path=None):
        self.cached_paths.append(cached_path)
        return self._state

    def kill(self) -> None:
        self.killed = True


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


_GB: int = 1000**3


def _topology(cfg: bd.BenchConfig) -> bt.Topology:
    return bt.build_topology(
        cfg.pattern, cfg.num_hosts, cfg.procs_per_host, cfg.lane_pairing, cfg.lane_shift
    )


def _plan(cfg: bd.BenchConfig, topo: bt.Topology) -> bt.MemoryPlan:
    return bt.plan_memory(
        topo,
        ops=cfg.concurrent_ops,
        payload_bytes=cfg.payload_bytes,
        source_on_gpu=cfg.source_on_gpu,
        dest_on_gpu=cfg.dest_on_gpu,
    )


def _record(*, submit_ms: float = 200.0, spans: int = 4) -> bs.RunRecord:
    """A record holding one cold iteration and ``spans`` warm ones."""
    runs = bs.RunRecord()
    runs.register_ms.append(5.0)
    for index in range(spans + 1):
        phase = bt.COLD_QP if index == 0 else bt.WARM
        into = runs.record(phase)
        into.add_sample(
            bs.Sample(bt.Slot(0, 0), build_ms=1.0, submit_ms=submit_ms),
            initiator_bytes=_GB,
        )
        into.add_iteration(
            span_ms=submit_ms + 5.0, iteration_bytes=2 * _GB, slowest_ms=submit_ms
        )
    runs.integrity_ok = True
    runs.negative_control_ok = True
    return runs


def test_the_banner_states_the_shape_before_anything_is_provisioned(capsys) -> None:
    cfg = _config("--pattern", "ring", "--num-hosts", "4", "--procs-per-host", "2")
    topo = _topology(cfg)

    bd._print_banner(cfg, topo, _plan(cfg, topo), ["Mode: test"])

    printed = capsys.readouterr().out
    assert "Mode: test" in printed
    # One line per direction, each naming the graph that direction will drive.
    assert printed.count("ring: 4x2 procs, 8 edges, same lanes") == 2
    assert "read;" in printed and "write;" in printed
    assert "Memory: source=gpu dest=gpu" in printed
    # One outgoing pool plus one incoming, at one op of 1024 MB each.
    assert "up to 2 buffers per proc, 2.05 GB device per proc" in printed
    assert "3 x (3 ramp + 10 warm) iterations" in printed
    assert "Transport: ibverbs" in printed
    assert cfg.output_csv in printed


def test_the_banner_names_the_hosts_a_pattern_leaves_idle(capsys) -> None:
    """A p2p run over an eight-host job uses two of them, which is worth saying
    out loud before the other six sit there costing capacity."""
    cfg = _config("--pattern", "p2p", "--num-hosts", "8")
    topo = _topology(cfg)

    bd._print_banner(cfg, topo, _plan(cfg, topo), [])

    assert "never touches: [2, 3, 4, 5, 6, 7]" in capsys.readouterr().out


def test_the_shape_columns_describe_the_graph() -> None:
    cfg = _config(
        "--pattern", "all-to-all", "--num-hosts", "4", "--procs-per-host", "2"
    )
    topo = _topology(cfg)

    shape = bd._shape_columns(cfg, topo, _plan(cfg, topo), bt.READ)

    assert (shape.num_hosts, shape.procs_per_host) == (4, 2)
    assert shape.num_edges == 24, "4 hosts fully connected, 2 same-lane pairs each"
    assert shape.num_initiators == 8, "every proc pulls under read"
    assert shape.max_degree == 3
    assert shape.max_in_degree == 3
    assert shape.max_ops_per_action == 3, "one op per edge, three edges in"
    assert shape.bytes_per_iteration == 24 * 1024 * 1000**2
    assert shape.max_buffers_per_proc == 4, "one outgoing plus three incoming"


def test_the_config_columns_record_what_was_asked_for() -> None:
    cfg = _config("--cpu", "--pattern", "ring", "--verify", "off")

    config = bd._config_columns(cfg, _record())

    assert config.schema_version == bs.SCHEMA_VERSION
    assert (config.source_device, config.dest_device) == ("cpu", "cpu")
    assert config.verify_mode == "off"
    assert config.local_only == 0
    assert config.cold_proc_runs == 1, "one generation of procs, so one cold_qp"
    assert config.integrity_ok == "True"


def test_a_check_that_never_ran_is_neither_pass_nor_fail() -> None:
    """`--verify off` leaves the flags unset, and reporting them as False would
    read as a corrupted transfer."""
    assert bd._flag(None) == "skipped"
    assert bd._flag(True) == "True"
    assert bd._flag(False) == "False"


def test_reporting_writes_a_row_per_direction_and_phase(tmp_path, capsys) -> None:
    output_csv = str(tmp_path / "results.csv")
    cfg = _config("--pattern", "ring", "--num-hosts", "4", "--output-csv", output_csv)
    topo = _topology(cfg)
    records = {bt.READ: _record(submit_ms=400.0), bt.WRITE: _record(submit_ms=200.0)}

    bd._report(cfg, topo, _plan(cfg, topo), records)

    with open(output_csv) as stream:
        rows = list(csv.DictReader(stream))
    assert list(rows[0]) == list(bs.summary_header())
    assert {(row["direction"], row["phase"]) for row in rows} == {
        (direction, phase) for direction in bt.DIRECTIONS for phase in bt.PHASES
    }
    assert all(row["pattern"] == "ring" for row in rows)

    warm = {row["direction"]: row for row in rows if row["phase"] == bt.WARM}
    assert float(warm[bt.READ]["submit_ms_median"]) == 400.0
    assert float(warm[bt.WRITE]["submit_ms_median"]) == 200.0
    # Halving the time doubles the rate, and only warm rows carry one at all.
    assert float(warm[bt.WRITE]["agg_gbs_median"]) > float(
        warm[bt.READ]["agg_gbs_median"]
    )
    cold = [row for row in rows if row["phase"] == bt.COLD_QP]
    assert all(row["agg_gbs_median"] == "" for row in cold)

    printed = capsys.readouterr().out
    assert "build p50 (ms)" in printed
    assert output_csv in printed


def test_the_transport_and_thread_count_are_pinned_before_any_proc_starts(
    monkeypatch,
) -> None:
    """The procs inherit this configuration, so it has to be set first."""
    settings: list[dict] = []
    monkeypatch.setattr(
        bd.monarch, "configure", lambda **kw: settings.append(kw), raising=False
    )

    bd._configure_rdma(_config("--transport", "ibverbs"))
    bd._configure_rdma(_config("--transport", "tcp", "--rdma-runtime-threads", "16"))

    assert settings[0] == {"rdma_allow_tcp_fallback": False}
    assert settings[1] == {
        "rdma_disable_ibverbs": True,
        "rdma_allow_tcp_fallback": True,
        "rdma_runtime_worker_threads": 16,
    }


def test_local_only_puts_every_host_on_this_machine(monkeypatch) -> None:
    peers = _FakePeers()
    host = _FakeHost(_FakeProcs(peers))
    monkeypatch.setattr(bd, "this_host", lambda: host)
    parser = argparse.ArgumentParser()
    bd.add_benchmark_args(parser)
    cfg = bd.config_from_args(
        parser.parse_args(
            ["--num-hosts", "4", "--procs-per-host", "2", "run", "--local-only"]
        )
    )

    procs, spawned = bd._spawn_procs_and_actors(cfg, None, "unused")

    assert host.per_host == {"hosts": 4, "lanes": 2}, "both dimensions, one machine"
    assert procs is host.procs
    assert spawned is peers
    assert procs.spawned == [("rdma_bench_peer", bench_peer.Peer)]


def test_a_scheduled_run_takes_its_hosts_from_the_job() -> None:
    """The job supplies the hosts dimension, so the driver asks only for lanes."""
    peers = _FakePeers()
    host = _FakeHost(_FakeProcs(peers), hosts=4)
    job = _FakeJob("mesh0", host)
    cfg = _config("--num-hosts", "4", "--procs-per-host", "2")

    procs, spawned = bd._spawn_procs_and_actors(cfg, job, "mesh0")

    assert job.cached_paths == [None]
    assert host.per_host == {"lanes": 2}
    assert (procs, spawned) == (host.procs, peers)


def test_a_job_of_the_wrong_size_is_refused() -> None:
    host = _FakeHost(_FakeProcs(_FakePeers()), hosts=2)
    job = _FakeJob("mesh0", host)

    with pytest.raises(RuntimeError, match="--num-hosts 4 but the job provisioned 2"):
        bd._spawn_procs_and_actors(_config("--num-hosts", "4"), job, "mesh0")

    assert host.per_host is None, "nothing is spawned onto the wrong job"


def test_a_scheduled_run_without_a_job_is_a_bug_not_a_local_run() -> None:
    with pytest.raises(AssertionError, match="--local-only"):
        bd._spawn_procs_and_actors(_config(), None, "mesh0")


async def test_every_proc_must_have_the_configuration_the_client_pinned() -> None:
    check = _FakeEndpoint(_value_mesh([{}] * 4, lanes=4))
    cfg = _config("--transport", "tcp", "--rdma-runtime-threads", "16")

    await bd._check_config(cfg, _FakePeers(check_config=check))

    ((expected,),) = check.casts
    assert expected == {
        "rdma_disable_ibverbs": True,
        "rdma_allow_tcp_fallback": True,
        "rdma_runtime_worker_threads": 16,
    }, "exactly what _configure_rdma pinned on the client"


async def test_a_proc_that_did_not_inherit_the_configuration_fails_the_run() -> None:
    held = [{}, {"rdma_allow_tcp_fallback": True}, {}, {}]
    peers = _FakePeers(check_config=_FakeEndpoint(_value_mesh(held, lanes=4)))

    with pytest.raises(RuntimeError, match=r"h0/l1 has \{'rdma_allow_tcp_fallback'"):
        await bd._check_config(_config("--transport", "ibverbs"), peers)


async def test_the_run_is_told_every_proc_that_disagreed() -> None:
    held = [{"rdma_allow_tcp_fallback": True}] * 2
    peers = _FakePeers(check_config=_FakeEndpoint(_value_mesh(held, lanes=2)))

    with pytest.raises(RuntimeError, match="h0/l0 .*; h0/l1 ") as caught:
        await bd._check_config(_config("--transport", "ibverbs"), peers)

    assert "{'rdma_allow_tcp_fallback': False}" in str(caught.value), (
        "the message names what was expected as well as what was held"
    )
