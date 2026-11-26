# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Checks `slurm/slurm_network.py`.

Route tables are literal `/proc/net/route` text. The rendezvous tests use
temporary directories and an injected reader that hides a published record,
as a network filesystem's lookup cache can. None of these tests needs a
network.
"""

import ipaddress
import json
import os
import threading
import time
from pathlib import Path

import pytest
import slurm_network

_RUN = "4242/attempt.abc123"
_HEADER = "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT"


def _route(
    iface: str,
    destination: str = "00000000",
    flags: str = "0003",
    metric: int = 0,
    mask: str = "00000000",
) -> str:
    return f"{iface}\t{destination}\t0105800A\t{flags}\t0\t0\t{metric}\t{mask}\t0\t0\t0"


def _table(*routes: str) -> str:
    return "\n".join([_HEADER, *routes]) + "\n"


def _ready(slot: int, run: str = _RUN) -> slurm_network.Ready:
    return slurm_network.Ready(
        run,
        slot,
        f"node-{slot}",
        1000,
        ipaddress.IPv4Address(f"10.0.0.{slot + 1}"),
        40000 + slot,
    )


def _write(directory: Path, slot: int, data: bytes) -> None:
    (directory / slurm_network.ready_name(slot)).write_bytes(data)


def test_lowest_metric_default_route_wins() -> None:
    table = _table(
        _route("eth1", metric=100),
        _route("eth0", metric=0),
        _route("eth0", destination="0080800A", flags="0001", mask="00C0FFFF"),
    )
    assert slurm_network.default_interface(table) == "eth0"


def test_duplicate_default_routes_on_one_interface_are_not_ambiguous() -> None:
    table = _table(_route("eth0"), _route("eth0"))
    assert slurm_network.default_interface(table) == "eth0"


def test_no_default_route_fails() -> None:
    table = _table(
        _route("eth0", destination="0080800A", flags="0001", mask="00C0FFFF")
    )
    with pytest.raises(slurm_network.NetworkError, match="no up IPv4 default route"):
        slurm_network.default_interface(table)


def test_down_default_route_is_ignored() -> None:
    with pytest.raises(slurm_network.NetworkError, match="no up IPv4 default route"):
        slurm_network.default_interface(_table(_route("eth0", flags="0002")))


def test_equal_metric_default_routes_on_two_interfaces_fail() -> None:
    table = _table(_route("eth0", metric=10), _route("eth1", metric=10))
    with pytest.raises(slurm_network.NetworkError, match="ambiguous.*eth0, eth1"):
        slurm_network.default_interface(table)


def test_malformed_route_fails() -> None:
    with pytest.raises(slurm_network.NetworkError, match="malformed route"):
        slurm_network.default_interface(_table(_route("eth0", flags="zz")))


def test_unreadable_route_table_fails(tmp_path: Path) -> None:
    with pytest.raises(slurm_network.NetworkError, match="read .*absent"):
        slurm_network.discover(tmp_path / "absent")


@pytest.mark.parametrize("ip", ["0.0.0.0", "127.0.0.1", "224.0.0.1"])
def test_unusable_address_fails(ip: str) -> None:
    with pytest.raises(slurm_network.NetworkError, match="unusable address"):
        slurm_network.check_address(ipaddress.IPv4Address(ip))


def test_routable_address_passes() -> None:
    slurm_network.check_address(ipaddress.IPv4Address("10.128.5.165"))


def test_missing_interface_fails() -> None:
    with pytest.raises(slurm_network.NetworkError, match="interface chr-nope0"):
        slurm_network.interface_ipv4("chr-nope0")


def test_discovery_rejects_a_loopback_default_route(tmp_path: Path) -> None:
    # Whether this namespace's loopback has an address or not, the result is
    # unusable for another node.
    route = tmp_path / "route"
    route.write_text(_table(_route("lo")))
    with pytest.raises(slurm_network.NetworkError):
        slurm_network.discover(route)


def test_published_record_round_trips(tmp_path: Path) -> None:
    slurm_network.publish(tmp_path, slurm_network.ready_name(1), _ready(1).to_json())
    assert slurm_network.await_ready(tmp_path, 1, _RUN, 1.0) == _ready(1)
    assert [p.name for p in tmp_path.iterdir()] == ["ready-1.json"]


def test_record_appears_only_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    renames: list[tuple[str, str]] = []
    rename = os.rename

    def checked_rename(src: str | Path, dst: str | Path) -> None:
        assert not Path(dst).exists()
        assert Path(src).parent == Path(dst).parent
        assert json.loads(Path(src).read_bytes()) == _ready(1).to_json()
        renames.append((str(src), str(dst)))
        rename(src, dst)

    monkeypatch.setattr(slurm_network.os, "rename", checked_rename)
    path = slurm_network.publish(
        tmp_path, slurm_network.ready_name(1), _ready(1).to_json()
    )
    assert renames == [(renames[0][0], str(path))]


def test_existing_own_record_is_refused(tmp_path: Path) -> None:
    name = slurm_network.ready_name(0)
    slurm_network.publish(tmp_path, name, _ready(0).to_json())
    before = (tmp_path / name).read_bytes()
    with pytest.raises(slurm_network.RecordError, match="already exists"):
        slurm_network.publish(
            tmp_path, name, _ready(0, run="4242/attempt.new").to_json()
        )
    assert (tmp_path / name).read_bytes() == before
    assert [p.name for p in tmp_path.iterdir()] == [name]


def test_record_from_another_run_fails(tmp_path: Path) -> None:
    old = _ready(1, run="4242/attempt.old").to_json()
    _write(tmp_path, 1, json.dumps(old).encode())
    with pytest.raises(slurm_network.RecordError, match="from run"):
        slurm_network.await_ready(tmp_path, 1, _RUN, 1.0)


def test_record_for_another_slot_fails(tmp_path: Path) -> None:
    _write(tmp_path, 1, json.dumps(_ready(0).to_json()).encode())
    with pytest.raises(slurm_network.RecordError, match="for slot 0"):
        slurm_network.await_ready(tmp_path, 1, _RUN, 1.0)


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("ip", "10.0.0", "address"),
        ("ip", "fe80::1", "address"),
        ("port", 0, "port"),
        ("port", "40001", "port"),
        ("node", "", "node"),
        ("uid", True, "uid"),
        ("v", 2, "format"),
    ],
)
def test_invalid_record_field_fails(
    tmp_path: Path, field: str, value: object, error: str
) -> None:
    record = _ready(1).to_json() | {field: value}
    _write(tmp_path, 1, json.dumps(record).encode())
    with pytest.raises(slurm_network.RecordError, match=error):
        slurm_network.await_ready(tmp_path, 1, _RUN, 1.0)


def test_record_with_extra_field_fails(tmp_path: Path) -> None:
    record = _ready(1).to_json() | {"extra": 1}
    _write(tmp_path, 1, json.dumps(record).encode())
    with pytest.raises(slurm_network.RecordError, match="fields"):
        slurm_network.await_ready(tmp_path, 1, _RUN, 1.0)


def test_malformed_record_fails(tmp_path: Path) -> None:
    _write(tmp_path, 1, b'{"v": 1, "run"')
    with pytest.raises(slurm_network.RecordError, match="malformed"):
        slurm_network.await_ready(tmp_path, 1, _RUN, 1.0)


def test_oversized_record_fails(tmp_path: Path) -> None:
    _write(tmp_path, 1, b" " * (slurm_network.MAX_RECORD + 1))
    with pytest.raises(slurm_network.RecordError, match="larger than"):
        slurm_network.await_ready(tmp_path, 1, _RUN, 1.0)


def test_missing_peer_fails_at_its_budget(tmp_path: Path) -> None:
    start = time.monotonic()
    with pytest.raises(slurm_network.RecordError, match="no ready-1.json after 0.5 s"):
        slurm_network.await_ready(tmp_path, 1, _RUN, 0.5, poll=0.05)
    elapsed = time.monotonic() - start
    assert 0.5 <= elapsed < 2.0


def test_record_published_late_is_read_within_budget(tmp_path: Path) -> None:
    reads: list[bool] = []

    def read(path: Path) -> bytes | None:
        data = slurm_network.read_record(path)
        reads.append(data is not None)
        return data

    timer = threading.Timer(
        0.3,
        slurm_network.publish,
        (tmp_path, slurm_network.ready_name(1), _ready(1).to_json()),
    )
    timer.start()
    try:
        ready = slurm_network.await_ready(tmp_path, 1, _RUN, 5.0, read=read, poll=0.05)
    finally:
        timer.join()
    assert ready == _ready(1)
    assert reads[0] is False and reads[-1] is True


def test_record_hidden_after_publication_is_read_within_budget(tmp_path: Path) -> None:
    slurm_network.publish(tmp_path, slurm_network.ready_name(1), _ready(1).to_json())
    hidden = 0

    def read(path: Path) -> bytes | None:
        nonlocal hidden
        if hidden < 5:
            assert path.exists()
            hidden += 1
            return None
        return slurm_network.read_record(path)

    ready = slurm_network.await_ready(tmp_path, 1, _RUN, 5.0, read=read, poll=0.05)
    assert ready == _ready(1)
    assert hidden == 5


def test_no_read_starts_after_the_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A fake clock whose sleeps overshoot. The record appears at the deadline,
    # so a read started then would succeed.
    slurm_network.publish(tmp_path, slurm_network.ready_name(1), _ready(1).to_json())
    clock = [0.0]
    reads: list[float] = []

    def sleep(seconds: float) -> None:
        clock[0] += seconds + 0.01

    def read(path: Path) -> bytes | None:
        reads.append(clock[0])
        return slurm_network.read_record(path) if clock[0] >= 1.0 else None

    monkeypatch.setattr(slurm_network.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(slurm_network.time, "sleep", sleep)
    with pytest.raises(slurm_network.RecordError, match="no ready-1.json after 1 s"):
        slurm_network.await_ready(tmp_path, 1, _RUN, 1.0, read=read, poll=0.2)
    assert len(reads) > 1
    assert max(reads) < 1.0


def test_record_hidden_past_a_short_budget_fails(tmp_path: Path) -> None:
    slurm_network.publish(tmp_path, slurm_network.ready_name(1), _ready(1).to_json())
    with pytest.raises(slurm_network.RecordError, match="no ready-1.json"):
        slurm_network.await_ready(
            tmp_path, 1, _RUN, 0.3, read=lambda path: None, poll=0.05
        )


def _route6(
    iface: str, metric: int = 100, flags: int = 3, source_prefix: int = 0
) -> str:
    return f"{'0' * 32} 00 {'0' * 32} {source_prefix:02x} {'0' * 32} {metric:08x} 00000000 00000000 {flags:08x} {iface}"


def _address6(ip: str, iface: str = "eth0", flags: int = 0x80) -> str:
    address = int(ipaddress.IPv6Address(ip))
    return f"{address:032x} 02 40 00 {flags:02x} {iface}"


def test_ipv6_default_route_selects_lowest_metric() -> None:
    routes = "\n".join([_route6("eth1", 200), _route6("eth0", 100)])
    assert slurm_network.default_ipv6_interface(routes) == "eth0"


def test_ipv6_default_route_ignores_down_reject_and_source_specific_routes() -> None:
    routes = "\n".join(
        [
            _route6("eth1", 0, 0),
            _route6("lo", 0, 0x201),
            _route6("eth2", 0, source_prefix=128),
            _route6("eth0"),
        ]
    )
    assert slurm_network.default_ipv6_interface(routes) == "eth0"


@pytest.mark.parametrize("routes", ["", _route6("eth0", flags=0)])
def test_no_ipv6_default_route_fails(routes: str) -> None:
    with pytest.raises(slurm_network.NetworkError, match="no up IPv6 default route"):
        slurm_network.default_ipv6_interface(routes)


def test_tied_ipv6_default_routes_fail() -> None:
    with pytest.raises(
        slurm_network.NetworkError, match="ambiguous IPv6 default route"
    ):
        slurm_network.default_ipv6_interface(_route6("eth0") + "\n" + _route6("eth1"))


@pytest.mark.parametrize(
    "route", ["bad line", _route6("eth0").replace("00000003", "zz")]
)
def test_malformed_ipv6_route_fails(route: str) -> None:
    with pytest.raises(slurm_network.NetworkError, match="malformed IPv6 route"):
        slurm_network.default_ipv6_interface(route)


def test_ipv6_address_selects_only_usable_address() -> None:
    addresses = "\n".join(
        [
            _address6("fe80::1"),
            _address6("::1"),
            _address6("ff02::1"),
            _address6("::"),
            _address6("2001:db8::2", flags=0xA0),
            _address6("2001:db8::3", flags=0x40),
            _address6("2001:db8::4", flags=0x08),
            _address6("fd00::1"),
            _address6("fd00::2", "eth1"),
        ]
    )
    assert slurm_network.interface_ipv6("eth0", addresses) == ipaddress.IPv6Address(
        "fd00::1"
    )


@pytest.mark.parametrize(
    "addresses",
    ["", _address6("fe80::1"), _address6("fd00::1") + "\n" + _address6("2001:db8::1")],
)
def test_missing_or_ambiguous_ipv6_address_fails(addresses: str) -> None:
    with pytest.raises(
        slurm_network.NetworkError, match="expected one usable IPv6 address"
    ):
        slurm_network.interface_ipv6("eth0", addresses)


def test_ipv6_discovery_reads_route_and_address_files(tmp_path: Path) -> None:
    route, addresses = tmp_path / "route6", tmp_path / "addresses6"
    route.write_text(_route6("eth0"))
    addresses.write_text(_address6("fd00::1"))
    assert slurm_network.discover_ipv6(route, addresses) == (
        "eth0",
        ipaddress.IPv6Address("fd00::1"),
    )
    addresses.unlink()
    with pytest.raises(slurm_network.NetworkError, match="read IPv6 configuration"):
        slurm_network.discover_ipv6(route, addresses)


def test_ipv6_ready_record_round_trips(tmp_path: Path) -> None:
    ready = slurm_network.Ready(
        _RUN, 1, "node-1", 1000, ipaddress.IPv6Address("fd00::1"), 40001
    )
    slurm_network.publish(tmp_path, slurm_network.ready_name(1), ready.to_json())
    assert slurm_network.await_ready(tmp_path, 1, _RUN, 1) == ready


@pytest.mark.parametrize("ip", ["fe80::1%eth0", "::ffff:10.0.0.1", 1])
def test_scoped_mapped_or_nonstring_ready_address_fails(ip: object) -> None:
    data = json.dumps(_ready(1).to_json() | {"ip": ip}).encode()
    with pytest.raises(slurm_network.RecordError, match="address"):
        slurm_network.parse_ready(data, _RUN, 1)
