# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Address discovery and the two-worker rendezvous for Slurm checks.

A worker takes an address of the interface that carries its default route
for the requested IP family. The two workers publish their addresses as records
in a shared attempt directory and wait, within a budget, for each other's.
"""

import contextlib
import fcntl
import ipaddress
import json
import os
import socket
import struct
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

FORMAT = 1
MAX_RECORD = 4096
ROUTE_TABLE = Path("/proc/net/route")
IPV6_ROUTE_TABLE = Path("/proc/net/ipv6_route")
IPV6_ADDRESSES = Path("/proc/net/if_inet6")
IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address

_RTF_UP = 0x1
_SIOCGIFADDR = 0x8915
_READY_FIELDS = frozenset({"v", "run", "slot", "node", "uid", "ip", "port"})


class NetworkError(Exception):
    """No usable address for this worker."""


class RecordError(Exception):
    """A rendezvous record is missing, malformed or from another run."""


def default_interface(route_table: str) -> str:
    """Returns the interface of the unique lowest-metric up default route.

    `route_table` is the text of `/proc/net/route`.
    """
    candidates: list[tuple[int, str]] = []
    for line in route_table.splitlines()[1:]:
        fields = line.split()
        if len(fields) < 8:
            continue
        try:
            flags, metric = int(fields[3], 16), int(fields[6])
        except ValueError as e:
            raise NetworkError(f"malformed route: {line!r}") from e
        iface, destination, mask = fields[0], fields[1], fields[7]
        if destination == "00000000" and mask == "00000000" and flags & _RTF_UP:
            candidates.append((metric, iface))
    if not candidates:
        raise NetworkError("no up IPv4 default route")
    best = min(metric for metric, _ in candidates)
    names = sorted({iface for metric, iface in candidates if metric == best})
    if len(names) > 1:
        raise NetworkError(
            f"ambiguous default route: metric {best} on {', '.join(names)}"
        )
    return names[0]


def interface_ipv4(name: str) -> ipaddress.IPv4Address:
    """Returns the IPv4 address of interface `name`, from SIOCGIFADDR."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            ifreq = fcntl.ioctl(
                sock.fileno(), _SIOCGIFADDR, struct.pack("256s", name.encode()[:15])
            )
    except OSError as e:
        raise NetworkError(f"interface {name}: {e}") from e
    return ipaddress.IPv4Address(ifreq[20:24])


def default_ipv6_interface(route_table: str) -> str:
    """Selects the unique lowest-metric up IPv6 default-route interface."""
    candidates: list[tuple[int, str]] = []
    for line in route_table.splitlines():
        fields = line.split()
        if len(fields) != 10:
            raise NetworkError(f"malformed IPv6 route: {line!r}")
        try:
            destination, prefix, source_prefix = (
                int(fields[0], 16),
                int(fields[1], 16),
                int(fields[3], 16),
            )
            metric, flags = int(fields[5], 16), int(fields[8], 16)
        except ValueError as e:
            raise NetworkError(f"malformed IPv6 route: {line!r}") from e
        if destination == 0 and prefix == 0 and source_prefix == 0:
            if flags & _RTF_UP and not flags & 0x200:  # RTF_REJECT
                candidates.append((metric, fields[9]))
    if not candidates:
        raise NetworkError("no up IPv6 default route")
    best = min(metric for metric, _ in candidates)
    names = sorted({name for metric, name in candidates if metric == best})
    if len(names) != 1:
        raise NetworkError(f"ambiguous IPv6 default route: {', '.join(names)}")
    return names[0]


def interface_ipv6(name: str, addresses: str) -> ipaddress.IPv6Address:
    """Reads one usable, non-link-local IPv6 address from /proc/net/if_inet6."""
    candidates: set[ipaddress.IPv6Address] = set()
    for line in addresses.splitlines():
        fields = line.split()
        if len(fields) != 6:
            raise NetworkError(f"malformed IPv6 address: {line!r}")
        if fields[5] != name:
            continue
        try:
            address = ipaddress.IPv6Address(int(fields[0], 16))
            flags = int(fields[4], 16)
        except ValueError as e:
            raise NetworkError(f"malformed IPv6 address: {line!r}") from e
        # IFA_F_DADFAILED, IFA_F_DEPRECATED and IFA_F_TENTATIVE.
        if flags & (0x08 | 0x20 | 0x40):
            continue
        if not (
            address.is_unspecified
            or address.is_loopback
            or address.is_multicast
            or address.is_link_local
            or address.ipv4_mapped is not None
        ):
            candidates.add(address)
    if len(candidates) != 1:
        raise NetworkError(
            f"interface {name}: expected one usable IPv6 address, found {len(candidates)}"
        )
    return candidates.pop()


def discover_ipv6(
    route_path: Path = IPV6_ROUTE_TABLE, address_path: Path = IPV6_ADDRESSES
) -> tuple[str, ipaddress.IPv6Address]:
    """Returns the default-route interface and its unambiguous IPv6 address."""
    try:
        routes, addresses = route_path.read_text(), address_path.read_text()
    except OSError as e:
        raise NetworkError(f"read IPv6 configuration: {e}") from e
    iface = default_ipv6_interface(routes)
    return iface, interface_ipv6(iface, addresses)


def check_address(ip: IPAddress) -> None:
    """Rejects an address another node can't send to."""
    if ip.is_unspecified or ip.is_loopback or ip.is_multicast:
        raise NetworkError(f"unusable address {ip}")


def discover(route_path: Path = ROUTE_TABLE) -> tuple[str, ipaddress.IPv4Address]:
    """Returns the default-route interface and its IPv4 address."""
    try:
        route_table = route_path.read_text()
    except OSError as e:
        raise NetworkError(f"read {route_path}: {e}") from e
    iface = default_interface(route_table)
    ip = interface_ipv4(iface)
    check_address(ip)
    return iface, ip


@dataclass(frozen=True)
class Ready:
    """A worker's bound address, as it publishes it."""

    run: str
    slot: int
    node: str
    uid: int
    ip: IPAddress
    port: int

    def to_json(self) -> dict[str, object]:
        return {
            "v": FORMAT,
            "run": self.run,
            "slot": self.slot,
            "node": self.node,
            "uid": self.uid,
            "ip": str(self.ip),
            "port": self.port,
        }


def ready_name(slot: int) -> str:
    return f"ready-{slot}.json"


def result_name(slot: int) -> str:
    return f"result-{slot}.json"


def publish(directory: Path, name: str, record: Mapping[str, object]) -> Path:
    """Publishes `record` as `directory/name`, complete or not at all.

    The record is written to a temporary file in `directory`, then renamed into
    place. Only one writer publishes each name, so refusing a name that already
    exists means a record is never replaced; the check and the rename are not one
    atomic step.
    """
    path = directory / name
    if path.exists():
        raise RecordError(f"{path} already exists")
    data = json.dumps(record, sort_keys=True).encode()
    fd, temp = tempfile.mkstemp(dir=directory, prefix=f".{name}.")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.rename(temp, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temp)
        raise
    return path


def read_record(path: Path) -> bytes | None:
    """Returns a record's bytes, at most one more than the cap; None if absent."""
    try:
        with path.open("rb") as f:
            return f.read(MAX_RECORD + 1)
    except FileNotFoundError:
        return None


def parse_ready(data: bytes, run: str, slot: int) -> Ready:
    """Parses the ready record of `slot` in `run`."""
    if len(data) > MAX_RECORD:
        raise RecordError(f"ready record larger than {MAX_RECORD} bytes")
    try:
        obj = json.loads(data.decode())
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise RecordError(f"malformed ready record: {e}") from e
    if not isinstance(obj, dict) or set(obj) != _READY_FIELDS:
        raise RecordError(f"ready record fields: {obj!r}")
    if obj["v"] != FORMAT:
        raise RecordError(f"ready record format {obj['v']!r}, expected {FORMAT}")
    if obj["run"] != run:
        raise RecordError(f"ready record from run {obj['run']!r}, expected {run!r}")
    if obj["slot"] != slot:
        raise RecordError(f"ready record for slot {obj['slot']!r}, expected {slot}")
    node, uid, ip, port = obj["node"], obj["uid"], obj["ip"], obj["port"]
    if not isinstance(node, str) or not node:
        raise RecordError(f"ready record node {node!r}")
    if type(uid) is not int:
        raise RecordError(f"ready record uid {uid!r}")
    if type(port) is not int or not 0 < port < 65536:
        raise RecordError(f"ready record port {port!r}")
    if not isinstance(ip, str):
        raise RecordError(f"ready record address {ip!r}")
    try:
        address = ipaddress.ip_address(ip)
    except (ValueError, TypeError) as e:
        raise RecordError(f"ready record address {ip!r}") from e
    if isinstance(address, ipaddress.IPv6Address) and (
        address.is_link_local or address.ipv4_mapped is not None or address.scope_id
    ):
        raise RecordError(f"ready record address {ip!r}: scoped or mapped IPv6")
    return Ready(run, slot, node, uid, address, port)


def await_ready(
    directory: Path,
    slot: int,
    run: str,
    budget: float,
    *,
    read: Callable[[Path], bytes | None] = read_record,
    poll: float = 0.2,
) -> Ready:
    """Waits up to `budget` seconds for the ready record of `slot`.

    A missing record is retried until the budget runs out; no read starts after
    that. A record that is present but malformed or from another run fails at
    once.
    """
    path = directory / ready_name(slot)
    deadline = time.monotonic() + budget
    while (data := read(path)) is None:
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(poll, remaining))
        if time.monotonic() >= deadline:
            raise RecordError(f"no {path.name} after {budget:g} s")
    return parse_ready(data, run, slot)
