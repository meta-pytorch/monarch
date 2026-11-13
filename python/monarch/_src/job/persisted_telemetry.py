# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Query telemetry that collectors persisted before their job ended.

`PersistedTelemetryActor` loads `TelemetryConfig.persist_dir` logs into a fresh
`DatabaseScanner` and serves the scan endpoints `QueryEngine` uses, so the
same SQL and dashboard work after the job is gone.
"""

from __future__ import annotations

import datetime
from typing import Any, TypedDict

from monarch._rust_bindings.monarch_distributed_telemetry import _post_scan_counts
from monarch._rust_bindings.monarch_distributed_telemetry.database_scanner import (
    DatabaseScanner,
    PersistedInstance,
)
from monarch._rust_bindings.monarch_hyperactor.mailbox import PortId
from monarch._rust_bindings.monarch_hyperactor.proc import ActorAddr
from monarch.actor import Actor, ActorError, context, endpoint
from monarch.distributed_telemetry.engine import QueryEngine


class InstanceSummary(TypedDict):
    """What loading one collector's persist log found."""

    name: str
    segments: int
    clean: bool
    torn_bytes: int
    rows: int
    first_timestamp_us: int | None
    last_timestamp_us: int | None


def _summary(instance: PersistedInstance) -> InstanceSummary:
    return {
        "name": instance.name,
        "segments": instance.segments,
        "clean": instance.clean,
        "torn_bytes": instance.torn_bytes,
        "rows": instance.rows,
        "first_timestamp_us": instance.first_timestamp_us,
        "last_timestamp_us": instance.last_timestamp_us,
    }


class PersistedTelemetryActor(Actor):
    """Read-only telemetry collector over persist logs."""

    def __init__(self, roots: list[str]) -> None:
        # An actor that fails to construct is an unhandled fault that exits the
        # client, so a load error is reported by `instances` instead.
        self._scanner: DatabaseScanner | None = None
        self._instances: list[InstanceSummary] = []
        self._error: str | None = None
        try:
            scanner, instances = DatabaseScanner.open_persisted(roots)
        except Exception as error:  # Rust load errors are plain `Exception`.
            self._error = str(error)
            return
        self._scanner = scanner
        self._instances = [_summary(instance) for instance in instances]

    def _scanner_or_raise(self) -> DatabaseScanner:
        if self._scanner is None:
            raise RuntimeError(self._error)
        return self._scanner

    @endpoint
    def instances(self) -> list[InstanceSummary]:
        """Return what loading each collector's persist log found."""
        self._scanner_or_raise()
        return self._instances

    @endpoint
    def table_names(self) -> list[str]:
        """Get list of table names available in the loaded store."""
        return self._scanner_or_raise().table_names()

    @endpoint
    def schema_for(self, table: str) -> bytes:
        """Get schema for a table in Arrow IPC format."""
        return bytes(self._scanner_or_raise().schema_for(table))

    @endpoint
    def scan(
        self,
        dest: PortId,
        sql: str,
    ) -> list[tuple[ActorAddr, int]]:
        """Scan the loaded store and return this collector's batch count."""
        return [self._scanner_or_raise().scan(dest, sql)]

    @endpoint
    def query_scan(
        self,
        dest: PortId,
        sql: str,
    ) -> None:
        """Scan the loaded store and post its batch count to the query engine."""
        batch_count = self._scanner_or_raise().scan(dest, sql)
        _post_scan_counts(context().actor_instance._as_rust(), dest, [batch_count])


def open_persisted_engine(
    roots: list[str],
) -> tuple[QueryEngine, list[InstanceSummary]]:
    """Load persist logs under `roots` and return a query engine over them.

    Each root is a `{persist_dir}/{apply_id}` directory. Raises `RuntimeError`
    if a root cannot be loaded.
    Read logs with the Monarch build that wrote them; table schemas come from
    the stored Arrow IPC streams.
    """
    actor: Any = context().actor_instance.proc_mesh.spawn(
        "persisted_telemetry", PersistedTelemetryActor, roots
    )
    try:
        instances: list[InstanceSummary] = actor.instances.call_one().get()
    except ActorError as error:
        raise RuntimeError(str(error.exception)) from None
    return QueryEngine(actor), instances


def _format_timestamp(timestamp_us: int | None) -> str:
    if timestamp_us is None:
        return "-"
    moment = datetime.datetime.fromtimestamp(
        timestamp_us / 1_000_000, tz=datetime.timezone.utc
    )
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def format_instances(instances: list[InstanceSummary]) -> str:
    """Return one line per loaded collector.

    Among collectors that did not close their log cleanly, the one whose last
    row is earliest is marked `first to stop`: it is where to start looking.
    """
    stopped = [
        instance
        for instance in instances
        if not instance["clean"] and instance["last_timestamp_us"] is not None
    ]
    first_to_stop = min(
        stopped, key=lambda instance: instance["last_timestamp_us"] or 0, default=None
    )
    lines = []
    for instance in instances:
        line = (
            f"{instance['name']} {'clean' if instance['clean'] else 'unclean'} "
            f"segments={instance['segments']} rows={instance['rows']} "
            f"last={_format_timestamp(instance['last_timestamp_us'])} "
            f"torn={instance['torn_bytes']}B"
        )
        if instance is first_to_stop:
            line += " first to stop"
        lines.append(line)
    return "\n".join(lines)
