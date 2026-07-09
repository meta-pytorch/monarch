#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Benchmark for stressing telemetry with small actor self-messages.
"""

from __future__ import annotations

import argparse
import time
from typing import cast

from monarch._src.job._telemetry_query_client import QueryEngineClient
from monarch.actor import Actor, current_rank, endpoint, ProcMesh
from monarch.job import ProcessJob, TelemetryConfig

Status = dict[str, int]
QueryRow = dict[str, object]
QueryRows = list[QueryRow]

_POLL_SEC = 0.1

_TICK_MESSAGE_COUNT_SQL = (
    "SELECT COUNT(*) AS tick_messages "
    "FROM messages m "
    "JOIN actors a ON m.to_actor_id = a.id "
    "JOIN meshes mesh ON a.mesh_id = mesh.id "
    "WHERE mesh.given_name = 'self_message' AND m.endpoint = 'tick'"
)


class SelfMessagingActor(Actor):
    def __init__(self) -> None:
        self._self_actor: SelfMessagingActor | None = None
        self._payload: bytes = b""
        self._processed: int = 0
        self._scheduled: int = 0
        self._target: int = 0
        self._inflight: int = 1

    @endpoint
    def configure(self, actors: SelfMessagingActor) -> None:
        self._self_actor = actors.slice(**dict(current_rank()))

    @endpoint
    def start_loop(self, messages: int, payload: bytes, inflight: int) -> Status:
        if self._scheduled != self._processed:
            raise RuntimeError("self-message loop is already running")
        if messages < 0:
            raise RuntimeError("messages must be non-negative")
        if inflight <= 0:
            raise RuntimeError("inflight must be greater than zero")

        self._payload = payload
        self._target = self._processed + messages
        self._inflight = inflight
        self._fill_window()
        return self._status()

    @endpoint
    def tick(self, payload: bytes) -> int:
        if len(payload) != len(self._payload):
            raise RuntimeError("received an unexpected payload size")

        self._processed += 1
        self._fill_window()
        return self._processed

    @endpoint
    def status(self) -> Status:
        return self._status()

    def _status(self) -> Status:
        return {
            "processed": self._processed,
            "scheduled": self._scheduled,
            "target": self._target,
            "inflight": self._inflight,
        }

    def _fill_window(self) -> None:
        self_actor = self._self_actor
        if self_actor is None:
            raise RuntimeError("actor has not been configured")

        while (
            self._scheduled < self._target
            and self._scheduled - self._processed < self._inflight
        ):
            self_actor.tick.broadcast(self._payload)
            self._scheduled += 1


def _sum_stat(statuses: list[Status], key: str) -> int:
    total = 0
    for status in statuses:
        total += status[key]
    return total


def _wait_for_completion(
    actors: SelfMessagingActor,
    timeout_sec: float,
    poll_sec: float,
) -> tuple[bool, list[Status], float]:
    deadline = time.monotonic() + timeout_sec
    start = time.monotonic()
    statuses: list[Status] = []

    while True:
        statuses = list(actors.status.call().get().values())
        complete = all(status["processed"] >= status["target"] for status in statuses)
        if complete:
            return True, statuses, time.monotonic() - start
        if time.monotonic() >= deadline:
            return False, statuses, time.monotonic() - start
        time.sleep(poll_sec)


def _run_loop(
    actors: SelfMessagingActor,
    messages_per_proc: int,
    payload: bytes,
    inflight: int,
    timeout_sec: float,
    poll_sec: float,
) -> tuple[bool, list[Status], float, float]:
    start = time.monotonic()
    actors.start_loop.call(messages_per_proc, payload, inflight).get()
    queue_elapsed = time.monotonic() - start
    complete, statuses, drain_elapsed = _wait_for_completion(
        actors,
        timeout_sec,
        poll_sec,
    )
    return complete, statuses, queue_elapsed, drain_elapsed


def _query_tick_message_count(client: QueryEngineClient) -> int:
    rows = cast(QueryRows, client.query(_TICK_MESSAGE_COUNT_SQL).get("rows", []))
    if not rows:
        return 0

    tick_messages = rows[0].get("tick_messages")
    if not isinstance(tick_messages, int):
        raise RuntimeError(f"unexpected tick_messages value: {tick_messages!r}")
    return tick_messages


def _query_tick_message_count_once(
    client: QueryEngineClient | None,
) -> tuple[int | None, float]:
    start = time.monotonic()
    rows = None if client is None else _query_tick_message_count(client)
    return rows, time.monotonic() - start


def _wait_for_tick_message_count(
    client: QueryEngineClient | None,
    expected: int,
    timeout_sec: float,
    poll_sec: float,
) -> tuple[int | None, float]:
    deadline = time.monotonic() + timeout_sec
    start = time.monotonic()
    if client is None:
        return None, time.monotonic() - start

    latest = 0
    while True:
        latest = _query_tick_message_count(client)
        if latest >= expected or time.monotonic() >= deadline:
            return latest, time.monotonic() - start
        time.sleep(poll_sec)


def _print_results(
    args: argparse.Namespace,
    actor_count: int,
    complete: bool,
    before_statuses: list[Status],
    after_statuses: list[Status],
    queue_elapsed: float,
    drain_elapsed: float,
    telemetry_tick_rows_at_completion: int | None,
    telemetry_tick_rows_at_completion_latency: float,
    telemetry_catchup_tick_rows: int | None,
    telemetry_catchup_elapsed: float,
    telemetry_tick_rows: int | None,
) -> None:
    total_messages = actor_count * int(args.messages_per_proc)
    total_expected_tick_rows = actor_count * (
        int(args.warmup_messages_per_proc) + int(args.messages_per_proc)
    )
    processed_delta = _sum_stat(after_statuses, "processed") - _sum_stat(
        before_statuses, "processed"
    )
    processed_at_completion = _sum_stat(after_statuses, "processed")
    end_to_end_elapsed = queue_elapsed + drain_elapsed
    end_to_end_rate = processed_delta / max(end_to_end_elapsed, 1e-9)

    print(f"procs={args.procs}")
    print(f"actors={actor_count}")
    print(f"messages_per_proc={args.messages_per_proc}")
    print(f"total_self_messages={total_messages}")
    print(f"warmup_messages_per_proc={args.warmup_messages_per_proc}")
    print(f"telemetry_enabled={str(not args.disable_telemetry).lower()}")
    print(f"payload_bytes={args.payload_bytes}")
    print(f"inflight={args.inflight}")
    print(f"complete={str(complete).lower()}")
    print(f"processed_messages={processed_delta}")
    print(f"telemetry_expected_tick_rows={total_expected_tick_rows}")
    print(f"telemetry_processed_tick_rows_at_completion={processed_at_completion}")
    if telemetry_tick_rows_at_completion is None:
        print("telemetry_tick_rows_at_completion=unavailable")
        print("telemetry_missing_tick_rows_at_completion=unavailable")
    else:
        missing_at_completion = max(
            processed_at_completion - telemetry_tick_rows_at_completion,
            0,
        )
        print(f"telemetry_tick_rows_at_completion={telemetry_tick_rows_at_completion}")
        print(f"telemetry_missing_tick_rows_at_completion={missing_at_completion}")
    print(
        "telemetry_tick_rows_at_completion_latency_ms="
        f"{telemetry_tick_rows_at_completion_latency * 1000.0:.3f}"
    )
    if telemetry_catchup_tick_rows is None:
        print("telemetry_catchup_tick_rows=unavailable")
        print("telemetry_catchup_missing_tick_rows=unavailable")
    else:
        catchup_missing_rows = max(
            processed_at_completion - telemetry_catchup_tick_rows,
            0,
        )
        print(f"telemetry_catchup_tick_rows={telemetry_catchup_tick_rows}")
        print(f"telemetry_catchup_missing_tick_rows={catchup_missing_rows}")
    print(f"telemetry_catchup_elapsed_ms={telemetry_catchup_elapsed * 1000.0:.3f}")
    if telemetry_tick_rows is None:
        print("telemetry_tick_rows=unavailable")
        print("telemetry_missing_tick_rows=unavailable")
    else:
        missing_tick_rows = max(total_expected_tick_rows - telemetry_tick_rows, 0)
        print(f"telemetry_tick_rows={telemetry_tick_rows}")
        print(f"telemetry_missing_tick_rows={missing_tick_rows}")
    print(f"queue_elapsed_ms={queue_elapsed * 1000.0:.3f}")
    print(f"drain_elapsed_ms={drain_elapsed * 1000.0:.3f}")
    print(f"end_to_end_elapsed_ms={end_to_end_elapsed * 1000.0:.3f}")
    print(f"end_to_end_messages_per_sec={end_to_end_rate:.3f}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="stress telemetry with actor self-message loops",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--procs",
        type=int,
        default=16,
        help="Local worker processes to spawn on the single host; use 16 for bottleneck sweeps and smaller values for smoke runs.",
    )
    parser.add_argument(
        "--messages",
        dest="messages_per_proc",
        type=int,
        default=50_000,
        help="Measured self-messages to send per actor; larger values reduce fixed startup and drain effects.",
    )
    parser.add_argument(
        "--warmup-messages",
        dest="warmup_messages_per_proc",
        type=int,
        default=500,
        help="Self-messages to send per actor before measurement so the measured loop avoids cold-start effects.",
    )
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=128,
        help="Payload bytes attached to each self-message; sweep this to expose payload copy overhead.",
    )
    parser.add_argument(
        "--inflight",
        type=int,
        default=8,
        help="Maximum outstanding self-messages per actor; sweep this to distinguish latency/window limits from throughput limits.",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=240.0,
        help="Maximum seconds to wait for actor completion and final telemetry row visibility before reporting an incomplete run.",
    )
    parser.add_argument(
        "--telemetry-catchup-timeout-sec",
        type=float,
        default=15.0,
        help="Seconds to wait after actor completion for telemetry rows to become query-visible before stopping the catch-up check.",
    )
    parser.add_argument(
        "--disable-telemetry",
        action="store_true",
        help="Run the same actor loop without ProcessJob telemetry to isolate actor/message throughput from telemetry overhead.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.procs <= 0:
        raise RuntimeError("--procs must be greater than zero")
    if args.messages_per_proc < 0:
        raise RuntimeError("--messages must be non-negative")
    if args.warmup_messages_per_proc < 0:
        raise RuntimeError("--warmup-messages must be non-negative")
    if args.payload_bytes < 0:
        raise RuntimeError("--payload-bytes must be non-negative")
    if args.inflight <= 0:
        raise RuntimeError("--inflight must be greater than zero")
    if args.timeout_sec <= 0:
        raise RuntimeError("--timeout-sec must be greater than zero")
    if args.telemetry_catchup_timeout_sec < 0:
        raise RuntimeError("--telemetry-catchup-timeout-sec must be non-negative")

    job = ProcessJob({"hosts": 1})
    if not args.disable_telemetry:
        job = job.enable_telemetry(
            TelemetryConfig(
                include_dashboard=True,
                dashboard_port=0,
                use_sidecar=True,
            )
        )
    payload = bytes(args.payload_bytes)
    proc_mesh: ProcMesh | None = None

    try:
        state = job.state(cached_path=None)
        if state.telemetry_url is not None:
            print(f"telemetry_url={state.telemetry_url}")
        if state.dashboard_url is not None:
            print(f"dashboard_url={state.dashboard_url}")

        telemetry_client = state.query_engine_client
        proc_mesh = state.hosts.spawn_procs(per_host={"procs": args.procs})
        actors = proc_mesh.spawn("self_message", SelfMessagingActor)
        actors.configure.call(actors).get()
        actor_count = actors.size()

        if args.warmup_messages_per_proc:
            warmup_complete, _, _, _ = _run_loop(
                actors,
                args.warmup_messages_per_proc,
                payload,
                args.inflight,
                args.timeout_sec,
                _POLL_SEC,
            )
            print(f"warmup_complete={str(warmup_complete).lower()}")

        before_statuses: list[Status] = list(actors.status.call().get().values())
        complete, after_statuses, queue_elapsed, drain_elapsed = _run_loop(
            actors,
            args.messages_per_proc,
            payload,
            args.inflight,
            args.timeout_sec,
            _POLL_SEC,
        )
        (
            telemetry_tick_rows_at_completion,
            telemetry_tick_rows_at_completion_latency,
        ) = _query_tick_message_count_once(telemetry_client)
        processed_at_completion = _sum_stat(after_statuses, "processed")
        if (
            telemetry_tick_rows_at_completion is not None
            and telemetry_tick_rows_at_completion >= processed_at_completion
        ):
            telemetry_catchup_tick_rows = telemetry_tick_rows_at_completion
            telemetry_catchup_elapsed = 0.0
        else:
            (
                telemetry_catchup_tick_rows,
                telemetry_catchup_elapsed,
            ) = _wait_for_tick_message_count(
                telemetry_client,
                processed_at_completion,
                args.telemetry_catchup_timeout_sec,
                _POLL_SEC,
            )

        proc_mesh.stop("telemetry self-message benchmark complete").get()
        proc_mesh = None
        telemetry_tick_rows, _ = _wait_for_tick_message_count(
            telemetry_client,
            actor_count * (args.warmup_messages_per_proc + args.messages_per_proc),
            args.timeout_sec,
            _POLL_SEC,
        )
        _print_results(
            args,
            actor_count,
            complete,
            before_statuses,
            after_statuses,
            queue_elapsed,
            drain_elapsed,
            telemetry_tick_rows_at_completion,
            telemetry_tick_rows_at_completion_latency,
            telemetry_catchup_tick_rows,
            telemetry_catchup_elapsed,
            telemetry_tick_rows,
        )
    finally:
        if proc_mesh is not None:
            proc_mesh.stop("telemetry self-message benchmark complete").get()
        job.kill()


if __name__ == "__main__":
    main()
