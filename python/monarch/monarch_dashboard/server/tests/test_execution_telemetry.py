# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Exercise native DataFusion SQL and bounded command/rank pages."""

import socket
import struct
import time

import pyarrow as pa
import requests
from monarch._src.job.execution_history import execution_schemas
from monarch.job import ProcessJob, TelemetryConfig


def test_native_execution_history_queries_are_bounded():
    def check(name, condition):
        assert condition, name
        print("PASS", name, flush=True)

    def ipc(schema, rows):
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as w:
            w.write_batch(pa.RecordBatch.from_pylist(rows, schema=schema))
        return sink.getvalue().to_pybytes()

    j = ProcessJob({"workers": 1}).enable_telemetry(
        TelemetryConfig(
            retention_secs=0,
            include_dashboard=False,
            snapshot_interval_secs=0,
        )
    )
    try:
        state = j.state(cached_path=None)
        url = state.telemetry_url
        print("LOCAL PROBE API", url, flush=True)
        now = time.time_ns() // 1000
        executions = []
        statuses = []
        ranks = []
        for i in range(10121):
            identifier = f"{i:032x}"
            active = 10000 <= i < 10120
            stale = i == 10120
            failed = i < 10000 and i % 100 == 0
            command = (
                "printf '100%_done\\path'"
                if i == 0
                else "python long_spmd.py"
                if active or stale
                else "ls -lah /opt/workspace"
            )
            executions.append(
                {
                    "id": identifier,
                    "timestamp_us": now - 60_000_000 + i,
                    "command": command,
                    "workdir": None,
                    "output_dir": None,
                }
            )
            statuses.append(
                {
                    "execution_id": identifier,
                    "timestamp_us": now - 50_000_000 if stale else now,
                    "status": "running"
                    if active or stale
                    else "failed"
                    if failed
                    else "succeeded",
                    "returncode": None if active or stale else 7 if failed else 0,
                    "error": None,
                }
            )
        for i in range(205):
            ranks.append(
                {
                    "id": f"{i:032x}",
                    "execution_id": "0" * 32,
                    "timestamp_us": now,
                    "mesh": "workers",
                    "rank": f"{i:03}",
                    "flat_rank": i,
                    "coordinates": '{"hosts":0}',
                    "host_rank": 0,
                    "returncode": 7 if i == 204 else 0,
                }
            )
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(30)
            s.connect(f"/tmp/monarch_{j.apply_id}/telemetry.sock")
            for table, rows in zip(execution_schemas(), (executions, statuses, ranks)):
                payload = ipc(execution_schemas()[table], rows)
                name = table.encode()
                s.sendall(
                    struct.pack("!H", len(name))
                    + name
                    + struct.pack("!I", len(payload))
                    + payload
                )

        def api(route):
            r = requests.get(url + "/api/" + route, timeout=90)
            assert r.ok, r.text
            r.raise_for_status()
            return r

        deadline = time.monotonic() + 30
        while True:
            page = api("executions?limit=10").json()
            if sum(page["counts"].values()) == 10121:
                break
            assert time.monotonic() < deadline, page
            time.sleep(0.2)
        check(
            "Native DataFusion query retains 10,121 independent invocations",
            page["counts"]
            == {"failed": 100, "succeeded": 9900, "running": 120, "unknown": 1},
        )
        check(
            "Large active list and history have bounded response sizes",
            len(page["active"]) == 100 and len(page["history"]) == 10,
        )
        result = api("executions?limit=10&search=100%25_done%5Cpath").json()
        check(
            "Percent, underscore and backslash search are literal",
            len(result["history"]) == 1 and result["history"][0]["id"] == "0" * 32,
        )
        detail = api("executions/" + "0" * 32)
        data = detail.json()
        check(
            "Rank details report 205 outcomes with only 100 summaries",
            data["rank_count"] == 205 and len(data["ranks"]) == 100,
        )
        check(
            "Rank pages contain only identities and exit results",
            len(detail.content) < 40000
            and all("stdout" not in r for r in data["ranks"]),
        )
        summary = api("summary").json()
        check(
            "Native fleet health query works with execution SQL",
            summary["health_status"]
            == ("unknown" if summary["health_coverage"]["unknown"] else "healthy")
            and summary["errors"]["failed_actors"] == [],
        )
        response = api("executions?limit=10")
        check(
            "Browsing does not create extra execution records",
            sum(response.json()["counts"].values()) == 10121,
        )
    finally:
        j.kill()
