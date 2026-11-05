# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Native execution telemetry across root and worker collector namespaces."""

import json
import shutil
import time

from monarch._src.job.execution_history import ExecutionRecorder
from monarch._src.job.telemetry_actor import TelemetryActor
from monarch._src.job.telemetry_ingest import telemetry_socket_dir
from monarch.actor import endpoint, HostMesh
from monarch.distributed_telemetry.engine import QueryEngine
from monarch.job import ProcessJob


class ExecutionTelemetryRoot(TelemetryActor):
    @endpoint
    def start_validation_worker(self, hosts: HostMesh, apply_id: str) -> bool:
        return (
            self._start_worker_collector(
                hosts, TelemetryActor, apply_id, "execution_validation_worker"
            )
            is not None
        )


def test_worker_local_command_records_are_queryable() -> None:
    created_ns = time.time_ns()
    job = ProcessJob({"workers": 1})
    root = None
    namespaces = []
    try:
        state = job.state(cached_path=None)
        namespaces = [
            f"{job.apply_id}-validation-root",
            f"{job.apply_id}-validation-worker",
        ]
        # Separate namespaces emulate collectors on physically separate hosts.
        procs = state.workers.spawn_procs(name="execution_validation_root")
        root = procs.spawn("root", ExecutionTelemetryRoot, namespaces[0], 0).slice(
            hosts=0
        )
        assert root.activate.call_one().get()
        assert root.start_validation_worker.call_one(state.workers, namespaces[1]).get()
        expected = []
        for namespace, returncode in zip(namespaces, (0, -15)):
            recorder = ExecutionRecorder(namespace, ["validation-command"], None, None)
            assert recorder.id is not None
            recorder.record_rank("workers", "rank0", {"returncode": returncode})
            normalized = 0 if returncode == 0 else 143
            recorder.finish(normalized)
            expected.append(
                {
                    "id": recorder.id,
                    "returncode": normalized,
                    "status": "succeeded" if normalized == 0 else "failed",
                }
            )
        query = QueryEngine(root)
        deadline = time.monotonic() + 30
        while True:
            rows = query.query(
                "SELECT e.id, r.returncode, s.status FROM executions e "
                "JOIN execution_rank_results r ON r.execution_id = e.id "
                "JOIN execution_status_events s ON s.execution_id = e.id "
                "WHERE s.status IN ('succeeded', 'failed') ORDER BY r.returncode"
            ).to_pylist()
            if rows == expected:
                break
            assert time.monotonic() < deadline, rows
            time.sleep(0.05)
        print(
            json.dumps(
                {
                    "job_apply_id": job.apply_id,
                    "created_ns": created_ns,
                    "configuration": {"workers": 1},
                    "collector_namespaces": namespaces,
                    "results": rows,
                }
            ),
            flush=True,
        )
    finally:
        try:
            if root is not None:
                root.stop_worker_collectors.call_one().get()
        finally:
            job.kill()
            for namespace in namespaces:
                shutil.rmtree(telemetry_socket_dir(namespace), ignore_errors=True)
