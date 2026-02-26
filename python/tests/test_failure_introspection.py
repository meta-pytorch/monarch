# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Integration tests for failure introspection.

Verifies that when an actor fails, the introspection API exposes:
- failure_info on the failed actor (error message, root cause, timestamp)
- is_poisoned / failed_actor_count on the owning proc
- healthy sibling procs remain unpoisoned
"""

import asyncio
import json
import ssl
import urllib.request

import monarch.actor
import pytest
from monarch._src.actor.host_mesh import this_host
from monarch.actor import Actor, endpoint
from monarch.config import parametrize_config


class ActorCrash(BaseException):
    """BaseException subclass that triggers supervision (not caught by handler)."""

    pass


class FailWorker(Actor):
    @endpoint
    async def work(self) -> None:
        pass

    @endpoint
    async def crash(self) -> None:
        raise ActorCrash("GPU memory corruption")


def _fetch_json(url: str) -> dict:
    # Skip hostname verification — in CI the server's x509 identity cert
    # may not match the machine hostname.
    ctx = ssl.create_default_context()
    if url.startswith("https"):
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(url, timeout=5, context=ctx) as resp:
        return json.loads(resp.read())


def _encode(ref: str) -> str:
    return urllib.parse.quote(ref, safe="")


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_failed_actor_has_failure_info() -> None:
    """After an actor crashes, its introspection payload has failure_info."""
    original_hook = monarch.actor.unhandled_fault_hook
    faulted = asyncio.Event()
    monarch.actor.unhandled_fault_hook = lambda failure: faulted.set()
    try:
        host = this_host()
        base = await host._spawn_admin()

        procs = host.spawn_procs(per_host={"replica": 2})
        workers = procs.spawn("worker", FailWorker)

        await workers.work.call()

        # Crash replica 0.
        try:
            await workers.slice(replica=0).crash.call_one()
        except Exception:
            pass

        # Wait for supervision to propagate.
        await asyncio.wait_for(faulted.wait(), timeout=15.0)
        await asyncio.sleep(2)

        # Find the poisoned proc by walking the tree.
        root = _fetch_json(f"{base}/v1/root")
        poisoned_proc = None
        failed_worker_ref = None

        for host_ref in root["children"]:
            host_data = _fetch_json(f"{base}/v1/{_encode(host_ref)}")
            for proc_ref in host_data.get("children", []):
                proc_data = _fetch_json(f"{base}/v1/{_encode(proc_ref)}")
                props = proc_data.get("properties", {}).get("Proc")
                if props and props.get("is_poisoned"):
                    poisoned_proc = proc_data
                    for stopped in props.get("stopped_children", []):
                        if "worker" in stopped:
                            failed_worker_ref = stopped
                            break
                    break
            if poisoned_proc:
                break

        # --- Assert proc is poisoned ---
        assert poisoned_proc is not None, "No poisoned proc found"
        proc_props = poisoned_proc["properties"]["Proc"]
        assert proc_props["is_poisoned"] is True
        assert proc_props["failed_actor_count"] >= 1
        assert len(proc_props["stopped_children"]) >= 1

        # --- Assert failed worker has failure_info ---
        assert failed_worker_ref is not None, "No failed worker in stopped_children"
        worker_data = _fetch_json(f"{base}/v1/{_encode(failed_worker_ref)}")
        actor_props = worker_data["properties"]["Actor"]

        assert "failed" in actor_props["actor_status"].lower()
        fi = actor_props["failure_info"]
        assert fi is not None, "failure_info should be present"
        assert "GPU memory corruption" in fi["error_message"]
        assert fi["root_cause_actor"] != ""
        assert fi["occurred_at"] != ""
        assert fi["is_propagated"] is False

    finally:
        monarch.actor.unhandled_fault_hook = original_hook
        await procs.stop()


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_healthy_procs_not_poisoned() -> None:
    """Procs without failed actors should not be poisoned."""
    original_hook = monarch.actor.unhandled_fault_hook
    faulted = asyncio.Event()
    monarch.actor.unhandled_fault_hook = lambda failure: faulted.set()
    try:
        host = this_host()
        base = await host._spawn_admin()

        procs = host.spawn_procs(per_host={"replica": 3})
        workers = procs.spawn("worker", FailWorker)

        await workers.work.call()

        # Crash only replica 0.
        try:
            await workers.slice(replica=0).crash.call_one()
        except Exception:
            pass

        await asyncio.wait_for(faulted.wait(), timeout=15.0)
        await asyncio.sleep(2)

        root = _fetch_json(f"{base}/v1/root")
        poisoned_count = 0
        healthy_count = 0

        for host_ref in root["children"]:
            host_data = _fetch_json(f"{base}/v1/{_encode(host_ref)}")
            for proc_ref in host_data.get("children", []):
                proc_data = _fetch_json(f"{base}/v1/{_encode(proc_ref)}")
                props = proc_data.get("properties", {}).get("Proc")
                if not props:
                    continue
                if props.get("is_poisoned"):
                    poisoned_count += 1
                else:
                    healthy_count += 1

        assert poisoned_count == 1, f"Expected 1 poisoned proc, got {poisoned_count}"
        assert healthy_count >= 2, f"Expected >=2 healthy procs, got {healthy_count}"

    finally:
        monarch.actor.unhandled_fault_hook = original_hook
        await procs.stop()
