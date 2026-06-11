# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Inbound Ordering Reproducer
===========================

Brings up a small mesh, induces deterministic sequence-gap stalls on a
receiver actor, exposes the mesh-admin HTTP endpoint, and waits.

Manual workflow::

    buck2 run fbcode//monarch/python/examples:inbound_ordering_workload

The workload prints three ready-to-paste lines on startup -- a ``curl``
example, a TUI launch command, and a verifier command. Copy whichever
you need; do not hand-construct URLs. The printed ``curl`` and verifier
commands already include ``--cacert`` / ``--cert`` / ``--key`` flags
when the admin URL is HTTPS (omitted for plain HTTP), and the receiver
actor reference in the curl URL is already percent-encoded.

Expected at ``/v1/<encoded-receiver-ref>.inbound_ordering``:

  - enabled: true, snapshot_complete: true
  - known_session_count == 3 (sender_a + sender_b + the workload's
    root client; the root client opens a session when it calls the
    receiver's bootstrap ``whoami`` endpoint, and that session
    remains in OrderedSender's session table even though it's idle)
  - returned_buffered_session_count == 2 (only sender_a / sender_b
    have buffered messages; the idle root-client session does not
    contribute to the returned_* rollups)
  - 2 sessions with buffered_count > 0; senders are distinct ActorAddr
    strings (the sender_a / sender_b spawn labels do appear in the
    actor_addr string)
  - both stalled-sender sessions expected_next_seq == 1
  - session A: buffered_count = 5, oldest_buffered_seq = 2,
    newest_buffered_seq = 6
  - session B: buffered_count = 3, oldest_buffered_seq = 2,
    newest_buffered_seq = 4
"""

import argparse
import asyncio
import logging
import urllib.parse

from monarch._src.actor.telemetry import TracingForwarder
from monarch.actor import Actor, context, endpoint
from monarch.job import ProcessJob

logger: logging.Logger = logging.getLogger("inbound_ordering_workload")
logger.addHandler(TracingForwarder())
logger.setLevel(logging.INFO)


class StalledReceiver(Actor):
    """Has a no-op handler; messages buffer when the seq before them is
    missing. Handler never runs for buffered messages -- ``OrderedSender``
    doesn't release them until the gap fills, which never happens here.
    """

    @endpoint
    async def whoami(self):
        # Return this actor's own address so the workload can pass it
        # to the senders for ``_debug_skip_next_ordering_seq``. There's
        # no Python-side accessor on ``ActorMesh`` that returns a
        # rank's ``ActorAddr``, so the receiver hands its own out via
        # a one-shot endpoint.
        return context().actor_instance.actor_id

    @endpoint
    async def handle_msg(self, payload: int) -> None:
        pass


class Sender(Actor):
    """Reserves seq 1 on the receiver's PythonMessage handler port, then
    fire-and-forgets ``send_count`` messages; they enter ``OrderedSender``
    as seqs 2..=send_count+1 and buffer (gap at seq 1)."""

    def __init__(self, receiver, receiver_addr) -> None:
        self._receiver = receiver
        self._receiver_addr = receiver_addr

    @endpoint
    async def stall_and_send(self, skip_count: int, send_count: int) -> None:
        instance = context().actor_instance
        # pyrefly: ignore [missing-attribute]
        instance._debug_skip_next_ordering_seq(self._receiver_addr, skip_count)
        # MUST be fire-and-forget. ``await self._receiver.handle_msg.call_one(i)``
        # would hang on the first call: the message is buffered behind the
        # missing seq 1, the handler never runs, the reply never arrives,
        # and seqs 3..=N are never sent. ``.broadcast()`` does not await a
        # reply.
        for i in range(send_count):
            self._receiver.handle_msg.broadcast(i)


async def async_main(args: argparse.Namespace) -> None:
    job = ProcessJob({"hosts": 1}).enable_admin()
    state = job.state(cached_path=None)
    host = state.hosts

    receiver_proc = host.spawn_procs(name="inbound_ordering_receiver")
    # Two separate singleton proc meshes for the two senders so each
    # sender has its own Instance (and therefore its own Sequencer and
    # SEQ_INFO session_id) by construction. Using one shared proc mesh
    # with two spawns relies on subtler ActorMesh semantics; two procs
    # makes the "two distinct session owners" promise structural.
    sender_a_proc = host.spawn_procs(name="inbound_ordering_sender_a")
    sender_b_proc = host.spawn_procs(name="inbound_ordering_sender_b")

    receiver = receiver_proc.spawn("stalled_receiver", StalledReceiver)
    # Get the receiver's address by asking it -- there's no direct
    # Python-side accessor on ``ActorMesh`` that returns a rank's
    # ``ActorAddr``. ``PyActorAddr`` is picklable via ``__reduce__``,
    # so it can be passed back over the endpoint result and forward
    # as a constructor argument to the senders.
    receiver_addr = await receiver.whoami.call_one()

    sender_a = sender_a_proc.spawn("sender_a", Sender, receiver, receiver_addr)
    sender_b = sender_b_proc.spawn("sender_b", Sender, receiver, receiver_addr)

    # Two distinct senders -> receiver shows 2 stalled sessions
    # (different session_ids, one per sender Instance's Sequencer).
    await sender_a.stall_and_send.call_one(skip_count=1, send_count=5)
    await sender_b.stall_and_send.call_one(skip_count=1, send_count=3)

    # Match pyspy_workload.py: emit mTLS flags conditionally when the
    # admin URL is HTTPS so the printed commands are paste-ready in
    # both local and production environments.
    mtls_flags = (
        " --cacert /var/facebook/rootcanal/ca.pem"
        " --cert /var/facebook/x509_identities/server.pem"
        " --key /var/facebook/x509_identities/server.pem"
        if state.admin_url.startswith("https://")
        else ""
    )

    # Actor refs contain '/' and must be percent-encoded in URL path
    # positions. Single source of truth is the ``receiver_addr``
    # PyActorAddr obtained from ``whoami`` above; Python ``ActorMesh``
    # does not expose an ``actor_addr()`` accessor.
    receiver_ref = str(receiver_addr)
    receiver_ref_encoded = urllib.parse.quote(receiver_ref, safe="")

    print(f"Mesh admin server listening on {state.admin_url}", flush=True)
    print(f"  - Stalled receiver: {receiver_ref}", flush=True)
    print(
        f"  - curl: curl{mtls_flags} {state.admin_url}/v1/{receiver_ref_encoded}",
        flush=True,
    )
    print(
        "  - TUI:   "
        f"buck2 run fbcode//monarch/hyperactor_mesh_admin_tui:hyperactor_mesh_admin_tui "
        f"-- --addr {state.admin_url}",
        flush=True,
    )
    print(
        "  - Verifier: "
        f"buck2 run fbcode//monarch/python/examples:verify_inbound_ordering "
        f"-- --admin-url {state.admin_url}"
        f"{mtls_flags}",
        flush=True,
    )

    try:
        await asyncio.sleep(float("inf"))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("\nShutting down...", flush=True)
        await sender_a_proc.stop()
        await sender_b_proc.stop()
        await receiver_proc.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
