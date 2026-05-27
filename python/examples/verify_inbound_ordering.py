# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Inbound Ordering Verifier
=========================

Walks ``/v1/root`` -> first host -> ``inbound_ordering_receiver`` proc ->
``stalled_receiver`` actor, fetches ``/v1/{receiver}``, and asserts the
shape produced by ``inbound_ordering_workload.py``.

Exit codes::

  0  PASS
  1  FAIL  (any assertion failed; details on stderr)
  2  ERROR (network / TLS / unexpected payload structure)
"""

import argparse
import json
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_ERROR = 2

FETCH_TIMEOUT_SEC = 10.0

# Polling parameters: wait for the receiver's ``OrderedSender`` to
# publish the stalled shape through introspection. Fire-and-forget
# ``.broadcast()`` returning is not proof that the buffer is visible
# yet.
POLL_TIMEOUT_SEC = 30.0
POLL_INTERVAL_SEC = 0.5


class VerifierError(Exception):
    """Raised by helper functions; ``main()`` classifies into ``EXIT_ERROR``."""


def build_ssl_context(args: argparse.Namespace) -> Optional[ssl.SSLContext]:
    if not args.cacert:
        return None
    ctx = ssl.create_default_context(cafile=args.cacert)
    ctx.load_cert_chain(certfile=args.cert, keyfile=args.key)
    return ctx


def _enc(ref: str) -> str:
    # Mesh-admin refs contain '/' inside actor ids; default ``safe='/'``
    # would leave them unescaped and the proxy would treat them as
    # path segments. Match verify_pyspy.py.
    return urllib.parse.quote(ref, safe="")


def fetch_json(url: str, ctx: Optional[ssl.SSLContext]) -> dict:
    try:
        with urllib.request.urlopen(
            url, context=ctx, timeout=FETCH_TIMEOUT_SEC
        ) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError as e:
        raise VerifierError(f"GET {url} failed: {e}") from e
    except json.JSONDecodeError as e:
        raise VerifierError(f"GET {url} returned invalid JSON: {e}") from e


def find_actor_ref(root: dict, base_url: str, ctx: Optional[ssl.SSLContext]) -> str:
    """Walk root -> hosts -> procs -> actor named ``stalled_receiver``.

    ActorAddr.to_string() is ``{actor_uid}.{proc_id}@{location}`` where
    ``actor_uid`` is ``label<base58>``. Match the label exactly --
    substring would also match the controller
    (``actor_mesh_controller_stalled_receiver<...>``), which is a
    different actor with a different session table.
    """
    for host_ref in root.get("children", []):
        host = fetch_json(f"{base_url}/v1/{_enc(host_ref)}", ctx)
        for proc_ref in host.get("children", []):
            proc = fetch_json(f"{base_url}/v1/{_enc(proc_ref)}", ctx)
            for actor_ref in proc.get("children", []):
                label = actor_ref.split("<", 1)[0]
                if label == "stalled_receiver":
                    return actor_ref
    raise VerifierError(
        "stalled_receiver actor not found in topology; is the workload running?"
    )


def _matches_expected(inbound: Optional[dict]) -> bool:
    """Lightweight predicate used only for polling. Final assertions
    happen in ``main()`` so failure messages remain rich."""
    if inbound is None:
        return False
    if not inbound.get("enabled") or not inbound.get("snapshot_complete"):
        return False
    return inbound.get("returned_buffered_message_count") == 8


def poll_inbound_ordering(
    base_url: str, receiver_ref: str, ctx: Optional[ssl.SSLContext]
) -> tuple[dict, Optional[dict]]:
    """Poll ``/v1/{receiver}`` until ``inbound_ordering`` matches the
    expected stalled shape or ``POLL_TIMEOUT_SEC`` elapses. Returns
    ``(actor_payload, last_inbound_ordering_or_None)``. Raises
    ``VerifierError`` on transport error."""
    deadline = time.monotonic() + POLL_TIMEOUT_SEC
    last_inbound: Optional[dict] = None
    while True:
        actor = fetch_json(f"{base_url}/v1/{_enc(receiver_ref)}", ctx)
        actor_variant = actor.get("properties", {}).get("Actor")
        if actor_variant is None:
            raise VerifierError(
                f"receiver is not Actor variant: {actor.get('properties')}"
            )
        inbound = actor_variant.get("inbound_ordering")
        last_inbound = inbound
        if _matches_expected(inbound):
            return actor, inbound
        if time.monotonic() >= deadline:
            return actor, last_inbound
        time.sleep(POLL_INTERVAL_SEC)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--admin-url", required=True)
    parser.add_argument("--cacert", default=None)
    parser.add_argument("--cert", default=None)
    parser.add_argument("--key", default=None)
    args = parser.parse_args()

    try:
        ctx = build_ssl_context(args)
        root = fetch_json(f"{args.admin_url}/v1/root", ctx)
        receiver_ref = find_actor_ref(root, args.admin_url, ctx)
        _actor, inbound = poll_inbound_ordering(args.admin_url, receiver_ref, ctx)
    except VerifierError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(EXIT_ERROR)

    failures: list[str] = []

    def check(cond: bool, msg: str) -> None:
        if not cond:
            failures.append(msg)

    # IO-7: live actor built through ``Instance::new`` must expose Some.
    check(inbound is not None, "IO-7: inbound_ordering is None")
    if inbound is not None:
        # Wrap the indexed-access block so a missing/renamed field in
        # a successful payload becomes a structured FAIL (with the
        # offending payload printed at the bottom of main) rather
        # than a raw KeyError / TypeError traceback. The verifier is
        # an "agent-first" diagnostic artifact; tracebacks bury the
        # signal.
        try:
            # IO-1 / IO-4: tri-state branch + snapshot_complete derivation.
            check(inbound["enabled"] is True, "IO-1: enabled must be True")
            skipped = inbound["skipped_session_count"]
            check(
                inbound["snapshot_complete"] == (skipped == 0),
                f"IO-4: snapshot_complete must equal (skipped_session_count == 0); "
                f"got snapshot_complete={inbound['snapshot_complete']}, skipped={skipped}",
            )
            check(
                inbound["snapshot_complete"] is True,
                "IO-4 (deterministic case): snapshot_complete must be True",
            )

            # IO-5: known_session_count totals returned + skipped.
            # Deterministic value for this workload is 3 (asserted
            # separately below): sender_a + sender_b + the workload's
            # bootstrap client session.
            sessions = inbound["sessions"]
            check(
                inbound["known_session_count"] == len(sessions) + skipped,
                f"IO-5: known_session_count must equal sessions.len() + skipped_session_count; "
                f"got known={inbound['known_session_count']}, "
                f"sessions={len(sessions)}, skipped={skipped}",
            )
            # 3 sessions, not 2: the workload's root client opens a
            # session on the receiver via the ``whoami`` bootstrap
            # call (used to extract the receiver's ``ActorAddr`` for
            # ``_debug_skip_next_ordering_seq``). That session is idle
            # (buffered_count == 0), so it shows up in
            # known_session_count but NOT in the returned_* rollups.
            check(
                inbound["known_session_count"] == 3,
                f"IO-5 (deterministic case): known_session_count must be 3 "
                f"(sender_a + sender_b + workload bootstrap client); "
                f"got {inbound['known_session_count']}",
            )

            # IO-6: returned_* rollups equal recomputation over returned sessions.
            check(
                inbound["returned_buffered_session_count"]
                == sum(1 for s in sessions if s["buffered_count"] > 0),
                "IO-6: returned_buffered_session_count must equal count of returned "
                "sessions with buffered_count > 0",
            )
            check(
                inbound["returned_buffered_message_count"]
                == sum(s["buffered_count"] for s in sessions),
                "IO-6: returned_buffered_message_count must equal sum of "
                "buffered_count over returned sessions",
            )
            check(
                inbound["returned_max_buffered_count"]
                == max((s["buffered_count"] for s in sessions), default=0),
                "IO-6: returned_max_buffered_count must equal max of "
                "buffered_count over returned sessions",
            )

            # Deterministic content totals (not invariants -- workload shape).
            check(
                inbound["returned_buffered_message_count"] == 8,
                f"workload shape: returned_buffered_message_count expected 8 (5+3); "
                f"got {inbound['returned_buffered_message_count']}",
            )
            check(
                inbound["returned_max_buffered_count"] == 5,
                f"workload shape: returned_max_buffered_count expected 5; "
                f"got {inbound['returned_max_buffered_count']}",
            )

            # Per-session shape (sorted by buffered_count desc).
            sorted_sessions = sorted(sessions, key=lambda s: -s["buffered_count"])
            if len(sorted_sessions) >= 2:
                sa, sb = sorted_sessions[0], sorted_sessions[1]
                check(
                    sa["buffered_count"] == 5,
                    f"session A buffered_count != 5: {sa['buffered_count']}",
                )
                check(
                    sb["buffered_count"] == 3,
                    f"session B buffered_count != 3: {sb['buffered_count']}",
                )
                for s in (sa, sb):
                    check(
                        s["expected_next_seq"] == 1,
                        f"session expected_next_seq != 1: {s['expected_next_seq']}",
                    )
                    check(
                        s["oldest_buffered_seq"] == 2,
                        f"session oldest_buffered_seq != 2: {s['oldest_buffered_seq']}",
                    )
                check(
                    sa["newest_buffered_seq"] == 6,
                    f"session A newest_buffered_seq != 6: {sa['newest_buffered_seq']}",
                )
                check(
                    sb["newest_buffered_seq"] == 4,
                    f"session B newest_buffered_seq != 4: {sb['newest_buffered_seq']}",
                )

                # Both sessions have non-empty sender strings AND the two
                # senders are distinct (proves two real session owners).
                for s in (sa, sb):
                    check(
                        isinstance(s.get("sender"), str) and len(s["sender"]) > 0,
                        f"session sender must be a non-empty string: {s.get('sender')!r}",
                    )
                check(
                    sa.get("sender") != sb.get("sender"),
                    f"two stalled sessions must have distinct senders; "
                    f"both report {sa.get('sender')!r}",
                )

                # Operator confidence: print observed sender addresses.
                print(f"  session A sender: {sa.get('sender')}", file=sys.stderr)
                print(f"  session B sender: {sb.get('sender')}", file=sys.stderr)
        except (KeyError, TypeError) as e:
            # The payload was successfully fetched but is missing a
            # field the verifier indexes into (renamed wire field,
            # dropped rollup, sessions not a list, etc.). Treat as a
            # FAIL -- the API contract was violated, not the
            # transport.
            failures.append(
                f"malformed inbound_ordering payload: {type(e).__name__}: {e}"
            )

    if failures:
        for f in failures:
            print(f"FAIL: {f}", file=sys.stderr)
        if inbound is not None:
            print(
                f"  last observed inbound_ordering: {json.dumps(inbound, indent=2)}",
                file=sys.stderr,
            )
        sys.exit(EXIT_FAIL)

    print("OK: inbound_ordering API matches expected stalled state")


if __name__ == "__main__":
    main()
