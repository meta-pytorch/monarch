# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
py-spy Endpoint Verifier
=========================

Automated verification script for the ``GET /v1/pyspy/{proc_reference}``
endpoint.  Discovers workload procs via structured JSON traversal of
``/v1/root`` → ``/v1/{host_ref}`` → ``/v1/{proc_ref}`` (not by parsing
the ``/v1/tree`` ASCII output), samples their stacks, and checks for
mode-specific evidence that py-spy captures meaningful Python frames.

Exit codes::

  0  PASS — mode-specific evidence found
  1  FAIL — no evidence or unexpected errors
  2  SKIP — py-spy not available (all BinaryNotFound)

Usage::

    buck2 run fbcode//monarch/python/examples:verify_pyspy -- \\
        --admin-url https://host:1729 --mode cpu --samples 10

With mTLS::

    buck2 run fbcode//monarch/python/examples:verify_pyspy -- \\
        --admin-url https://host:1729 --mode cpu --samples 10 \\
        --cacert /var/facebook/rootcanal/ca.pem \\
        --cert /var/facebook/x509_identities/server.pem \\
        --key /var/facebook/x509_identities/server.pem
"""

import argparse
import json
import ssl
import sys
import urllib.parse
import urllib.request
from typing import Optional


# -- Mode-specific frame evidence ------------------------------------

# Frames we expect to see in py-spy stacks for each mode.  These match
# the named helpers in pyspy_workload.py.
MODE_EVIDENCE = {
    "cpu": ["do_cpu_work", "_cpu_burn_loop", "process_batch"],
    "block": ["do_blocking_work", "time.sleep", "process_batch"],
    "mixed": ["do_cpu_work", "_cpu_burn_loop", "process_batch"],
}

# Prefix used by pyspy_workload.py for worker proc names.
WORKER_PROC_PREFIX = "pyspy_worker"

# Quality gate: warn if hit rate (evidence / ok) falls below these
# mode-specific thresholds.  Does not affect PASS/FAIL — just a
# signal that evidence is unusually rare.
MODE_QUALITY_THRESHOLD = {
    "cpu": 0.7,
    "block": 0.8,
    "mixed": 0.3,
}

EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_SKIP = 2


def build_ssl_context(args: argparse.Namespace) -> Optional[ssl.SSLContext]:
    """Build an SSL context from CLI cert flags, or None for plain HTTP."""
    if not args.cacert:
        return None
    ctx = ssl.create_default_context(cafile=args.cacert)
    ctx.load_cert_chain(certfile=args.cert, keyfile=args.key)
    return ctx


def fetch_json(url: str, ctx: Optional[ssl.SSLContext]) -> dict:
    """GET a URL and parse JSON response."""
    req = urllib.request.Request(url)
    resp = urllib.request.urlopen(req, context=ctx, timeout=10)
    return json.loads(resp.read())


def resolve_ref(
    admin_url: str, ref: str, ctx: Optional[ssl.SSLContext]
) -> Optional[dict]:
    """Resolve a single reference via /v1/{ref}.  Returns None on
    error (timeout, 404, etc.)."""
    encoded = urllib.parse.quote(ref, safe="")
    try:
        return fetch_json(f"{admin_url}/v1/{encoded}", ctx)
    except Exception:
        return None


def discover_workload_procs(admin_url: str, ctx: Optional[ssl.SSLContext]) -> list[str]:
    """Walk root -> hosts -> procs -> actors via structured JSON
    traversal and return proc references that contain actors matching
    the worker prefix.

    Traversal: /v1/root (children) -> /v1/{host_ref} (children) ->
    /v1/{proc_ref} (children) -> filter by WORKER_PROC_PREFIX in
    actor reference string.  The proc itself is the py-spy target
    (not the actor), since py-spy attaches to the OS process."""
    root = fetch_json(f"{admin_url}/v1/root", ctx)
    procs = []

    for host_ref in root.get("children", []):
        host_node = resolve_ref(admin_url, host_ref, ctx)
        if host_node is None:
            continue
        for proc_ref in host_node.get("children", []):
            proc_node = resolve_ref(admin_url, proc_ref, ctx)
            if proc_node is None:
                continue
            # Check if any actor child is a worker (not a
            # controller/mesh-infra actor that happens to contain
            # the prefix in its name).  The actor name is the last
            # comma-separated segment; match the prefix there.
            for actor_ref in proc_node.get("children", []):
                actor_name = (
                    actor_ref.rsplit(",", 1)[-1] if "," in actor_ref else actor_ref
                )
                if actor_name.startswith(WORKER_PROC_PREFIX):
                    procs.append(proc_ref)
                    break

    return procs


def sample_pyspy(
    admin_url: str,
    proc_ref: str,
    ctx: Optional[ssl.SSLContext],
) -> dict:
    """Hit the pyspy endpoint once and return the parsed JSON."""
    encoded = urllib.parse.quote(proc_ref, safe="")
    return fetch_json(f"{admin_url}/v1/pyspy/{encoded}", ctx)


def has_evidence(stack: str, mode: str) -> bool:
    """Check whether a stack contains mode-specific evidence frames."""
    patterns = MODE_EVIDENCE.get(mode, [])
    return any(p in stack for p in patterns)


def short_name(proc_ref: str) -> str:
    """Extract a short display name from a proc reference."""
    return proc_ref.split(",")[-1] if "," in proc_ref else proc_ref


def run_verification(args: argparse.Namespace) -> int:
    """Run the verification and return exit code."""
    ctx = build_ssl_context(args)

    # Discover workload procs via structured JSON traversal.
    print(f"Discovering workload procs at {args.admin_url} ...")
    procs = discover_workload_procs(args.admin_url, ctx)
    if not procs:
        print(
            f"FAIL: no workload procs found (looking for '{WORKER_PROC_PREFIX}' prefix)"
        )
        return EXIT_FAIL

    print(f"Found {len(procs)} workload proc(s)\n")

    total_ok = 0
    total_not_found = 0
    total_failed = 0
    total_evidence = 0

    for proc_ref in procs:
        name = short_name(proc_ref)
        ok = 0
        not_found = 0
        failed = 0
        evidence = 0

        for _ in range(args.samples):
            try:
                result = sample_pyspy(args.admin_url, proc_ref, ctx)
            except Exception as e:
                print(f"  {name}: ERROR {e}")
                failed += 1
                continue

            if "Ok" in result:
                ok += 1
                stack = result["Ok"].get("stack", "")
                if has_evidence(stack, args.mode):
                    evidence += 1
            elif "BinaryNotFound" in result:
                not_found += 1
            elif "Failed" in result:
                failed += 1

        total_ok += ok
        total_not_found += not_found
        total_failed += failed
        total_evidence += evidence

        if evidence > 0:
            status = "evidence"
        elif ok > 0:
            status = "no-evidence"
        elif not_found > 0:
            status = "skip"
        else:
            status = "fail"
        print(
            f"  {name}: {ok} ok ({evidence} with evidence), "
            f"{not_found} not-found, {failed} failed  [{status}]"
        )

    print()

    # Warn on mixed Ok/BinaryNotFound (possible packaging drift).
    if total_ok > 0 and total_not_found > 0:
        print(
            f"WARN: mixed results — {total_ok} Ok and "
            f"{total_not_found} BinaryNotFound across procs "
            f"(possible py-spy packaging drift)"
        )

    # Evaluate.
    if total_failed > 0:
        print(f"FAIL: {total_failed} failed response(s)")
        return EXIT_FAIL

    if total_ok == 0 and total_not_found > 0:
        print("SKIP: py-spy not available (all BinaryNotFound)")
        return EXIT_SKIP

    if total_evidence == 0:
        print(
            f"FAIL: {total_ok} Ok response(s) but none contained "
            f"mode-specific evidence for --mode={args.mode}"
        )
        print(f"  Expected frames: {MODE_EVIDENCE.get(args.mode, [])}")
        return EXIT_FAIL

    hit_rate = total_evidence / total_ok
    threshold = MODE_QUALITY_THRESHOLD.get(args.mode, 0.5)

    print(
        f"PASS: {total_evidence}/{total_ok} snapshot(s) contained "
        f"evidence for --mode={args.mode} (hit rate: {hit_rate:.0%})"
    )
    if hit_rate < threshold:
        print(
            f"WARN: hit rate {hit_rate:.0%} is below quality "
            f"threshold {threshold:.0%} for --mode={args.mode}"
        )

    return EXIT_PASS


def main() -> None:
    p = argparse.ArgumentParser(description="py-spy endpoint verifier")
    p.add_argument("--admin-url", required=True, help="Mesh admin base URL")
    p.add_argument(
        "--mode",
        choices=["cpu", "block", "mixed"],
        default="cpu",
        help="Expected workload mode (default: cpu)",
    )
    p.add_argument(
        "--samples",
        type=int,
        default=5,
        help="Number of samples per proc (default: 5)",
    )
    mtls = p.add_argument_group("mTLS (all three required together, or none)")
    mtls.add_argument("--cacert", help="CA certificate")
    mtls.add_argument("--cert", help="Client certificate")
    mtls.add_argument("--key", help="Client key")
    args = p.parse_args()
    if any([args.cacert, args.cert, args.key]) and not all(
        [args.cacert, args.cert, args.key]
    ):
        p.error("--cacert, --cert, and --key must all be provided together")

    sys.exit(run_verification(args))


if __name__ == "__main__":
    main()
