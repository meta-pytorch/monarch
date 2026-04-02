#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Benchmark persistent chunk cache for remotemount across actor restarts.

Calls remoterun.sh repeatedly with modifications between runs to
exercise cache hit, partial update, and full transfer paths.
Cache files persist in /tmp/monarch_remotemount_cache/ on worker
hosts, so subsequent runs on the same hosts skip unchanged data.

Each scenario spawns fresh actors (new subprocess) to simulate real
restarts. The persistent cache on disk is the only state that carries
over between scenarios.

Scenarios:
  1. Cold start (no cache on workers)
  2. No change (cache hit -> skip transfer)
  3. Rewrite data.bin same size (partial -> dirty blocks only)
  4. Rewrite all .py files (partial -> many dirty blocks)
  5. Delete one file (size change -> stale -> full transfer)

Usage:
  python3 examples/remotemount/bench_incremental.py --sizes 1 --hosts 2
  python3 examples/remotemount/bench_incremental.py --sizes 1,2,4 --hosts 2,4,8
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

import fire

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RUN_SCRIPT = os.path.join(SCRIPT_DIR, "remoterun.sh")
NUM_PY = 1000
SCENARIOS = [
    "Cold start",
    "No change",
    "Rewrite data.bin",
    "Rewrite .py",
    "Delete file",
]


def _parse_list(value):
    """Parse comma-separated string or numeric value into a list of ints."""
    if isinstance(value, (int, float)):
        return [int(value)]
    if isinstance(value, (list, tuple)):
        return [int(x) for x in value]
    return [int(x) for x in str(value).split(",")]


def _create_test_dir(base_dir, total_gb):
    """Create test directory with a large data file and many .py files."""
    shutil.rmtree(base_dir, ignore_errors=True)
    os.makedirs(base_dir, exist_ok=True)

    src_dir = os.path.join(base_dir, "src")
    os.makedirs(src_dir, exist_ok=True)
    for i in range(NUM_PY):
        with open(os.path.join(src_dir, f"mod_{i}.py"), "w") as f:
            f.write(f"# Module {i}\n" * 50 + f"def func_{i}(): return {i}\n")

    py_size = sum(
        os.path.getsize(os.path.join(src_dir, f)) for f in os.listdir(src_dir)
    )
    data_size = max(0, int(total_gb * 1024**3) - py_size)
    data_path = os.path.join(base_dir, "data.bin")
    with open(data_path, "wb") as f:
        remaining = data_size
        while remaining > 0:
            chunk = min(remaining, 64 * 1024 * 1024)
            f.write(os.urandom(chunk))
            remaining -= chunk

    total_mb = sum(
        os.path.getsize(os.path.join(r, fn))
        for r, _, fs in os.walk(base_dir)
        for fn in fs
    ) // (1024 * 1024)
    print(f"  Created {total_mb}MB test directory ({NUM_PY} .py files)")


_PROGRESS_KEYWORDS = [
    "timings:",
    "fresh",
    "partial",
    "stale",
    "up-to-date",
    "skipping transfer",
    "block transfer",
    "fan-out",
    "open() timings",
    "pack_directory",
    "channel",
    "error",
    "fail",
    "exception",
    "traceback",
    "valueerror",
    "job failed",
    "mast_reconnect",
    "job.state()",
]


def _run(
    label, num_hosts, source_dir, script="#!/bin/bash\necho done\n", extra_args=None
):
    """Run remoterun via run.sh and return elapsed time."""
    print(f"  {label}")

    cmd = [
        "bash",
        RUN_SCRIPT,
        source_dir,
        "stdin",
        "--num_hosts",
        str(num_hosts),
        "--gpus_per_host",
        "1",
    ]
    if extra_args:
        cmd.extend(extra_args)

    t0 = time.time()
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    proc.stdin.write(script.encode())
    proc.stdin.close()

    # Read raw bytes to catch \r progress bars (not just \n lines).
    buf = b""
    output_bytes = b""
    while True:
        chunk = os.read(proc.stdout.fileno(), 4096)
        if not chunk:
            break
        output_bytes += chunk
        buf += chunk
        # Split on both \n and \r to catch progress bars.
        while b"\n" in buf or b"\r" in buf:
            idx_n = buf.find(b"\n")
            idx_r = buf.find(b"\r")
            if idx_n == -1:
                idx = idx_r
            elif idx_r == -1:
                idx = idx_n
            else:
                idx = min(idx_n, idx_r)
            line = buf[:idx].decode(errors="replace").strip()
            buf = buf[idx + 1 :]
            if not line:
                continue
            elapsed_so_far = time.time() - t0
            lower = line.lower()
            if any(k in lower for k in _PROGRESS_KEYWORDS):
                print(f"    [{elapsed_so_far:6.1f}s] {line}")
                sys.stdout.flush()
            elif "completed" in lower or "%" in line:
                # conda_pack progress bars — throttle to % changes only.
                pct = re.search(r"(\d+)%", line)
                pct_val = pct.group(1) if pct else ""
                if not hasattr(_run, "_last_pct") or _run._last_pct != pct_val:
                    _run._last_pct = pct_val
                    print(f"    [{elapsed_so_far:6.1f}s] {line}")
                    sys.stdout.flush()

    proc.wait()
    elapsed = time.time() - t0

    output = output_bytes.decode(errors="replace")
    lines = output.splitlines()

    # Extract classification from logs.
    classification = ""
    for line in lines:
        m = re.search(
            r"(\d+)\s+fresh.*?(\d+)\s+partial.*?(\d+)\s+stale",
            line,
            re.IGNORECASE,
        )
        if m:
            classification = f"({m.group(1)}F/{m.group(2)}P/{m.group(3)}S)"
            break
        if re.search(r"up-to-date|skipping transfer", line, re.IGNORECASE):
            classification = "(all fresh)"
            break

    status = "OK" if proc.returncode == 0 else f"FAIL({proc.returncode})"
    print(f"  {label:.<40s} {elapsed:7.1f}s  {classification}  {status}")

    if proc.returncode != 0:
        for line in lines[-5:]:
            print(f"    {line}")

    return elapsed


def _run_with_dummy(label, num_hosts, warmup_dir, script, extra_args=None):
    """Run a script on workers using a tiny dummy source directory."""
    shutil.rmtree(warmup_dir, ignore_errors=True)
    os.makedirs(warmup_dir, exist_ok=True)
    with open(os.path.join(warmup_dir, "dummy.txt"), "w") as f:
        f.write("x\n")
    elapsed = _run(
        label, num_hosts, source_dir=warmup_dir, script=script, extra_args=extra_args
    )
    shutil.rmtree(warmup_dir, ignore_errors=True)
    return elapsed


def _warmup(num_hosts, warmup_dir):
    """Allocate MAST job and pre-spawn actors."""
    print(f"\n  Allocating MAST job ({num_hosts} hosts)...")
    t0 = time.time()
    _run_with_dummy("warmup", num_hosts, warmup_dir, "#!/bin/bash\necho ready\n")
    elapsed = time.time() - t0
    print(f"  MAST job allocated in {elapsed:.0f}s\n")


def _clear_worker_cache(num_hosts, warmup_dir):
    """Clear persistent cache on all workers."""
    _run_with_dummy(
        "clear cache",
        num_hosts,
        warmup_dir,
        "#!/bin/bash\nrm -rf /tmp/monarch_remotemount_cache/\necho cleared\n",
    )


def _kill_job():
    """Kill MAST jobs and clear local state."""
    print("  Killing MAST jobs...")
    result = subprocess.run(
        ["mast", "list-jobs", "--prefix", "monarch-", "--output", "json"],
        capture_output=True,
        text=True,
    )
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            jobs = data if isinstance(data, list) else [data]
            for job in jobs:
                name = job.get("name", job.get("job_name", ""))
                if name:
                    print(f"    Killing {name}...")
                    subprocess.run(
                        ["mast", "kill", name, "--comment", "bench cleanup"],
                        capture_output=True,
                        text=True,
                    )
        except (json.JSONDecodeError, TypeError):
            pass
    try:
        os.remove(".monarch/job_state.pkl")
    except FileNotFoundError:
        pass
    print("  Done.")


def _hash_directory(path):
    """Hash a directory tree deterministically."""
    import xxhash

    h = xxhash.xxh64()
    for root, dirs, files in sorted(os.walk(path)):
        dirs.sort()
        for fname in sorted(files):
            fpath = os.path.join(root, fname)
            rel = os.path.relpath(fpath, path)
            h.update(rel.encode())
            with open(fpath, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    h.update(chunk)
    return h.hexdigest()


def _worker_hash_script(mount_path):
    """Generate a bash script that hashes the FUSE-mounted directory."""
    return f"""\
#!/bin/bash
python -c "
import os, xxhash
mount = '{mount_path}'
h = xxhash.xxh64()
for root, dirs, files in sorted(os.walk(mount)):
    dirs.sort()
    for fname in sorted(files):
        fpath = os.path.join(root, fname)
        rel = os.path.relpath(fpath, mount)
        h.update(rel.encode())
        with open(fpath, 'rb') as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
print('HASH:' + h.hexdigest())
"
"""


def _verify_content(label, num_hosts, source_dir, extra_args=None):
    """Verify mounted content matches client by comparing directory hashes."""
    sys.stdout.write(f"  {label:.<40s}")
    sys.stdout.flush()

    # Hash on client (in-process, uses conda env's xxhash).
    client_hash = _hash_directory(source_dir)

    # Hash on workers (via remoterun).
    cmd = [
        "bash",
        RUN_SCRIPT,
        source_dir,
        "stdin",
        "--num_hosts",
        str(num_hosts),
        "--gpus_per_host",
        "1",
    ]
    if extra_args:
        cmd.extend(extra_args)

    result = subprocess.run(
        cmd, input=_worker_hash_script(source_dir), capture_output=True, text=True
    )
    output = result.stdout + result.stderr

    worker_hashes = []
    for line in output.splitlines():
        if line.strip().startswith("HASH:"):
            worker_hashes.append(line.strip().split("HASH:")[1])

    if not worker_hashes:
        print(f" FAIL (no worker hashes, rc={result.returncode})")
        if result.returncode != 0:
            for line in result.stderr.strip().splitlines()[-5:]:
                print(f"    {line}")
        return False

    mismatches = [i for i, wh in enumerate(worker_hashes) if wh != client_hash]
    if mismatches:
        print(f" FAIL (mismatch on workers {mismatches})")
        print(f"    client={client_hash}")
        for i in mismatches:
            print(f"    worker[{i}]={worker_hashes[i]}")
        return False

    print(f" OK ({len(worker_hashes)} workers, hash={client_hash[:12]})")
    return True


def _run_scenarios(num_hosts, size_gb, bench_dir, extra_args=None):
    """Run all 5 scenarios for a given payload size, return dict of times."""
    src_dir = os.path.join(bench_dir, "src")
    data_path = os.path.join(bench_dir, "data.bin")
    results = {}

    # 1. Cold start (cache was cleared before this call)
    results["Cold start"] = _run(
        "Cold start", num_hosts, bench_dir, extra_args=extra_args
    )
    _verify_content("  verify cold start", num_hosts, bench_dir, extra_args)

    # 2. No change (cache hit from cold start)
    results["No change"] = _run(
        "No change", num_hosts, bench_dir, extra_args=extra_args
    )

    # 3. Rewrite data.bin (same size -> partial update)
    data_size = os.path.getsize(data_path)
    with open(data_path, "wb") as f:
        remaining = data_size
        while remaining > 0:
            chunk = min(remaining, 64 * 1024 * 1024)
            f.write(os.urandom(chunk))
            remaining -= chunk
    results["Rewrite data.bin"] = _run(
        "Rewrite data.bin", num_hosts, bench_dir, extra_args=extra_args
    )
    _verify_content("  verify rewrite data.bin", num_hosts, bench_dir, extra_args)

    # 4. Rewrite all .py files (partial update)
    # Use same-length prefix ("Modify" vs "Module" = 6 chars each) and
    # same return value so total file size is unchanged → partial not stale.
    for i in range(NUM_PY):
        with open(os.path.join(src_dir, f"mod_{i}.py"), "w") as f:
            f.write(f"# Modify {i}\n" * 50 + f"def func_{i}(): return {i}\n")
    results["Rewrite .py"] = _run(
        "Rewrite .py files", num_hosts, bench_dir, extra_args=extra_args
    )
    _verify_content("  verify rewrite .py", num_hosts, bench_dir, extra_args)

    # 5. Delete file (total size changes -> stale -> full transfer)
    os.remove(os.path.join(src_dir, "mod_0.py"))
    results["Delete file"] = _run(
        "Delete file", num_hosts, bench_dir, extra_args=extra_args
    )
    _verify_content("  verify delete file", num_hosts, bench_dir, extra_args)

    return results


def _print_table(all_results):
    """Print markdown table of results."""
    if not all_results:
        return

    host_counts = sorted({h for h, _ in all_results.keys()})
    sizes = sorted({s for _, s in all_results.keys()})

    for num_hosts in host_counts:
        print(f"\n### {num_hosts} host{'s' if num_hosts > 1 else ''}\n")
        print(
            "| Payload | Cold start | No change | Rewrite data.bin "
            "| Rewrite .py | Delete file |"
        )
        print(
            "|---------|-----------|-----------|-----------------|"
            "-------------|-------------|"
        )
        for size_gb in sizes:
            key = (num_hosts, size_gb)
            if key not in all_results:
                continue
            r = all_results[key]
            cols = [f"{size_gb}GB"]
            for scenario in SCENARIOS:
                t = r.get(scenario, float("nan"))
                cols.append(f"{t:.1f}s")
            print("| " + " | ".join(cols) + " |")
    print()


def _run_host_count(num_hosts, size_list, work_dir, extra_args=None, reuse_job=False):
    """Run all payload sizes for a single host count.

    Uses work_dir for .monarch/job_state.pkl isolation and
    per-host-count temp directories for bench/warmup data.
    Returns dict mapping (num_hosts, size_gb) -> scenario results.
    """
    bench_dir = os.path.join(work_dir, "bench")
    warmup_dir = os.path.join(work_dir, "warmup")
    os.makedirs(work_dir, exist_ok=True)

    # Run from work_dir so .monarch/job_state.pkl is isolated.
    orig_cwd = os.getcwd()
    os.chdir(work_dir)
    os.makedirs(".monarch", exist_ok=True)

    try:
        print(f"\n{'=' * 60}")
        print(f"  {num_hosts} host{'s' if num_hosts > 1 else ''}")
        print(f"{'=' * 60}")

        state_pkl = os.path.join(work_dir, ".monarch", "job_state.pkl")
        if reuse_job and os.path.exists(state_pkl):
            print(f"\n  Reusing existing MAST job (found {state_pkl})")
        else:
            if reuse_job:
                print(f"\n  No cached job found at {state_pkl}, allocating new job...")
            _warmup(num_hosts, warmup_dir)

        host_results = {}
        for size_gb in size_list:
            print(f"\n--- {size_gb}GB payload, {num_hosts} hosts ---")
            _create_test_dir(bench_dir, size_gb)
            _clear_worker_cache(num_hosts, warmup_dir)

            results = _run_scenarios(
                num_hosts, size_gb, bench_dir, extra_args=extra_args
            )
            host_results[(num_hosts, size_gb)] = results

            shutil.rmtree(bench_dir, ignore_errors=True)

        _print_table(host_results)
        if not reuse_job:
            _kill_job()
        else:
            print("  (--reuse_job: keeping MAST job alive for reuse)")
        return host_results
    finally:
        os.chdir(orig_cwd)


def main(
    host_type="gb200",
    sizes="1",
    hosts="2",
    streams=8,
    reuse_job=False,
    work_dir="",
):
    """Run persistent cache benchmark across payload sizes and host counts.

    Args:
        host_type: MAST host type (default: gb200). Options: gb200, gb300, grandteton
        sizes: Comma-separated GB values (e.g., "1,2,4,8,16,32,64,128")
        hosts: Comma-separated host counts (e.g., "1,2,4,8,16")
        streams: Number of parallel TLS streams per host (default: 8)
        reuse_job: If True, keep the MAST job alive after the benchmark
            so subsequent runs can reuse the same workers instantly.
        work_dir: Persistent directory for job state. If empty, a temp dir
            is created. Use with --reuse_job to reuse workers across runs.
    """
    size_list = _parse_list(sizes)
    host_list = _parse_list(hosts)
    streams = int(streams)

    # Set host type for remoterun.sh.
    os.environ["MONARCH_HOST_TYPE"] = host_type

    print("=" * 60)
    print("  Persistent Cache Benchmark")
    print(f"  Sizes: {size_list} GB")
    print(f"  Hosts: {host_list}")
    print(f"  Streams: {streams}")
    print(f"  Host type: {host_type}")
    print("=" * 60)

    base_work_dir = (
        work_dir if work_dir else tempfile.mkdtemp(prefix="bench_incremental_")
    )
    os.makedirs(base_work_dir, exist_ok=True)

    extra_args = ["--num_parallel_streams", str(streams)]

    all_results = {}
    for num_hosts in host_list:
        work_dir = os.path.join(base_work_dir, f"h{num_hosts}")
        try:
            host_results = _run_host_count(
                num_hosts,
                size_list,
                work_dir,
                extra_args=extra_args,
                reuse_job=reuse_job,
            )
            all_results.update(host_results)
        except Exception as e:
            print(f"\n  ERROR: {num_hosts} hosts failed: {e}")

    shutil.rmtree(base_work_dir, ignore_errors=True)

    # Final summary.
    print(f"\n{'=' * 60}")
    print("  Final Results")
    print(f"{'=' * 60}")
    _print_table(all_results)
    print("Done.")


if __name__ == "__main__":
    fire.Fire(main)
