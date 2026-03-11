"""
Runner that detects hangs in repro_timeout.py by monitoring output timing.
Kills the subprocess if no output is received for > 5x the warmup average.

Usage:
    source ~/oss.sh && python run_detect_hang.py
"""

import select
import subprocess
import sys
import time

script = sys.argv[1] if len(sys.argv) > 1 else "repro_timeout.py"

proc = subprocess.Popen(
    ["python", script],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
)

times = []
threshold = None
last = time.monotonic()

while True:
    ready = select.select([proc.stdout], [], [], 0.2)[0]
    if ready:
        line = proc.stdout.readline()
        if not line:
            break
        now = time.monotonic()
        print(line, end="", flush=True)
        if line.startswith("iter "):
            elapsed = now - last
            last = now
            times.append(elapsed)
            print(f"  ({elapsed:.3f}s)", flush=True)
            # skip iter 0 (cold start); use iters 1-3 for warmup
            if len(times) == 4:
                avg = sum(times[1:]) / len(times[1:])
                threshold = max(5 * avg, avg + 1.0)
                print(f"[warmup avg={avg:.3f}s, threshold={threshold:.2f}s]", flush=True)
        else:
            last = now  # reset on any non-iter output
    else:
        if threshold is not None:
            gap = time.monotonic() - last
            if gap > threshold:
                print(f"\nHUNG: no output for {gap:.2f}s (threshold {threshold:.2f}s)", flush=True)
                # collect all pids: the main proc + any monarch worker processes
                pids = [proc.pid]
                try:
                    children = subprocess.check_output(
                        ["pgrep", "-P", str(proc.pid)], text=True
                    ).strip().split()
                    pids += [int(p) for p in children]
                except Exception:
                    pass
                try:
                    monarch_procs = subprocess.check_output(
                        ["pgrep", "-f", "monarch"], text=True
                    ).strip().split()
                    pids += [int(p) for p in monarch_procs]
                except Exception:
                    pass
                pids = list(dict.fromkeys(pids))  # dedup preserve order
                for pid in pids:
                    print(f"\n{'='*60}\npy-spy dump PID {pid}:\n{'='*60}", flush=True)
                    try:
                        out = subprocess.check_output(
                            ["py-spy", "dump", "--pid", str(pid)],
                            stderr=subprocess.STDOUT, timeout=15
                        ).decode()
                        print(out, flush=True)
                    except Exception as e:
                        print(f"  (failed: {e})", flush=True)

                # gdb native thread dump on bootstrap_main processes (the worker proc)
                try:
                    bootstrap_pids = subprocess.check_output(
                        ["pgrep", "-f", "bootstrap_main"], text=True
                    ).strip().split()
                    # find the one that is a descendant of our proc
                    for bpid in bootstrap_pids:
                        ppid = subprocess.check_output(
                            ["ps", "-o", "ppid=", "-p", bpid], text=True
                        ).strip()
                        if ppid in [str(p) for p in pids]:
                            print(f"\n{'='*60}\ngdb native threads for bootstrap_main PID {bpid}:\n{'='*60}", flush=True)
                            out = subprocess.check_output(
                                ["gdb", "-batch", "-ex", "thread apply all bt", "-p", bpid],
                                stderr=subprocess.STDOUT, timeout=30
                            ).decode()
                            # filter out blas threads, show everything else
                            filtered = []
                            current_thread = []
                            for line in out.splitlines():
                                if line.startswith("Thread "):
                                    if current_thread and "blas_thread_server" not in "\n".join(current_thread):
                                        filtered.extend(current_thread)
                                    current_thread = [line]
                                else:
                                    current_thread.append(line)
                            if current_thread and "blas_thread_server" not in "\n".join(current_thread):
                                filtered.extend(current_thread)
                            print("\n".join(filtered), flush=True)
                            break
                except Exception as e:
                    print(f"gdb failed: {e}", flush=True)

                proc.kill()
                break

proc.wait()
print(f"EXIT: {proc.returncode}")
