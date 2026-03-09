# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import argparse
import atexit
import os
import shutil
import signal
import subprocess
import sys

from monarch.monarch_dashboard.server.app import create_app

_DEFAULT_DB = os.environ.get(
    "MONARCH_DB_PATH",
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "fake_data", "fake_data.db"
    ),
)


def start_dashboard(db_path, host="0.0.0.0", port=5000, rebuild=False):
    pkg_dir = os.path.dirname(os.path.abspath(__file__))
    # Force rebuild if requested
    build_dir = os.path.join(pkg_dir, "frontend", "build")
    if rebuild and os.path.isdir(build_dir):
        print(">> --rebuild: removing frontend/build/ ...")
        shutil.rmtree(build_dir)

    # Auto-build frontend if needed
    build_index = os.path.join(build_dir, "index.html")
    if not os.path.isfile(build_index):
        frontend_dir = os.path.join(pkg_dir, "frontend")
        if os.path.isdir(frontend_dir):
            try:
                print(">> Building frontend...")
                node_modules = os.path.join(frontend_dir, "node_modules")
                if not os.path.isdir(node_modules):
                    subprocess.run(
                        ["/usr/bin/npm", "install"], cwd=frontend_dir, check=True
                    )
                subprocess.run(
                    ["npx", "react-scripts", "build"], cwd=frontend_dir, check=True
                )
                print(">> Frontend built!")
            except (subprocess.CalledProcessError, FileNotFoundError) as e:
                print(f">> Frontend build failed: {e}")
                print(
                    ">> Continuing in API-only mode (use pre-built frontend or buck2 run)"
                )
        else:
            print(">> No frontend directory found (API-only mode)")

    app = create_app(db_path)
    app.run(host=host, port=port)


def _launch_simulator(db_path, interval, failure_at, host_failure=False):
    """Launch fake_data/simulate.py as a background subprocess."""
    pkg_dir = os.path.dirname(os.path.abspath(__file__))
    sim_script = os.path.join(pkg_dir, "fake_data", "simulate.py")
    cmd = [
        sys.executable,
        sim_script,
        "--db",
        db_path,
        "--interval",
        str(interval),
        "--failure-at",
        str(failure_at),
    ]
    if host_failure:
        cmd.append("--host-failure")
    print(f">> Launching simulator (failure at {failure_at}s) ...")
    proc = subprocess.Popen(cmd)

    def _cleanup():
        if proc.poll() is None:
            print(">> Stopping simulator ...")
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    atexit.register(_cleanup)
    return proc


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monarch Dashboard")
    parser.add_argument("--db", default=_DEFAULT_DB)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Force frontend rebuild (rm -rf frontend/build, npm install, build)",
    )
    parser.add_argument(
        "--simulate",
        action="store_true",
        help="Run the live data simulator alongside the server",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Simulator tick interval in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--failure-at",
        type=float,
        default=270.0,
        help="Seconds until simulator triggers a failure (default: 270)",
    )
    parser.add_argument(
        "--host-failure",
        action="store_true",
        help="Cascade failure to entire host mesh (downward propagation)",
    )
    args = parser.parse_args()

    sim_proc = None
    if args.simulate:
        sim_proc = _launch_simulator(
            args.db, args.interval, args.failure_at, args.host_failure
        )
    elif not os.path.exists(args.db):
        print(f"Database not found: {args.db}")
        exit(1)

    print(f"Starting Monarch Dashboard on http://{args.host}:{args.port}")
    start_dashboard(args.db, args.host, args.port, rebuild=args.rebuild)
