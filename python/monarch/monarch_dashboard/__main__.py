# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import argparse
import atexit
import os
import signal
import subprocess
import sys

from monarch.monarch_dashboard import _PKG
from monarch.monarch_dashboard.server.app import create_app
from monarch.monarch_dashboard.server.db import SQLiteAdapter

_DEFAULT_DB = os.environ.get(
    "MONARCH_DB_PATH",
    str(_PKG / "fake_data" / "fake_data.db"),
)


def start_dashboard(db_path, host="0.0.0.0", port=5000):
    build_index = _PKG / "frontend" / "build" / "index.html"
    if build_index.is_file():
        print(">> Serving pre-built frontend assets")
    else:
        print(
            ">> WARNING: No frontend build found. Serving API-only.\n"
            ">> To build the frontend, run: python setup.py build_frontend or run uv build --wheel --no-build-isolation"
        )

    app = create_app(SQLiteAdapter(db_path))
    app.run(host=host, port=port, debug=False)


def _launch_simulator(db_path, interval, failure_at):
    """Launch fake_data/simulate.py as a background subprocess."""
    sim_ref = _PKG / "fake_data" / "simulate.py"
    sim_path = str(sim_ref)
    if not os.path.isfile(sim_path):
        print(f">> ERROR: simulate.py not found at {sim_path}")
        sys.exit(1)
    cmd = [
        sys.executable,
        sim_path,
        "--db",
        db_path,
        "--interval",
        str(interval),
        "--failure-at",
        str(failure_at),
    ]
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


def main():
    """Entry point for the monarch-dashboard CLI command."""
    parser = argparse.ArgumentParser(
        description="CLI Entrypoint for the Monarch Dashboard. You should not need to use this unless bringing up the dashboard manually."
    )
    parser.add_argument("--db", default=_DEFAULT_DB)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
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
    args = parser.parse_args()

    sim_proc = None
    if args.simulate:
        sim_proc = _launch_simulator(args.db, args.interval, args.failure_at)
    elif not os.path.exists(args.db):
        print(f"Database not found: {args.db}")
        exit(1)

    print(f"Starting Monarch Dashboard on http://{args.host}:{args.port}")
    start_dashboard(args.db, args.host, args.port)


if __name__ == "__main__":
    main()
