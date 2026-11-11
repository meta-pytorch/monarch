# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Standalone dashboard using Arrow fixtures and native DataFusion queries."""

import argparse
import logging
import threading

import pyarrow as pa
from monarch.monarch_dashboard import _PKG
from monarch.monarch_dashboard.fake_data.generate import generate, SCHEMAS
from monarch.monarch_dashboard.fake_data.simulate import _run_simulation
from monarch.monarch_dashboard.fake_data.store import ArrowAdapter
from monarch.monarch_dashboard.server.app import create_app

logger: logging.Logger = logging.getLogger(__name__)


def start_dashboard(
    data: ArrowAdapter, host: str = "0.0.0.0", port: int = 5000
) -> None:
    """Serve the dashboard with a local DataFusion fixture adapter."""
    if not (_PKG / "frontend" / "build" / "index.html").is_file():
        logger.warning("no frontend build found; serving API only")
    create_app(data).run(host=host, port=port, debug=False)


def main() -> None:
    """Run a static demo or live simulator without a telemetry job."""
    parser = argparse.ArgumentParser(
        description="Preview the Monarch dashboard with Arrow fixtures."
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument(
        "--simulate", action="store_true", help="Run the live data simulator"
    )
    parser.add_argument(
        "--interval", type=float, default=1.0, help="Simulator tick interval in seconds"
    )
    parser.add_argument(
        "--failure-at",
        type=float,
        default=270.0,
        help="Seconds until the simulated failure",
    )
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval must be positive")
    if not args.simulate:
        start_dashboard(generate(), args.host, args.port)
        return

    data = ArrowAdapter(
        {
            name: pa.Table.from_pylist([], schema=schema)
            for name, schema in SCHEMAS.items()
        }
    )
    stop = threading.Event()
    simulator = threading.Thread(
        target=_run_simulation,
        args=(data, args.interval, args.failure_at, stop),
        daemon=True,
    )
    simulator.start()
    try:
        start_dashboard(data, args.host, args.port)
    finally:
        stop.set()
        simulator.join()


if __name__ == "__main__":
    main()
