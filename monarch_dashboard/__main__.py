# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import argparse
import os
import shutil
import subprocess

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
        else:
            print(">> No frontend directory found (API-only mode)")

    app = create_app(db_path)
    app.run(host=host, port=port)


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
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"Database not found: {args.db}")
        exit(1)

    print(f"Starting Monarch Dashboard on http://{args.host}:{args.port}")
    start_dashboard(args.db, args.host, args.port, rebuild=args.rebuild)
