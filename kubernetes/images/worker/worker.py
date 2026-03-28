# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import logging
import os
import socket

from monarch.actor import run_worker_loop_forever

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("monarch-worker")


def main():
    # 1. Get configuration from Environment Variables (set by K8s)
    port = os.environ.get("MONARCH_PORT", "26600")

    # 2. Get the hostname (In K8s StatefullSet, this is "pod-name-0")
    hostname = socket.gethostname()

    # 3. Construct the bind address
    # We bind to the hostname so it matches the DNS record the driver uses
    address = f"tcp://{hostname}:{port}"

    logger.info(f"--- Starting Monarch Worker ---")
    logger.info(f"Identity: {hostname}")
    logger.info(f"Listening on: {address}")

    # 4. Run the loop
    run_worker_loop_forever(address=address, ca="trust_all_connections")


if __name__ == "__main__":
    main()
