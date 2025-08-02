# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import argparse
import json
import random
import socket
import sys
import time

import requests


def get_local_ip():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        local_ip = sock.getsockname()[0]
    except Exception:
        local_ip = "127.0.0.1"
    finally:
        sock.close()
    return local_ip


def register_with_master(master_ip):
    worker_ip = get_local_ip()
    hostname = socket.gethostname()

    registration_data = {
        "ip": worker_ip,
        "hostname": hostname,
        "timestamp": time.time(),
    }

    print(
        f"Worker {hostname} trying to register IP {worker_ip} with master {master_ip}"
    )

    # Retry indefinitely with exponential backoff
    attempt = 0
    base_delay = 5
    max_delay = 300  # 5 minutes max delay

    while True:
        try:
            response = requests.post(
                f"http://{master_ip}:8080/register",
                data=json.dumps(registration_data),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )

            if response.status_code == 200:
                print(f"Successfully registered worker IP {worker_ip} with master")
                return True

        except Exception as e:
            attempt += 1
            # Exponential backoff with jitter
            delay = min(base_delay * (2 ** min(attempt, 6)), max_delay)
            jitter = random.uniform(0.5, 1.5)
            actual_delay = delay * jitter

            print(f"Attempt {attempt} failed: {e}")
            print(f"Retrying in {actual_delay:.1f} seconds...")
            time.sleep(actual_delay)


def main():
    parser = argparse.ArgumentParser(description="Worker node registration")
    parser.add_argument("master_ip", help="IP address of the master node")

    args = parser.parse_args()

    print(f"Starting worker registration with master: {args.master_ip}")

    # Add a small random delay to avoid all workers hitting at once
    time.sleep(random.uniform(1, 10))

    success = register_with_master(args.master_ip)
    if success:
        print("Registration completed successfully")
        return 0
    else:
        print("Registration failed")
        return 1


if __name__ == "__main__":
    exit(main())
