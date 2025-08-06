# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


def extract_ips_simple(file_path):
    """
    Simple extraction assuming each line contains an IP address.
    """
    ip_set = set()

    try:
        with open(file_path, "r") as file:
            for line in file:
                ip = line.strip()
                if ip:  # Skip empty lines
                    ip_set.add(ip)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found")
    except Exception as e:
        print(f"Error reading file: {e}")

    return ip_set


# Usage
file_path = "/tmp/worker_nodes.txt"
ip_addresses = extract_ips_simple(file_path)

print("Extracted IP addresses:")
for ip in sorted(ip_addresses):
    print(ip)

print(f"\nIP set: {ip_addresses}")
