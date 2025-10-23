import argparse
import json
import random
import socket
import subprocess
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


def get_public_ip():
    """Get the public IP address of this worker node using external service."""
    try:
        # Using ipify.org service to get public IP
        response = requests.get("https://api.ipify.org", timeout=10)
        if response.status_code == 200:
            return response.text.strip()
    except Exception as e:
        print(f"Failed to get public IP from ipify.org: {e}")

    # Fallback to alternative service
    try:
        response = requests.get("https://icanhazip.com", timeout=10)
        if response.status_code == 200:
            return response.text.strip()
    except Exception as e:
        print(f"Failed to get public IP from icanhazip.com: {e}")

    # If all external services fail, return None
    print("Unable to determine public IP address")
    return None


def get_public_ip_with_curl():
    """Get the public IP address using curl command line tool."""
    try:
        result = subprocess.run(
            ["curl", "-4", "ifconfig.me"], capture_output=True, text=True, timeout=15
        )

        if result.returncode == 0 and result.stdout.strip():
            ip = result.stdout.strip()
            # Basic validation - check if it looks like an IP address
            parts = ip.split(".")
            if len(parts) == 4 and all(
                part.isdigit() and 0 <= int(part) <= 255 for part in parts
            ):
                return ip

    except subprocess.TimeoutExpired:
        print("Curl request to ifconfig.me timed out")
    except Exception as e:
        print(f"Failed to get public IP using curl: {e}")

    # Try ifconfig as a fallback to get network interface info
    try:
        result = subprocess.run(
            ["ifconfig"], capture_output=True, text=True, timeout=10
        )

        if result.returncode == 0:
            # This won't give us the public IP directly, but can help debug network issues
            print("Network interfaces available (for debugging):")
            lines = result.stdout.split("\n")
            for line in lines[:10]:  # Show first 10 lines
                if line.strip():
                    print(f"  {line}")

    except Exception as e:
        print(f"Failed to run ifconfig: {e}")

    print("Unable to determine public IP address using curl")
    return None


def register_with_master(master_ip, master_port=8080):
    # worker_ip = get_local_ip()
    worker_ip = get_public_ip_with_curl()
    hostname = socket.gethostname()

    registration_data = {
        "ip": worker_ip,
        "hostname": hostname,
        "timestamp": time.time(),
    }

    print(
        f"Worker {hostname} trying to register IP {worker_ip} with master {master_ip} and {master_port}"
    )

    # Retry indefinitely with exponential backoff
    attempt = 0
    base_delay = 5
    max_delay = 300  # 5 minutes max delay

    while True:
        try:
            response = requests.post(
                f"http://{master_ip}:{master_port}/register",
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
    parser.add_argument(
        "master_port",
        help="PORT address of the connection between the worker and master node",
    )

    args = parser.parse_args()

    print(
        f"Starting worker registration with master: {args.master_ip} and PORT {args.master_port}"
    )

    # Add a small random delay to avoid all workers hitting at once
    time.sleep(random.uniform(1, 10))

    success = register_with_master(args.master_ip, args.master_port)
    if success:
        print("Registration completed successfully")
        return 0
    else:
        print("Registration failed")
        return 1


if __name__ == "__main__":
    exit(main())
