# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer


class MasterNodeServer:
    def __init__(self, expected_workers=2, max_wait_hours=2, port=8080):
        self.worker_nodes = []
        self.expected_workers = expected_workers
        self.max_wait_hours = max_wait_hours
        self.server_running = True
        self.httpd = None
        self.port = port
        self.master_ip = self.get_master_public_ip_curl()

    @staticmethod
    def get_master_ip():
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname)

    @staticmethod
    def get_master_public_ip():
        """Get the public IP address of the master node by querying an external service"""
        try:
            # Try multiple services in case one is down
            services = [
                "https://api.ipify.org",
                "https://checkip.amazonaws.com",
                "https://ipecho.net/plain",
            ]

            for service in services:
                try:
                    with urllib.request.urlopen(service, timeout=10) as response:
                        public_ip = response.read().decode("utf-8").strip()
                        # Basic validation that we got an IP address
                        if "." in public_ip and len(public_ip.split(".")) == 4:
                            return public_ip
                except Exception:
                    continue

            # If all services fail, return None
            return None

        except Exception as e:
            print(f"Error getting public IP: {e}")
            return None

    @staticmethod
    def get_master_public_ip_curl():
        """Get the public IP address using curl command (simpler approach)"""
        try:
            result = subprocess.run(
                ["curl", "-4", "ifconfig.me"],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode == 0:
                public_ip = result.stdout.strip()
                # Basic validation that we got an IP address
                if "." in public_ip and len(public_ip.split(".")) == 4:
                    return public_ip

            return None

        except Exception as e:
            print(f"Error getting public IP with curl: {e}")
            return None

    def create_handler_class(self):
        """Create handler class with access to server instance"""
        server_instance = self

        class NodeRegistrationHandler(BaseHTTPRequestHandler):
            def do_POST(self):
                if self.path == "/register":
                    try:
                        content_length = int(self.headers["Content-Length"])
                        post_data = self.rfile.read(content_length)
                        node_info = json.loads(post_data.decode("utf-8"))

                        # Store worker node IP (avoid duplicates)
                        if node_info["ip"] not in server_instance.worker_nodes:
                            server_instance.worker_nodes.append(node_info["ip"])
                            print(
                                f"Registered worker node: {node_info['ip']} ({len(server_instance.worker_nodes)}/{server_instance.expected_workers})"
                            )

                            # Save to file immediately when each node registers
                            server_instance.save_worker_nodes_to_file()

                        self.send_response(200)
                        self.end_headers()
                        self.wfile.write(b"OK")

                        # Check if we have all workers
                        if (
                            len(server_instance.worker_nodes)
                            >= server_instance.expected_workers
                        ):
                            print("All worker nodes registered!")
                            # Signal the server to stop
                            threading.Thread(
                                target=server_instance.stop_server_delayed
                            ).start()

                    except Exception as e:
                        print(f"Error processing registration: {e}")
                        self.send_response(500)
                        self.end_headers()

            def do_GET(self):
                if self.path == "/status":
                    status = {
                        "registered_workers": len(server_instance.worker_nodes),
                        "expected_workers": server_instance.expected_workers,
                        "worker_ips": server_instance.worker_nodes,
                    }
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(status).encode("utf-8"))

        return NodeRegistrationHandler

    def save_worker_nodes_to_file(self):
        """Save worker node IPs to files"""
        # Save IPs
        with open("/tmp/worker_nodes.txt", "w") as f:
            for ip in self.worker_nodes:
                f.write(f"{ip}\n")

        # Save count
        with open("/tmp/worker_count.txt", "w") as f:
            f.write(f"{len(self.worker_nodes)}\n")

    def stop_server_delayed(self):
        """Stop server after a short delay"""
        time.sleep(2)  # Give time for the response to be sent
        self.server_running = False
        if self.httpd:
            self.httpd.shutdown()

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""

        def signal_handler(sig, frame):
            print("\nShutting down server...")
            self.server_running = False
            if self.httpd:
                self.httpd.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def start_registration_server(self):
        """Start the HTTP registration server with safe port handling"""
        print(f"Starting server on port {self.port}...")
        handler_class = self.create_handler_class()

        try:
            # Create server with socket reuse option
            self.httpd = HTTPServer(("0.0.0.0", self.port), handler_class)

            # Enable socket reuse to handle TIME_WAIT states
            self.httpd.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            print(f"Server started on {self.master_ip}:{self.port}")

            # Handle requests until all workers register
            while (
                self.server_running and len(self.worker_nodes) < self.expected_workers
            ):
                try:
                    self.httpd.timeout = 10
                    self.httpd.handle_request()
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    print(f"Server error: {e}")
                    time.sleep(1)

            print("Registration server stopped")

        except OSError as e:
            if e.errno == 98:  # Address already in use
                print(f"Port {self.port} is busy!")
                print(f"Solutions:")
                print(f"   1. Wait a few minutes and try again")
                print(f"   2. Restart your notebook kernel")
                print(f"   3. Check: lsof -i :{self.port} (to see what's using it)")
                raise RuntimeError(
                    "Port {self.port} is not available. Please try again in a few minutes."
                )
            else:
                print(f"Server error: {e}")
                raise

        finally:
            # Clean shutdown
            if self.httpd:
                try:
                    self.httpd.socket.close()
                except:
                    pass

    def wait_for_workers_with_status(self):
        """Wait for workers and show periodic status updates"""
        start_time = time.time()

        while len(self.worker_nodes) < self.expected_workers:
            elapsed = int(time.time() - start_time)
            print(
                f"Waiting for workers... ({len(self.worker_nodes)}/{self.expected_workers} registered) - Elapsed: {elapsed}s"
            )
            time.sleep(30)  # Status update every 30 seconds

            # Optional: Add a maximum wait time if needed
            if self.max_wait_hours > 0 and elapsed > self.max_wait_hours * 3600:
                print(f"Timeout waiting for workers after {self.max_wait_hours} hours")
                break

    def save_cluster_info(self):
        """Save complete cluster information to files"""
        # Save master IP
        with open("/tmp/master_ip.txt", "w") as f:
            f.write(self.master_ip)

        # Save worker IPs
        self.save_worker_nodes_to_file()

        # Save complete cluster info
        cluster_info = {
            "master_ip": self.master_ip,
            "worker_ips": self.worker_nodes,
            "total_workers": len(self.worker_nodes),
            "expected_workers": self.expected_workers,
            "registration_complete": len(self.worker_nodes) >= self.expected_workers,
        }

        with open("/tmp/cluster_info.json", "w") as f:
            json.dump(cluster_info, f, indent=2)

        return cluster_info

    def run(self):
        """Main method to run the master server"""
        self.setup_signal_handlers()

        print(f"Master node IP: {self.master_ip}")
        print(f"Expecting {self.expected_workers} worker nodes to register...")

        # Start registration server in background
        server_thread = threading.Thread(target=self.start_registration_server)
        server_thread.daemon = True
        server_thread.start()

        # Wait for worker nodes with status updates
        status_thread = threading.Thread(target=self.wait_for_workers_with_status)
        status_thread.daemon = True
        status_thread.start()

        # Keep main thread alive until all workers register
        server_thread.join()

        print(f"Final registered worker nodes: {self.worker_nodes}")

        # Save all cluster information
        cluster_info = self.save_cluster_info()

        print("Worker IPs saved to /tmp/worker_nodes.txt")
        print("Cluster info saved to /tmp/cluster_info.json")

        return cluster_info


def run_master_server(expected_workers=2, max_wait_hours=0, port=8080):
    """
    Notebook-friendly function to run the master server.

    Args:
        expected_workers: Number of worker nodes to expect
        max_wait_hours: Maximum hours to wait (0 for no limit)

    Returns:
        dict: Cluster information when complete
    """
    server = MasterNodeServer(
        expected_workers=expected_workers, max_wait_hours=max_wait_hours, port=port
    )
    return server.run()


def main():
    """Command line interface"""
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Master node registration server")
    parser.add_argument(
        "--expected-workers",
        type=int,
        default=2,
        help="Number of worker nodes to expect (default: 2)",
    )
    parser.add_argument(
        "--max-wait-hours",
        type=int,
        default=0,
        help="Maximum hours to wait for workers (default: 0 for no limit)",
    )

    args = parser.parse_args()

    # Create and run server
    server = MasterNodeServer(
        expected_workers=args.expected_workers, max_wait_hours=args.max_wait_hours
    )

    cluster_info = server.run()
    return cluster_info


if __name__ == "__main__":
    main()
