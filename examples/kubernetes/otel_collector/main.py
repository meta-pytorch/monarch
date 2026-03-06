#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
OTEL Collector integration example for Monarch on Kubernetes.

Demonstrates exporting Monarch's built-in actor metrics (mailbox posts,
messages sent/received, queue sizes, etc.) to an OpenTelemetry Collector
running in the same Kubernetes cluster.

The OTEL_EXPORTER_OTLP_ENDPOINT env var is set on both the controller
and worker pods, enabling the OTLP/HTTP metric exporter in Monarch's
telemetry layer. Metrics are pushed to the collector, which exports
them via its debug exporter (stdout) and a Prometheus scrape endpoint.
"""

import argparse
import os
import socket
import textwrap
import time

from kubernetes import client
from monarch.actor import Actor, endpoint
from monarch.job.kubernetes import KubernetesJob


_OTEL_ENDPOINT = "http://otel-collector.monarch-tests.svc.cluster.local:4318"

_WORKER_BOOTSTRAP_SCRIPT: str = textwrap.dedent("""\
    import os
    import socket
    from monarch.actor import run_worker_loop_forever
    port = os.environ.get("MONARCH_PORT", "26600")
    hostname = socket.getfqdn()
    address = f"tcp://{hostname}:{port}"
    run_worker_loop_forever(address=address, ca="trust_all_connections")
""")


class WorkActor(Actor):
    """Actor that performs work to generate telemetry."""

    @endpoint
    def do_work(self, iterations: int) -> dict:
        """Run a loop to generate actor message metrics."""
        total = 0
        for i in range(iterations):
            total += i * i
        return {
            "hostname": socket.gethostname(),
            "iterations": iterations,
            "result": total,
        }

    @endpoint
    def ping(self) -> str:
        return f"pong from {socket.gethostname()}"


def build_worker_pod_spec(port: int) -> client.V1PodSpec:
    """Build a V1PodSpec with OTEL_EXPORTER_OTLP_ENDPOINT configured."""
    return client.V1PodSpec(
        containers=[
            client.V1Container(
                name="worker",
                image="ghcr.io/meta-pytorch/monarch:latest",
                command=["python", "-u", "-c", _WORKER_BOOTSTRAP_SCRIPT],
                env=[
                    client.V1EnvVar(name="MONARCH_PORT", value=str(port)),
                    client.V1EnvVar(
                        name="OTEL_EXPORTER_OTLP_ENDPOINT",
                        value=_OTEL_ENDPOINT,
                    ),
                ],
            )
        ]
    )


def main():
    parser = argparse.ArgumentParser(
        description="Monarch OTEL Collector Kubernetes example"
    )
    parser.add_argument(
        "--num-replicas",
        type=int,
        default=2,
        help="Number of worker replicas (default: 2)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of work rounds to generate metrics (default: 5)",
    )
    args = parser.parse_args()

    otel_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    print(f"Controller OTEL_EXPORTER_OTLP_ENDPOINT: {otel_endpoint}")

    print("Connecting to Kubernetes...")
    job = KubernetesJob(namespace="monarch-tests")
    port = 26600
    job.add_mesh(
        "workers",
        num_replicas=args.num_replicas,
        pod_spec=build_worker_pod_spec(port),
        port=port,
    )

    print(f"Waiting for {args.num_replicas} worker pod(s)...")
    state = job.state()
    host_mesh = state.workers
    procs = host_mesh.spawn_procs()

    print("Spawning actors...")
    actors = procs.spawn("work_actor", WorkActor)

    # Run several rounds of work to generate a stream of metrics.
    for i in range(args.iterations):
        results = actors.ping.call().get()
        print(f"  Round {i + 1}: {list(results)}")
        work_results = actors.do_work.call(1000).get()
        for _, result in work_results.items():
            print(
                f"    {result['hostname']}: computed {result['iterations']} iterations"
            )

    # Wait for the periodic metric reader to flush at least once.
    print("Waiting for metrics to flush to collector...")
    time.sleep(3)

    print()
    print("Verify metrics in the OTEL collector logs:")
    print("  kubectl logs -n monarch-tests deployment/otel-collector --tail=100")
    print()
    print("Scrape Prometheus endpoint:")
    print("  kubectl port-forward -n monarch-tests svc/otel-collector 8889:8889")
    print("  curl http://localhost:8889/metrics")

    procs.stop().get()
    job.kill()
    print("Done.")


if __name__ == "__main__":
    main()
