# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Exporting Metrics and Logs with OpenTelemetry
==============================================

This example shows how to export Monarch's built-in metrics and logs to an
`OpenTelemetry Collector <https://opentelemetry.io/docs/collector/>`_ and
visualize them with `Grafana <https://grafana.com/grafana/>`_. We deploy the
full observability stack on Kubernetes using the
`Monarch CRD and operator <https://github.com/meta-pytorch/monarch-kubernetes/>`_.

When ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set, Monarch's telemetry layer exports
metrics and logs via OTLP/HTTP to the specified collector. Metrics are exported
automatically. Log export additionally requires ``USE_UNIFIED_LAYER=true``,
which enables the unified tracing layer that wires up the OTLP log sink.
Built-in actor system metrics (mailbox posts, messages sent/received, queue
sizes, etc.) and log events are exported with no code changes.

Architecture
------------

::

    ┌──────────────┐
    │  Controller  │──OTLP/HTTP──┐
    │  (main.py)   │             │
    └──────────────┘             ▼
                            ┌────────────────┐     ┌────────────┐
                            │ OTEL Collector │────▶│  stdout    │
                            │  (port 4318)   │     │  (debug)   │
                            └────────────────┘     └────────────┘
    ┌──────────────┐             ▲           │     ┌────────────┐     ┌────────────┐
    │  Worker pods │──OTLP/HTTP──┘           ├────▶│ Prometheus │────▶│ Prometheus │
    │  (mesh)      │                         │     │  (scrape   │     │  (server   │
    └──────────────┘                         │     │  port 8889)│     │  port 9090)│
                                             │     └────────────┘     └──────┬─────┘
                                             │     ┌────────────┐            │
                                             └────▶│   Loki     │            │
                                                   │ (port 3100)│            │
                                                   └──────┬─────┘            │
                                                          │                  │
                                             ┌────────────┴──────────────────┘
                                             │       Grafana                 │
                                             │     (port 3000)               │
                                             │  Prometheus + Loki            │
                                             └───────────────────────────────┘

Both the controller and worker pods set ``OTEL_EXPORTER_OTLP_ENDPOINT``
pointing at the collector service. The collector receives OTLP metrics and
logs and fans them out to:

- **debug exporter** -- logs metrics and log records to stdout (verify with
  ``kubectl logs``)
- **prometheus exporter** -- exposes a ``/metrics`` endpoint on port 8889
- **loki exporter** -- forwards log records to
  `Grafana Loki <https://grafana.com/oss/loki/>`_ for aggregation and querying

`Grafana <https://grafana.com/grafana/>`_ connects to both Prometheus and Loki
as datasources, providing a unified UI for exploring metrics and logs.

Prerequisites
-------------

- A Kubernetes cluster with the
  `Monarch CRD and operator <https://github.com/meta-pytorch/monarch-kubernetes/>`_
  installed
- ``kubectl`` configured to access the cluster

Deploy the Observability Stack
------------------------------

The Kubernetes manifests live alongside this script in the ``otel_collector/``
directory. Deploy them in order::

    # Create the namespace
    kubectl create namespace monarch-tests

    # Deploy Loki (log aggregation backend)
    kubectl apply -f otel_collector/loki.yaml
    kubectl rollout status deployment/loki -n monarch-tests

    # Deploy the OTEL collector
    kubectl apply -f otel_collector/otel-collector.yaml
    kubectl rollout status deployment/otel-collector -n monarch-tests

    # Deploy Prometheus (scrapes metrics from the collector)
    kubectl apply -f otel_collector/prometheus.yaml
    kubectl rollout status deployment/prometheus -n monarch-tests

    # Deploy Grafana (visualization)
    kubectl apply -f otel_collector/grafana.yaml
    kubectl rollout status deployment/grafana -n monarch-tests

    # Deploy the controller pod (includes RBAC)
    kubectl apply -f otel_collector/controller.yaml
    kubectl wait --for=condition=Ready pod/otel-controller -n monarch-tests --timeout=120s

Run the Example
---------------

Copy this script to the controller pod and run it::

    kubectl cp otel_collector.py monarch-tests/otel-controller:/tmp/main.py
    kubectl exec -it otel-controller -n monarch-tests -- \\
        python /tmp/main.py --num-replicas=2 --iterations=100

The script provisions a ``MonarchMesh`` with ``OTEL_EXPORTER_OTLP_ENDPOINT``
set on worker pods, spawns actors, runs several rounds of work, then cleans up.

Visualize in Grafana
--------------------

Port-forward and open the Grafana UI::

    kubectl port-forward -n monarch-tests svc/grafana 3000:3000

Open http://localhost:3000 (no login required).

**Explore Metrics**

1. Go to **Explore** (compass icon in the left sidebar).
2. Select **Prometheus** datasource from the dropdown.
3. Try these queries:

   - ``mailbox_posts_total`` -- total mailbox posts across all actors
   - ``actor_messages_sent_total`` -- messages sent by the actor system
   - ``actor_messages_received_total`` -- messages received
   - ``rate(mailbox_posts_total[1m])`` -- mailbox post rate per second

**Explore Logs**

1. Go to **Explore** (compass icon in the left sidebar).
2. Select **Loki** datasource from the dropdown.
3. Try these queries:

   - ``{service_name="monarch-worker"}`` -- all worker logs
   - ``{service_name="monarch-controller"}`` -- all controller logs
   - ``{service_name=~".+"}`` -- all logs from any service
   - ``{service_name="monarch-worker"} |= "error"`` -- filter for error messages

Verify Metrics and Logs
-----------------------

Check the collector's debug output to confirm data is being received::

    kubectl logs -n monarch-tests deployment/otel-collector --tail=100

You should see metric data points logged with names like ``mailbox.posts``,
``actor.messages_sent``, ``actor.messages_received``, etc., as well as log
records from the actor system.

To view metrics via Prometheus::

    # Port-forward the Prometheus endpoint
    kubectl port-forward -n monarch-tests svc/otel-collector 8889:8889

    # Scrape metrics
    curl -s http://localhost:8889/metrics | head -50

Cleanup
-------

::

    kubectl delete -f otel_collector/controller.yaml
    kubectl delete -f otel_collector/grafana.yaml
    kubectl delete -f otel_collector/prometheus.yaml
    kubectl delete -f otel_collector/otel-collector.yaml
    kubectl delete -f otel_collector/loki.yaml
    kubectl delete namespace monarch-tests

Configuration
-------------

.. list-table::
   :header-rows: 1

   * - Environment Variable
     - Default
     - Description
   * - ``OTEL_EXPORTER_OTLP_ENDPOINT``
     - (unset)
     - Collector endpoint. When set, enables OTLP metric and log export.
   * - ``OTEL_SERVICE_NAME``
     - ``unknown_service``
     - Service name attached to all exported telemetry.
   * - ``OTEL_METRIC_EXPORT_INTERVAL``
     - ``1s``
     - How often the periodic metric reader pushes to the exporter.
   * - ``USE_UNIFIED_LAYER``
     - ``false``
     - Must be ``true`` to enable the unified tracing layer and OTLP log sink.
   * - ``ENABLE_OTEL_METRICS``
     - ``true``
     - Set to ``false`` to disable OTel metrics entirely.
   * - ``OTEL_EXPORTER_OTLP_HEADERS``
     - (unset)
     - Additional headers for the OTLP exporter (e.g., auth tokens).
   * - ``OTEL_EXPORTER_OTLP_TIMEOUT``
     - (unset)
     - Timeout for OTLP export requests.
"""

# %%
# Controller Script
# -----------------
# The controller script below spawns actors on Kubernetes worker pods and
# exercises them to generate metrics and log events. Both the controller
# and worker pods have ``OTEL_EXPORTER_OTLP_ENDPOINT`` set, so all telemetry
# is automatically forwarded to the collector.

import argparse
import logging
import os
import socket
import time

from kubernetes.client import V1Container, V1EnvVar, V1PodSpec
from monarch._src.job.kubernetes import _WORKER_BOOTSTRAP_SCRIPT
from monarch.actor import Actor, endpoint
from monarch.job.kubernetes import KubernetesJob

logger: logging.Logger = logging.getLogger(__name__)

_OTEL_ENDPOINT = "http://otel-collector.monarch-tests.svc.cluster.local:4318"


# %%
# Define an actor that performs work to generate telemetry.


class WorkActor(Actor):
    """Actor that performs work to generate telemetry."""

    @endpoint
    def do_work(self, iterations: int) -> dict:
        """Run a loop to generate actor message metrics."""
        logger.info("starting work with %d iterations", iterations)
        total = 0
        for i in range(iterations):
            total += i * i
        logger.info("completed work: result=%d", total)
        return {
            "hostname": socket.gethostname(),
            "iterations": iterations,
            "result": total,
        }

    @endpoint
    def ping(self) -> str:
        logger.info("received ping")
        return f"pong from {socket.gethostname()}"


# %%
# Build the worker pod spec with OTEL environment variables.


def build_worker_pod_spec(port: int) -> V1PodSpec:
    """Build a V1PodSpec with OTEL_EXPORTER_OTLP_ENDPOINT configured."""
    return V1PodSpec(
        containers=[
            V1Container(
                name="worker",
                image="ghcr.io/meta-pytorch/monarch:latest",
                image_pull_policy="Always",
                command=["python", "-u", "-c", _WORKER_BOOTSTRAP_SCRIPT],
                env=[
                    V1EnvVar(name="MONARCH_PORT", value=str(port)),
                    V1EnvVar(
                        name="OTEL_EXPORTER_OTLP_ENDPOINT",
                        value=_OTEL_ENDPOINT,
                    ),
                    V1EnvVar(
                        name="OTEL_SERVICE_NAME",
                        value="monarch-worker",
                    ),
                    V1EnvVar(
                        name="USE_UNIFIED_LAYER",
                        value="true",
                    ),
                    V1EnvVar(
                        name="MONARCH_FILE_LOG",
                        value="trace",
                    ),
                ],
            )
        ]
    )


# %%
# The main function provisions workers, spawns actors, and runs several
# rounds of work to produce a stream of metrics and logs.


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

    # Wait for the periodic metric reader and log sink to flush.
    print("Waiting for metrics and logs to flush to collector...")
    time.sleep(10)

    print()
    print("Verify metrics and logs in the OTEL collector logs:")
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
