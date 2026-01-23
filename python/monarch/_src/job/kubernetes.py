# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import sys
from typing import Any, Dict, List

try:
    from kubernetes import client, config, watch
    from kubernetes.client.rest import ApiException
except ImportError:
    raise RuntimeError(
        "please install kubernetes to use KubernetesJob. `pip install kubernetes`"
    )

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import configure
from monarch._src.actor.bootstrap import attach_to_workers
from monarch._src.job.job import JobState, JobTrait


logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stderr))
logger.propagate = False

# Default monarch port for worker communication
_DEFAULT_MONARCH_PORT: int = 26600
_RFC_1123_MAX_LEN = 63


class KubernetesJob(JobTrait):
    """
    Job implementation for Kubernetes that connects to pre-provisioned pods.

    It primarily uses Kubernetes label selectors to discover pods, making it compatible
    with pods from the MonarchMesh CRD and operator, third-party schedulers, or manually
    provisioned pods. Once discovered, it waits for pods to be ready and connects workers
    in rank order (0 to N-1) for deterministic communication.
    """

    def __init__(
        self,
        namespace: str,
        timeout: int | None = None,
    ) -> None:
        """
        Initialize a KubernetesJob that connects to pre-provisioned pods using MonarchMesh CRD and operator.

        Args:
            namespace: Kubernetes namespace to search for pods for all meshes
            timeout: Maximum seconds to wait for pods to be ready for each mesh (default: None, wait indefinitely)
        """
        configure(default_transport=ChannelTransport.TcpWithHostname)
        self._namespace = namespace
        self._timeout = timeout
        self._meshes: Dict[str, Dict[str, Any]] = {}
        super().__init__()

    # TODO: Consider adding monarch-rank label instead of relying on StatefulSet index by default if using MonarchMesh CRD.
    def add_mesh(
        self,
        name: str,
        num_replicas: int,
        label_selector: str | None = None,
        pod_rank_label: str = "apps.kubernetes.io/pod-index",
    ) -> None:
        """
        Add a mesh specification. Meshes are discovered by label selector which expects to contain
        "app.kubernetes.io/name=monarch-worker" and "monarch.pytorch.org/mesh-name=<name>" by default if using MonarchMesh CRD and operator.

        Requires all pod ranks from 0 to num_replicas-1 specified by pod rank label to be ready before the mesh becomes available.

        Args:
            name: Name of the mesh. Must follow RFC 1123 DNS label standard and Monarch hostname restriction:
                  * At most 63 characters
                  * only lowercase alphanumeric characters
                  * must start with an alphabetic character,
                  * and end with an alphanumeric character.
            num_replicas: Number of pod replicas (expects all ranks 0 to num_replicas-1)
            label_selector: Custom Kubernetes label selector for pod discovery (default: "app.kubernetes.io/name=monarch-worker,monarch.pytorch.org/mesh-name=<name>")
            pod_rank_label: Label key containing the pod rank for ordering workers (default: "apps.kubernetes.io/pod-index")

        Raises:
            ValueError: If name does not follow RFC 1123 DNS label standard with Monarch restriction.
        """
        if len(name) == 0:
            raise ValueError("Empty mesh name is invalid.")
        if len(name) > _RFC_1123_MAX_LEN:
            raise ValueError(
                f"Mesh name '{name}' is invalid. Name must contain at most 63 characters."
            )
        if not name.isalnum() or not name.islower():
            raise ValueError(
                f"Mesh name '{name}' is invalid. Name must contain only lowercase alphanumeric characters."
            )
        if not name[0].isalpha():
            raise ValueError(
                f"Mesh name '{name}' is invalid. Name must start with an alphabetic character."
            )
        if not name[-1].isalnum():
            raise ValueError(
                f"Mesh name '{name}' is invalid. Name must end with an alphanumeric character."
            )

        self._meshes[name] = {
            "label_selector": label_selector
            or f"app.kubernetes.io/name=monarch-worker,monarch.pytorch.org/mesh-name={name}",
            "num_replicas": num_replicas,
            "pod_rank_label": pod_rank_label,
        }

    def _create(self, client_script: str | None) -> None:
        """
        Create operation is a no-op for KubernetesJob since we attach to existing pods.
        """
        if client_script is not None:
            raise RuntimeError("KubernetesJob cannot run batch-mode scripts")

        if not self._meshes:
            raise ValueError("At least one mesh must be added using add_mesh()")

    def _is_pod_worker_ready(self, pod: client.V1Pod) -> bool:
        """
        Check if a pod is ready using pod status conditions.

        Args:
            pod: Kubernetes pod object

        Returns:
            True if pod has Ready condition with status True

        """
        if not pod.status.conditions:
            return False

        for condition in pod.status.conditions:
            if condition.type == "Ready":
                return condition.status == "True"

        return False

    def _get_pod_rank(self, pod: client.V1Pod, pod_rank_label: str) -> int:
        """
        Extract the pod rank from the specified label.

        Args:
            pod: Kubernetes pod object
            pod_rank_label: Label key containing the pod rank

        Returns:
            Pod rank as an integer

        Raises:
            ValueError: If pod rank label is missing or invalid
        """
        if not pod.metadata.labels:
            raise ValueError(
                f"Pod {pod.metadata.name} has no labels, cannot determine pod rank"
            )

        pod_rank_str = pod.metadata.labels.get(pod_rank_label)
        if pod_rank_str is None:
            raise ValueError(
                f"Pod {pod.metadata.name} missing required label '{pod_rank_label}'"
            )

        try:
            return int(pod_rank_str)
        except ValueError as e:
            raise ValueError(
                f"Pod {pod.metadata.name} has invalid pod rank '{pod_rank_str}': {e}"
            ) from e

    def _wait_for_ready_pods(
        self,
        label_selector: str,
        num_replicas: int,
        pod_rank_label: str,
        timeout: int | None = None,
    ) -> List[tuple[str, int]]:
        """
        Wait for all required pod ranks to be ready matching the label selector.

        Ensures all ranks from 0 to num_replicas-1 are available before returning and ignores any pod outside this range.

        Args:
            label_selector: Kubernetes label selector
            num_replicas: Number of pod replicas (expects ranks 0 to num_replicas-1)
            pod_rank_label: Label key containing the pod rank for ordering
            timeout: Maximum seconds to wait (None for no timeout)

        Returns:
            List of (pod_ip, monarch_port) tuples sorted by pod rank (0 to num_replicas-1)

        Raises:
            RuntimeError: If timeout reached, missing ranks, or watch error
        """
        ready_pods_by_rank: Dict[int, tuple[str, int]] = {}

        # Load in-cluster Kubernetes configuration
        try:
            config.load_incluster_config()
        except config.ConfigException as e:
            raise RuntimeError(
                "Failed to load in-cluster Kubernetes config. "
                "KubernetesJob must run inside a Kubernetes cluster."
            ) from e

        c = client.CoreV1Api()
        w = watch.Watch()

        try:
            for event in w.stream(
                c.list_namespaced_pod,
                namespace=self._namespace,
                label_selector=label_selector,
                timeout_seconds=timeout,
            ):
                event_type = event["type"]
                pod = event["object"]

                # Handle ERROR events immediately
                if event_type == "ERROR":
                    raise RuntimeError(f"Watch error: {event.get('object', {})}")

                # Extract pod rank (skip pods without valid rank label)
                try:
                    pod_rank = self._get_pod_rank(pod, pod_rank_label)
                except ValueError:
                    continue

                # Skip pods outside expected range
                if pod_rank < 0 or pod_rank >= num_replicas:
                    logger.warning(
                        f"Pod {pod.metadata.name} has rank {pod_rank} outside expected range [0, {num_replicas - 1}]"
                    )
                    continue

                # Handle DELETED events
                if event_type == "DELETED":
                    ready_pods_by_rank.pop(pod_rank, None)
                    continue

                # Only process ADDED/MODIFIED events from here
                if event_type not in ("ADDED", "MODIFIED"):
                    continue

                # Update ready pods based on current state
                if self._is_pod_worker_ready(pod):
                    ready_pods_by_rank[pod_rank] = (
                        pod.status.pod_ip,
                        self._discover_monarch_port(pod),
                    )

                    # Check if we have all required ranks (0 to num_replicas-1)
                    if len(ready_pods_by_rank) == num_replicas:
                        return [
                            ready_pods_by_rank[rank] for rank in range(num_replicas)
                        ]
                else:
                    # Pod is no longer ready, remove its rank
                    ready_pods_by_rank.pop(pod_rank, None)

            # Watch ended without finding all required ranks
            missing_ranks = set(range(num_replicas)) - set(ready_pods_by_rank.keys())
            raise RuntimeError(
                f"Watch ended with {len(ready_pods_by_rank)}/{num_replicas} ranks. "
                f"Missing ranks: {sorted(missing_ranks)}"
            )
        except ApiException as e:
            raise RuntimeError(f"Failed to watch pods: {e}") from e
        finally:
            w.stop()

    # TODO: Consider using named port instead of env var for monarch port
    def _discover_monarch_port(self, pod: client.V1Pod) -> int:
        """
        Discover the monarch port from the pod specification.

        Checks in order:
        1. MONARCH_PORT environment variable in container spec
        2. Falls back to default monarch_port

        Args:
            pod: Kubernetes pod object

        Returns:
            Port number for monarch communication
        """
        for container in pod.spec.containers:
            # Check for MONARCH_PORT env var
            if container.env:
                for env_var in container.env:
                    if env_var.name == "MONARCH_PORT" and env_var.value:
                        try:
                            return int(env_var.value)
                        except ValueError:
                            logger.warning(
                                f"Invalid MONARCH_PORT '{env_var.value}' in pod {pod.metadata.name}"
                            )
                            break

        return _DEFAULT_MONARCH_PORT

    def _state(self) -> JobState:
        """
        Get the current state by connecting to ready pods for each mesh.

        Returns:
            JobState containing HostMesh objects for each configured mesh
        """
        host_meshes = {}

        for mesh_name, mesh_config in self._meshes.items():
            label_selector = mesh_config["label_selector"]
            num_replicas = mesh_config["num_replicas"]
            pod_rank_label = mesh_config["pod_rank_label"]

            # Wait for pods to be ready and discover their ports
            pod_endpoints = self._wait_for_ready_pods(
                label_selector, num_replicas, pod_rank_label, timeout=self._timeout
            )

            # Create worker addresses using discovered IPs and ports
            workers = [f"tcp://{pod_ip}:{port}" for pod_ip, port in pod_endpoints]

            # Create host mesh by attaching to workers
            host_mesh = attach_to_workers(
                name=mesh_name,
                ca="trust_all_connections",
                workers=workers,  # type: ignore[arg-type]
            )
            host_meshes[mesh_name] = host_mesh

        return JobState(host_meshes)

    def can_run(self, spec: "JobTrait") -> bool:
        """
        Check if this job can run the given spec.

        Verifies that:
        1. The spec is a KubernetesJob with matching configuration
        2. The required pods are available and ready

        Args:
            spec: JobTrait specification to check

        Returns:
            True if this job matches the spec and all required pods are available
        """
        if not (
            isinstance(spec, KubernetesJob)
            and spec._namespace == self._namespace
            and spec._meshes == self._meshes
            and self.active
        ):
            return False

        try:
            for mesh_config in self._meshes.values():
                self._wait_for_ready_pods(
                    mesh_config["label_selector"],
                    mesh_config["num_replicas"],
                    mesh_config["pod_rank_label"],
                    timeout=5,
                )
            return True
        except RuntimeError:
            return False

    def _kill(self) -> None:
        """
        Kill operation - not implemented for KubernetesJob.

        Raises:
            NotImplementedError: KubernetesJob does not support killing pods
        """
        raise NotImplementedError(
            "KubernetesJob currently does not support killing pods."
        )
