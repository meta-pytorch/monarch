# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import dataclasses
import logging
import sys
import textwrap
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

# MonarchMesh CRD coordinates
_MONARCHMESH_GROUP = "monarch.pytorch.org"
_MONARCHMESH_VERSION = "v1alpha1"
_MONARCHMESH_PLURAL = "monarchmeshes"

# Bootstrap script for provisioned worker pods.
# Each worker discovers its own FQDN and starts listening for connections.
_WORKER_BOOTSTRAP_SCRIPT: str = textwrap.dedent("""\
    import os
    import socket
    from monarch.actor import run_worker_loop_forever
    port = os.environ.get("MONARCH_PORT", "26600")
    hostname = socket.getfqdn()
    address = f"tcp://{hostname}:{port}"
    run_worker_loop_forever(address=address, ca="trust_all_connections")
""")


@dataclasses.dataclass(frozen=True)
class ImageSpec:
    """Container image specification for provisioning worker pods.

    Use this to provision MonarchMesh workers with a specific container
    image and optional K8s resource requests/limits::

        # Simple — image only
        ImageSpec("ghcr.io/meta-pytorch/monarch:latest")

        # With GPU resources
        ImageSpec("ghcr.io/meta-pytorch/monarch:latest",
                  resources={"nvidia.com/gpu": 4})

    Pass the resulting object to ``KubernetesJob.add_mesh(image_spec=...)``.
    """

    image: str
    """Required container image to use for worker pods."""

    resources: dict[str, str | int] | None = None
    """Optional K8s resource requests/limits (e.g. ``{"nvidia.com/gpu": 4}``)."""


class KubernetesJob(JobTrait):
    """
    Job implementation for Kubernetes that discovers and connects to pods.

    Supports two modes:

    *Pre-provisioned* -- connect to pre-provisioned pods discovered via label
    selectors. Compatible with the MonarchMesh operator, third-party
    schedulers, or manually created pods. Used when ``image_spec`` or ``pod_spec``
    is not specified in ``add_mesh``.

    *Provisioning* -- create MonarchMesh CRDs via the K8s API so the
    pre-installed operator provisions StatefulSets and Services
    automatically. Pass ``image_spec`` or ``pod_spec`` (a ``V1PodSpec``) to
    ``add_mesh`` to enable provisioning for that
    mesh. If the MonarchMesh CRD
    already exists, it is patched instead
    of created.
    """

    def __init__(
        self,
        namespace: str,
        timeout: int | None = None,
        max_retries: int = 3,
    ) -> None:
        """
        Initialize a KubernetesJob.

        Args:
            namespace: Kubernetes namespace for all meshes
            timeout: Maximum seconds to wait for pods to be ready for each mesh (default: None, wait indefinitely)
            max_retries: Maximum number of times to retry pod discovery if
                         pods change between discovery and attachment (default: 3)
        """
        configure(default_transport=ChannelTransport.TcpWithHostname)
        self._namespace = namespace
        self._timeout = timeout
        self._max_retries = max_retries
        self._meshes: Dict[str, Dict[str, Any]] = {}
        super().__init__()

    # TODO: Consider adding monarch-rank label instead of relying on StatefulSet index by default if using MonarchMesh CRD.
    def add_mesh(
        self,
        name: str,
        num_replicas: int,
        label_selector: str | None = None,
        pod_rank_label: str = "apps.kubernetes.io/pod-index",
        image_spec: ImageSpec | None = None,
        port: int = _DEFAULT_MONARCH_PORT,
        pod_spec: client.V1PodSpec | None = None,
        labels: dict[str, str] | None = None,
    ) -> None:
        """
        Add a mesh specification.

        In *attach-only* mode (default), meshes are discovered by label
        selector. In *provisioning* mode (``image_spec`` or ``pod_spec``
        supplied), a MonarchMesh CRD is created so the operator can
        provision the pods.

        Args:
            name: Name of the mesh. Must follow RFC 1123 DNS label standard and Monarch hostname restriction:
                  * At most 63 characters
                  * only lowercase alphanumeric characters
                  * must start with an alphabetic character,
                  * and end with an alphanumeric character.
            num_replicas: Number of pod replicas (expects all ranks 0 to num_replicas-1)
            label_selector: Custom label selector for pod discovery. Cannot be set when provisioning.
            pod_rank_label: Label key containing the pod rank. Cannot be customized when provisioning.
            image_spec: ``ImageSpec`` with container image and optional resources for simple provisioning.
                   Mutually exclusive with ``pod_spec``.
            port: Monarch worker port (default: 26600).
            pod_spec: ``V1PodSpec`` for advanced provisioning (e.g. custom volumes, sidecars).
                      Mutually exclusive with ``image_spec``.
            labels: Optional labels to apply to the MonarchMesh CRD metadata.
                    Only used when provisioning (``image_spec`` or ``pod_spec`` supplied).

        Raises:
            ValueError: On invalid name or conflicting parameters.
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

        provisioned = image_spec is not None or pod_spec is not None

        if image_spec is not None and pod_spec is not None:
            raise ValueError("'image_spec' and 'pod_spec' are mutually exclusive.")
        if provisioned and label_selector is not None:
            raise ValueError("'label_selector' cannot be customized when provisioning.")
        if provisioned and pod_rank_label != "apps.kubernetes.io/pod-index":
            raise ValueError("'pod_rank_label' cannot be customized when provisioning.")
        if not provisioned and labels is not None:
            raise ValueError("'labels' can only be set when provisioning.")

        mesh_entry: Dict[str, Any] = {
            "label_selector": label_selector
            or f"app.kubernetes.io/name=monarch-worker,monarch.pytorch.org/mesh-name={name}",
            "num_replicas": num_replicas,
            "pod_rank_label": pod_rank_label,
            "provisioned": provisioned,
            "port": port,
        }

        if labels is not None:
            mesh_entry["labels"] = labels

        if image_spec is not None:
            mesh_entry["pod_spec"] = self._build_worker_pod_spec(image_spec, port)
        elif pod_spec is not None:
            mesh_entry["pod_spec"] = pod_spec

        self._meshes[name] = mesh_entry

    def _create(self, client_script: str | None) -> None:
        """
        Create MonarchMesh CRDs for provisioned meshes.

        Attach-only meshes are a no-op. For provisioned meshes, the
        MonarchMesh custom resource is created (or patched if it already
        exists) so the operator can provision the StatefulSet and
        headless Service.
        """
        if client_script is not None:
            raise RuntimeError("KubernetesJob cannot run batch-mode scripts")

        if not self._meshes:
            raise ValueError("At least one mesh must be added using add_mesh()")

        provisioned = {
            name: cfg for name, cfg in self._meshes.items() if cfg.get("provisioned")
        }
        if not provisioned:
            return

        # TODO: Add support for out-of-cluster config
        try:
            config.load_incluster_config()
        except config.ConfigException as e:
            raise RuntimeError(
                "Failed to load in-cluster Kubernetes config. "
                "KubernetesJob must run inside a Kubernetes cluster."
            ) from e

        api = client.CustomObjectsApi()
        api_client = client.ApiClient()

        for mesh_name, mesh_config in provisioned.items():
            pod_spec_dict = api_client.sanitize_for_serialization(
                mesh_config["pod_spec"]
            )
            metadata: Dict[str, Any] = {
                "name": mesh_name,
                "namespace": self._namespace,
            }
            if "labels" in mesh_config:
                metadata["labels"] = mesh_config["labels"]

            body: Dict[str, Any] = {
                "apiVersion": f"{_MONARCHMESH_GROUP}/{_MONARCHMESH_VERSION}",
                "kind": "MonarchMesh",
                "metadata": metadata,
                "spec": {
                    "replicas": mesh_config["num_replicas"],
                    "port": mesh_config["port"],
                    "podTemplate": pod_spec_dict,
                },
            }

            try:
                api.create_namespaced_custom_object(
                    group=_MONARCHMESH_GROUP,
                    version=_MONARCHMESH_VERSION,
                    namespace=self._namespace,
                    plural=_MONARCHMESH_PLURAL,
                    body=body,
                )
                logger.info("Created MonarchMesh '%s'", mesh_name)
            except ApiException as e:
                if e.status == 409:
                    # TODO: Consider throwing an error instead of patching if the CRD already exists.
                    api.patch_namespaced_custom_object(
                        group=_MONARCHMESH_GROUP,
                        version=_MONARCHMESH_VERSION,
                        namespace=self._namespace,
                        plural=_MONARCHMESH_PLURAL,
                        name=mesh_name,
                        body=body,
                    )
                    logger.info("MonarchMesh '%s' already exists, patched", mesh_name)
                else:
                    raise

    @staticmethod
    def _build_worker_pod_spec(
        image_spec: ImageSpec,
        port: int,
    ) -> client.V1PodSpec:
        """
        Build a V1PodSpec for the MonarchMesh CRD.

        Generates a single-container pod spec with a worker bootstrap
        script that starts ``run_worker_loop_forever``.

        Args:
            image_spec: ImageSpec with container image and optional resources.
            port: Monarch worker port.

        Returns:
            V1PodSpec suitable for the ``podTemplate`` CRD field.
        """
        resources = None
        if image_spec.resources is not None:
            k8s_resources = {str(k): str(v) for k, v in image_spec.resources.items()}
            resources = client.V1ResourceRequirements(
                requests=k8s_resources,
                limits=k8s_resources,
            )
        container = client.V1Container(
            name="worker",
            image=image_spec.image,
            command=["python", "-u", "-c", _WORKER_BOOTSTRAP_SCRIPT],
            env=[client.V1EnvVar(name="MONARCH_PORT", value=str(port))],
            resources=resources,
        )
        return client.V1PodSpec(containers=[container])

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

    def _verify_pods_ready(
        self,
        label_selector: str,
        num_replicas: int,
        pod_rank_label: str,
        expected_endpoints: List[tuple[str, int]],
    ) -> bool:
        """
        Verify that discovered pod endpoints are still current.

        Does a single list-pods call and compares the Ready pods against
        the endpoints returned by ``_wait_for_ready_pods()``.

        Args:
            label_selector: Kubernetes label selector
            num_replicas: Expected number of replicas
            pod_rank_label: Label key containing the pod rank
            expected_endpoints: The (pod_ip, port) tuples to verify

        Returns:
            True if all pods are still Ready with matching IPs and ports
        """
        try:
            config.load_incluster_config()
        except config.ConfigException:
            return False

        c = client.CoreV1Api()
        pods = c.list_namespaced_pod(
            namespace=self._namespace,
            label_selector=label_selector,
        )

        ready_by_rank: Dict[int, tuple[str, int]] = {}
        for pod in pods.items:
            try:
                rank = self._get_pod_rank(pod, pod_rank_label)
            except ValueError:
                continue
            if rank < 0 or rank >= num_replicas:
                continue
            if self._is_pod_worker_ready(pod):
                ready_by_rank[rank] = (
                    pod.status.pod_ip,
                    self._discover_monarch_port(pod),
                )

        if len(ready_by_rank) != num_replicas:
            return False

        current_endpoints = [ready_by_rank[rank] for rank in range(num_replicas)]
        return current_endpoints == expected_endpoints

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

    def _discover_all_mesh_endpoints(
        self,
    ) -> Dict[str, List[tuple[str, int]]]:
        """
        Wait for all meshes' pods to be ready.

        Returns:
            Mapping of mesh name to list of (pod_ip, port) tuples sorted by rank.
        """
        all_endpoints: Dict[str, List[tuple[str, int]]] = {}
        for mesh_name, mesh_config in self._meshes.items():
            all_endpoints[mesh_name] = self._wait_for_ready_pods(
                mesh_config["label_selector"],
                mesh_config["num_replicas"],
                mesh_config["pod_rank_label"],
                timeout=self._timeout,
            )
        return all_endpoints

    def _verify_all_mesh_endpoints(
        self,
        all_endpoints: Dict[str, List[tuple[str, int]]],
    ) -> bool:
        """
        Verify that every mesh's discovered endpoints are still current.

        Args:
            all_endpoints: Mapping of mesh name to expected (pod_ip, port) tuples.

        Returns:
            True if all meshes still have the same Ready pods.
        """
        for mesh_name, expected in all_endpoints.items():
            mesh_config = self._meshes[mesh_name]
            if not self._verify_pods_ready(
                mesh_config["label_selector"],
                mesh_config["num_replicas"],
                mesh_config["pod_rank_label"],
                expected,
            ):
                return False
        return True

    def _state(self) -> JobState:
        """
        Get the current state by connecting to ready pods for each mesh.

        Discovers all meshes' pods, then verifies that none changed during
        discovery. If any mesh's pods shifted, retries the entire discovery
        up to ``max_retries`` times before attaching.

        Returns:
            JobState containing HostMesh objects for each configured mesh

        Raises:
            RuntimeError: If stable endpoints cannot be obtained after retries.
        """
        all_endpoints: Dict[str, List[tuple[str, int]]] | None = None

        for attempt in range(self._max_retries):
            all_endpoints = self._discover_all_mesh_endpoints()

            if self._verify_all_mesh_endpoints(all_endpoints):
                break

            logger.warning(
                "Mesh endpoints changed after discovery (attempt %d/%d), retrying",
                attempt + 1,
                self._max_retries,
            )
            all_endpoints = None

        if all_endpoints is None:
            raise RuntimeError(
                f"Failed to get stable mesh endpoints after {self._max_retries} attempts"
            )

        host_meshes = {}
        for mesh_name, pod_endpoints in all_endpoints.items():
            workers = [f"tcp://{pod_ip}:{port}" for pod_ip, port in pod_endpoints]
            host_meshes[mesh_name] = attach_to_workers(
                name=mesh_name,
                ca="trust_all_connections",
                workers=workers,  # type: ignore[arg-type]
            )

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
        Delete MonarchMesh CRDs for provisioned meshes.

        Raises:
            NotImplementedError: If no provisioned meshes exist (all
                meshes are attach-only).
        """
        provisioned = [
            name for name, cfg in self._meshes.items() if cfg.get("provisioned")
        ]
        if not provisioned:
            raise NotImplementedError(
                "KubernetesJob currently does not support killing pods."
            )

        try:
            config.load_incluster_config()
        except config.ConfigException as e:
            raise RuntimeError(
                "Failed to load in-cluster Kubernetes config. "
                "KubernetesJob must run inside a Kubernetes cluster."
            ) from e

        api = client.CustomObjectsApi()

        for mesh_name in provisioned:
            try:
                api.delete_namespaced_custom_object(
                    group=_MONARCHMESH_GROUP,
                    version=_MONARCHMESH_VERSION,
                    namespace=self._namespace,
                    plural=_MONARCHMESH_PLURAL,
                    name=mesh_name,
                )
                logger.info("Deleted MonarchMesh '%s'", mesh_name)
            except ApiException as e:
                if e.status == 404:
                    logger.info("MonarchMesh '%s' already deleted", mesh_name)
                else:
                    raise
