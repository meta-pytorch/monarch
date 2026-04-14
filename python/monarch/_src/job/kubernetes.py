# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import dataclasses
import logging
import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any, Dict, List, NotRequired, TypedDict

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
from monarch.actor import context


logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stderr))
logger.propagate = False

# Default monarch port for worker communication
_DEFAULT_MONARCH_PORT: int = 26600
_DEFAULT_DUPLEX_PORT: int = 34000
_RFC_1123_MAX_LEN = 63

# MonarchMesh CRD coordinates
_MONARCHMESH_GROUP = "monarch.pytorch.org"
_MONARCHMESH_VERSION = "v1alpha1"
_MONARCHMESH_PLURAL = "monarchmeshes"

# Bootstrap script for provisioned worker pods.
# Each worker discovers its own FQDN and starts listening for connections.
# Optionally listens on a duplex address if MONARCH_DUPLEX_PORT is set.
_WORKER_BOOTSTRAP_SCRIPT: str = textwrap.dedent("""\
    import os
    import socket
    from monarch.actor import run_worker_loop_forever
    port = os.environ.get("MONARCH_PORT", "26600")
    duplex = os.environ.get("MONARCH_DUPLEX_PORT")
    hostname = socket.getfqdn()
    address = f"tcp://{hostname}:{port}"
    if duplex:
        duplex = f"tcp://0.0.0.0:{duplex}"
    run_worker_loop_forever(address=address, ca="trust_all_connections", duplex_address=duplex)
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


@dataclasses.dataclass(frozen=True)
class KubeConfig:
    """Kubernetes configuration for connecting to the cluster.

    Use this to specify a kubeconfig file for out-of-cluster usage of
    KubernetesJob::

        KubeConfig(kubeconfig="/path/to/kubeconfig")

    If both local and remote are none, in-cluster configuration is used.
    """

    local: Path | None = None
    remote: client.Configuration | None = None

    @classmethod
    def from_path(cls, path: str) -> "KubeConfig":
        """Create a KubeConfig from a local file path."""
        return cls(local=Path(path).expanduser())

    @classmethod
    def from_config(cls, config: client.Configuration) -> "KubeConfig":
        """Create a KubeConfig from a remote host"""
        return cls(remote=config)

    @property
    def out_of_cluster(self) -> bool:
        """Whether this kubeconfig is for out-of-cluster usage."""
        return self.remote is not None or self.local is not None

    def load(self) -> None:
        if self.local is not None:
            try:
                config.load_kube_config(config_file=str(self.local))
            except config.ConfigException as e:
                raise RuntimeError(
                    f"Failed to load kubeconfig file '{self.local}'"
                ) from e
        elif self.remote is not None:
            client.Configuration.set_default(self.remote)
        else:
            try:
                config.load_incluster_config()
            except config.ConfigException as e:
                raise RuntimeError(
                    "Failed to load in-cluster Kubernetes config. "
                    "KubernetesJob must run inside a Kubernetes cluster."
                ) from e


@dataclasses.dataclass(frozen=True)
class _MonarchMeshPod:
    name: str
    ip: str
    port: int
    duplex_port: int | None


class _MeshConfig(TypedDict):
    service_name: str
    label_selector: str
    num_replicas: int
    pod_rank_label: str
    provisioned: bool
    port: int
    labels: NotRequired[Dict[str, str]]
    pod_spec: NotRequired[client.V1PodSpec]


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
        kubeconfig: KubeConfig | None = None,
        attach_to: str | None = None,
    ) -> None:
        """
        Initialize a KubernetesJob.

        Args:
            namespace: Kubernetes namespace for all meshes
            timeout: Maximum seconds to wait for pods to be ready for each mesh (default: None, wait indefinitely)
            kubeconfig: Path to a kubeconfig file for out-of-cluster configuration (default: None, use in-cluster config)
            attach_to: ZMQ-style address of a duplex endpoint for out-of-cluster
                access (e.g. `"tcp://127.0.0.1:34000"`).
                To expose one, ensure at least one mesh pod runs
                ``run_worker_loop_forever(..., duplex_address="tcp://hostname:port")``,
                then use ``kubectl port-forward pod/<pod-name> <local>:<duplex_port>``
                to forward the pod to localhost.
                The user is responsible for ensuring the address is reachable.
                Should only be used in out-of-cluster mode, where kubeconfig is provided.
        """
        configure(default_transport=ChannelTransport.TcpWithHostname)
        self._namespace = namespace
        self._timeout = timeout
        self._kubeconfig: KubeConfig = kubeconfig or KubeConfig()
        self._attach_to = attach_to
        self._meshes: Dict[str, _MeshConfig] = {}
        self._port_forward_processes: List[subprocess.Popen[str]] = []
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
        duplex_port: int | None = None,
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
            duplex_port: Optional duplex port for out-of-cluster client attachment. If specified, the hosts
                in this mesh will listen on this port for duplex connections for
                any proc that wants to attach.
                If kubeconfig is in out-of-cluster mode, this is automatically set
                to the default duplex port (34000).
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

        mesh_entry: _MeshConfig = {
            # The service name has a suffix appended to it controlled by the MonarchMesh config.
            "service_name": f"{name}-svc",
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
            # Set a default for the out-of-cluster case. If the user specified
            # a value already it overwrites the default.
            if duplex_port is None and self._kubeconfig.out_of_cluster:
                duplex_port = _DEFAULT_DUPLEX_PORT
            mesh_entry["pod_spec"] = self._build_worker_pod_spec(
                image_spec, port, duplex_port=duplex_port
            )
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

        self._kubeconfig.load()

        api_client = client.ApiClient()
        api = client.CustomObjectsApi(api_client)

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
        duplex_port: int | None = None,
    ) -> client.V1PodSpec:
        """
        Build a V1PodSpec for the MonarchMesh CRD.

        Generates a single-container pod spec with a worker bootstrap
        script that starts ``run_worker_loop_forever``.

        Args:
            image_spec: ImageSpec with container image and optional resources.
            port: Monarch worker port.
            duplex_port: Optional duplex port for out-of-cluster client attachment.

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
        env = [client.V1EnvVar(name="MONARCH_PORT", value=str(port))]
        if duplex_port is not None:
            env.append(
                client.V1EnvVar(name="MONARCH_DUPLEX_PORT", value=str(duplex_port))
            )
        container = client.V1Container(
            name="worker",
            image=image_spec.image,
            command=["python", "-u", "-c", _WORKER_BOOTSTRAP_SCRIPT],
            env=env,
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
    ) -> List[_MonarchMeshPod]:
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
        ready_pods_by_rank: Dict[int, _MonarchMeshPod] = {}

        # Load Kubernetes configuration
        self._kubeconfig.load()

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
                    logger.warning(
                        f"Skipping pod {pod.metadata.name} due to missing or invalid pod rank label '{pod_rank_label}'"
                    )
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
                    ready_pods_by_rank[pod_rank] = _MonarchMeshPod(
                        name=pod.metadata.name,
                        ip=pod.status.pod_ip,
                        port=self._discover_monarch_port(pod),
                        duplex_port=self._discover_monarch_duplex_port(pod),
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

    def _discover_monarch_duplex_port(self, pod: client.V1Pod) -> int | None:
        """Discover MONARCH_DUPLEX_PORT from a pod's container env vars."""
        for container in pod.spec.containers:
            if container.env:
                for env_var in container.env:
                    if env_var.name == "MONARCH_DUPLEX_PORT" and env_var.value:
                        try:
                            return int(env_var.value)
                        except ValueError:
                            logger.warning(
                                f"Invalid MONARCH_DUPLEX_PORT '{env_var.value}' in pod {pod.metadata.name}"
                            )
                            break
        return None

    def _port_forward_to_pod(self, pod: _MonarchMeshPod) -> str:
        """Start kubectl port-forward to the pod's duplex port.

        Returns the local ``tcp://127.0.0.1:<local_port>`` address.
        """
        if shutil.which("kubectl") is None:
            raise RuntimeError(
                "kubectl is required for out-of-cluster port forwarding but was not found in PATH"
            )
        if pod.duplex_port is None:
            raise RuntimeError(f"Pod {pod.name} has no duplex port configured")

        cmd = [
            "kubectl",
            "port-forward",
            "--namespace",
            self._namespace,
            f"pod/{pod.name}",
            f":{pod.duplex_port}",
        ]
        if self._kubeconfig.local is not None:
            cmd.extend(["--kubeconfig", str(self._kubeconfig.local)])

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        if process.stdout is None:
            raise RuntimeError(
                f"failed to open stdout for kubectl port-forward to pod {pod.name}"
            )

        first_line = process.stdout.readline()
        if not first_line:
            stderr_output = process.stderr.read() if process.stderr else ""
            process.wait()
            raise RuntimeError(
                f"kubectl port-forward produced no output for pod {pod.name}: {stderr_output}"
            )

        match = re.search(
            r"Forwarding from (?:127\.0\.0\.1|\[::1\]):(\d+) ->", first_line
        )
        if not match:
            process.kill()
            process.wait()
            raise RuntimeError(
                f"could not parse local port from kubectl output for pod {pod.name}: {first_line}"
            )

        local_port = int(match.group(1))
        self._port_forward_processes.append(process)
        logger.info(
            "Port forwarding established to pod/%s on local port %d",
            pod.name,
            local_port,
        )
        return f"tcp://127.0.0.1:{local_port}"

    def _state(self) -> JobState:
        """
        Get the current state by connecting to ready pods for each mesh.

        Returns:
            JobState containing HostMesh objects for each configured mesh
        """
        host_meshes = {}
        attach_to = self._attach_to

        # Discover all mesh pods first so we can set up port-forwarding
        # before attaching to any workers.
        all_mesh_pods: Dict[str, List[_MonarchMeshPod]] = {}
        for mesh_name, mesh_config in self._meshes.items():
            # Wait for pods to be ready and discover their ports
            pods = self._wait_for_ready_pods(
                mesh_config["label_selector"],
                mesh_config["num_replicas"],
                mesh_config["pod_rank_label"],
                timeout=self._timeout,
            )
            all_mesh_pods[mesh_name] = pods

        # Set up out-of-cluster client attachment before connecting to workers.
        if self._kubeconfig.out_of_cluster and attach_to is None:
            # No explicit attach_to — try to auto-forward to a pod with a
            # duplex port (provisioned pods get one automatically).
            for pods in all_mesh_pods.values():
                for pod in pods:
                    if pod.duplex_port is not None:
                        attach_to = self._port_forward_to_pod(pod)
                        break
                if attach_to is not None:
                    break
            if attach_to is None:
                raise RuntimeError(
                    "out-of-cluster mode requires a duplex address, but no pod has "
                    "MONARCH_DUPLEX_PORT configured and no attach_to was provided"
                )

        if attach_to is not None:
            logger.info("Attaching context to duplex address: %s", attach_to)
            context(attach_to=attach_to)

        for mesh_name, pods in all_mesh_pods.items():
            # Create worker addresses using discovered IPs and ports
            workers = [f"tcp://{pod.ip}:{pod.port}" for pod in pods]
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
        Delete MonarchMesh CRDs for provisioned meshes.

        Raises:
            NotImplementedError: If no provisioned meshes exist (all
                meshes are attach-only).
        """
        for process in self._port_forward_processes:
            if process.poll() is None:
                process.terminate()
                process.wait()

        provisioned = [
            name for name, cfg in self._meshes.items() if cfg.get("provisioned")
        ]
        if not provisioned:
            raise NotImplementedError(
                "KubernetesJob currently does not support killing pods."
            )

        self._kubeconfig.load()
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
