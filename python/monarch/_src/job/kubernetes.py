# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import dataclasses
import logging
import re
import select
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any, TypedDict

try:
    from kubernetes import client, config, watch
    from kubernetes.client.rest import ApiException
    from kubernetes.config.kube_config import KubeConfigMerger
except ImportError:
    raise RuntimeError(
        "please install kubernetes to use KubernetesJob. `pip install kubernetes`"
    )

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import (
    configure,
    get_propagatable_config_env,
)
from monarch._src.actor.bootstrap import attach_to_workers
from monarch._src.job.job import JobState, JobTrait
from monarch.actor import attach


logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stderr))
logger.propagate = False

# Default monarch port for worker communication
_DEFAULT_MONARCH_PORT: int = 26600
_RFC_1123_MAX_LEN = 63

# Seconds to wait for `kubectl port-forward` to report it is ready before giving
# up, so a silently hung forward cannot stall job initialization indefinitely.
_PORT_FORWARD_START_TIMEOUT_SECONDS: int = 30

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
    address = f"tcp://{hostname}:{port}@tcp://0.0.0.0:{port}"
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


@dataclasses.dataclass(frozen=True)
class KubeConfig:
    """Kubernetes configuration for connecting to the cluster.

    Use this to specify a kubeconfig file for out-of-cluster usage of
    KubernetesJob::

        KubeConfig.from_path("/path/to/kubeconfig")

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
                proxy_url = _get_kubeconfig_proxy_url(self.local)
            except config.ConfigException as e:
                raise RuntimeError(
                    f"Failed to load kubeconfig file '{self.local}'"
                ) from e
            if proxy_url is not None:
                configuration = client.Configuration.get_default_copy()
                configuration.proxy = proxy_url
                client.Configuration.set_default(configuration)
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


def _get_kubeconfig_proxy_url(path: Path) -> str | None:
    merged_config: Any = KubeConfigMerger(str(path)).config
    if merged_config is None:
        return None

    # A malformed kubeconfig (e.g. missing current-context or a dangling
    # cluster reference) raises KeyError/TypeError/AttributeError here. Surface
    # it as a ConfigException so KubeConfig.load transforms it into a clear
    # RuntimeError rather than leaking a raw traceback.
    try:
        context = merged_config["contexts"].get_with_name(
            merged_config["current-context"]
        )["context"]
        cluster = merged_config["clusters"].get_with_name(context["cluster"])["cluster"]
        proxy_url = cluster.safe_get("proxy-url")
    except (KeyError, TypeError, AttributeError) as e:
        raise config.ConfigException(f"malformed kubeconfig '{path}': {e}") from e
    if proxy_url is not None and not isinstance(proxy_url, str):
        raise config.ConfigException("kubeconfig proxy-url must be a string")
    return proxy_url


@dataclasses.dataclass(frozen=True)
class _MonarchMeshPod:
    name: str
    ip: str
    port: int


class _MeshConfigRequired(TypedDict):
    service_name: str
    label_selector: str
    num_replicas: int
    pod_rank_label: str
    provisioned: bool
    port: int


class _MeshConfig(_MeshConfigRequired, total=False):
    labels: dict[str, str]
    annotations: dict[str, str]
    pod_template: client.V1PodTemplateSpec


class KubernetesJob(JobTrait):
    """
    Job implementation for Kubernetes that discovers and connects to pods.

    Supports two modes:

    *Pre-provisioned* -- connect to pre-provisioned pods discovered via label
    selectors. Compatible with the MonarchMesh operator, third-party
    schedulers, or manually created pods. Used when ``image_spec`` or
    ``pod_template`` is not specified in ``add_mesh``.

    *Provisioning* -- create MonarchMesh CRDs via the K8s API so the
    pre-installed operator provisions StatefulSets and Services
    automatically. Pass ``image_spec`` or ``pod_template`` (a
    ``V1PodTemplateSpec``) to ``add_mesh`` to enable provisioning for that
    mesh. If the MonarchMesh CRD already exists, it is patched instead
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
            attach_to: ZMQ-style address of a worker's monarch port for out-of-cluster
                access (e.g. `"tcp://127.0.0.1:26600"`). The monarch port doubles
                as the duplex attach address. Use
                ``kubectl port-forward pod/<pod-name> <local>:26600``
                to forward the pod to localhost.
                When ``kubeconfig`` is provided (out-of-cluster mode) and this is
                not set, port-forwarding is set up automatically.
        """
        configure(default_transport=ChannelTransport.TcpWithHostname)
        self._namespace = namespace
        self._timeout = timeout
        self._kubeconfig: KubeConfig = kubeconfig or KubeConfig()
        self._attach_to = attach_to
        self._meshes: dict[str, _MeshConfig] = {}
        self._port_forward_processes: list[subprocess.Popen[str]] = []
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
        pod_template: client.V1PodTemplateSpec | None = None,
        labels: dict[str, str] | None = None,
        annotations: dict[str, str] | None = None,
    ) -> None:
        """
        Add a mesh specification.

        In *attach-only* mode (default), meshes are discovered by label
        selector. In *provisioning* mode (``image_spec`` or ``pod_template``
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
                   Mutually exclusive with ``pod_template``.
            port: Monarch worker port (default: 26600).
            pod_template: ``V1PodTemplateSpec`` for advanced provisioning (e.g. custom volumes, sidecars,
                          pod-level labels/annotations). Mutually exclusive with ``image_spec``.
            labels: Optional labels to apply to the MonarchMesh CRD metadata.
                    Propagated by the operator to the StatefulSet metadata. To set
                    labels on the worker pods, use ``pod_template.metadata.labels``.
                    Only used when provisioning (``image_spec`` or ``pod_template`` supplied).
            annotations: Optional annotations to apply to the MonarchMesh CRD metadata.
                    Propagated by the operator to the StatefulSet metadata. To set
                    annotations on the worker pods, use ``pod_template.metadata.annotations``.
                    Only used when provisioning (``image_spec`` or ``pod_template`` supplied).

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

        provisioned = image_spec is not None or pod_template is not None

        if image_spec is not None and pod_template is not None:
            raise ValueError("'image_spec' and 'pod_template' are mutually exclusive.")
        if provisioned and label_selector is not None:
            raise ValueError("'label_selector' cannot be customized when provisioning.")
        if provisioned and pod_rank_label != "apps.kubernetes.io/pod-index":
            raise ValueError("'pod_rank_label' cannot be customized when provisioning.")
        if not provisioned and labels is not None:
            raise ValueError("'labels' can only be set when provisioning.")
        if not provisioned and annotations is not None:
            raise ValueError("'annotations' can only be set when provisioning.")

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

        if annotations is not None:
            mesh_entry["annotations"] = annotations

        if image_spec is not None:
            mesh_entry["pod_template"] = self._build_worker_pod_template(
                image_spec, port
            )
        elif pod_template is not None:
            mesh_entry["pod_template"] = pod_template

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
            pod_template_dict = api_client.sanitize_for_serialization(
                mesh_config["pod_template"]
            )
            metadata: dict[str, Any] = {
                "name": mesh_name,
                "namespace": self._namespace,
            }
            if "labels" in mesh_config:
                metadata["labels"] = mesh_config["labels"]
            if "annotations" in mesh_config:
                metadata["annotations"] = mesh_config["annotations"]

            body: dict[str, Any] = {
                "apiVersion": f"{_MONARCHMESH_GROUP}/{_MONARCHMESH_VERSION}",
                "kind": "MonarchMesh",
                "metadata": metadata,
                "spec": {
                    "replicas": mesh_config["num_replicas"],
                    "port": mesh_config["port"],
                    "podTemplate": pod_template_dict,
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
    def _build_worker_pod_template(
        image_spec: ImageSpec,
        port: int,
    ) -> client.V1PodTemplateSpec:
        """
        Build a V1PodTemplateSpec for the MonarchMesh CRD.

        Generates a single-container pod template with a worker bootstrap
        script that starts ``run_worker_loop_forever``.

        Args:
            image_spec: ImageSpec with container image and optional resources.
            port: Monarch worker port.

        Returns:
            V1PodTemplateSpec suitable for the ``podTemplate`` CRD field.
        """
        resources = None
        if image_spec.resources is not None:
            k8s_resources = {str(k): str(v) for k, v in image_spec.resources.items()}
            resources = client.V1ResourceRequirements(
                requests=k8s_resources,
                limits=k8s_resources,
            )
        worker_env = {**get_propagatable_config_env(), "MONARCH_PORT": str(port)}
        container = client.V1Container(
            name="worker",
            image=image_spec.image,
            command=["python", "-u", "-c", _WORKER_BOOTSTRAP_SCRIPT],
            env=[
                client.V1EnvVar(name=key, value=value)
                for key, value in worker_env.items()
            ],
            resources=resources,
        )
        return client.V1PodTemplateSpec(
            spec=client.V1PodSpec(containers=[container]),
        )

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
    ) -> list[_MonarchMeshPod]:
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
        ready_pods_by_rank: dict[int, _MonarchMeshPod] = {}

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

    def _port_forward_to_pod(self, pod: _MonarchMeshPod) -> str:
        """Start kubectl port-forward to the pod's monarch port.

        The frontend address doubles as the duplex attach address, so
        port-forwarding the monarch port is sufficient for both regular
        messaging and out-of-cluster client attachment.

        Returns the local ``tcp://127.0.0.1:<local_port>`` address.
        """
        if shutil.which("kubectl") is None:
            raise RuntimeError(
                "kubectl is required for out-of-cluster port forwarding but was not found in PATH"
            )

        cmd = [
            "kubectl",
            "port-forward",
            "--namespace",
            self._namespace,
            f"pod/{pod.name}",
            f":{pod.port}",
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

        # kubectl prints "Forwarding from ..." to stdout once the tunnel is up.
        # Guard the blocking read so a silently hung forward cannot stall job
        # initialization indefinitely.
        ready, _, _ = select.select(
            [process.stdout], [], [], _PORT_FORWARD_START_TIMEOUT_SECONDS
        )
        if not ready:
            process.kill()
            process.wait()
            raise RuntimeError(
                f"kubectl port-forward to pod {pod.name} did not start within "
                f"{_PORT_FORWARD_START_TIMEOUT_SECONDS}s"
            )

        first_line = process.stdout.readline()
        if not first_line:
            # kubectl closed stdout without announcing readiness. Terminate it
            # and drain stderr with a deadline so a process that closed stdout
            # while still running cannot block us.
            process.terminate()
            try:
                _, stderr_output = process.communicate(
                    timeout=_PORT_FORWARD_START_TIMEOUT_SECONDS
                )
            except subprocess.TimeoutExpired:
                process.kill()
                _, stderr_output = process.communicate()
            raise RuntimeError(
                f"kubectl port-forward produced no output for pod {pod.name}: "
                f"{stderr_output}"
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

    def _terminate_port_forwards(self) -> None:
        """Terminate any running ``kubectl port-forward`` subprocesses."""
        for process in self._port_forward_processes:
            if process.poll() is None:
                process.terminate()
                process.wait()
        self._port_forward_processes.clear()

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
        all_mesh_pods: dict[str, list[_MonarchMeshPod]] = {}
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
        # If anything below fails after a port-forward subprocess is started,
        # terminate it so we do not leak kubectl processes and bound ports.
        try:
            if self._kubeconfig.out_of_cluster and attach_to is None:
                # No explicit attach_to — auto-forward to the first pod's
                # monarch port (which doubles as the duplex attach address).
                for pods in all_mesh_pods.values():
                    if pods:
                        attach_to = self._port_forward_to_pod(pods[0])
                        break
                if attach_to is None:
                    raise RuntimeError(
                        "out-of-cluster mode requires at least one ready pod "
                        "and no attach_to was provided"
                    )

            if attach_to is not None:
                logger.info(
                    "Attaching client gateway via duplex address: %s", attach_to
                )
                attach(attach_to)

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
        except Exception:
            self._terminate_port_forwards()
            raise

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
        self._terminate_port_forwards()

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
