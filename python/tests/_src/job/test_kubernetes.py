# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from unittest.mock import MagicMock, patch

from kubernetes import config as k8s_config
from kubernetes.client import (
    V1Container,
    V1EnvVar,
    V1ObjectMeta,
    V1Pod,
    V1PodCondition,
    V1PodSpec,
    V1PodStatus,
)
from kubernetes.client.rest import ApiException
from monarch._src.job.kubernetes import (
    _DEFAULT_MONARCH_PORT,
    _MONARCHMESH_GROUP,
    _MONARCHMESH_PLURAL,
    _MONARCHMESH_VERSION,
    _WORKER_BOOTSTRAP_SCRIPT,
    ImageSpec,
    KubernetesJob,
)


def _make_pod(
    name: str,
    rank: int,
    ready: bool,
    ip: str = "10.0.0.1",
    rank_label: str = "apps.kubernetes.io/pod-index",
    monarch_port: int | None = None,
) -> V1Pod:
    """Build a minimal V1Pod for testing."""
    env = []
    if monarch_port is not None:
        env.append(V1EnvVar(name="MONARCH_PORT", value=str(monarch_port)))

    conditions = []
    if ready:
        conditions.append(V1PodCondition(type="Ready", status="True"))
    else:
        conditions.append(V1PodCondition(type="Ready", status="False"))

    return V1Pod(
        metadata=V1ObjectMeta(
            name=name,
            labels={
                "app.kubernetes.io/name": "monarch-worker",
                "monarch.pytorch.org/mesh-name": "workers",
                rank_label: str(rank),
            },
        ),
        spec=V1PodSpec(
            containers=[V1Container(name="worker", image="test:latest", env=env)],
        ),
        status=V1PodStatus(
            pod_ip=ip,
            conditions=conditions,
        ),
    )


class TestAddMesh(unittest.TestCase):
    """Tests for KubernetesJob.add_mesh name validation and mesh registration."""

    @patch(
        "monarch._src.job.kubernetes.configure",
    )
    def _make_job(self, mock_configure: MagicMock) -> KubernetesJob:
        return KubernetesJob(namespace="default")

    # -- name validation -------------------------------------------------------

    def test_valid_name(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=2)
        self.assertIn("workers", job._meshes)
        job.add_mesh("a" * 63, num_replicas=1)
        self.assertIn("a" * 63, job._meshes)

    def test_empty_name_rejected(self) -> None:
        job = self._make_job()
        with self.assertRaises(ValueError, msg="Empty mesh name"):
            job.add_mesh("", num_replicas=1)

    def test_name_too_long_rejected(self) -> None:
        job = self._make_job()
        with self.assertRaises(ValueError, msg="at most 63"):
            job.add_mesh("a" * 64, num_replicas=1)

    def test_uppercase_rejected(self) -> None:
        job = self._make_job()
        with self.assertRaises(ValueError, msg="lowercase"):
            job.add_mesh("Workers", num_replicas=1)

    def test_special_chars_rejected(self) -> None:
        job = self._make_job()
        for bad in ("my-mesh", "my_mesh", "my.mesh", "mesh!"):
            with self.subTest(name=bad):
                with self.assertRaises(ValueError):
                    job.add_mesh(bad, num_replicas=1)

    def test_starts_with_digit_rejected(self) -> None:
        job = self._make_job()
        with self.assertRaises(ValueError, msg="start with an alphabetic"):
            job.add_mesh("1workers", num_replicas=1)

    def test_max_length_accepted(self) -> None:
        job = self._make_job()
        name = "a" * 63
        job.add_mesh(name, num_replicas=1)
        self.assertIn(name, job._meshes)

    # -- label selector defaults -----------------------------------------------

    def test_default_label_selector(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=2)
        expected = "app.kubernetes.io/name=monarch-worker,monarch.pytorch.org/mesh-name=workers"
        self.assertEqual(job._meshes["workers"]["label_selector"], expected)

    def test_custom_label_selector(self) -> None:
        job = self._make_job()
        selector = "app=custom"
        job.add_mesh("workers", num_replicas=2, label_selector=selector)
        self.assertEqual(job._meshes["workers"]["label_selector"], selector)

    def test_custom_pod_rank_label(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=2, pod_rank_label="custom-rank")
        self.assertEqual(job._meshes["workers"]["pod_rank_label"], "custom-rank")

    def test_default_pod_rank_label(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=2)
        self.assertEqual(
            job._meshes["workers"]["pod_rank_label"],
            "apps.kubernetes.io/pod-index",
        )

    # -- provisioning parameters -----------------------------------------------

    def test_image_marks_provisioned(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=2, image_spec=ImageSpec("myimage:latest"))
        self.assertTrue(job._meshes["workers"]["provisioned"])
        self.assertIn("pod_spec", job._meshes["workers"])

    def test_pod_spec_marks_provisioned(self) -> None:
        job = self._make_job()
        spec = V1PodSpec(
            containers=[V1Container(name="w", image="img")],
        )
        job.add_mesh("workers", num_replicas=1, pod_spec=spec)
        self.assertTrue(job._meshes["workers"]["provisioned"])
        self.assertEqual(job._meshes["workers"]["pod_spec"], spec)

    def test_attach_only_not_provisioned(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1)
        self.assertFalse(job._meshes["workers"]["provisioned"])
        self.assertNotIn("pod_spec", job._meshes["workers"])

    def test_image_and_pod_spec_mutually_exclusive(self) -> None:
        job = self._make_job()
        with self.assertRaises(
            ValueError, msg="image and pod_sepc are mutually exclusive"
        ):
            job.add_mesh(
                "workers",
                num_replicas=1,
                image_spec=ImageSpec("img"),
                pod_spec=V1PodSpec(containers=[]),
            )

    def test_label_selector_forbidden_with_provisioning(self) -> None:
        job = self._make_job()
        with self.assertRaises(
            ValueError, msg="label_selector cannot be specified when image is specified"
        ):
            job.add_mesh(
                "workers",
                num_replicas=1,
                image_spec=ImageSpec("img"),
                label_selector="app=custom",
            )

    def test_pod_rank_label_forbidden_with_provisioning(self) -> None:
        job = self._make_job()
        with self.assertRaises(
            ValueError, msg="pod_rank_label cannot be specified when image is specified"
        ):
            job.add_mesh(
                "workers",
                num_replicas=1,
                image_spec=ImageSpec("img"),
                pod_rank_label="custom-rank",
            )

    def test_custom_port_stored(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1, image_spec=ImageSpec("img"), port=9999)
        self.assertEqual(job._meshes["workers"]["port"], 9999)

    # -- labels ----------------------------------------------------------------

    def test_labels_stored_when_provisioning(self) -> None:
        job = self._make_job()
        labels = {"team": "infra", "env": "staging"}
        job.add_mesh(
            "workers", num_replicas=1, image_spec=ImageSpec("img"), labels=labels
        )
        self.assertEqual(job._meshes["workers"]["labels"], labels)

    def test_labels_forbidden_without_provisioning(self) -> None:
        job = self._make_job()
        with self.assertRaises(
            ValueError, msg="labels can only be set when provisioning"
        ):
            job.add_mesh("workers", num_replicas=1, labels={"team": "infra"})

    def test_no_labels_by_default(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1, image_spec=ImageSpec("img"))
        self.assertNotIn("labels", job._meshes["workers"])


class TestCreate(unittest.TestCase):
    """Tests for KubernetesJob._create guards."""

    @patch("monarch._src.job.kubernetes.configure")
    def _make_job(self, mock_configure: MagicMock) -> KubernetesJob:
        return KubernetesJob(namespace="default")

    def test_batch_script_raises(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1)
        with self.assertRaises(RuntimeError, msg="batch-mode"):
            job._create("some_script.py")

    def test_no_meshes_raises(self) -> None:
        job = self._make_job()
        with self.assertRaises(ValueError, msg="At least one mesh"):
            job._create(None)

    def test_create_noop_with_mesh(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1)
        # Should not raise.
        job._create(None)

    @patch("monarch._src.job.kubernetes.client.ApiClient")
    @patch("monarch._src.job.kubernetes.client.CustomObjectsApi")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_create_creates_crd_for_provisioned_mesh(
        self,
        mock_load_config: MagicMock,
        mock_custom_api_cls: MagicMock,
        mock_api_client_cls: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh(
            "workers", num_replicas=3, image_spec=ImageSpec("myimage:latest"), port=9999
        )

        mock_api = MagicMock()
        mock_custom_api_cls.return_value = mock_api

        # ApiClient.sanitize_for_serialization converts V1PodSpec to dict
        mock_api_client = MagicMock()
        mock_api_client_cls.return_value = mock_api_client
        mock_api_client.sanitize_for_serialization.return_value = {
            "containers": [
                {
                    "name": "worker",
                    "image": "myimage:latest",
                    "command": ["python", "-u", "-c", _WORKER_BOOTSTRAP_SCRIPT],
                    "env": [{"name": "MONARCH_PORT", "value": "9999"}],
                }
            ]
        }

        job._create(None)

        mock_api.create_namespaced_custom_object.assert_called_once()
        call_kwargs = mock_api.create_namespaced_custom_object.call_args
        self.assertEqual(call_kwargs.kwargs["group"], _MONARCHMESH_GROUP)
        self.assertEqual(call_kwargs.kwargs["version"], _MONARCHMESH_VERSION)
        self.assertEqual(call_kwargs.kwargs["namespace"], "default")
        self.assertEqual(call_kwargs.kwargs["plural"], _MONARCHMESH_PLURAL)
        body = call_kwargs.kwargs["body"]
        self.assertEqual(body["metadata"]["name"], "workers")
        self.assertEqual(body["spec"]["replicas"], 3)
        self.assertEqual(
            body["spec"]["podTemplate"]["containers"][0]["image"], "myimage:latest"
        )
        self.assertEqual(body["spec"]["port"], 9999)

    @patch("monarch._src.job.kubernetes.client.ApiClient")
    @patch("monarch._src.job.kubernetes.client.CustomObjectsApi")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_create_includes_labels_in_crd(
        self,
        mock_load_config: MagicMock,
        mock_custom_api_cls: MagicMock,
        mock_api_client_cls: MagicMock,
    ) -> None:
        job = self._make_job()
        labels = {"kueue.x-k8s.io/queue-name": "my-queue", "team": "team"}
        job.add_mesh(
            "workers",
            num_replicas=1,
            image_spec=ImageSpec("img"),
            labels=labels,
        )

        mock_api = MagicMock()
        mock_custom_api_cls.return_value = mock_api

        job._create(None)

        body = mock_api.create_namespaced_custom_object.call_args.kwargs["body"]
        self.assertEqual(body["metadata"]["labels"], labels)

    @patch("monarch._src.job.kubernetes.client.ApiClient")
    @patch("monarch._src.job.kubernetes.client.CustomObjectsApi")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_create_omits_labels_when_not_set(
        self,
        mock_load_config: MagicMock,
        mock_custom_api_cls: MagicMock,
        mock_api_client_cls: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1, image_spec=ImageSpec("img"))

        mock_api = MagicMock()
        mock_custom_api_cls.return_value = mock_api

        job._create(None)

        body = mock_api.create_namespaced_custom_object.call_args.kwargs["body"]
        self.assertNotIn("labels", body["metadata"])

    @patch("monarch._src.job.kubernetes.client.ApiClient")
    @patch("monarch._src.job.kubernetes.client.CustomObjectsApi")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_create_patches_on_conflict(
        self,
        mock_load_config: MagicMock,
        mock_custom_api_cls: MagicMock,
        mock_api_client_cls: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1, image_spec=ImageSpec("img"))

        mock_api = MagicMock()
        mock_custom_api_cls.return_value = mock_api
        mock_api.create_namespaced_custom_object.side_effect = ApiException(
            status=409, reason="Conflict"
        )

        job._create(None)

        mock_api.patch_namespaced_custom_object.assert_called_once()

    @patch("monarch._src.job.kubernetes.client.ApiClient")
    @patch("monarch._src.job.kubernetes.client.CustomObjectsApi")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_create_skips_attach_only_meshes(
        self,
        mock_load_config: MagicMock,
        mock_custom_api_cls: MagicMock,
        mock_api_client_cls: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh("attach", num_replicas=1)
        job.add_mesh("provisioned", num_replicas=1, image_spec=ImageSpec("img"))

        mock_api = MagicMock()
        mock_custom_api_cls.return_value = mock_api

        job._create(None)

        # Only the provisioned mesh should generate a CRD call.
        mock_api.create_namespaced_custom_object.assert_called_once()
        body = mock_api.create_namespaced_custom_object.call_args.kwargs["body"]
        self.assertEqual(body["metadata"]["name"], "provisioned")

    def test_create_noop_when_all_attach_only(self) -> None:
        """No K8s API calls when no meshes are provisioned."""
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1)
        # Should not raise or call any K8s API.
        job._create(None)

    @patch(
        "monarch._src.job.kubernetes.config.load_incluster_config",
        side_effect=k8s_config.ConfigException("not in cluster"),
    )
    def test_create_provisioned_not_in_cluster_raises(
        self,
        mock_load_config: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1, image_spec=ImageSpec("img"))
        with self.assertRaises(RuntimeError, msg="in-cluster"):
            job._create(None)


class TestBuildWorkerPodSpec(unittest.TestCase):
    """Tests for KubernetesJob._build_worker_pod_spec."""

    def test_basic_pod_spec(self) -> None:
        spec = KubernetesJob._build_worker_pod_spec(
            ImageSpec("myimage:latest"), port=26600
        )
        self.assertEqual(len(spec.containers), 1)
        container = spec.containers[0]
        self.assertEqual(container.name, "worker")
        self.assertEqual(container.image, "myimage:latest")
        self.assertEqual(
            container.command, ["python", "-u", "-c", _WORKER_BOOTSTRAP_SCRIPT]
        )
        self.assertEqual(len(container.env), 1)
        self.assertEqual(container.env[0].name, "MONARCH_PORT")
        self.assertEqual(container.env[0].value, "26600")
        self.assertIsNone(container.resources)

    def test_custom_port_in_env(self) -> None:
        spec = KubernetesJob._build_worker_pod_spec(ImageSpec("img"), port=9999)
        self.assertEqual(spec.containers[0].env[0].value, "9999")

    def test_resources_set(self) -> None:
        spec = KubernetesJob._build_worker_pod_spec(
            ImageSpec(
                "img",
                resources={"cpu": "4", "memory": "8Gi", "nvidia.com/gpu": 2},
            ),
            port=26600,
        )
        container = spec.containers[0]
        expected = {"cpu": "4", "memory": "8Gi", "nvidia.com/gpu": "2"}
        self.assertEqual(container.resources.requests, expected)
        self.assertEqual(container.resources.limits, expected)


class TestIsPodWorkerReady(unittest.TestCase):
    """Tests for KubernetesJob._is_pod_worker_ready."""

    @patch("monarch._src.job.kubernetes.configure")
    def _make_job(self, mock_configure: MagicMock) -> KubernetesJob:
        return KubernetesJob(namespace="default")

    def test_ready_pod(self) -> None:
        job = self._make_job()
        pod = _make_pod("w-0", rank=0, ready=True)
        self.assertTrue(job._is_pod_worker_ready(pod))

    def test_not_ready_pod(self) -> None:
        job = self._make_job()
        pod = _make_pod("w-0", rank=0, ready=False)
        self.assertFalse(job._is_pod_worker_ready(pod))

    def test_no_conditions(self) -> None:
        job = self._make_job()
        pod = _make_pod("w-0", rank=0, ready=True)
        pod.status.conditions = None
        self.assertFalse(job._is_pod_worker_ready(pod))

    def test_no_ready_condition(self) -> None:
        job = self._make_job()
        pod = _make_pod("w-0", rank=0, ready=True)
        pod.status.conditions = [
            V1PodCondition(type="Initialized", status="True"),
        ]
        self.assertFalse(job._is_pod_worker_ready(pod))


class TestGetPodRank(unittest.TestCase):
    """Tests for KubernetesJob._get_pod_rank."""

    @patch("monarch._src.job.kubernetes.configure")
    def _make_job(self, mock_configure: MagicMock) -> KubernetesJob:
        return KubernetesJob(namespace="default")

    def test_valid_rank(self) -> None:
        job = self._make_job()
        pod = _make_pod("w-0", rank=3, ready=True)
        self.assertEqual(job._get_pod_rank(pod, "apps.kubernetes.io/pod-index"), 3)

    def test_missing_labels(self) -> None:
        job = self._make_job()
        pod = _make_pod("w-0", rank=0, ready=True)
        pod.metadata.labels = None
        with self.assertRaises(ValueError, msg="no labels"):
            job._get_pod_rank(pod, "apps.kubernetes.io/pod-index")

    def test_missing_rank_label(self) -> None:
        job = self._make_job()
        pod = _make_pod("w-0", rank=0, ready=True)
        with self.assertRaises(ValueError, msg="missing required label"):
            job._get_pod_rank(pod, "nonexistent-label")

    def test_non_integer_rank(self) -> None:
        job = self._make_job()
        pod = _make_pod("w-0", rank=0, ready=True)
        pod.metadata.labels["apps.kubernetes.io/pod-index"] = "abc"
        with self.assertRaises(ValueError, msg="invalid pod rank"):
            job._get_pod_rank(pod, "apps.kubernetes.io/pod-index")


class TestDiscoverMonarchPort(unittest.TestCase):
    """Tests for KubernetesJob._discover_monarch_port."""

    @patch("monarch._src.job.kubernetes.configure")
    def _make_job(self, mock_configure: MagicMock) -> KubernetesJob:
        return KubernetesJob(namespace="default")

    def test_default_port(self) -> None:
        job = self._make_job()
        pod = _make_pod("w-0", rank=0, ready=True)
        self.assertEqual(job._discover_monarch_port(pod), _DEFAULT_MONARCH_PORT)

    def test_custom_port_from_env(self) -> None:
        job = self._make_job()
        pod = _make_pod("w-0", rank=0, ready=True, monarch_port=12345)
        self.assertEqual(job._discover_monarch_port(pod), 12345)

    def test_invalid_port_falls_back_to_default(self) -> None:
        job = self._make_job()
        pod = _make_pod("w-0", rank=0, ready=True)
        pod.spec.containers[0].env = [
            V1EnvVar(name="MONARCH_PORT", value="not_a_number"),
        ]
        self.assertEqual(job._discover_monarch_port(pod), _DEFAULT_MONARCH_PORT)

    def test_empty_port_value_falls_back_to_default(self) -> None:
        job = self._make_job()
        pod = _make_pod("w-0", rank=0, ready=True)
        pod.spec.containers[0].env = [
            V1EnvVar(name="MONARCH_PORT", value=""),
        ]
        self.assertEqual(job._discover_monarch_port(pod), _DEFAULT_MONARCH_PORT)


class TestWaitForReadyPods(unittest.TestCase):
    """Tests for KubernetesJob._wait_for_ready_pods using mocked Watch."""

    @patch("monarch._src.job.kubernetes.configure")
    def _make_job(self, mock_configure: MagicMock) -> KubernetesJob:
        return KubernetesJob(namespace="test-ns")

    @patch("monarch._src.job.kubernetes.watch.Watch")
    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_all_pods_ready(
        self,
        mock_load_config: MagicMock,
        mock_core_api: MagicMock,
        mock_watch_cls: MagicMock,
    ) -> None:
        job = self._make_job()

        mock_watch = MagicMock()
        mock_watch_cls.return_value = mock_watch
        mock_watch.stream.return_value = [
            {"type": "ADDED", "object": _make_pod("w-0", 0, True, ip="10.0.0.1")},
            {"type": "ADDED", "object": _make_pod("w-1", 1, True, ip="10.0.0.2")},
        ]

        result = job._wait_for_ready_pods(
            label_selector="app=test",
            num_replicas=2,
            pod_rank_label="apps.kubernetes.io/pod-index",
        )

        self.assertEqual(result, [("10.0.0.1", 26600), ("10.0.0.2", 26600)])
        mock_watch.stop.assert_called_once()

    @patch("monarch._src.job.kubernetes.watch.Watch")
    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_pod_becomes_ready_on_modified(
        self,
        mock_load_config: MagicMock,
        mock_core_api: MagicMock,
        mock_watch_cls: MagicMock,
    ) -> None:
        job = self._make_job()

        pod0_not_ready = _make_pod("w-0", 0, False, ip="10.0.0.1")
        pod0_ready = _make_pod("w-0", 0, True, ip="10.0.0.1")

        mock_watch = MagicMock()
        mock_watch_cls.return_value = mock_watch
        mock_watch.stream.return_value = [
            {"type": "ADDED", "object": pod0_not_ready},
            {"type": "ADDED", "object": _make_pod("w-1", 1, True, ip="10.0.0.2")},
            {"type": "MODIFIED", "object": pod0_ready},
        ]

        result = job._wait_for_ready_pods(
            label_selector="app=test",
            num_replicas=2,
            pod_rank_label="apps.kubernetes.io/pod-index",
        )

        self.assertEqual(result, [("10.0.0.1", 26600), ("10.0.0.2", 26600)])

    @patch("monarch._src.job.kubernetes.watch.Watch")
    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_deleted_pod_removed(
        self,
        mock_load_config: MagicMock,
        mock_core_api: MagicMock,
        mock_watch_cls: MagicMock,
    ) -> None:
        """A pod that is deleted should be removed from the ready set; the watch
        must continue until a replacement arrives."""
        job = self._make_job()

        mock_watch = MagicMock()
        mock_watch_cls.return_value = mock_watch
        mock_watch.stream.return_value = [
            # Only rank 0 initially; rank 1 not yet present.
            {"type": "ADDED", "object": _make_pod("w-0", 0, True, ip="10.0.0.1")},
            # delete rank 0 before rank 1 appears
            {"type": "DELETED", "object": _make_pod("w-0", 0, True, ip="10.0.0.1")},
            # replacement for rank 0
            {"type": "ADDED", "object": _make_pod("w-0", 0, True, ip="10.0.0.3")},
            # rank 1 arrives
            {"type": "ADDED", "object": _make_pod("w-1", 1, True, ip="10.0.0.2")},
        ]

        result = job._wait_for_ready_pods(
            label_selector="app=test",
            num_replicas=2,
            pod_rank_label="apps.kubernetes.io/pod-index",
        )

        # Rank 0 should be the replacement IP.
        self.assertEqual(result[0], ("10.0.0.3", 26600))
        self.assertEqual(result[1], ("10.0.0.2", 26600))

    @patch("monarch._src.job.kubernetes.watch.Watch")
    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_error_event_raises(
        self,
        mock_load_config: MagicMock,
        mock_core_api: MagicMock,
        mock_watch_cls: MagicMock,
    ) -> None:
        job = self._make_job()

        mock_watch = MagicMock()
        mock_watch_cls.return_value = mock_watch
        mock_watch.stream.return_value = [
            {"type": "ERROR", "object": {"message": "gone"}},
        ]

        with self.assertRaises(RuntimeError, msg="Watch error"):
            job._wait_for_ready_pods(
                label_selector="app=test",
                num_replicas=1,
                pod_rank_label="apps.kubernetes.io/pod-index",
            )

    @patch("monarch._src.job.kubernetes.watch.Watch")
    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_watch_ends_missing_ranks(
        self,
        mock_load_config: MagicMock,
        mock_core_api: MagicMock,
        mock_watch_cls: MagicMock,
    ) -> None:
        """Watch ending before all ranks are ready should raise."""
        job = self._make_job()

        mock_watch = MagicMock()
        mock_watch_cls.return_value = mock_watch
        mock_watch.stream.return_value = [
            {"type": "ADDED", "object": _make_pod("w-0", 0, True, ip="10.0.0.1")},
            # rank 1 never appears, stream ends
        ]

        with self.assertRaises(RuntimeError, msg="Missing ranks"):
            job._wait_for_ready_pods(
                label_selector="app=test",
                num_replicas=2,
                pod_rank_label="apps.kubernetes.io/pod-index",
            )

    @patch("monarch._src.job.kubernetes.watch.Watch")
    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_out_of_range_ranks_ignored(
        self,
        mock_load_config: MagicMock,
        mock_core_api: MagicMock,
        mock_watch_cls: MagicMock,
    ) -> None:
        job = self._make_job()

        mock_watch = MagicMock()
        mock_watch_cls.return_value = mock_watch
        mock_watch.stream.return_value = [
            # Out-of-range rank should be skipped.
            {"type": "ADDED", "object": _make_pod("w-99", 99, True, ip="10.0.99.1")},
            {"type": "ADDED", "object": _make_pod("w-0", 0, True, ip="10.0.0.1")},
        ]

        result = job._wait_for_ready_pods(
            label_selector="app=test",
            num_replicas=1,
            pod_rank_label="apps.kubernetes.io/pod-index",
        )

        self.assertEqual(result, [("10.0.0.1", 26600)])

    @patch(
        "monarch._src.job.kubernetes.config.load_incluster_config",
        side_effect=k8s_config.ConfigException("not in cluster"),
    )
    def test_not_in_cluster_raises(self, mock_load_config: MagicMock) -> None:
        job = self._make_job()
        with self.assertRaises(RuntimeError, msg="in-cluster"):
            job._wait_for_ready_pods(
                label_selector="app=test",
                num_replicas=1,
                pod_rank_label="apps.kubernetes.io/pod-index",
            )

    @patch("monarch._src.job.kubernetes.watch.Watch")
    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_custom_monarch_port_discovered(
        self,
        mock_load_config: MagicMock,
        mock_core_api: MagicMock,
        mock_watch_cls: MagicMock,
    ) -> None:
        job = self._make_job()

        mock_watch = MagicMock()
        mock_watch_cls.return_value = mock_watch
        mock_watch.stream.return_value = [
            {
                "type": "ADDED",
                "object": _make_pod("w-0", 0, True, ip="10.0.0.1", monarch_port=9999),
            },
        ]

        result = job._wait_for_ready_pods(
            label_selector="app=test",
            num_replicas=1,
            pod_rank_label="apps.kubernetes.io/pod-index",
        )

        self.assertEqual(result, [("10.0.0.1", 9999)])


class TestVerifyPodsReady(unittest.TestCase):
    """Tests for KubernetesJob._verify_pods_ready."""

    @patch("monarch._src.job.kubernetes.configure")
    def _make_job(self, mock_configure: MagicMock) -> KubernetesJob:
        return KubernetesJob(namespace="test-ns")

    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_matching_endpoints_returns_true(
        self,
        mock_load_config: MagicMock,
        mock_core_api_cls: MagicMock,
    ) -> None:
        job = self._make_job()

        mock_api = MagicMock()
        mock_core_api_cls.return_value = mock_api
        mock_api.list_namespaced_pod.return_value = MagicMock(
            items=[
                _make_pod("w-0", 0, True, ip="10.0.0.1"),
                _make_pod("w-1", 1, True, ip="10.0.0.2"),
            ]
        )

        result = job._verify_pods_ready(
            label_selector="app=test",
            num_replicas=2,
            pod_rank_label="apps.kubernetes.io/pod-index",
            expected_endpoints=[("10.0.0.1", 26600), ("10.0.0.2", 26600)],
        )
        self.assertTrue(result)

    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_changed_ip_returns_false(
        self,
        mock_load_config: MagicMock,
        mock_core_api_cls: MagicMock,
    ) -> None:
        job = self._make_job()

        mock_api = MagicMock()
        mock_core_api_cls.return_value = mock_api
        mock_api.list_namespaced_pod.return_value = MagicMock(
            items=[
                _make_pod("w-0", 0, True, ip="10.0.0.99"),
                _make_pod("w-1", 1, True, ip="10.0.0.2"),
            ]
        )

        result = job._verify_pods_ready(
            label_selector="app=test",
            num_replicas=2,
            pod_rank_label="apps.kubernetes.io/pod-index",
            expected_endpoints=[("10.0.0.1", 26600), ("10.0.0.2", 26600)],
        )
        self.assertFalse(result)

    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_pod_no_longer_ready_returns_false(
        self,
        mock_load_config: MagicMock,
        mock_core_api_cls: MagicMock,
    ) -> None:
        job = self._make_job()

        mock_api = MagicMock()
        mock_core_api_cls.return_value = mock_api
        mock_api.list_namespaced_pod.return_value = MagicMock(
            items=[
                _make_pod("w-0", 0, False, ip="10.0.0.1"),
                _make_pod("w-1", 1, True, ip="10.0.0.2"),
            ]
        )

        result = job._verify_pods_ready(
            label_selector="app=test",
            num_replicas=2,
            pod_rank_label="apps.kubernetes.io/pod-index",
            expected_endpoints=[("10.0.0.1", 26600), ("10.0.0.2", 26600)],
        )
        self.assertFalse(result)

    @patch(
        "monarch._src.job.kubernetes.config.load_incluster_config",
        side_effect=k8s_config.ConfigException("not in cluster"),
    )
    def test_config_failure_returns_false(
        self,
        mock_load_config: MagicMock,
    ) -> None:
        job = self._make_job()
        result = job._verify_pods_ready(
            label_selector="app=test",
            num_replicas=1,
            pod_rank_label="apps.kubernetes.io/pod-index",
            expected_endpoints=[("10.0.0.1", 26600)],
        )
        self.assertFalse(result)


class TestStateDurability(unittest.TestCase):
    """Tests for _state() retry logic: discover all meshes, then verify all."""

    @patch("monarch._src.job.kubernetes.configure")
    def _make_job(
        self, mock_configure: MagicMock, max_retries: int = 3
    ) -> KubernetesJob:
        return KubernetesJob(namespace="test-ns", max_retries=max_retries)

    @patch("monarch._src.job.kubernetes.attach_to_workers")
    @patch.object(KubernetesJob, "_verify_all_mesh_endpoints", return_value=True)
    @patch.object(
        KubernetesJob,
        "_discover_all_mesh_endpoints",
        return_value={"workers": [("10.0.0.1", 26600)]},
    )
    def test_state_succeeds_on_first_attempt(
        self,
        mock_discover: MagicMock,
        mock_verify: MagicMock,
        mock_attach: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1)
        mock_attach.return_value = MagicMock()

        job._state()

        mock_discover.assert_called_once()
        mock_verify.assert_called_once()
        mock_attach.assert_called_once_with(
            name="workers",
            ca="trust_all_connections",
            workers=["tcp://10.0.0.1:26600"],
        )

    @patch("monarch._src.job.kubernetes.attach_to_workers")
    @patch.object(
        KubernetesJob,
        "_verify_all_mesh_endpoints",
        side_effect=[False, True],
    )
    @patch.object(
        KubernetesJob,
        "_discover_all_mesh_endpoints",
        side_effect=[
            {"workers": [("10.0.0.1", 26600)]},
            {"workers": [("10.0.0.99", 26600)]},
        ],
    )
    def test_state_retries_on_verification_failure(
        self,
        mock_discover: MagicMock,
        mock_verify: MagicMock,
        mock_attach: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1)
        mock_attach.return_value = MagicMock()

        job._state()

        self.assertEqual(mock_discover.call_count, 2)
        self.assertEqual(mock_verify.call_count, 2)
        mock_attach.assert_called_once_with(
            name="workers",
            ca="trust_all_connections",
            workers=["tcp://10.0.0.99:26600"],
        )

    @patch.object(KubernetesJob, "_verify_all_mesh_endpoints", return_value=False)
    @patch.object(
        KubernetesJob,
        "_discover_all_mesh_endpoints",
        return_value={"workers": [("10.0.0.1", 26600)]},
    )
    def test_state_raises_after_max_retries(
        self,
        mock_discover: MagicMock,
        mock_verify: MagicMock,
    ) -> None:
        job = self._make_job(max_retries=2)
        job.add_mesh("workers", num_replicas=1)
        with self.assertRaises(RuntimeError, msg="Failed to get stable mesh endpoints"):
            job._state()
        self.assertEqual(mock_discover.call_count, 2)
        self.assertEqual(mock_verify.call_count, 2)

    @patch("monarch._src.job.kubernetes.attach_to_workers")
    @patch.object(
        KubernetesJob,
        "_verify_all_mesh_endpoints",
        side_effect=[False, True],
    )
    @patch.object(
        KubernetesJob,
        "_discover_all_mesh_endpoints",
        side_effect=[
            {
                "mesh1": [("10.0.0.1", 26600)],
                "mesh2": [("10.0.1.1", 26600)],
            },
            {
                "mesh1": [("10.0.0.1", 26600)],
                "mesh2": [("10.0.1.99", 26600)],
            },
        ],
    )
    def test_state_retries_all_meshes_together(
        self,
        mock_discover: MagicMock,
        mock_verify: MagicMock,
        mock_attach: MagicMock,
    ) -> None:
        """When one mesh's pods change, all meshes are re-discovered."""
        job = self._make_job()
        job.add_mesh("mesh1", num_replicas=1)
        job.add_mesh("mesh2", num_replicas=1)
        mock_attach.return_value = MagicMock()

        job._state()

        self.assertEqual(mock_discover.call_count, 2)
        self.assertEqual(mock_attach.call_count, 2)


class TestKill(unittest.TestCase):
    """Tests for KubernetesJob._kill."""

    @patch("monarch._src.job.kubernetes.configure")
    def _make_job(self, mock_configure: MagicMock) -> KubernetesJob:
        return KubernetesJob(namespace="default")

    def test_kill_attach_only_raises_not_implemented(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1)
        with self.assertRaises(NotImplementedError):
            job._kill()

    @patch("monarch._src.job.kubernetes.client.CustomObjectsApi")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_kill_deletes_provisioned_crds(
        self,
        mock_load_config: MagicMock,
        mock_custom_api_cls: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=2, image_spec=ImageSpec("img"))

        mock_api = MagicMock()
        mock_custom_api_cls.return_value = mock_api

        job._kill()

        mock_api.delete_namespaced_custom_object.assert_called_once_with(
            group=_MONARCHMESH_GROUP,
            version=_MONARCHMESH_VERSION,
            namespace="default",
            plural=_MONARCHMESH_PLURAL,
            name="workers",
        )

    @patch("monarch._src.job.kubernetes.client.CustomObjectsApi")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_kill_ignores_404(
        self,
        mock_load_config: MagicMock,
        mock_custom_api_cls: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1, image_spec=ImageSpec("img"))

        mock_api = MagicMock()
        mock_custom_api_cls.return_value = mock_api
        mock_api.delete_namespaced_custom_object.side_effect = ApiException(
            status=404, reason="Not Found"
        )

        # Should not raise.
        job._kill()

    @patch("monarch._src.job.kubernetes.client.CustomObjectsApi")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_kill_reraises_non_404(
        self,
        mock_load_config: MagicMock,
        mock_custom_api_cls: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1, image_spec=ImageSpec("img"))

        mock_api = MagicMock()
        mock_custom_api_cls.return_value = mock_api
        mock_api.delete_namespaced_custom_object.side_effect = ApiException(
            status=500, reason="Internal Server Error"
        )

        with self.assertRaises(ApiException):
            job._kill()


class TestCanRun(unittest.TestCase):
    """Tests for KubernetesJob.can_run."""

    @patch("monarch._src.job.kubernetes.configure")
    def _make_job(
        self, mock_configure: MagicMock, namespace: str = "default"
    ) -> KubernetesJob:
        return KubernetesJob(namespace=namespace)

    def test_different_type_returns_false(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1)
        self.assertFalse(job.can_run(MagicMock()))

    def test_different_namespace_returns_false(self) -> None:
        job = self._make_job(namespace="ns1")
        job.add_mesh("workers", num_replicas=1)
        job._status = "running"

        spec = self._make_job(namespace="ns2")
        spec.add_mesh("workers", num_replicas=1)
        self.assertFalse(job.can_run(spec))

    def test_different_meshes_returns_false(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1)
        job._status = "running"

        spec = self._make_job()
        spec.add_mesh("trainers", num_replicas=1)
        self.assertFalse(job.can_run(spec))

    def test_not_active_returns_false(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1)
        # Not active (status is "not_running").

        spec = self._make_job()
        spec.add_mesh("workers", num_replicas=1)
        self.assertFalse(job.can_run(spec))

    @patch("monarch._src.job.kubernetes.watch.Watch")
    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_matching_and_active_returns_true(
        self,
        mock_load_config: MagicMock,
        mock_core_api: MagicMock,
        mock_watch_cls: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1)
        job._status = "running"

        mock_watch = MagicMock()
        mock_watch_cls.return_value = mock_watch
        mock_watch.stream.return_value = [
            {"type": "ADDED", "object": _make_pod("w-0", 0, True, ip="10.0.0.1")},
        ]

        spec = self._make_job()
        spec.add_mesh("workers", num_replicas=1)
        self.assertTrue(job.can_run(spec))

    @patch(
        "monarch._src.job.kubernetes.config.load_incluster_config",
        side_effect=RuntimeError("not in cluster"),
    )
    def test_pods_unavailable_returns_false(
        self,
        mock_load_config: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1)
        job._status = "running"

        spec = self._make_job()
        spec.add_mesh("workers", num_replicas=1)
        self.assertFalse(job.can_run(spec))


if __name__ == "__main__":
    unittest.main()
