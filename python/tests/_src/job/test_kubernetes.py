# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import copy
import json
import os
import pickle
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest.mock import call, MagicMock, patch

from kubernetes import config as k8s_config
from kubernetes.client import (
    V1Container,
    V1EnvVar,
    V1ObjectMeta,
    V1Pod,
    V1PodCondition,
    V1PodSpec,
    V1PodStatus,
    V1PodTemplateSpec,
)
from kubernetes.client.rest import ApiException
from monarch._src.job.job import LocalJob
from monarch._src.job.kubernetes import (
    _DEFAULT_MONARCH_PORT,
    _MONARCHMESH_GROUP,
    _MONARCHMESH_PLURAL,
    _MONARCHMESH_VERSION,
    _MonarchMeshPod,
    _PORT_FORWARD_TERMINATE_TIMEOUT_SECONDS,
    _WORKER_BOOTSTRAP_SCRIPT,
    ImageSpec,
    KubeConfig,
    KubernetesJob,
)
from monarch._src.job.service_identity import (
    serialize_service_proc_ids,
    SERVICE_PROC_IDS_ENV,
    SERVICE_PROC_RANK_ENV,
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


def _write_exec_kubeconfig(
    path: Path, plugin: str, *, provide_cluster_info: bool = False
) -> None:
    path.write_text(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "current-context": "test-context",
                "clusters": [
                    {
                        "name": "test-cluster",
                        "cluster": {"server": "https://example.invalid"},
                    }
                ],
                "contexts": [
                    {
                        "name": "test-context",
                        "context": {
                            "cluster": "test-cluster",
                            "user": "test-user",
                        },
                    }
                ],
                "users": [
                    {
                        "name": "test-user",
                        "user": {
                            "exec": {
                                "apiVersion": "client.authentication.k8s.io/v1",
                                "command": sys.executable,
                                "args": ["-c", plugin],
                                "provideClusterInfo": provide_cluster_info,
                            }
                        },
                    }
                ],
            }
        )
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
        image_spec = ImageSpec("myimage:latest")
        job.add_mesh("workers", num_replicas=2, image_spec=image_spec)
        self.assertTrue(job._meshes["workers"]["provisioned"])
        self.assertEqual(job._meshes["workers"]["image_spec"], image_spec)
        self.assertNotIn("pod_template", job._meshes["workers"])
        self.assertEqual(job._service_proc_ids, {})

    def test_pod_template_marks_provisioned(self) -> None:
        job = self._make_job()
        template = V1PodTemplateSpec(
            spec=V1PodSpec(containers=[V1Container(name="w", image="img")]),
        )
        job.add_mesh("workers", num_replicas=1, pod_template=template)
        self.assertTrue(job._meshes["workers"]["provisioned"])
        self.assertEqual(job._meshes["workers"]["pod_template"], template)

    def test_attach_only_not_provisioned(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1)
        self.assertFalse(job._meshes["workers"]["provisioned"])
        self.assertNotIn("pod_template", job._meshes["workers"])

    def test_image_and_pod_template_mutually_exclusive(self) -> None:
        job = self._make_job()
        with self.assertRaises(
            ValueError, msg="image and pod_template are mutually exclusive"
        ):
            job.add_mesh(
                "workers",
                num_replicas=1,
                image_spec=ImageSpec("img"),
                pod_template=V1PodTemplateSpec(spec=V1PodSpec(containers=[])),
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

    # -- annotations -----------------------------------------------------------

    def test_annotations_stored_when_provisioning(self) -> None:
        job = self._make_job()
        annotations = {"team": "infra", "scheduler": "kueue"}
        job.add_mesh(
            "workers",
            num_replicas=1,
            image_spec=ImageSpec("img"),
            annotations=annotations,
        )
        self.assertEqual(job._meshes["workers"]["annotations"], annotations)

    def test_annotations_forbidden_without_provisioning(self) -> None:
        job = self._make_job()
        with self.assertRaises(
            ValueError, msg="annotations can only be set when provisioning"
        ):
            job.add_mesh("workers", num_replicas=1, annotations={"team": "infra"})

    def test_no_annotations_by_default(self) -> None:
        job = self._make_job()
        job.add_mesh("workers", num_replicas=1, image_spec=ImageSpec("img"))
        self.assertNotIn("annotations", job._meshes["workers"])


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

        # ApiClient.sanitize_for_serialization converts V1PodTemplateSpec to dict
        mock_api_client = MagicMock()
        mock_api_client_cls.return_value = mock_api_client
        mock_api_client.sanitize_for_serialization.return_value = {
            "spec": {
                "containers": [
                    {
                        "name": "worker",
                        "image": "myimage:latest",
                        "command": ["python", "-u", "-c", _WORKER_BOOTSTRAP_SCRIPT],
                        "env": [{"name": "MONARCH_PORT", "value": "9999"}],
                    }
                ]
            }
        }

        job._create(None)

        service_proc_ids = job._service_proc_ids["workers"]
        self.assertEqual(len(service_proc_ids), 3)
        self.assertEqual(len(set(service_proc_ids)), 3)
        pod_template = mock_api_client.sanitize_for_serialization.call_args.args[0]
        self.assertEqual(
            pod_template.spec.containers[0].env[2].value,
            serialize_service_proc_ids(service_proc_ids),
        )
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
            body["spec"]["podTemplate"]["spec"]["containers"][0]["image"],
            "myimage:latest",
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
    def test_create_includes_annotations_in_crd(
        self,
        mock_load_config: MagicMock,
        mock_custom_api_cls: MagicMock,
        mock_api_client_cls: MagicMock,
    ) -> None:
        job = self._make_job()
        annotations = {"kueue.x-k8s.io/queue-name": "my-queue", "owner": "team"}
        job.add_mesh(
            "workers",
            num_replicas=1,
            image_spec=ImageSpec("img"),
            annotations=annotations,
        )

        mock_api = MagicMock()
        mock_custom_api_cls.return_value = mock_api

        job._create(None)

        body = mock_api.create_namespaced_custom_object.call_args.kwargs["body"]
        self.assertEqual(body["metadata"]["annotations"], annotations)

    @patch("monarch._src.job.kubernetes.client.ApiClient")
    @patch("monarch._src.job.kubernetes.client.CustomObjectsApi")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_create_omits_annotations_when_not_set(
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
        self.assertNotIn("annotations", body["metadata"])

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

    @patch("monarch._src.job.kubernetes.client.CustomObjectsApi")
    @patch("monarch._src.job.kubernetes.config.load_incluster_config")
    def test_create_preserves_pod_template_metadata(
        self,
        mock_load_config: MagicMock,
        mock_custom_api_cls: MagicMock,
    ) -> None:
        """Pod-level labels/annotations on ``pod_template`` reach the CRD body."""
        job = self._make_job()
        pod_template = V1PodTemplateSpec(
            metadata=V1ObjectMeta(
                labels={"pod-label": "pod-value"},
                annotations={"pod-annotation": "ann-value"},
            ),
            spec=V1PodSpec(containers=[V1Container(name="worker", image="img")]),
        )
        job.add_mesh("workers", num_replicas=1, pod_template=pod_template)

        mock_api = MagicMock()
        mock_custom_api_cls.return_value = mock_api

        job._create(None)

        body = mock_api.create_namespaced_custom_object.call_args.kwargs["body"]
        template_metadata = body["spec"]["podTemplate"]["metadata"]
        self.assertEqual(template_metadata["labels"], {"pod-label": "pod-value"})
        self.assertEqual(
            template_metadata["annotations"], {"pod-annotation": "ann-value"}
        )

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


class TestBuildWorkerPodTemplate(unittest.TestCase):
    """Tests for KubernetesJob._build_worker_pod_template."""

    def test_basic_pod_template(self) -> None:
        service_proc_ids = KubernetesJob._allocate_service_proc_ids(1)
        template = KubernetesJob._build_worker_pod_template(
            ImageSpec("myimage:latest"),
            port=26600,
            service_proc_ids=service_proc_ids,
        )
        self.assertEqual(len(template.spec.containers), 1)
        container = template.spec.containers[0]
        self.assertEqual(container.name, "worker")
        self.assertEqual(container.image, "myimage:latest")
        self.assertEqual(
            container.command, ["python", "-u", "-c", _WORKER_BOOTSTRAP_SCRIPT]
        )
        env = {entry.name: entry for entry in container.env}
        self.assertEqual(env["MONARCH_PORT"].value, "26600")
        self.assertEqual(
            env[SERVICE_PROC_RANK_ENV].value_from.field_ref.field_path,
            "metadata.labels['apps.kubernetes.io/pod-index']",
        )
        self.assertEqual(
            env[SERVICE_PROC_IDS_ENV].value,
            serialize_service_proc_ids(service_proc_ids),
        )
        self.assertIsNone(container.resources)

    def test_custom_port_in_env(self) -> None:
        template = KubernetesJob._build_worker_pod_template(
            ImageSpec("img"),
            port=9999,
            service_proc_ids=KubernetesJob._allocate_service_proc_ids(1),
        )
        self.assertEqual(template.spec.containers[0].env[0].value, "9999")

    def test_resources_set(self) -> None:
        template = KubernetesJob._build_worker_pod_template(
            ImageSpec(
                "img",
                resources={"cpu": "4", "memory": "8Gi", "nvidia.com/gpu": 2},
            ),
            port=26600,
            service_proc_ids=KubernetesJob._allocate_service_proc_ids(1),
        )
        container = template.spec.containers[0]
        expected = {"cpu": "4", "memory": "8Gi", "nvidia.com/gpu": "2"}
        self.assertEqual(container.resources.requests, expected)
        self.assertEqual(container.resources.limits, expected)


class KubeConfigTest(unittest.TestCase):
    """Tests for KubeConfig loading."""

    @patch("monarch._src.job.kubernetes.client.Configuration.set_default")
    @patch("monarch._src.job.kubernetes.load_kube_config")
    def test_local_load_preserves_proxy_url(
        self,
        mock_load_kube_config: MagicMock,
        mock_set_default: MagicMock,
    ) -> None:
        with NamedTemporaryFile("w") as kubeconfig:
            kubeconfig.write(
                """
apiVersion: v1
kind: Config
current-context: test-context
clusters:
  - name: test-cluster
    cluster:
      server: https://example.invalid
      proxy-url: http://fwdproxy:8080
contexts:
  - name: test-context
    context:
      cluster: test-cluster
      user: test-user
users:
  - name: test-user
    user:
      token: test-token
"""
            )
            kubeconfig.flush()
            configuration = MagicMock()
            mock_load_kube_config.return_value = configuration

            KubeConfig.from_path(kubeconfig.name).load()

        mock_load_kube_config.assert_called_once_with(Path(kubeconfig.name))
        self.assertEqual(configuration.proxy, "http://fwdproxy:8080")
        mock_set_default.assert_called_once_with(configuration)

    def test_local_load_malformed_kubeconfig_raises(self) -> None:
        # current-context references a context that does not exist, so proxy-url
        # resolution fails. The error must surface as a RuntimeError, not a raw
        # traceback from the underlying kubeconfig parsing.
        with NamedTemporaryFile("w") as kubeconfig:
            kubeconfig.write(
                """
apiVersion: v1
kind: Config
current-context: missing-context
clusters: []
contexts: []
users: []
"""
            )
            kubeconfig.flush()
            with self.assertRaises(RuntimeError, msg="kubeconfig"):
                KubeConfig.from_path(kubeconfig.name).load()

    @patch("monarch._src.job.kubernetes.client.Configuration.set_default")
    def test_cached_path_loads_yaml_exec_credentials_and_refreshes(
        self, mock_set_default: MagicMock
    ) -> None:
        with TemporaryDirectory() as directory:
            counter = Path(directory) / "count"
            plugin = (
                "import json, os; from pathlib import Path; "
                "assert json.loads(os.environ['KUBERNETES_EXEC_INFO'])"
                "['kind'] == 'ExecCredential'; "
                f"path = Path({str(counter)!r}); "
                "count = int(path.read_text()) + 1 if path.exists() else 1; "
                "path.write_text(str(count)); "
                "print('apiVersion: client.authentication.k8s.io/v1\\n' "
                "      'kind: ExecCredential\\n' "
                "      'status:\\n' "
                "      '  expirationTimestamp: 2000-01-01T00:00:00Z\\n' "
                "      f'  token: yaml-token-{count}')"
            )
            config_path = Path(directory) / "config"
            _write_exec_kubeconfig(config_path, plugin)

            restored = pickle.loads(
                pickle.dumps(KubeConfig.from_path(str(config_path)))
            )
            restored.load()

            configuration = mock_set_default.call_args.args[0]
            self.assertIn("Bearer yaml-token-1", configuration.api_key.values())
            self.assertIsNotNone(configuration.refresh_api_key_hook)
            configuration.refresh_api_key_hook(configuration)
            self.assertIn("Bearer yaml-token-2", configuration.api_key.values())

    @patch("monarch._src.job.kubernetes.client.Configuration.set_default")
    def test_local_load_preserves_json_exec_credentials(
        self, mock_set_default: MagicMock
    ) -> None:
        credential = json.dumps(
            {
                "apiVersion": "client.authentication.k8s.io/v1",
                "kind": "ExecCredential",
                "status": {"token": "json-token"},
            }
        )
        with NamedTemporaryFile("w") as kubeconfig:
            _write_exec_kubeconfig(Path(kubeconfig.name), f"print({credential!r})")
            kubeconfig.flush()

            KubeConfig.from_path(kubeconfig.name).load()

        configuration = mock_set_default.call_args.args[0]
        self.assertIn("Bearer json-token", configuration.api_key.values())

    @patch("monarch._src.job.kubernetes.client.Configuration.set_default")
    def test_exec_plugin_runs_from_kubeconfig_directory(
        self, mock_set_default: MagicMock
    ) -> None:
        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "config"
            (Path(directory) / "token").write_text("relative-token")
            plugin = (
                "from pathlib import Path; "
                "token = Path('token').read_text(); "
                "print('apiVersion: client.authentication.k8s.io/v1\\n' "
                "      'kind: ExecCredential\\n' "
                "      'status:\\n' "
                "      f'  token: {token}')"
            )
            _write_exec_kubeconfig(config_path, plugin)

            KubeConfig.from_path(str(config_path)).load()

        configuration = mock_set_default.call_args.args[0]
        self.assertIn("Bearer relative-token", configuration.api_key.values())

    @patch("monarch._src.job.kubernetes.client.Configuration.set_default")
    def test_exec_plugin_receives_requested_cluster_info(
        self, mock_set_default: MagicMock
    ) -> None:
        plugin = (
            "import json, os; "
            "info = json.loads(os.environ['KUBERNETES_EXEC_INFO']); "
            "assert info['spec']['cluster']['server'] == "
            "'https://example.invalid'; "
            "print('apiVersion: client.authentication.k8s.io/v1\\n' "
            "      'kind: ExecCredential\\n' "
            "      'status:\\n' "
            "      '  token: cluster-info-token')"
        )
        with NamedTemporaryFile("w") as kubeconfig:
            _write_exec_kubeconfig(
                Path(kubeconfig.name), plugin, provide_cluster_info=True
            )
            kubeconfig.flush()

            KubeConfig.from_path(kubeconfig.name).load()

        configuration = mock_set_default.call_args.args[0]
        self.assertIn("Bearer cluster-info-token", configuration.api_key.values())

    def test_exec_credential_output_must_be_an_object(self) -> None:
        with NamedTemporaryFile("w") as kubeconfig:
            _write_exec_kubeconfig(Path(kubeconfig.name), "print('scalar')")
            kubeconfig.flush()

            with self.assertRaisesRegex(
                RuntimeError, "Failed to load kubeconfig"
            ) as context:
                KubeConfig.from_path(kubeconfig.name).load()
        self.assertIn("must be an object", str(context.exception.__cause__))

    def test_malformed_exec_credential_output_is_rejected(self) -> None:
        with NamedTemporaryFile("w") as kubeconfig:
            _write_exec_kubeconfig(Path(kubeconfig.name), "print('{[')")
            kubeconfig.flush()

            with self.assertRaisesRegex(
                RuntimeError, "Failed to load kubeconfig"
            ) as context:
                KubeConfig.from_path(kubeconfig.name).load()
        self.assertIn(
            "failed to decode process output as JSON or YAML",
            str(context.exception.__cause__),
        )

    @patch("monarch._src.job.kubernetes.client.Configuration.set_default")
    def test_exec_credential_accepts_client_certificate(
        self, mock_set_default: MagicMock
    ) -> None:
        certificate = "test-client-certificate-data"
        private_key = "test-client-key-data"
        credential = json.dumps(
            {
                "apiVersion": "client.authentication.k8s.io/v1",
                "kind": "ExecCredential",
                "status": {
                    "clientCertificateData": certificate,
                    "clientKeyData": private_key,
                },
            }
        )
        with NamedTemporaryFile("w") as kubeconfig:
            _write_exec_kubeconfig(Path(kubeconfig.name), f"print({credential!r})")
            kubeconfig.flush()

            KubeConfig.from_path(kubeconfig.name).load()

        configuration = mock_set_default.call_args.args[0]
        self.assertEqual(Path(configuration.cert_file).read_text(), certificate)
        self.assertEqual(Path(configuration.key_file).read_text(), private_key)

    def test_exec_credential_certificate_requires_private_key(self) -> None:
        credential = json.dumps(
            {
                "apiVersion": "client.authentication.k8s.io/v1",
                "kind": "ExecCredential",
                "status": {"clientCertificateData": "certificate"},
            }
        )
        with NamedTemporaryFile("w") as kubeconfig:
            _write_exec_kubeconfig(Path(kubeconfig.name), f"print({credential!r})")
            kubeconfig.flush()

            with self.assertRaisesRegex(
                RuntimeError, "Failed to load kubeconfig"
            ) as context:
                KubeConfig.from_path(kubeconfig.name).load()
        self.assertIn("missing clientKeyData", str(context.exception.__cause__))

    def test_exec_credential_requires_authentication_data(self) -> None:
        credential = json.dumps(
            {
                "apiVersion": "client.authentication.k8s.io/v1",
                "kind": "ExecCredential",
                "status": {},
            }
        )
        with NamedTemporaryFile("w") as kubeconfig:
            _write_exec_kubeconfig(Path(kubeconfig.name), f"print({credential!r})")
            kubeconfig.flush()

            with self.assertRaisesRegex(
                RuntimeError, "Failed to load kubeconfig"
            ) as context:
                KubeConfig.from_path(kubeconfig.name).load()
        self.assertIn(
            "missing token or clientCertificateData",
            str(context.exception.__cause__),
        )

    def test_exec_credential_failure_is_reported_during_load(self) -> None:
        with NamedTemporaryFile("w") as kubeconfig:
            _write_exec_kubeconfig(
                Path(kubeconfig.name), "raise SystemExit('credential failure')"
            )
            kubeconfig.flush()

            with self.assertRaisesRegex(
                RuntimeError, "Failed to load kubeconfig"
            ) as context:
                KubeConfig.from_path(kubeconfig.name).load()
        self.assertIn("process returned", str(context.exception.__cause__))

    def test_copy_preserves_in_memory_config(self) -> None:
        kubeconfig = KubeConfig.from_config(MagicMock())

        copied = copy.deepcopy(kubeconfig)

        self.assertIsNotNone(copied.remote)
        self.assertFalse(copied._requires_rebind)


class TestSerialization(unittest.TestCase):
    @patch("monarch._src.job.kubernetes.configure")
    def test_runtime_state_is_not_pickled(self, mock_configure: MagicMock) -> None:
        job = KubernetesJob(
            namespace="test-ns",
            kubeconfig=KubeConfig.from_path("/tmp/kubeconfig"),
        )
        job.add_mesh("workers", 1, image_spec=ImageSpec("image"))
        job._status = "running"
        job._service_proc_ids["workers"] = KubernetesJob._allocate_service_proc_ids(1)
        job._prepared_mesh_pods = {
            "workers": [_MonarchMeshPod(name="workers-0", ip="10.0.0.1", port=26600)]
        }
        forward = MagicMock()
        job._port_forward_processes = [forward]
        job._port_forward_cleanup_registered = True

        restored = pickle.loads(pickle.dumps(job))

        self.assertIs(job._port_forward_processes[0], forward)
        self.assertIsNone(restored._prepared_mesh_pods)
        self.assertEqual(restored._port_forward_processes, [])
        self.assertFalse(restored._port_forward_cleanup_registered)
        self.assertEqual(restored._service_proc_ids, job._service_proc_ids)
        mock_configure.assert_called()

    @patch("monarch._src.job.kubernetes.configure")
    def test_in_memory_config_cache_requires_current_spec(
        self, mock_configure: MagicMock
    ) -> None:
        live_config = MagicMock()
        job = KubernetesJob(
            namespace="test-ns",
            kubeconfig=KubeConfig.from_config(live_config),
        )

        with self.assertLogs(
            "monarch._src.job.kubernetes", level="WARNING"
        ) as captured:
            restored = pickle.loads(pickle.dumps(job))

        self.assertIs(job._kubeconfig.remote, live_config)
        self.assertIsNone(restored._kubeconfig.remote)
        self.assertTrue(restored._kubeconfig.out_of_cluster)
        self.assertNotEqual(restored._kubeconfig, KubeConfig())
        self.assertEqual(len(captured.output), 1)
        self.assertIn(
            "omitting process-local Kubernetes configuration", captured.output[0]
        )
        self.assertIn("KubeConfig.from_path()", captured.output[0])
        with self.assertRaisesRegex(RuntimeError, "requires the current job spec"):
            restored._kubeconfig.load()

    @patch("monarch._src.job.kubernetes.configure")
    def test_incompatible_local_spec_evicts_unusable_remote_cache(
        self, mock_configure: MagicMock
    ) -> None:
        cached = KubernetesJob(
            namespace="test-ns",
            kubeconfig=KubeConfig.from_config(MagicMock()),
        )
        cached.add_mesh("workers", 1, image_spec=ImageSpec("image"))
        cached._status = "running"

        with TemporaryDirectory() as directory:
            cache_path = f"{directory}/job.pkl"
            cached.dump(cache_path)

            with self.assertLogs("monarch._src.job.job", level="WARNING") as captured:
                result = LocalJob()._load_cached(cache_path)

            self.assertIsNone(result)
            self.assertFalse(os.path.exists(cache_path))
            self.assertIn("'namespace': 'test-ns'", captured.output[0])
            self.assertIn("'monarch_meshes': ['workers']", captured.output[0])

    @patch("monarch._src.job.kubernetes.configure")
    def test_cache_hit_rebinds_current_connection_inputs_before_can_run(
        self, mock_configure: MagicMock
    ) -> None:
        cached_config = MagicMock()
        cached = KubernetesJob(
            namespace="test-ns",
            timeout=1,
            kubeconfig=KubeConfig.from_config(cached_config),
            attach_to="tcp://old:1234",
        )
        cached.add_mesh("workers", 1, image_spec=ImageSpec("image"))
        cached._status = "running"
        service_proc_ids = KubernetesJob._allocate_service_proc_ids(1)
        cached._service_proc_ids["workers"] = service_proc_ids

        current_config = MagicMock()
        spec = KubernetesJob(
            namespace="test-ns",
            timeout=99,
            kubeconfig=KubeConfig.from_config(current_config),
            attach_to="tcp://new:5678",
        )
        spec.add_mesh("workers", 1, image_spec=ImageSpec("image"))

        def can_run(running: KubernetesJob, current: KubernetesJob) -> bool:
            self.assertIs(current, spec)
            self.assertIs(running._kubeconfig, spec._kubeconfig)
            self.assertEqual(running._attach_to, "tcp://new:5678")
            self.assertEqual(running._timeout, 99)
            return True

        with TemporaryDirectory() as directory:
            cache_path = f"{directory}/job.pkl"
            cached.dump(cache_path)
            with patch.object(KubernetesJob, "can_run", autospec=True) as mock_can_run:
                mock_can_run.side_effect = can_run
                restored = spec._load_cached(cache_path)

        self.assertIsNotNone(restored)
        assert isinstance(restored, KubernetesJob)
        self.assertIs(restored._kubeconfig, spec._kubeconfig)
        self.assertEqual(restored._service_proc_ids["workers"], service_proc_ids)


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

        self.assertEqual(
            result,
            [
                _MonarchMeshPod(name="w-0", ip="10.0.0.1", port=26600),
                _MonarchMeshPod(name="w-1", ip="10.0.0.2", port=26600),
            ],
        )
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

        self.assertEqual(
            result,
            [
                _MonarchMeshPod(name="w-0", ip="10.0.0.1", port=26600),
                _MonarchMeshPod(name="w-1", ip="10.0.0.2", port=26600),
            ],
        )

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
        self.assertEqual(result[0].ip, "10.0.0.3")
        self.assertEqual(result[0].port, 26600)
        self.assertEqual(result[1].ip, "10.0.0.2")
        self.assertEqual(result[1].port, 26600)

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

        self.assertEqual(
            result,
            [_MonarchMeshPod(name="w-0", ip="10.0.0.1", port=26600)],
        )

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

        self.assertEqual(
            result,
            [_MonarchMeshPod(name="w-0", ip="10.0.0.1", port=9999)],
        )


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

    @patch.object(KubernetesJob, "_wait_for_ready_pods", return_value=[])
    def test_matching_provisioned_job_reuses_cached_service_proc_ids(
        self,
        mock_wait_for_ready_pods: MagicMock,
    ) -> None:
        cached = self._make_job()
        cached.add_mesh("workers", 2, image_spec=ImageSpec("image"))
        service_proc_ids = KubernetesJob._allocate_service_proc_ids(2)
        cached._service_proc_ids["workers"] = service_proc_ids
        cached._status = "running"
        restored = pickle.loads(pickle.dumps(cached))

        spec = self._make_job()
        spec.add_mesh("workers", 2, image_spec=ImageSpec("image"))

        self.assertTrue(restored.can_run(spec))
        self.assertEqual(
            restored._service_proc_ids["workers"],
            service_proc_ids,
        )

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


class TestPortForwardToPod(unittest.TestCase):
    """Tests for KubernetesJob._port_forward_to_pod."""

    @patch("monarch._src.job.kubernetes.configure")
    def _make_job(self, mock_configure: MagicMock) -> KubernetesJob:
        return KubernetesJob(
            namespace="test-ns",
            kubeconfig=KubeConfig.from_path("/tmp/kubeconfig"),
        )

    @patch("monarch._src.job.kubernetes.shutil.which", return_value=None)
    def test_missing_kubectl_raises(self, mock_which: MagicMock) -> None:
        job = self._make_job()
        pod = _MonarchMeshPod(name="mesh1-0", ip="10.0.0.1", port=26600)
        with self.assertRaises(RuntimeError, msg="kubectl"):
            job._port_forward_to_pod(pod)

    @patch("monarch._src.job.kubernetes.atexit.unregister")
    @patch("monarch._src.job.kubernetes.atexit.register")
    @patch("monarch._src.job.kubernetes.select.select", return_value=([1], [], []))
    @patch("monarch._src.job.kubernetes.subprocess.Popen")
    @patch("monarch._src.job.kubernetes.shutil.which", return_value="/usr/bin/kubectl")
    def test_port_forward_uses_pod(
        self,
        mock_which: MagicMock,
        mock_popen: MagicMock,
        mock_select: MagicMock,
        mock_atexit_register: MagicMock,
        mock_atexit_unregister: MagicMock,
    ) -> None:
        job = self._make_job()
        pod = _MonarchMeshPod(name="mesh1-0", ip="10.0.0.1", port=26600)

        mock_process = MagicMock()
        mock_process.stdout.readline.return_value = (
            "Forwarding from 127.0.0.1:45678 -> 26600\n"
        )
        mock_popen.return_value = mock_process

        result = job._port_forward_to_pod(pod)
        second_result = job._port_forward_to_pod(pod)

        self.assertEqual(result, "tcp://127.0.0.1:45678")
        self.assertEqual(second_result, result)
        cmd = mock_popen.call_args[0][0]
        self.assertIn("pod/mesh1-0", cmd)
        self.assertIn(":26600", cmd)
        self.assertIn("--namespace", cmd)
        self.assertIn("test-ns", cmd)
        mock_atexit_register.assert_called_once_with(job._terminate_port_forwards)

        mock_process.poll.return_value = 0
        job._terminate_port_forwards()
        mock_atexit_unregister.assert_called_once_with(job._terminate_port_forwards)
        self.assertFalse(job._port_forward_cleanup_registered)

    def test_terminate_port_forwards_is_idempotent(self) -> None:
        job = self._make_job()
        process = MagicMock()
        process.poll.return_value = None
        job._port_forward_processes = [process]

        job._terminate_port_forwards()
        job._terminate_port_forwards()

        process.terminate.assert_called_once()
        process.wait.assert_called_once_with(
            timeout=_PORT_FORWARD_TERMINATE_TIMEOUT_SECONDS
        )
        process.kill.assert_not_called()
        self.assertEqual(job._port_forward_processes, [])

    def test_terminate_port_forward_kills_after_timeout(self) -> None:
        job = self._make_job()
        process = MagicMock()
        process.poll.return_value = None
        process.wait.side_effect = [
            subprocess.TimeoutExpired("kubectl", 0.5),
            0,
        ]
        job._port_forward_processes = [process]

        job._terminate_port_forwards()

        process.terminate.assert_called_once()
        process.kill.assert_called_once()
        self.assertEqual(
            process.wait.call_args_list,
            [
                call(timeout=_PORT_FORWARD_TERMINATE_TIMEOUT_SECONDS),
                call(timeout=_PORT_FORWARD_TERMINATE_TIMEOUT_SECONDS),
            ],
        )
        self.assertEqual(job._port_forward_processes, [])

    @patch("monarch._src.job.kubernetes.logger.warning")
    def test_terminate_warns_when_killed_process_does_not_exit(
        self, mock_warning: MagicMock
    ) -> None:
        job = self._make_job()
        process = MagicMock()
        process.poll.return_value = None
        process.wait.side_effect = subprocess.TimeoutExpired("kubectl", 0.5)
        job._port_forward_processes = [process]

        job._terminate_port_forwards()

        process.terminate.assert_called_once()
        process.kill.assert_called_once()
        mock_warning.assert_called_once_with(
            "kubectl port-forward did not exit after being killed"
        )
        self.assertEqual(job._port_forward_processes, [])

    @patch("monarch._src.job.kubernetes.logger.warning")
    def test_terminate_os_error_attempts_kill_and_continues(
        self, mock_warning: MagicMock
    ) -> None:
        job = self._make_job()
        blocked = MagicMock()
        blocked.poll.return_value = None
        blocked.terminate.side_effect = PermissionError("denied")
        blocked.kill.side_effect = PermissionError("denied")
        running = MagicMock()
        running.poll.return_value = None
        job._port_forward_processes = [blocked, running]

        job._terminate_port_forwards()

        blocked.kill.assert_called_once()
        running.terminate.assert_called_once()
        running.wait.assert_called_once_with(
            timeout=_PORT_FORWARD_TERMINATE_TIMEOUT_SECONDS
        )
        mock_warning.assert_has_calls(
            [
                call(
                    "failed to terminate or reap kubectl port-forward; attempting kill",
                    exc_info=True,
                ),
                call(
                    "failed to kill or reap kubectl port-forward",
                    exc_info=True,
                ),
            ]
        )
        self.assertEqual(job._port_forward_processes, [])

    @patch("monarch._src.job.kubernetes.select.select", return_value=([1], [], []))
    @patch("monarch._src.job.kubernetes.subprocess.Popen")
    @patch("monarch._src.job.kubernetes.shutil.which", return_value="/usr/bin/kubectl")
    def test_port_forward_no_output_raises(
        self, mock_which: MagicMock, mock_popen: MagicMock, mock_select: MagicMock
    ) -> None:
        job = self._make_job()
        pod = _MonarchMeshPod(name="mesh1-0", ip="10.0.0.1", port=26600)

        mock_process = MagicMock()
        mock_process.stdout.readline.return_value = ""
        mock_process.communicate.return_value = ("", "connection refused")
        mock_popen.return_value = mock_process

        with self.assertRaises(RuntimeError, msg="no output"):
            job._port_forward_to_pod(pod)
        mock_process.terminate.assert_called_once()

    @patch("monarch._src.job.kubernetes.select.select", return_value=([1], [], []))
    @patch("monarch._src.job.kubernetes.subprocess.Popen")
    @patch("monarch._src.job.kubernetes.shutil.which", return_value="/usr/bin/kubectl")
    def test_port_forward_unparseable_output_raises(
        self, mock_which: MagicMock, mock_popen: MagicMock, mock_select: MagicMock
    ) -> None:
        job = self._make_job()
        pod = _MonarchMeshPod(name="mesh1-0", ip="10.0.0.1", port=26600)

        mock_process = MagicMock()
        mock_process.stdout.readline.return_value = "unexpected output\n"
        mock_process.kill.return_value = None
        mock_process.wait.return_value = 1
        mock_popen.return_value = mock_process

        with self.assertRaises(RuntimeError, msg="could not parse"):
            job._port_forward_to_pod(pod)

    @patch("monarch._src.job.kubernetes.select.select", return_value=([], [], []))
    @patch("monarch._src.job.kubernetes.subprocess.Popen")
    @patch("monarch._src.job.kubernetes.shutil.which", return_value="/usr/bin/kubectl")
    def test_port_forward_start_timeout_raises(
        self, mock_which: MagicMock, mock_popen: MagicMock, mock_select: MagicMock
    ) -> None:
        job = self._make_job()
        pod = _MonarchMeshPod(name="mesh1-0", ip="10.0.0.1", port=26600)

        mock_process = MagicMock()
        mock_popen.return_value = mock_process

        # select reports the port-forward never became readable within the
        # deadline, so we kill it and raise before ever reading stdout.
        with self.assertRaises(RuntimeError, msg="did not start within"):
            job._port_forward_to_pod(pod)
        mock_process.kill.assert_called_once()
        mock_process.stdout.readline.assert_not_called()


class TestStateOutOfCluster(unittest.TestCase):
    """Tests for KubernetesJob._state in out-of-cluster mode.

    Covers the hello_mesh (attach-only) and hello_provision (provisioned) flows.
    """

    @patch("monarch._src.job.kubernetes.configure")
    def _make_job(
        self,
        mock_configure: MagicMock,
        attach_to: str | None = None,
    ) -> KubernetesJob:
        return KubernetesJob(
            namespace="monarch-tests",
            kubeconfig=KubeConfig.from_path("/tmp/kubeconfig"),
            attach_to=attach_to,
        )

    def _mock_watch_for_pods(
        self,
        mock_watch_cls: MagicMock,
        pods_by_call: list[list[V1Pod]],
    ) -> None:
        """Set up mock watch to return different pod lists for successive calls."""
        mock_watch = MagicMock()
        mock_watch_cls.return_value = mock_watch
        # Each call to stream returns the next set of pods
        mock_watch.stream.side_effect = [
            [{"type": "ADDED", "object": pod} for pod in pods] for pods in pods_by_call
        ]

    @patch("monarch._src.job.kubernetes.attach_to_workers")
    @patch("monarch._src.job.job.attach")
    @patch("monarch._src.job.kubernetes.KubernetesJob._port_forward_to_pod")
    @patch("monarch._src.job.kubernetes.watch.Watch")
    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.load_kube_config")
    def test_hello_mesh_out_of_cluster_auto_forwards(
        self,
        mock_load_config: MagicMock,
        mock_core_api: MagicMock,
        mock_watch_cls: MagicMock,
        mock_port_forward: MagicMock,
        mock_attach: MagicMock,
        mock_attach_to_workers: MagicMock,
    ) -> None:
        """hello_mesh flow: attach-only meshes auto port-forward to the first pod."""
        job = self._make_job()
        job.add_mesh("mesh1", 2)

        self._mock_watch_for_pods(
            mock_watch_cls,
            [
                [
                    _make_pod("mesh1-0", 0, True, ip="10.0.0.1"),
                    _make_pod("mesh1-1", 1, True, ip="10.0.0.2"),
                ],
            ],
        )

        mock_port_forward.return_value = "tcp://127.0.0.1:45678"
        mock_attach.return_value = MagicMock()

        job._state()

        # Should port-forward to the first pod.
        mock_port_forward.assert_called_once()
        forwarded_pod = mock_port_forward.call_args[0][0]
        self.assertEqual(forwarded_pod.name, "mesh1-0")
        mock_attach.assert_called_once_with("tcp://127.0.0.1:45678")

    @patch("monarch._src.job.kubernetes.attach_to_workers")
    @patch("monarch._src.job.job.attach")
    @patch("monarch._src.job.kubernetes.KubernetesJob._port_forward_to_pod")
    @patch("monarch._src.job.kubernetes.watch.Watch")
    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.load_kube_config")
    def test_hello_provision_out_of_cluster_auto_forwards(
        self,
        mock_load_config: MagicMock,
        mock_core_api: MagicMock,
        mock_watch_cls: MagicMock,
        mock_port_forward: MagicMock,
        mock_attach: MagicMock,
        mock_attach_to_workers: MagicMock,
    ) -> None:
        """hello_provision flow: provisioned meshes auto port-forward."""
        job = self._make_job()
        job.add_mesh(
            "mesh1", 2, image_spec=ImageSpec("ghcr.io/meta-pytorch/monarch:latest")
        )
        service_proc_ids = KubernetesJob._allocate_service_proc_ids(2)
        job._service_proc_ids["mesh1"] = service_proc_ids

        self._mock_watch_for_pods(
            mock_watch_cls,
            [
                [
                    _make_pod("mesh1-0", 0, True, ip="10.0.0.1"),
                    _make_pod("mesh1-1", 1, True, ip="10.0.0.2"),
                ],
            ],
        )

        mock_port_forward.return_value = "tcp://127.0.0.1:55555"
        mock_attach.return_value = MagicMock()

        job._state()

        mock_port_forward.assert_called_once()
        forwarded_pod = mock_port_forward.call_args[0][0]
        self.assertEqual(forwarded_pod.name, "mesh1-0")
        mock_attach.assert_called_once_with("tcp://127.0.0.1:55555")
        self.assertEqual(
            mock_attach_to_workers.call_args.kwargs["workers"],
            [
                f"{service_proc_ids[0]}@tcp://10.0.0.1:26600",
                f"{service_proc_ids[1]}@tcp://10.0.0.2:26600",
            ],
        )

    @patch("monarch._src.job.kubernetes.attach_to_workers")
    @patch("monarch._src.job.job.attach")
    @patch("monarch._src.job.kubernetes.KubernetesJob._port_forward_to_pod")
    @patch("monarch._src.job.kubernetes.watch.Watch")
    @patch("monarch._src.job.kubernetes.client.CoreV1Api")
    @patch("monarch._src.job.kubernetes.load_kube_config")
    def test_explicit_attach_to_skips_port_forward(
        self,
        mock_load_config: MagicMock,
        mock_core_api: MagicMock,
        mock_watch_cls: MagicMock,
        mock_port_forward: MagicMock,
        mock_attach: MagicMock,
        mock_attach_to_workers: MagicMock,
    ) -> None:
        """When --attach-to is provided, no automatic port-forward should happen."""
        job = self._make_job(attach_to="tcp://127.0.0.1:34000")
        job.add_mesh("mesh1", 1)

        self._mock_watch_for_pods(
            mock_watch_cls,
            [
                [_make_pod("mesh1-0", 0, True, ip="10.0.0.1")],
            ],
        )
        mock_attach.return_value = MagicMock()

        job._state()

        mock_port_forward.assert_not_called()
        mock_attach.assert_called_once_with("tcp://127.0.0.1:34000")

    @patch("monarch._src.job.kubernetes.attach_to_workers")
    @patch("monarch._src.job.job.attach")
    @patch("monarch._src.job.kubernetes.KubernetesJob._port_forward_to_pod")
    @patch.object(KubernetesJob, "_wait_for_ready_pods")
    def test_existing_process_attachment_skips_new_port_forward(
        self,
        mock_wait_for_ready_pods: MagicMock,
        mock_port_forward: MagicMock,
        mock_attach: MagicMock,
        mock_attach_to_workers: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh("mesh1", 1)
        pods = [_MonarchMeshPod(name="mesh1-0", ip="10.0.0.1", port=26600)]
        mock_wait_for_ready_pods.return_value = pods

        with (
            patch(
                "monarch._src.job.kubernetes._client_attached_to",
                return_value="tcp://127.0.0.1:45678",
            ),
            patch(
                "monarch._src.job.job._client_attached_to",
                return_value="tcp://127.0.0.1:45678",
            ),
            self.assertLogs(
                "monarch._src.job.kubernetes", level="INFO"
            ) as captured_logs,
        ):
            job._state()

        mock_port_forward.assert_not_called()
        mock_attach.assert_not_called()
        mock_attach_to_workers.assert_called_once()
        self.assertTrue(
            any(
                "original creator retains ownership" in line
                for line in captured_logs.output
            )
        )

    @patch("monarch._src.job.kubernetes.attach_to_workers")
    @patch("monarch._src.job.job.attach")
    @patch("monarch._src.job.kubernetes.KubernetesJob._port_forward_to_pod")
    @patch.object(KubernetesJob, "_wait_for_ready_pods")
    def test_owning_job_reuses_its_process_attachment(
        self,
        mock_wait_for_ready_pods: MagicMock,
        mock_port_forward: MagicMock,
        mock_attach: MagicMock,
        mock_attach_to_workers: MagicMock,
    ) -> None:
        job = self._make_job()
        job.add_mesh("mesh1", 1)
        job._port_forward_cleanup_registered = True
        mock_wait_for_ready_pods.return_value = [
            _MonarchMeshPod(name="mesh1-0", ip="10.0.0.1", port=26600)
        ]

        with (
            patch(
                "monarch._src.job.kubernetes._client_attached_to",
                return_value="tcp://127.0.0.1:45678",
            ),
            patch(
                "monarch._src.job.job._client_attached_to",
                return_value="tcp://127.0.0.1:45678",
            ),
            self.assertLogs(
                "monarch._src.job.kubernetes", level="INFO"
            ) as captured_logs,
        ):
            job._state()

        mock_port_forward.assert_not_called()
        mock_attach.assert_not_called()
        mock_attach_to_workers.assert_called_once()
        self.assertTrue(
            any("this job retains ownership" in line for line in captured_logs.output)
        )

    def test_components_bootstrap_before_host_mesh_materialization(self) -> None:
        job = self._make_job()
        job._status = "running"
        job._apply_id = "apply"
        raw_host = MagicMock()
        raw_state = MagicMock()
        raw_state._hosts = {"mesh1": raw_host}
        events = []

        def prepare_client_gateway() -> None:
            events.append("prepare")

        def materialize_state() -> MagicMock:
            events.append("state")
            return raw_state

        job._components = MagicMock()
        job._components.needs_sidecar.return_value = True
        job._components.before_connect.side_effect = lambda _job: events.append(
            "before_connect"
        )
        connect_via_gateway = []

        def connect(_job, host_meshes, via_gateway):
            events.append("connect")
            connect_via_gateway.append(via_gateway)
            return host_meshes

        job._components.connect.side_effect = connect

        with (
            patch.object(
                job,
                "_prepare_client_gateway",
                side_effect=prepare_client_gateway,
            ),
            patch.object(job, "_state", side_effect=materialize_state),
            patch(
                "monarch._src.job.job.create_job_sidecar",
                side_effect=lambda *_args, **_kwargs: events.append("sidecar"),
            ) as create_sidecar,
            patch(
                "monarch._src.job.job._client_attached_to",
                return_value="tcp://127.0.0.1:45678",
            ),
        ):
            host_meshes = job._connect_host_meshes(job)

        self.assertEqual(
            events,
            ["prepare", "sidecar", "before_connect", "state", "connect"],
        )
        self.assertEqual(host_meshes, {"mesh1": raw_host})
        self.assertEqual(
            create_sidecar.call_args.kwargs["attach_to"],
            "tcp://127.0.0.1:45678",
        )
        # Components are told the same thing the sidecar bootstrap above is gated on --
        # out of cluster, the workers advertise addresses this client cannot reach
        # directly. The event order already implies it, since "prepare" and "sidecar" run
        # only when it holds; this pins the value the components actually receive, so the
        # two cannot drift apart.
        self.assertEqual(connect_via_gateway, [True])


if __name__ == "__main__":
    unittest.main()
