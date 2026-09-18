# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml
from kubernetes import client
from kubernetes.config.config_exception import ConfigException
from kubernetes.config.exec_provider import ExecProvider
from kubernetes.config.kube_config import (
    FileOrData,
    KubeConfigLoader,
    KubeConfigMerger,
    parse_rfc3339,
)


def _decode_exec_credential(output: str) -> dict[str, Any]:
    try:
        value = json.loads(output)
    except ValueError:
        try:
            # ExecCredential fields are strings. BaseLoader keeps YAML
            # timestamps as strings instead of converting them to datetime.
            value = yaml.load(output, Loader=yaml.BaseLoader)
        except yaml.YAMLError as error:
            raise ConfigException(
                f"exec: failed to decode process output as JSON or YAML: {error}"
            ) from error

    if not isinstance(value, dict):
        raise ConfigException("exec: process output must be an object")
    return value


class _YamlCompatibleExecProvider(ExecProvider):
    def __init__(self, exec_config: Any, cwd: str, cluster: Any) -> None:
        # The cluster argument was added to ExecProvider after Kubernetes 28.
        # Initialize through the older signature so clients from 28 forward are
        # supported, and preserve the newer provideClusterInfo behavior here.
        super().__init__(exec_config, cwd)
        self._cluster_info = (
            cluster if exec_config.safe_get("provideClusterInfo") else None
        )

    def run(self, previous_response: Any = None) -> dict[str, Any]:
        is_interactive = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
        kubernetes_exec_info: dict[str, Any] = {
            "apiVersion": self.api_version,
            "kind": "ExecCredential",
            "spec": {"interactive": is_interactive},
        }

        if previous_response:
            kubernetes_exec_info["spec"]["response"] = previous_response

        # Cluster-info is relevant in > k8s v28 as noted in this class's init function.
        if self._cluster_info is not None:
            kubernetes_exec_info["spec"]["cluster"] = self._cluster_info.value
            extensions = self._cluster_info.value.get("extensions")
            if extensions:
                for extension in extensions:
                    if extension["name"] == "client.authentication.k8s.io/exec":
                        kubernetes_exec_info["spec"]["cluster"]["config"] = extension[
                            "extension"
                        ]
                        break
        self.env["KUBERNETES_EXEC_INFO"] = json.dumps(kubernetes_exec_info)

        # Run the credential-generating process, get the output, and raise on any errors.
        process = subprocess.Popen(
            self.args,
            stdout=subprocess.PIPE,
            stderr=sys.stderr if is_interactive else subprocess.PIPE,
            stdin=sys.stdin if is_interactive else None,
            cwd=self.cwd,
            env=self.env,
            text=True,
            shell=sys.platform in ("win32", "cygwin"),
        )
        stdout, stderr = process.communicate()
        exit_code = process.wait()
        if exit_code != 0:
            message = f"exec: process returned {exit_code}"
            if stderr and stderr.strip():
                message += f". {stderr.strip()}"
            raise ConfigException(message)

        # Use our JSON-or-YAML-fallback date-safe loading function to get the config in dict-form.
        data = _decode_exec_credential(stdout)

        # Validate content is what we expect and return the status.
        for key in ("apiVersion", "kind", "status"):
            if key not in data:
                raise ConfigException(f"exec: malformed response. missing key '{key}'")
        if data["apiVersion"] != self.api_version:
            raise ConfigException(
                "exec: plugin api version "
                f"{data['apiVersion']} does not match {self.api_version}"
            )
        status = data["status"]
        if not isinstance(status, dict):
            raise ConfigException("exec: malformed response. status must be an object")
        return status


class _YamlCompatibleKubeConfigLoader(KubeConfigLoader):
    def _load_from_exec_plugin(self) -> bool | None:
        if "exec" not in self._user:
            return None
        base_path = self._get_base_path(self._cluster.path)
        status = _YamlCompatibleExecProvider(
            self._user["exec"], base_path, self._cluster
        ).run()

        # Token is by far the most common usage.
        if "token" in status:
            self.token: str = f"Bearer {status['token']}"

        # Also supporting certs for completeness.
        elif "clientCertificateData" in status:
            if "clientKeyData" not in status:
                raise ConfigException(
                    "exec: missing clientKeyData field in plugin output"
                )
            self.cert_file = FileOrData(
                status,
                None,
                data_key_name="clientCertificateData",
                file_base_path=base_path,
                base64_file_content=False,
                temp_file_path=self._temp_file_path,
            ).as_file()
            self.key_file = FileOrData(
                status,
                None,
                data_key_name="clientKeyData",
                file_base_path=base_path,
                base64_file_content=False,
                temp_file_path=self._temp_file_path,
            ).as_file()
        else:
            raise ConfigException(
                "exec: missing token or clientCertificateData field in plugin output"
            )
        if "expirationTimestamp" in status:
            self.expiry = parse_rfc3339(status["expirationTimestamp"])
        return True


def load_kube_config(path: Path) -> client.Configuration:
    """Load a kubeconfig whose exec plugin may emit JSON or YAML."""
    merger = KubeConfigMerger(str(path))
    if merger.config is None:
        raise ConfigException("Invalid kube-config. No configuration found.")
    loader = _YamlCompatibleKubeConfigLoader(
        config_dict=merger.config,
        config_persister=merger.save_changes,
        # An explicit None tells the upstream loader to resolve relative paths
        # and the exec plugin's working directory from the kubeconfig's path.
        config_base_path=None,
    )
    configuration = client.Configuration()
    loader.load_and_set(configuration)
    return configuration
