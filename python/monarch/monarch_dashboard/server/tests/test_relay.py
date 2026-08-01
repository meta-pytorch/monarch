# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import json
import threading
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import MagicMock, patch

from monarch.monarch_dashboard.meta.mast import resolve_mast_dashboard_target
from monarch.monarch_dashboard.relay import (
    create_dashboard_relay_server,
    DashboardRelayServer,
)


class ResolveMastDashboardTargetTest(unittest.TestCase):
    @patch("monarch.monarch_dashboard.meta.mast._role_dashboard_is_reachable")
    @patch("monarch.monarch_dashboard.meta.mast._attach_to_existing_mast_job")
    def test_resolves_role_hostname(
        self,
        mock_attach_to_existing_mast_job: MagicMock,
        mock_role_dashboard_is_reachable: MagicMock,
    ) -> None:
        mock_role_dashboard_is_reachable.return_value = True
        role_info = MagicMock()
        role_info.name = "worker"
        role_info.hostnames = ["worker001.region"]
        job = MagicMock()
        job._get_role_infos.return_value = [role_info]
        mock_attach_to_existing_mast_job.return_value = job

        target = resolve_mast_dashboard_target("sample-mast-job")

        self.assertEqual("sample-mast-job", target.job_name)
        self.assertEqual("worker", target.role_name)
        self.assertEqual(
            "https://worker001.region.facebook.com:8265",
            target.upstream_url,
        )
        mock_attach_to_existing_mast_job.assert_called_once_with(
            app_handle="mast:///sample-mast-job",
            role_names=[],
        )
        job._wait_for_job_ready.assert_called_once()
        mock_role_dashboard_is_reachable.assert_called_once_with(
            role_info,
            dashboard_port=8265,
        )

    @patch("monarch.monarch_dashboard.meta.mast._role_dashboard_is_reachable")
    @patch("monarch.monarch_dashboard.meta.mast._attach_to_existing_mast_job")
    def test_resolves_custom_dashboard_port(
        self,
        mock_attach_to_existing_mast_job: MagicMock,
        mock_role_dashboard_is_reachable: MagicMock,
    ) -> None:
        mock_role_dashboard_is_reachable.return_value = True
        role_info = MagicMock()
        role_info.name = "worker"
        role_info.hostnames = ["worker001.region"]
        job = MagicMock()
        job._get_role_infos.return_value = [role_info]
        mock_attach_to_existing_mast_job.return_value = job

        target = resolve_mast_dashboard_target(
            "sample-mast-job",
            dashboard_port=9000,
        )

        self.assertEqual(
            "https://worker001.region.facebook.com:9000",
            target.upstream_url,
        )
        mock_role_dashboard_is_reachable.assert_called_once_with(
            role_info,
            dashboard_port=9000,
        )

    def test_rejects_mast_handle(self) -> None:
        with self.assertRaisesRegex(ValueError, "direct MAST job name"):
            resolve_mast_dashboard_target("mast:///sample-mast-job")

    @patch("monarch.monarch_dashboard.meta.mast._role_dashboard_is_reachable")
    @patch("monarch.monarch_dashboard.meta.mast._attach_to_existing_mast_job")
    def test_requires_role_name_for_multi_role_job(
        self,
        mock_attach_to_existing_mast_job: MagicMock,
        mock_role_dashboard_is_reachable: MagicMock,
    ) -> None:
        mock_role_dashboard_is_reachable.return_value = True
        worker = MagicMock()
        worker.name = "worker"
        worker.hostnames = ["worker001.region"]
        evaluator = MagicMock()
        evaluator.name = "evaluator"
        evaluator.hostnames = ["worker002.region"]
        job = MagicMock()
        job._get_role_infos.return_value = [worker, evaluator]
        mock_attach_to_existing_mast_job.return_value = job

        with self.assertRaisesRegex(ValueError, "Multiple MAST roles"):
            resolve_mast_dashboard_target("sample-mast-job")
        self.assertEqual(2, mock_role_dashboard_is_reachable.call_count)

    @patch("monarch.monarch_dashboard.meta.mast._role_dashboard_is_reachable")
    @patch("monarch.monarch_dashboard.meta.mast._attach_to_existing_mast_job")
    def test_infers_single_reachable_role(
        self,
        mock_attach_to_existing_mast_job: MagicMock,
        mock_role_dashboard_is_reachable: MagicMock,
    ) -> None:
        def is_reachable(role_info: MagicMock, **kwargs: object) -> bool:
            return role_info.name == "evaluator"

        mock_role_dashboard_is_reachable.side_effect = is_reachable
        worker = MagicMock()
        worker.name = "worker"
        worker.hostnames = ["worker001.region"]
        evaluator = MagicMock()
        evaluator.name = "evaluator"
        evaluator.hostnames = ["worker002.region"]
        job = MagicMock()
        job._get_role_infos.return_value = [worker, evaluator]
        mock_attach_to_existing_mast_job.return_value = job

        target = resolve_mast_dashboard_target("sample-mast-job")

        self.assertEqual("evaluator", target.role_name)
        self.assertEqual(
            "https://worker002.region.facebook.com:8265",
            target.upstream_url,
        )

    @patch("monarch.monarch_dashboard.meta.mast._role_dashboard_is_reachable")
    @patch("monarch.monarch_dashboard.meta.mast._attach_to_existing_mast_job")
    def test_uses_explicit_role_name(
        self,
        mock_attach_to_existing_mast_job: MagicMock,
        mock_role_dashboard_is_reachable: MagicMock,
    ) -> None:
        worker = MagicMock()
        worker.name = "worker"
        worker.hostnames = ["worker001.region"]
        evaluator = MagicMock()
        evaluator.name = "evaluator"
        evaluator.hostnames = ["worker002.region"]
        job = MagicMock()
        job._get_role_infos.return_value = [worker, evaluator]
        mock_attach_to_existing_mast_job.return_value = job

        target = resolve_mast_dashboard_target(
            "sample-mast-job",
            role_name="evaluator",
        )

        self.assertEqual("evaluator", target.role_name)
        self.assertEqual(
            "https://worker002.region.facebook.com:8265",
            target.upstream_url,
        )
        mock_attach_to_existing_mast_job.assert_called_once_with(
            app_handle="mast:///sample-mast-job",
            role_names=["evaluator"],
        )
        mock_role_dashboard_is_reachable.assert_not_called()


class _UpstreamHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        body = json.dumps(
            {
                "method": "GET",
                "path": self.path,
                "host": self.headers.get("Host"),
            }
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0"))
        request_body = self.rfile.read(content_length).decode("utf-8")
        body = json.dumps({"method": "POST", "body": request_body}).encode("utf-8")
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


class DashboardRelayServerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.upstream = ThreadingHTTPServer(("127.0.0.1", 0), _UpstreamHandler)
        self.upstream_thread = threading.Thread(
            target=self.upstream.serve_forever,
            daemon=True,
        )
        self.upstream_thread.start()

    def tearDown(self) -> None:
        self.upstream.shutdown()
        self.upstream.server_close()

    def _start_relay(self) -> DashboardRelayServer:
        relay = create_dashboard_relay_server(
            upstream_url=f"http://127.0.0.1:{self.upstream.server_port}",
            host="127.0.0.1",
            port=0,
            use_tls=False,
        )
        threading.Thread(target=relay.serve_forever, daemon=True).start()
        self.addCleanup(relay.shutdown)
        return relay

    def test_relay_proxies_get(self) -> None:
        relay = self._start_relay()
        with self.opener.open(f"{relay.url}/api/query?x=1") as response:
            payload = json.loads(response.read().decode("utf-8"))

        self.assertEqual("GET", payload["method"])
        self.assertEqual("/api/query?x=1", payload["path"])
        self.assertEqual(f"127.0.0.1:{self.upstream.server_port}", payload["host"])

    def test_relay_proxies_post(self) -> None:
        relay = self._start_relay()
        request = urllib.request.Request(
            f"{relay.url}/api/query",
            data=b'{"sql": "SELECT 1"}',
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.opener.open(request) as response:
            payload = json.loads(response.read().decode("utf-8"))
            status = response.status

        self.assertEqual(201, status)
        self.assertEqual("POST", payload["method"])
        self.assertEqual('{"sql": "SELECT 1"}', payload["body"])
