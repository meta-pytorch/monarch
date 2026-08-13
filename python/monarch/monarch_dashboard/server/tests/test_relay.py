# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import unittest
from collections.abc import AsyncIterator
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from monarch.monarch_dashboard.meta.mast import resolve_mast_dashboard_target
from monarch.monarch_dashboard.meta.relay import create_dashboard_relay_app


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


class DashboardRelayAppTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.release_stream = asyncio.Event()
        upstream_app = web.Application()
        upstream_app.router.add_route("*", "/{path:.*}", self._handle_upstream)
        self.upstream = TestServer(upstream_app)
        await self.upstream.start_server()
        self.upstream_url = str(self.upstream.make_url("/")).rstrip("/")

        relay_app = create_dashboard_relay_app(self.upstream_url)
        self.client = TestClient(TestServer(relay_app))
        await self.client.start_server()

    async def asyncTearDown(self) -> None:
        self.release_stream.set()
        await self.client.close()
        await self.upstream.close()

    async def _handle_upstream(self, request: web.Request) -> web.StreamResponse:
        if request.path == "/stream":
            response = web.StreamResponse()
            await response.prepare(request)
            await response.write(b"first")
            await self.release_stream.wait()
            await response.write(b"second")
            await response.write_eof()
            return response

        body = await request.read()
        return web.json_response(
            {
                "method": request.method,
                "path": request.raw_path,
                "host": request.headers.get("Host"),
                "body": body.decode("utf-8"),
            },
            status=201 if request.method == "POST" else 200,
        )

    async def test_relay_proxies_get(self) -> None:
        async with self.client.get("/api/query?x=1") as response:
            payload = await response.json()

        self.assertEqual("GET", payload["method"])
        self.assertEqual("/api/query?x=1", payload["path"])
        self.assertEqual(urlparse(self.upstream_url).netloc, payload["host"])

    async def test_relay_proxies_post(self) -> None:
        async with self.client.post(
            "/api/query",
            data=b'{"sql": "SELECT 1"}',
            headers={"Content-Type": "application/json"},
        ) as response:
            payload = await response.json()
            status = response.status

        self.assertEqual(201, status)
        self.assertEqual("POST", payload["method"])
        self.assertEqual('{"sql": "SELECT 1"}', payload["body"])

    async def test_relay_proxies_chunked_request(self) -> None:
        async def chunks() -> AsyncIterator[bytes]:
            yield b"SELECT "
            yield b"1"

        async with self.client.post("/api/query", data=chunks()) as response:
            payload = await response.json()

        self.assertEqual("SELECT 1", payload["body"])

    async def test_relay_streams_response(self) -> None:
        response = await self.client.get("/stream")
        self.assertEqual(b"first", await response.content.readexactly(5))

        self.release_stream.set()
        self.assertEqual(b"second", await response.read())
