# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any


class QueryEngineClient:
    """Parent-process handle to the telemetry sidecar's query API."""

    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        self._base_url: str = base_url.rstrip("/")
        self._timeout: float = timeout
        # Build an opener with an empty ProxyHandler so requests bypass any
        # ambient HTTP(S)/no_proxy environment proxies. The sidecar listens
        # on loopback/localhost; routing through a corporate proxy would fail
        # or hang, so we force a direct connection.
        self._opener: urllib.request.OpenerDirector = urllib.request.build_opener(
            urllib.request.ProxyHandler({})
        )

    def query(self, sql: str) -> dict[str, Any]:
        """Run a SQL query through the sidecar API and return its parsed
        JSON response (``{"columns": [...], "rows": [...]}`` on success)."""
        payload = json.dumps({"sql": sql}).encode("utf-8")
        request = urllib.request.Request(
            f"{self._base_url}/api/query",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            # urllib raises HTTPError before the caller can read the body, so
            # the server's error detail (e.g. a DataFusion parse/plan failure
            # returned as JSON) would otherwise be lost. Drain the body and
            # fold it into the reason so the message is actionable, then
            # re-raise a fresh HTTPError preserving url/code/headers/fp.
            body = error.read().decode("utf-8", errors="replace")
            raise urllib.error.HTTPError(
                error.url,
                error.code,
                f"{error.reason}: {body}",
                error.headers,
                error.fp,
            ) from error

    def store_pyspy_dump(
        self, dump_id: str, proc_ref: str, pyspy_result_json: str
    ) -> dict[str, Any]:
        """Store a py-spy dump through the sidecar API."""
        payload = json.dumps(
            {
                "dump_id": dump_id,
                "proc_ref": proc_ref,
                "pyspy_result_json": pyspy_result_json,
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            f"{self._base_url}/api/pyspy_dump",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self._opener.open(request, timeout=self._timeout) as response:
            return json.loads(response.read().decode("utf-8"))
