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


DEFAULT_QUERY_TIMEOUT_SEC: float = 10.0


class QueryEngineClient:
    """Parent-process handle to the telemetry sidecar's query API."""

    def __init__(
        self, base_url: str, timeout: float = DEFAULT_QUERY_TIMEOUT_SEC
    ) -> None:
        self._base_url: str = base_url.rstrip("/")
        self._timeout: float = timeout
        # Build an opener with an empty ProxyHandler so requests bypass any
        # ambient HTTP(S)/no_proxy environment proxies. The sidecar listens
        # on loopback/localhost; routing through a corporate proxy would fail
        # or hang, so we force a direct connection.
        self._opener: urllib.request.OpenerDirector = urllib.request.build_opener(
            urllib.request.ProxyHandler({})
        )

    @property
    def base_url(self) -> str:
        """URL of the connected telemetry service."""
        return self._base_url

    def query(self, sql: str, timeout: float | None = None) -> dict[str, Any]:
        """Run a SQL query through the sidecar API and return its parsed
        JSON response (``{"rows": [...]}`` on success)."""
        payload = json.dumps({"sql": sql}).encode("utf-8")
        request = urllib.request.Request(
            f"{self._base_url}/api/query",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            effective_timeout = self._timeout if timeout is None else timeout
            with self._opener.open(request, timeout=effective_timeout) as response:
                result = json.loads(response.read().decode("utf-8"))
                if not isinstance(result, dict) or not isinstance(
                    result.get("rows"), list
                ):
                    raise RuntimeError(f"invalid telemetry query response: {result!r}")
                if not all(isinstance(row, dict) for row in result["rows"]):
                    raise RuntimeError(
                        f"invalid telemetry query rows: {result['rows']!r}"
                    )
                return result
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
