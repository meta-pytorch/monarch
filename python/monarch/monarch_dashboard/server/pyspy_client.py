# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Client for triggering on-demand py-spy stack dumps via the Mesh Admin API.

py-spy is proc-level: the mesh-admin server's ``GET /v1/pyspy/{proc_reference}``
attaches py-spy to the OS process hosting that proc (``ProcAgent``/``HostAgent``)
and returns a structured ``PySpyResult`` JSON synchronously. This module is the
thin HTTP(S)+mTLS client the dashboard backend uses to reach it; the response is
passed through to the frontend verbatim for rendering.

Config (all overridable by env so this works outside the default devvm setup):
  * ``MONARCH_ADMIN_URL`` — full base URL of the mesh-admin server. If unset,
    falls back to ``https://<fqdn>:<MONARCH_MESH_ADMIN_PORT|1729>``.
  * ``MONARCH_MESH_ADMIN_{CACERT,CERT,KEY}`` — mTLS material; defaults to the
    standard Meta host identity paths. The server always speaks HTTPS.
"""

import json
import os
import socket
import ssl
import urllib.parse
import urllib.request
from typing import Any, Dict

_DEFAULT_PORT = "1729"
# Must exceed the mesh-admin bridge timeout (~13s) so a structured PySpyResult
# comes back even when py-spy uses its full native-unwinding budget.
_TIMEOUT_SECS = 22

_CACERT = os.environ.get("MONARCH_MESH_ADMIN_CACERT", "/var/facebook/rootcanal/ca.pem")
_CERT = os.environ.get(
    "MONARCH_MESH_ADMIN_CERT", "/var/facebook/x509_identities/server.pem"
)
_KEY = os.environ.get("MONARCH_MESH_ADMIN_KEY", _CERT)

_ctx_cache: ssl.SSLContext | None = None
_ctx_built = False


def mesh_admin_base_url() -> str:
    """Resolve the mesh-admin base URL (env override, else this host's FQDN)."""
    url = os.environ.get("MONARCH_ADMIN_URL")
    if url:
        return url.rstrip("/")
    port = os.environ.get("MONARCH_MESH_ADMIN_PORT", _DEFAULT_PORT)
    return f"https://{socket.getfqdn()}:{port}"


def _ssl_context() -> ssl.SSLContext | None:
    """Build (once) an mTLS context from the host identity certs, if present."""
    global _ctx_cache, _ctx_built
    if _ctx_built:
        return _ctx_cache
    _ctx_built = True
    if not os.path.exists(_CACERT):
        _ctx_cache = None
        return None
    ctx = ssl.create_default_context(cafile=_CACERT)
    if os.path.exists(_CERT):
        ctx.load_cert_chain(certfile=_CERT, keyfile=_KEY)
    _ctx_cache = ctx
    return ctx


def capture_pyspy_dump(proc_ref: str) -> Dict[str, Any]:
    """Trigger a fresh py-spy dump for ``proc_ref`` and return the PySpyResult.

    ``proc_ref`` is a proc reference string exactly as it appears in
    ``/api/dag`` proc-node ``entity_id`` fields. Raises on transport/TLS
    failure (the caller maps that to an error response).
    """
    base = mesh_admin_base_url()
    encoded = urllib.parse.quote(proc_ref, safe="")
    req = urllib.request.Request(f"{base}/v1/pyspy/{encoded}")
    resp = urllib.request.urlopen(req, context=_ssl_context(), timeout=_TIMEOUT_SECS)
    return json.loads(resp.read())
