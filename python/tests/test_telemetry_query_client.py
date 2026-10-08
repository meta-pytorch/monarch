# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import json
from unittest.mock import MagicMock

import pytest
from monarch._src.job._telemetry_query_client import QueryEngineClient


def test_query_uses_default_and_override_timeouts() -> None:
    client = QueryEngineClient("http://127.0.0.1:1234", timeout=10)
    opener = MagicMock()
    response = opener.open.return_value.__enter__.return_value
    response.read.return_value = b'{"rows": []}'
    client._opener = opener

    assert client.query("SELECT 1") == {"rows": []}
    assert opener.open.call_args.kwargs["timeout"] == 10

    assert client.query("SELECT 1", timeout=42) == {"rows": []}
    assert opener.open.call_count == 2
    assert opener.open.call_args.kwargs["timeout"] == 42


@pytest.mark.parametrize("result", [[], None, {}, {"rows": None}, {"rows": [42]}])
def test_query_rejects_invalid_response_shape(result) -> None:
    client = QueryEngineClient("http://127.0.0.1:1234")
    client._opener = MagicMock()
    response = client._opener.open.return_value.__enter__.return_value
    response.read.return_value = json.dumps(result).encode()
    with pytest.raises(RuntimeError, match="invalid telemetry query"):
        client.query("SELECT 1")
