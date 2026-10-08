# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

def run(
    addr: str,
    *,
    admin_port: int | None = None,
    refresh_ms: int = 2000,
    theme: str = "nord",
    lang: str = "en",
    tls_ca: str | None = None,
    tls_cert: str | None = None,
    tls_key: str | None = None,
    plaintext: bool = False,
) -> None: ...
