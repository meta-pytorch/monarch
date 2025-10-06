# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Type hints for the monarch_hyperactor.config Rust bindings.
"""

from typing import Any, Dict, Optional

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport

def reload_config_from_env() -> None:
    """
    Reload configuration from environment variables.

    This reads all HYPERACTOR_* environment variables and updates
    the global configuration.
    """
    ...

def configure(
    default_transport: Optional[ChannelTransport] = None,
) -> None:
    """
    Configure typed key-value pairs in the hyperactor global configuration.
    """
    ...

def get_configuration() -> Dict[str, Any]: ...
