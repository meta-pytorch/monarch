# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Type hints for the monarch_hyperactor.config Rust bindings.
"""

from typing import Any, Dict

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport

def reload_config_from_env() -> None:
    """
    Reload configuration from environment variables.

    This reads all HYPERACTOR_* environment variables and updates
    the global configuration.
    For any configuration setting not present in environment variables,
    this function will not change its value.
    """
    ...

def reset_config_to_defaults() -> None:
    """Reset all configuration to default values, ignoring environment variables.
    Call reload_config_from_env() to reload the environment variables.
    """
    ...

def configure(
    default_transport: ChannelTransport = ...,
    enable_log_forwarding: bool = ...,
    enable_file_capture: bool = ...,
    tail_log_lines: int = ...,
    **kwargs: object,
) -> None:
    """Change a configuration value in the global configuration. If called with
    no arguments, makes no changes. Does not reset any configuration"""
    ...

def get_configuration() -> Dict[str, Any]: ...
