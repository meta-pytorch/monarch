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
    """Configure Hyperactor runtime defaults for this process.

    This updates the **runtime** configuration layer from Python,
    setting the default channel transport and optional logging
    behaviour (forwarding, file capture, and how many lines to tail),
    plus any additional CONFIG-marked keys passed via **kwargs.

    Historically this API is named ``configure(...)``; conceptually it
    acts as "set runtime config for this process".
    """
    ...

def get_global_config() -> Dict[str, Any]:
    """Return a snapshot of the current Hyperactor configuration.

    The result is a plain dictionary view of the merged configuration
    (defaults plus any overrides from environment or Python), useful
    for debugging and tests.
    """
    ...

def get_runtime_config() -> Dict[str, Any]:
    """Return a snapshot of the Runtime layer configuration.

    The Runtime layer contains only configuration values set from
    Python via configure(). This returns only those Python-exposed
    keys currently in the Runtime layer (not merged across all layers
    like `get_global_config()`).

    This can be used to snapshot/restore Runtime state.
    """
    ...

def clear_runtime_config() -> None:
    """Clear all Runtime layer configuration overrides.

    Safely removes all entries from the Runtime config layer. Since
    the Runtime layer is exclusively populated via Python's
    `configure()`, this will not affect configuration from environment
    variables, config files, or built-in defaults.
    """

    ...
