# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Type hints for the monarch_hyperactor.config Rust bindings.
"""

def reload_config_from_env() -> None:
    """
    Reload configuration from environment variables.

    This reads all HYPERACTOR_* environment variables and updates
    the global configuration.
    """
    ...
