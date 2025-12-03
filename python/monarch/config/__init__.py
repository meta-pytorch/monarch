# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Configuration utilities for Monarch.

This module provides utilities for managing Monarch's runtime
configuration, particularly useful for testing and temporary
configuration overrides.
"""

import contextlib
from typing import Any, Dict, Iterator

from monarch._rust_bindings.monarch_hyperactor.config import (
    clear_runtime_config,
    configure,
    get_global_config,
    get_runtime_config,
)


__all__ = [
    "clear_runtime_config",
    "configure",
    "configured",
    "get_global_config",
    "get_runtime_config",
]


@contextlib.contextmanager
def configured(**overrides) -> Iterator[Dict[str, Any]]:
    """Temporarily apply Python-side config overrides for this
    process.

    This context manager:
      * snapshots the current **Runtime** configuration layer
        (`get_runtime_config()`),
      * applies the given `overrides` via `configure(**overrides)`,
        and
      * yields the **merged** view of config (`get_global_config()`),
        including defaults, env, file, and Runtime.

    On exit it restores the previous Runtime layer by:
      * clearing all Runtime entries, and
      * re-applying the saved snapshot.

    `configured` alters the global configuration; thus other threads
    will be subject to the overriden configuration while the context
    manager is active.

    Thus: this is intended for tests, which run as single threads;
    per-test overrides do not leak into other tests.

    Args:
        **overrides: Configuration key-value pairs to override for the
            duration of the context.

    Yields:
        Dict[str, Any]: The merged global configuration including all
            layers (defaults, environment, file, and runtime).

    Example:
        >>> from monarch.config import configured
        >>> with configured(enable_log_forwarding=True, tail_log_lines=100):
        ...     # Configuration is temporarily overridden
        ...     assert get_global_config()["enable_log_forwarding"] is True
        >>> # Configuration is automatically restored after the context

    """
    # Retrieve runtime
    prev = get_runtime_config()
    try:
        # Merge overrides into runtime
        configure(**overrides)

        # Snapshot of merged config (all layers)
        yield get_global_config()
    finally:
        # Restore previous runtime
        clear_runtime_config()
        configure(**prev)
