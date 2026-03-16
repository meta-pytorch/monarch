# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Utilities for tests that use Job instances."""

from contextlib import contextmanager
from typing import Generator, Optional

from monarch._src.job.job import JobState, JobTrait


@contextmanager
def scoped_state(
    job: JobTrait, cached_path: Optional[str] = ".monarch/job_state.pkl"
) -> Generator[JobState, None, None]:
    """Context manager that yields the job state and kills the job on exit.

    On a clean exit, gracefully shuts down all host meshes before killing
    the job. On an abnormal exit (exception), skips shutdown and goes
    straight to kill. Failures to kill are silently ignored, as the job
    may already be in a broken state.

    Example::

        with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
            host = state.hosts
            proc = host.spawn_procs(...)
    """
    state = job.state(cached_path=cached_path)
    success = False
    try:
        yield state
        success = True
    finally:
        try:
            if success:
                for host_mesh in state._hosts.values():
                    host_mesh.shutdown().get(timeout=10.0)
        finally:
            try:
                job.kill()
            except Exception:
                pass
