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
    """Context manager that yields the job state and cleans up on exit.

    On a clean exit, gracefully shuts down all host meshes. If graceful
    shutdown fails, kills the job to avoid leaking worker processes and
    re-raises so the failure is visible: tests that exit without fully
    cleaning up leak pending message acks and surface later as
    ``Unhandled monarch error on the root actor`` on some other, unrelated
    test. Making the failure loud and local beats the cross-test
    misattribution.

    If the body of the ``with`` raises, kills the job and propagates the
    original exception. We do not attempt graceful shutdown in that case
    because the mesh may already be in a broken state.

    Calling ``job.kill()`` after a *successful* shutdown causes the global
    monarch error handler to fire (due to pending message acks), so we
    only kill on failure paths.

    Example::

        with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
            host = state.hosts
            proc = host.spawn_procs(...)
    """
    state = job.state(cached_path=cached_path)
    body_ok = False
    shutdown_error: Optional[BaseException] = None
    try:
        yield state
        body_ok = True
    finally:
        if body_ok:
            try:
                for host_mesh in state._hosts.values():
                    # Skip meshes the test body already shut down.
                    if host_mesh._inner_host_mesh is None:
                        continue
                    host_mesh.shutdown().get(timeout=30.0)
            except BaseException as e:
                shutdown_error = e
        if not body_ok or shutdown_error is not None:
            try:
                job.kill()
            except Exception:
                pass

    if shutdown_error is not None:
        raise RuntimeError(
            "scoped_state: host_mesh.shutdown() did not complete within "
            "30s; workers have been killed but pending message acks may "
            "still surface as 'Unhandled monarch error' on the root client."
        ) from shutdown_error
