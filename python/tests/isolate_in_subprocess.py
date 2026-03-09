# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Decorator to run a test function in an isolated subprocess.

This is useful when a test needs process-level isolation (e.g., because it
uses a per-process singleton). The decorator replaces the test with a wrapper
that spawns a subprocess, runs the original test function in it, and
communicates the result back via OS pipes.

The test function is sent to the subprocess via cloudpickle over a pipe,
so no special import machinery is needed.

Result communication:
  - On success, skip, or exception: the subprocess writes a pickled result
    dict to a result pipe and exits with code 0.
  - On crash (segfault, kill, etc.): nothing is written and exit code != 0.
  The parent only reads from the result pipe when the subprocess exits
  cleanly.
"""

import asyncio
import functools
import os
import pickle
import signal
import subprocess
import sys
import traceback

import cloudpickle
import pytest


def isolate_in_subprocess(test_fn=None, *, env=None):
    """Decorator that runs a test in a separate subprocess.

    Usage::

        @isolate_in_subprocess
        async def test_something():
            ...

        @isolate_in_subprocess(env={"FOO": "bar"})
        async def test_with_env():
            ...

    Args:
        env: Extra environment variables for the subprocess.

    The decorated function can be sync or async. ``pytest.mark`` decorators
    applied *before* this decorator (i.e. listed after it in source) are
    evaluated by pytest in the parent process as usual.
    """
    if test_fn is None:
        return functools.partial(isolate_in_subprocess, env=env)

    if env is None:
        env = {}

    @functools.wraps(test_fn)
    def wrapper(*args, **kwargs):
        # Pipe for sending the pickled test function + args (parent writes, child reads).
        fn_read_fd, fn_write_fd = os.pipe()
        # Pipe for receiving the result (child writes, parent reads).
        result_read_fd, result_write_fd = os.pipe()

        sub_env = {**os.environ, **env}

        if "FB_XAR_INVOKED_NAME" in os.environ:
            # PAR/XAR mode: sys.executable is the PAR's bundled Python
            # runtime which cannot run arbitrary scripts.  Re-invoke the
            # PAR binary itself with PAR_MAIN_OVERRIDE pointing at this
            # module, following the pattern from proc_mesh.py.
            launch_cmd = [sys.argv[0], str(fn_read_fd), str(result_write_fd)]
            sub_env["PAR_MAIN_OVERRIDE"] = "isolate_in_subprocess"
            sub_env["PYTHONPATH"] = os.pathsep.join(sys.path)
        else:
            # OSS: use the Python interpreter directly.
            launch_cmd = [
                sys.executable,
                os.path.abspath(__file__),
                str(fn_read_fd),
                str(result_write_fd),
            ]
            # Ensure sibling test modules are importable.
            my_dir = os.path.dirname(os.path.abspath(__file__))
            sub_env["PYTHONPATH"] = os.pathsep.join(
                filter(None, [my_dir, sub_env.get("PYTHONPATH", "")])
            )

        proc = subprocess.Popen(
            launch_cmd,
            env=sub_env,
            pass_fds=(fn_read_fd, result_write_fd),
            start_new_session=True,
        )
        # Close the child's ends in the parent.
        os.close(fn_read_fd)
        os.close(result_write_fd)

        # Capture the current runtime config so it can be restored in the child.
        # Some config values contain unpicklable Rust objects, so we filter
        # to only those that survive pickling.
        from monarch.config import get_runtime_config

        runtime_config = {}
        for k, v in get_runtime_config().items():
            try:
                pickle.dumps(v)
                runtime_config[k] = v
            except Exception:
                pass

        # Send the test function, its arguments, and the runtime config to the child.
        with os.fdopen(fn_write_fd, "wb") as f:
            cloudpickle.dump((test_fn, args, kwargs, runtime_config), f)

        returncode = proc.wait()

        # Read the result before killing the process group, since we need
        # the pipe data while the child's fd is still valid.
        if returncode == 0:
            with os.fdopen(result_read_fd, "rb") as f:
                data = f.read()
        else:
            os.close(result_read_fd)
            data = None

        # Kill the entire process group to clean up any leaked child
        # processes (e.g. spawned proc meshes, worker processes).
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            pass  # Already gone.

        if returncode != 0:
            pytest.fail(
                f"Subprocess crashed with exit code {returncode}", pytrace=False
            )

        if not data:
            pytest.fail(
                "Subprocess exited cleanly but produced no result", pytrace=False
            )

        result = pickle.loads(data)

        if result["status"] == "passed":
            return
        elif result["status"] == "skipped":
            pytest.skip(result["reason"])
        elif result["status"] == "failed":
            pytest.fail(
                f"{result['exc_type']}: {result['message']}\n\n"
                f"Subprocess traceback:\n{result['traceback']}",
                pytrace=False,
            )

    wrapper.__wrapped__ = test_fn
    return wrapper


def _run_test(test_fn, args=(), kwargs=None):
    """Run *test_fn* and return a result dict."""
    if kwargs is None:
        kwargs = {}
    try:
        if asyncio.iscoroutinefunction(test_fn):
            asyncio.run(test_fn(*args, **kwargs))
        else:
            test_fn(*args, **kwargs)
        return {"status": "passed"}
    except pytest.skip.Exception as e:
        return {"status": "skipped", "reason": str(e)}
    except BaseException as e:
        return {
            "status": "failed",
            "exc_type": type(e).__name__,
            "message": str(e),
            "traceback": traceback.format_exc(),
        }


def subprocess_main() -> None:
    """Entry point: read a cloudpickled test function from a pipe, run it."""
    fn_read_fd = int(sys.argv[1])
    result_write_fd = int(sys.argv[2])

    with os.fdopen(fn_read_fd, "rb") as f:
        test_fn, args, kwargs, runtime_config = pickle.load(f)

    # Restore the parent's runtime config in this subprocess.
    from monarch.config import configure

    configure(**runtime_config)

    result = _run_test(test_fn, args, kwargs)

    with os.fdopen(result_write_fd, "wb") as f:
        f.write(pickle.dumps(result))

    sys.exit(0)


if __name__ == "__main__":
    subprocess_main()
