# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import asyncio
import ctypes

import click
from monarch._rust_bindings.monarch_extension.blocking import blocking_function
from monarch._rust_bindings.monarch_extension.panic import panicking_function
from monarch._src.actor.host_mesh import this_host
from monarch._src.actor.proc_mesh import ProcMesh
from monarch.actor import Actor, endpoint, send


def spawn_procs_on_this_host(per_host: dict[str, int]) -> ProcMesh:
    return this_host().spawn_procs(per_host=per_host)


class ErrorActor(Actor):
    """An actor that has endpoints cause segfaults."""

    @endpoint
    async def cause_segfault(self) -> None:
        """Endpoint that causes a segmentation fault."""
        # Create a C function pointer to an invalid memory address
        # This will reliably cause a segmentation fault when called
        function_type = ctypes.CFUNCTYPE(None)
        # Use a non-zero but invalid address to avoid ctypes null pointer checks
        invalid_address = 0xDEADBEEF
        invalid_function = function_type(invalid_address)
        # Calling this function will cause a segfault
        invalid_function()

    @endpoint
    async def cause_panic(self) -> None:
        """Endpoint that calls a Rust function that panics."""
        panicking_function()

    @endpoint
    async def cause_stuck(self) -> None:
        """Endpoint that causes the process to hang indefinitely."""
        blocking_function()

    @endpoint
    async def await_then_error(self) -> None:
        await asyncio.sleep(0.1)
        await asyncio.sleep(0.1)
        raise RuntimeError("oh noez")

    @endpoint
    async def get_pid(self) -> int:
        """Endpoint that returns the process PID."""
        import os

        return os.getpid()


class ErrorActorSync(Actor):
    """An actor that has endpoints cause segfaults."""

    @endpoint  # pyre-ignore
    def cause_segfault(self) -> None:
        """Endpoint that causes a segmentation fault."""
        # Create a C function pointer to an invalid memory address
        # This will reliably cause a segmentation fault when called
        function_type = ctypes.CFUNCTYPE(None)
        # Use a non-zero but invalid address to avoid ctypes null pointer checks
        invalid_address = 0xDEADBEEF
        invalid_function = function_type(invalid_address)
        # Calling this function will cause a segfault
        invalid_function()

    @endpoint  # pyre-ignore
    def cause_panic(self) -> None:
        """Endpoint that calls a Rust function that panics."""
        panicking_function()


def _run_error_test_sync(num_procs, sync_endpoint, endpoint_name):
    proc = spawn_procs_on_this_host({"gpus": num_procs})
    if sync_endpoint:
        actor_class = ErrorActorSync
    else:
        actor_class = ErrorActor
    error_actor = proc.spawn("error_actor", actor_class)

    # This output is checked in the test to make sure that the process actually got here
    print("Started function error_test", flush=True)

    if endpoint_name == "cause_segfault":
        endpoint = error_actor.cause_segfault
    elif endpoint_name == "cause_panic":
        endpoint = error_actor.cause_panic
    else:
        raise ValueError(f"Unknown endpoint name: {endpoint_name}")

    # Exercise both call() and call_one() in our tests, to check that error
    # aggregation behavior is consistent.
    if num_procs == 1:
        endpoint.call_one().get()
    else:
        endpoint.call().get()


def _run_error_test(num_procs, sync_endpoint, endpoint_name):
    import asyncio

    if sync_endpoint:
        actor_class = ErrorActorSync
    else:
        actor_class = ErrorActor

    async def run_test():
        proc = spawn_procs_on_this_host(per_host={"gpus": num_procs})
        error_actor = proc.spawn("error_actor", actor_class)

        # This output is checked in the test to make sure that the process actually got here
        print("Started function error_test", flush=True)

        if endpoint_name == "cause_segfault":
            endpoint = error_actor.cause_segfault
        elif endpoint_name == "cause_panic":
            endpoint = error_actor.cause_panic
        else:
            raise ValueError(f"Unknown endpoint name: {endpoint_name}")

        # Exercise both call() and call_one() in our tests, to check that error
        # aggregation behavior is consistent.
        if num_procs == 1:
            await endpoint.call_one()
        else:
            await endpoint.call()

    asyncio.run(run_test())


@click.group()
def main():
    pass


@main.command("error-endpoint")
@click.option("--num-procs", type=int, required=True)
@click.option("--sync-test-impl", type=bool, required=True)
@click.option("--sync-endpoint", type=bool, required=True)
@click.option("--endpoint-name", type=str, required=True)
def error_endpoint(num_procs, sync_test_impl, sync_endpoint, endpoint_name, v1):
    print(
        f"Running segfault test: {num_procs=} {sync_test_impl=} {sync_endpoint=}, {endpoint_name=}"
    )

    if sync_test_impl:
        _run_error_test_sync(num_procs, sync_endpoint, endpoint_name, v1)
    else:
        _run_error_test(num_procs, sync_endpoint, endpoint_name, v1)


@main.command("error-bootstrap")
def error_bootstrap():
    print("Started function error_bootstrap", flush=True)
    spawn_procs_on_this_host(
        {"gpus": 4}, env={"MONARCH_ERROR_DURING_BOOTSTRAP_FOR_TESTING": "1"}
    ).initialized.get()


async def _error_unmonitored():
    print("Started function _error_unmonitored", flush=True)

    proc = spawn_procs_on_this_host({"gpus": 1})
    actor = proc.spawn("error_actor", ErrorActor)

    # fire and forget
    send(actor.await_then_error, (), {}, None, "all")

    # Wait. Eventually a supervision event will get propagated and the process
    # will exit.
    #
    # If an event is not delivered, the test will time out before this sleep
    # finishes.
    await asyncio.sleep(300)


"""
TODO: This test should be enabled when stop() is fully implemented.
async def _error_unmonitored(v1):
    print("I actually ran")
    sys.stdout.flush()

    proc = spawn_procs_on_this_host(v1, {"gpus": 1})
    actor = proc.spawn("error_actor", ErrorActor)

    # fire and forget
    send(actor.cause_stuck, (), {}, None, "all")
    proc_mesh.stop()

    # Wait. Eventually a supervision event will get propagated and the process
    # will exit.
    #
    # If an event is not delivered, the test will time out before this sleep
    # finishes.
    await asyncio.sleep(300)
"""


@main.command("error-unmonitored")
def error_unmonitored():
    asyncio.run(_error_unmonitored())


async def _error_cleanup():
    """Test function that spawns an 8 process procmesh and calls an endpoint that returns a normal exception."""
    print("Started function _error_cleanup() for parent process", flush=True)

    # Spawn an 8 process procmesh
    proc = spawn_procs_on_this_host({"gpus": 8})
    error_actor = proc.spawn("error_actor", ErrorActor)

    print("Procmesh spawned, collecting child PIDs from actors", flush=True)

    # Get PIDs from all actor processes
    try:
        # Call get_pid endpoint on all actors to collect their PIDs
        pids = await error_actor.get_pid.call()
        child_pids = [str(pid) for _, pid in pids]
        print(f"CHILD_PIDS: {','.join(child_pids)}", flush=True)
    except Exception as e:
        print(f"Error getting child PIDs from actors: {e}", flush=True)

    print("About to call endpoint that raises exception", flush=True)

    # Call an endpoint that raises a normal exception
    try:
        await error_actor.await_then_error.call()
    except Exception as e:
        print(f"Expected exception caught: {e}", flush=True)
        # Re-raise to cause the process to exit with non-zero code
        raise


@main.command("error-cleanup")
def error_cleanup():
    """Command that spawns an 8 process procmesh and calls an endpoint that returns a normal exception."""
    asyncio.run(_error_cleanup())


if __name__ == "__main__":
    main()
