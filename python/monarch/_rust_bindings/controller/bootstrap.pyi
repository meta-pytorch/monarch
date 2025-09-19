# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import final, List, Optional, Tuple

@final
class ControllerCommand:
    """
    Python binding for the Rust ControllerCommand struct.
    """

    worker_world: str
    """The worker world to create"""

    system_addr: str
    """The system address to bootstrap with."""

    controller_actor_id: str = "controller[0].root"
    """The controller actor id to give to."""

    world_size: int
    """Global world size for this job"""

    num_procs_per_host: int = 8
    """The number of processes per host."""

    worker_name: str = "worker"
    """The worker name."""

    program: Optional[str] = None
    """
    The worker program to execute for each process.
    It is not needed if worker procs are directly launched without management from host actors.
    """

    supervision_query_interval_in_sec: int = 2
    """
    The supervision check interval in seconds.
    It indicates how often the controller will poll system actor to check the status of all procs/actors in a world.
    This decides how fast the client could observe a failure in the system.
    """

    supervision_update_interval_in_sec: int = 2
    """
    The supervision update interval in seconds.
    It indicates how often the controller proc should report its supervision status to the system.
    """

    worker_progress_check_interval_in_sec: int = 10
    """
    The worker progress check interval in seconds.
    It indicates how often the controller will check that progress is being made.
    """

    operation_timeout_in_sec: int = 120
    """
    The operation timeout duration interval in seconds.
    It indicates how long we will allow progress to stall for before letting the client know that worker(s) may be stuck.
    """

    operations_per_worker_progress_request: int = 100
    """
    The number of operations invoked before we proactively check worker progress.
    If a large number of operations are invoked all at once, it is expected that it will take a while for all operations
    to complete so we want to inject progress requests at a higher frequency to check if we are making progress.
    """

    fail_on_worker_timeout: bool = False
    """
    If a failure should be propagated to the client if a worker is detected to be stuck.
    """

    is_cpu_worker: bool = False
    """
    Whether to launch workers with CPU devices.
    """

    extra_proc_labels: Optional[List[Tuple[str, str]]] = None
    """Proc metadata which will be available through system."""

    def __init__(
        self,
        *,
        worker_world: str,
        system_addr: str,
        world_size: int,
        controller_actor_id: str = "controller[0].root",
        num_procs_per_host: int = 8,
        worker_name: str = "worker",
        program: Optional[str] = None,
        supervision_query_interval_in_sec: int = 2,
        supervision_update_interval_in_sec: int = 2,
        worker_progress_check_interval_in_sec: int = 10,
        operation_timeout_in_sec: int = 120,
        operations_per_worker_progress_request: int = 100,
        fail_on_worker_timeout: bool = False,
        is_cpu_worker: bool = False,
        extra_proc_labels: Optional[List[Tuple[str, str]]] = None,
    ) -> None:
        """
        Initialize a new ControllerCommand instance.

        Args:
            worker_world: The worker world to create
            system_addr: The system address to bootstrap with
            world_size: Global world size for this job
            controller_actor_id: The controller actor id to give to
            num_procs_per_host: The number of processes per host
            worker_name: The worker name
            program: The worker program to execute for each process
            supervision_query_interval_in_sec: The supervision check interval in seconds
            supervision_update_interval_in_sec: The supervision update interval in seconds
            worker_progress_check_interval_in_sec: The worker progress check interval in seconds
            operation_timeout_in_sec: The operation timeout duration interval in seconds
            operations_per_worker_progress_request: The number of operations invoked before we proactively check worker progress
            fail_on_worker_timeout: If a failure should be propagated to the client if a worker is detected to be stuck
            is_cpu_worker: Whether to launch workers with CPU devices
            extra_proc_labels: Proc metadata which will be available through system
        """
        pass

@final
class Index(int):
    """Python binding for the Rust Index type."""

    pass

@final
class RunCommand:
    """
    Python binding for the Rust RunCommand enum.
    Represents the different types of hyperactor to launch based on the subcommands.
    """

    @staticmethod
    def System(
        system_addr: str,
        supervision_update_timeout_in_sec: int = 20,
        world_eviction_timeout_in_sec: int = 10,
    ) -> "RunCommand":
        """
        Create a System command variant.

        Args:
            system_addr: The system address to bootstrap with
            supervision_update_timeout_in_sec: The supervision update timeout in seconds.
                A proc is considered dead if system doesn't get any supervision update
                from it within this timeout
            world_eviction_timeout_in_sec: Evict a world if it has been unhealthy for
                this many seconds

        Returns:
            A RunCommand.System instance
        """
        pass

    @staticmethod
    def Host(
        system_addr: str,
        host_world: str,
        host_rank: Index,
        supervision_update_interval_in_sec: int = 2,
    ) -> "RunCommand":
        """
        Create a Host command variant.

        Args:
            system_addr: The system address to bootstrap with
            host_world: The host world to create
            host_rank: The host rank; i.e., the index of the host in the world
            supervision_update_interval_in_sec: The supervision update interval in seconds,
                it indicates how often a proc should report its supervision status to the system

        Returns:
            A RunCommand.Host instance
        """
        pass

    @staticmethod
    def Controller(command: ControllerCommand) -> "RunCommand":
        """
        Create a Controller command variant.

        Args:
            command: The ControllerCommand to execute

        Returns:
            A RunCommand.Controller instance
        """
        pass

class ControllerServerRequest:
    """
    Python binding for the Rust ControllerServerRequest enum.
    """

    @final
    class Run(ControllerServerRequest):
        """
        Create a Run request variant.

        Args:
            command: The RunCommand to execute

        Returns:
            A ControllerServerRequest.Run instance
        """
        def __init__(
            self,
            command: RunCommand,
        ) -> None: ...

    @final
    class Exit(ControllerServerRequest):
        """
        Create an Exit request variant.

        Returns:
            A ControllerServerRequest.Exit instance
        """

        pass

    def to_json(self) -> str:
        """
        Convert this request to a JSON string.

        Returns:
            A JSON string representation of this request

        Raises:
            Exception: If serialization fails
        """
        pass

class ControllerServerResponse:
    """
    Python binding for the Rust ControllerServerResponse enum.
    """

    @final
    class Finished(ControllerServerResponse):
        """
        Create a Finished response variant.

        Args:
            error: An optional error message if the operation failed

        Returns:
            A ControllerServerResponse.Finished instance
        """

        error: Optional[str]

    @classmethod
    def from_json(cls, json: str) -> "ControllerServerResponse":
        """
        Create a ControllerServerResponse from a JSON string.

        Args:
            json: A JSON string representation of a ControllerServerResponse

        Returns:
            The deserialized ControllerServerResponse

        Raises:
            Exception: If deserialization fails
        """
        pass
