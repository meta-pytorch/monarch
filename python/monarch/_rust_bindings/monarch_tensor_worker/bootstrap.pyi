# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import final, Optional, Tuple

class WorkerServerRequest:
    """
    Python binding for the Rust WorkerServerRequest enum.
    """

    @final
    class Run(WorkerServerRequest):
        """
        Create a Run request variant.

        Args:
            world_id: The ID of the world
            proc_id: The ID of the process
            bootstrap_addr: The bootstrap address

        Returns:
            A WorkerServerRequest.Run instance
        """
        def __init__(
            self,
            *,
            world_id: str,
            proc_id: str,
            bootstrap_addr: str,
            labels: list[Tuple[str, str]],
        ) -> None: ...

    @final
    class Exit(WorkerServerRequest):
        """
        Create an Exit request variant.

        Returns:
            A WorkerServerRequest.Exit instance
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

class WorkerServerResponse:
    """
    Python binding for the Rust WorkerServerResponse enum.
    """

    @final
    class Finished(WorkerServerResponse):
        """
        Create a Finished response variant.

        Args:
            error: An optional error message if the operation failed

        Returns:
            A WorkerServerResponse.Finished instance
        """

        error: Optional[str]

    @classmethod
    def from_json(cls, json: str) -> "WorkerServerResponse":
        """
        Create a WorkerServerResponse from a JSON string.

        Args:
            json: A JSON string representation of a WorkerServerResponse

        Returns:
            The deserialized WorkerServerResponse

        Raises:
            Exception: If deserialization fails
        """
        pass
