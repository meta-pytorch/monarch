# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import string
from dataclasses import dataclass, field
from typing import Any, Optional

from monarch.tools.network import get_sockaddr
from torchx import specs
from torchx.specs.api import is_terminal

DEFAULT_REMOTE_ALLOCATOR_PORT = 26600

_TAG_MESHES_PREFIX = "monarch/meshes/${mesh_name}/"
_TAG_HOST_TYPE: str = _TAG_MESHES_PREFIX + "host_type"
_TAG_GPUS: str = _TAG_MESHES_PREFIX + "gpus"
_TAG_TRANSPORT: str = _TAG_MESHES_PREFIX + "transport"

_UNSET_INT = -1
_UNSET_STR = "__UNSET__"


@dataclass
class MeshSpec:
    """Doubles as the 'input' specifications of how to setup the mesh role
    when submitting the job and as the 'info' (describe) API's return value.
    """

    name: str
    num_hosts: int
    host_type: str = _UNSET_STR
    gpus: int = _UNSET_INT
    # NOTE: using str over monarch._rust_bindings.monarch_hyperactor.channel.ChannelTransport enum
    #  b/c the rust binding doesn't have Python enum semantics, hence doesn't serialize well
    transport: str = "tcp"
    port: int = DEFAULT_REMOTE_ALLOCATOR_PORT
    hostnames: list[str] = field(default_factory=list)
    state: specs.AppState = specs.AppState.UNSUBMITTED
    image: str = _UNSET_STR

    def server_addrs(
        self, transport: Optional[str] = None, port: Optional[int] = None
    ) -> list[str]:
        """
        Returns the hostnames (servers) in channel address format.
        `transport` and `port` is typically taken from this mesh spec's fields, but
        the caller can override them when calling this function.
        """

        transport = transport or self.transport
        port = port or self.port

        if transport == "tcp":
            # need to resolve hostnames to ip address for TCP
            return [
                f"tcp!{get_sockaddr(hostname, port)}" for hostname in self.hostnames
            ]
        elif transport == "metatls":
            return [f"metatls!{hostname}:{port}" for hostname in self.hostnames]
        else:
            raise ValueError(
                f"Unsupported transport: {transport}. Must be one of: 'tcp' or 'metatls'"
            )


def _tag(mesh_name: str, tag_template: str) -> str:
    return string.Template(tag_template).substitute(mesh_name=mesh_name)


def tag_as_metadata(mesh_spec: MeshSpec, appdef: specs.AppDef) -> None:
    appdef.metadata[_tag(mesh_spec.name, _TAG_HOST_TYPE)] = mesh_spec.host_type
    appdef.metadata[_tag(mesh_spec.name, _TAG_GPUS)] = str(mesh_spec.gpus)
    appdef.metadata[_tag(mesh_spec.name, _TAG_TRANSPORT)] = mesh_spec.transport


def mesh_spec_from_metadata(appdef: specs.AppDef, mesh_name: str) -> Optional[MeshSpec]:
    for role in appdef.roles:
        if role.name == mesh_name:
            return MeshSpec(
                name=mesh_name,
                image=role.image,
                num_hosts=role.num_replicas,
                host_type=appdef.metadata.get(
                    _tag(mesh_name, _TAG_HOST_TYPE), _UNSET_STR
                ),
                gpus=int(
                    appdef.metadata.get(_tag(mesh_name, _TAG_GPUS), str(_UNSET_INT))
                ),
                transport=appdef.metadata.get(_tag(mesh_name, _TAG_TRANSPORT), "tcp"),
                port=role.port_map.get("mesh", DEFAULT_REMOTE_ALLOCATOR_PORT),
            )

    return None


def mesh_spec_from_str(mesh_spec_str: str) -> MeshSpec:
    """Parses the given string into a MeshSpec.

    Args:
        mesh_spec_str: A string representation of the mesh specification
            in the format 'NAME:NUM_HOSTS:HOST_TYPE' (e.g. 'trainer:8:gpu.medium').
    """
    parts = mesh_spec_str.split(":")
    assert len(parts) == 3, (
        f"`{mesh_spec_str}` is not of the form 'NAME:NUM_HOSTS:HOST_TYPE'"
    )

    name, num_hosts, host_type = parts
    gpus = specs.resource(h=host_type).gpu

    assert num_hosts.isdigit(), f"`{num_hosts}` is not a number in: {mesh_spec_str}"

    return MeshSpec(name, int(num_hosts), host_type, gpus)


@dataclass
class ServerSpec:
    """Holds information (as returned by the 'describe' API of the scheduler)
    about the monarch server. This is the return value of ``monarch.tools.commands.info` API.
    """

    name: str
    state: specs.AppState
    meshes: list[MeshSpec]
    scheduler: str
    namespace: str = ""
    ui_url: Optional[str] = None
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def server_handle(self) -> str:
        return f"{self.scheduler}://{self.namespace}/{self.name}"

    @property
    def is_running(self) -> bool:
        return self.state == specs.AppState.RUNNING

    def host0(self, mesh_name: str) -> str:
        """The hostname of the first node in the given mesh.
        The return value of this method can be used to set `MASTER_ADDR` env var for torch.distributed.

        NOTE: the state of this server must be RUNNING for this method to return a valid value.

        Usage:

        .. code-block::python
            from monarch.tools.commands import get_or_create

            server_info = await get_or_create(...)
            assert server_info.is_running

            # allocate proc mesh -> create actor (code omitted for brevity)...

            trainer_actor.call(
                MASTER_ADDR=server_info.host0("trainer") # trainer mesh's 1st host
                MASTER_PORT=29500,
                ...
            )

        NOTE: The ordering of the hostnames is exactly the same as what comes back from the underlying
        scheduler's `describe_job` or `list_*` API. Please find the exact semantics in the
        respective scheduler's implementation in https://github.com/pytorch/torchx/tree/main/torchx/schedulers.
        """
        mesh_spec = self.get_mesh_spec(mesh_name)
        if self.is_running:
            # hostnames are only valid when the server is RUNNING
            if not mesh_spec.hostnames:
                raise RuntimeError(f"{self.server_handle} does not have any hosts")
            return mesh_spec.hostnames[0]
        elif self.state in [specs.AppState.SUBMITTED, specs.AppState.PENDING]:
            raise RuntimeError(
                f"{self.server_handle} is {self.state}."
                f" Use `monarch.tools.commands.server_ready()` to wait for the server to be {specs.AppState.RUNNING}"
            )
        elif is_terminal(self.state):
            raise RuntimeError(
                f"{self.server_handle} is {self.state}."
                " Use `monarch.tools.commands.get_or_create()` to create a new server"
            )
        else:
            raise RuntimeError(
                f"{self.server_handle} is in an invalid state: {self.state}. Please report this as a bug"
            )

    def get_mesh_spec(self, mesh_name: str) -> MeshSpec:
        for mesh_spec in self.meshes:
            if mesh_spec.name == mesh_name:
                return mesh_spec

        raise ValueError(
            f"Mesh: '{mesh_name}' not found in job: {self.name}. Try one of: {self.get_mesh_names()}"
        )

    def get_mesh_names(self) -> list[str]:
        return [m.name for m in self.meshes]

    def to_json(self) -> dict[str, Any]:
        """Returns the JSON form of this struct that can be printed to console by:

        .. code-block:: python

            import json

            server_spec = ServerSpec(...)
            print(json.dumps(server_spec, indent=2))
        """

        return {
            "name": self.name,
            "server_handle": self.server_handle,
            **({"ui_url": self.ui_url} if self.ui_url else {}),
            "state": self.state.name,
            "meshes": {
                mesh.name: {
                    "host_type": mesh.host_type,
                    "hosts": mesh.num_hosts,
                    "gpus": mesh.gpus,
                    "hostnames": mesh.hostnames,
                }
                for mesh in self.meshes
            },
        }
