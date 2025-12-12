# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import csv
import json
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import count
from typing import (
    Any,
    DefaultDict,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import torch


class Command(NamedTuple):
    """
    Represents a node in the control flow DAG that tracks command execution on workers.

    Each Command node captures an operation executed on a specific worker and stream,
    including its control dependencies and associated devices.

    Attributes:
        worker_rank (int): Worker that executed the command
        stream_name (str): Stream on which the command was executed
        command_id (int): Unique identifier for the command
        command_name (str): Name of command (CallFunction: aten:mm, SendTensor: 7, etc.)
        devices (List[int]): Device IDs associated with this command
        control_dependencies (List[int]): Command IDs this command depends on
        traceback (Sequence[traceback.FrameSummary]): Python traceback at command execution
        duration (int): Command execution duration in milliseconds
    """

    worker_rank: int
    stream_name: str
    command_id: int
    command_name: str
    devices: List[int]
    control_dependencies: List[int]
    traceback: Sequence[traceback.FrameSummary]
    duration: int = 0  # ms


class StorageCreationEvent(NamedTuple):
    command_id: int
    storage_id: int
    size: Optional[int]
    devices: List[int]
    mesh_ref: Optional[int]
    stream_name: str


class StorageDeletionEvent(NamedTuple):
    command_id: int
    storage_id: int
    size: Optional[int]
    devices: List[int]
    mesh_ref: Optional[int]
    stream_name: str


class TensorCreationEvent(NamedTuple):
    command_id: int
    DTensorRef: int
    storage_id: int
    dtype: Optional[torch.dtype]
    dims: Optional[
        tuple
    ]  # TODO: make sure dims here reflect tensor's and not storages'
    devices: List[int]
    mesh_ref: Optional[int]
    stream_name: str


class TensorAccessEvent(NamedTuple):
    command_id: int
    DTensorRef: int
    storage_id: int
    dtype: Optional[torch.dtype]
    dims: Optional[tuple]
    devices: List[int]
    mesh_ref: Optional[int]
    stream_name: str


class TensorMutationEvent(NamedTuple):
    command_id: int
    DTensorRef: int
    storage_id: int
    dtype: Optional[torch.dtype]
    dims: Optional[tuple]
    devices: List[int]
    mesh_ref: Optional[int]
    stream_name: str


class TensorDeletionEvent(NamedTuple):
    command_id: int
    DTensorRef: int
    storage_id: int
    dtype: Optional[torch.dtype]
    dims: Optional[tuple]
    devices: List[int]
    mesh_ref: Optional[int]
    stream_name: str
    traceback: Sequence[traceback.FrameSummary]


"""
    Represents a node in the data flow DAG that tracks tensor and storage lifecycle events.

    Each DataEvent captures a specific event in the lifecycle of tensors and storage objects,
    including creation, access, mutation, and deletion operations across workers and devices.
"""
DataEvent = Union[
    StorageCreationEvent,
    StorageDeletionEvent,
    TensorCreationEvent,
    TensorAccessEvent,
    TensorMutationEvent,
    TensorDeletionEvent,
]


@dataclass
class BorrowInfo:
    borrow_id: Optional[int] = None
    devices: Set[int] = field(default_factory=set)
    src_stream_name: Optional[str] = None
    dst_stream_name: Optional[str] = None
    create_id: Optional[int] = None
    firstuse_id: Optional[int] = None
    lastuse_id: Optional[int] = None
    drop_id: Optional[int] = None


@dataclass
class SendTensorInfo:
    result_tensor_id: Optional[int] = None
    src_devices: Optional[List[int]] = None
    src_stream_name: Optional[str] = None
    dst_devices: Optional[List[int]] = None
    dst_stream_name: Optional[str] = None
    result_tensor_dims: Optional[Tuple[int, ...]] = None


@dataclass
class TensorInfo:
    storage_id: Optional[int] = None
    DTensorRefs: Set[int] = field(default_factory=set)
    dtype: Optional[torch.dtype] = None
    dims: Tuple[int, ...] = field(default_factory=tuple)
    size: Optional[int] = None
    devices: Set[int] = field(default_factory=set)
    mesh_ref: Optional[int] = None
    stream_name: Optional[str] = None
    storage_create_id: Optional[int] = None
    tensor_create_ids: Set[int] = field(default_factory=set)
    access_ids: Set[int] = field(default_factory=set)
    mutation_ids: Set[int] = field(default_factory=set)
    lastuse_id: Optional[int] = None
    tensor_deletion_ids: Set[int] = field(default_factory=set)
    storage_deletion_id: Optional[int] = None


class IRGraph:
    """
    Represents an intermediate representation (IR) graph for distributed tensor operations.

    The IRGraph tracks both control flow (commands executed on workers) and data flow
    (tensor/storage lifecycle events) in distributed tensor computations. It consists of:

    1. Control DAG: Tracks command execution across workers and streams
    2. Data DAG: Tracks tensor and storage lifecycle events (creation, access, mutation, deletion)

    The graph can be exported to Chrome Trace format for visualization, and additional CSV
    exports provide detailed information about borrows, tensor sends, and data dependencies.

    Attributes:
        control_dag (List[Command]): Command nodes representing operations executed on workers
        data_dag (List[DataEvent]): Data events tracking tensor/storage lifecycle
        _control: Internal manager for control flow information (borrows, sendtensor)
        _data: Internal manager for data flow information (tensors, storage)
    """

    def __init__(self) -> None:
        self.control_dag: List[Command] = []
        self.data_dag: List[DataEvent] = []
        self._control: IRGraph._ControlManager = self._ControlManager()
        self._data: IRGraph._DataManager = self._DataManager()

    def insert_node(
        self,
        worker_rank: int,
        stream_name: str,
        command_id: int,
        command_name: str,
        devices: List[int],
        control_dependencies: List[int],
        traceback: Sequence[traceback.FrameSummary],
    ) -> None:
        new_dag_node = Command(
            worker_rank=worker_rank,
            stream_name=stream_name,
            command_id=command_id,
            command_name=command_name,
            devices=devices,
            control_dependencies=control_dependencies,
            traceback=traceback,
        )
        self.control_dag.append(new_dag_node)

    def add_borrow(
        self,
        borrow_id: int,
        device: int,
        src_stream_name: str,
        dst_stream_name: str,
        create_id: int,
    ) -> None:
        self._control.borrows_info[borrow_id].borrow_id = borrow_id
        self._control.borrows_info[borrow_id].devices.add(device)
        self._control.borrows_info[borrow_id].src_stream_name = src_stream_name
        self._control.borrows_info[borrow_id].dst_stream_name = dst_stream_name
        self._control.borrows_info[borrow_id].create_id = create_id

    def update_tensor(
        self,
        temp_id: int,
        ref: int,
        dtype: torch.dtype,
        dims: Tuple[int, ...],
        worker_rank: int,
        stream_name: str,
        command_id: int,
        mutate=False,
        borrow_src_tensor_ref: Optional[int] = None,
        tensor_size: Optional[int] = None,
        mesh_ref: Optional[int] = None,
    ) -> None:
        new_tensor_event = new_storage_event = False
        update_tensor_devices = update_storage_devices = False
        if temp_id not in self._data.id_to_storageid:
            if borrow_src_tensor_ref is None:
                new_storage_event = True
                storage_id = next(self._data.storageid_counter)
                self._data.id_to_storageid[temp_id] = storage_id
                self._data.data_dependency_info[storage_id].storage_id = storage_id
                self._data.data_dependency_info[storage_id].dtype = dtype
                self._data.data_dependency_info[storage_id].dims = dims
                self._data.data_dependency_info[storage_id].size = tensor_size
                self._data.data_dependency_info[storage_id].devices.add(worker_rank)
                self._data.data_dependency_info[storage_id].stream_name = stream_name
                self._data.data_dependency_info[
                    storage_id
                ].storage_create_id = command_id
            # borrow aliasing
            else:
                storage_id = self._data.tensorref_to_storageid[borrow_src_tensor_ref]
                self._data.id_to_storageid[temp_id] = storage_id
        else:
            storage_id = self._data.id_to_storageid[temp_id]
            if worker_rank not in self._data.data_dependency_info[storage_id].devices:
                update_storage_devices = True
                self._data.data_dependency_info[storage_id].devices.add(worker_rank)

        if ref not in self._data.tensorref_to_stream:
            new_tensor_event = True
            self._data.tensorref_to_storageid[ref] = storage_id
            self._data.tensorref_to_stream[ref] = stream_name
            self._data.storageid_to_tensorref[storage_id].add(ref)

            # Track mesh reference for this tensor
            self._data.tensorref_to_mesh_ref[ref] = mesh_ref

            # Track mesh reference for the storage if not set
            if storage_id not in self._data.storage_to_mesh_ref:
                self._data.storage_to_mesh_ref[storage_id] = mesh_ref

            self._data.data_dependency_info[storage_id].DTensorRefs.add(ref)
            self._data.data_dependency_info[storage_id].tensor_create_ids.add(
                command_id
            )

        self._data.data_dependency_info[storage_id].access_ids.add(command_id)
        self._data.data_dependency_info[
            storage_id
        ].lastuse_id = command_id  # commands are processed in increasing command_id
        if mutate:
            self._data.data_dependency_info[storage_id].mutation_ids.add(command_id)

        # Helper function to find or create events
        def find_or_create_event(event_type):
            # Look for existing event with same command_id and event_type
            # Look backwards since events are processed in increasing command_id
            for i in range(len(self.data_dag) - 1, -1, -1):
                event = self.data_dag[i]
                event_class_name = event.__class__.__name__
                if (
                    event.command_id == command_id
                    and event_class_name == event_type
                    and (not hasattr(event, "DTensorRef") or event.DTensorRef == ref)
                ):
                    # If worker_rank already exists, just return True
                    if worker_rank in event.devices:
                        return True

                    # Update devices list
                    updated_devices = event.devices + [worker_rank]
                    updated_event = event._replace(devices=updated_devices)
                    self.data_dag[i] = updated_event
                    return True
            return False

        if new_storage_event and not find_or_create_event("StorageCreationEvent"):
            self.data_dag.append(
                StorageCreationEvent(
                    command_id=command_id,
                    storage_id=storage_id,
                    size=tensor_size,
                    devices=[worker_rank],
                    mesh_ref=None,
                    stream_name=stream_name,
                )
            )
        if new_tensor_event and not find_or_create_event("TensorCreationEvent"):
            self.data_dag.append(
                TensorCreationEvent(
                    command_id=command_id,
                    DTensorRef=ref,
                    storage_id=storage_id,
                    dtype=dtype,
                    dims=dims,
                    devices=[worker_rank],
                    mesh_ref=None,
                    stream_name=stream_name,
                )
            )
        if not find_or_create_event("TensorAccessEvent"):
            self.data_dag.append(
                TensorAccessEvent(
                    command_id=command_id,
                    DTensorRef=ref,
                    storage_id=storage_id,
                    dtype=dtype,
                    dims=dims,
                    devices=[worker_rank],
                    mesh_ref=None,
                    stream_name=stream_name,
                )
            )
        if mutate and not find_or_create_event("TensorMutationEvent"):
            self.data_dag.append(
                TensorMutationEvent(
                    command_id=command_id,
                    DTensorRef=ref,
                    storage_id=storage_id,
                    dtype=dtype,
                    dims=dims,
                    devices=[worker_rank],
                    mesh_ref=None,
                    stream_name=stream_name,
                )
            )

        if update_storage_devices:
            find_or_create_event("StorageCreationEvent")
        if update_tensor_devices:
            find_or_create_event("TensorCreationEvent")

    def delete_tensor(
        self,
        ref: int,
        mesh_ranks: List[int],
        stream_name: str,
        command_id: int,
        tb: Sequence[traceback.FrameSummary],
    ) -> None:
        storage_id = self._data.tensorref_to_storageid[ref]

        self._data.data_dependency_info[storage_id].tensor_deletion_ids.add(command_id)

        self.data_dag.append(
            TensorDeletionEvent(
                command_id=command_id,
                DTensorRef=ref,
                storage_id=storage_id,
                dtype=self._data.data_dependency_info[storage_id].dtype,
                dims=self._data.data_dependency_info[storage_id].dims,
                devices=mesh_ranks,
                mesh_ref=None,
                stream_name=stream_name,
                traceback=tb,
            )
        )

        del self._data.tensorref_to_storageid[ref]
        self._data.storageid_to_tensorref[storage_id].remove(ref)

        if not self._data.storageid_to_tensorref[storage_id]:
            self.data_dag.append(
                StorageDeletionEvent(
                    command_id=command_id,
                    storage_id=storage_id,
                    size=self._data.data_dependency_info[storage_id].size,
                    devices=mesh_ranks,
                    mesh_ref=None,
                    stream_name=stream_name,
                )
            )

            self._data.data_dependency_info[storage_id].storage_deletion_id = command_id

    def add_sendtensor(
        self,
        result_tensor_id: int,
        src_devices: List[int],
        src_stream_name: str,
        dst_devices: List[int],
        dst_stream_name: str,
        result_tensor_dims: Tuple[int, ...],
    ) -> None:
        self._control.sendtensor_info[
            result_tensor_id
        ].result_tensor_id = result_tensor_id
        self._control.sendtensor_info[result_tensor_id].src_devices = src_devices
        self._control.sendtensor_info[
            result_tensor_id
        ].src_stream_name = src_stream_name
        self._control.sendtensor_info[result_tensor_id].dst_devices = dst_devices
        self._control.sendtensor_info[
            result_tensor_id
        ].dst_stream_name = dst_stream_name
        self._control.sendtensor_info[
            result_tensor_id
        ].result_tensor_dims = result_tensor_dims
        return

    def convert_devices_to_meshes(self) -> None:
        """
        Set mesh references directly on data events for proper mesh tracking.

        This method uses actual mesh references (mesh.ref) instead of device sets
        to identify unique meshes. This correctly handles cases where two different
        logical meshes use the same physical devices but have different topologies.
        """
        new_data_dag = []

        # Handle self.data_dag - use mesh_ref directly
        for event in self.data_dag:
            mesh_ref = self._get_mesh_ref_for_event(event)
            new_event = event._replace(mesh_ref=mesh_ref)
            new_data_dag.append(new_event)

        self.data_dag = new_data_dag

        # Handle self._data.data_dependency_info - use mesh_ref directly
        for storage_info in self._data.data_dependency_info.values():
            storage_id = storage_info.storage_id
            if storage_id is not None:
                mesh_ref = self._get_mesh_ref_for_storage(storage_id)
                storage_info.mesh_ref = mesh_ref

        return

    def _get_mesh_ref_for_event(self, event: DataEvent) -> Optional[int]:
        """
        Extract mesh reference for a data event.

        Args:
            event: A data event (TensorCreationEvent, StorageCreationEvent, etc.)

        Returns:
            The mesh reference (mesh.ref) associated with this event, or None if not found
        """
        # Handle tensor events (have DTensorRef attribute)
        if isinstance(
            event,
            (
                TensorCreationEvent,
                TensorAccessEvent,
                TensorMutationEvent,
                TensorDeletionEvent,
            ),
        ):
            return self._data.tensorref_to_mesh_ref.get(event.DTensorRef)
        # Handle storage events (have storage_id attribute)
        elif isinstance(event, (StorageCreationEvent, StorageDeletionEvent)):
            return self._data.storage_to_mesh_ref.get(event.storage_id)
        return None

    def _get_mesh_ref_for_storage(self, storage_id: int) -> Optional[int]:
        """
        Get mesh reference for a storage ID.

        Args:
            storage_id: The storage ID to look up

        Returns:
            The mesh reference associated with this storage, or None if not found
        """
        return self._data.storage_to_mesh_ref.get(storage_id)

    def remove_dag_item_type(
        self, command_types: Union[str, List[str]], print_removed_nodes: bool = False
    ) -> int:
        """
        Removes nodes from the DAG that match the specified command type(s).

        Args:
            command_types: A string or list of strings representing command types to remove.
                        Nodes with command_name that starts with any of these strings will be removed.

        Returns:
            int: The number of nodes removed from the DAG.

        Example:
            # Remove all 'Borrow' related commands
            graph.remove_dag_item_type('Borrow')

            # Remove multiple command types
            graph.remove_dag_item_type(['Reduce', 'SendTensor'])
        """
        if isinstance(command_types, str):
            command_types = [command_types]

        removed_nodes = [
            node
            for node in self.control_dag
            if any(node.command_name.startswith(ct) for ct in command_types)
        ]
        self.control_dag = [
            node
            for node in self.control_dag
            if not any(node.command_name.startswith(ct) for ct in command_types)
        ]

        num_removed = len(removed_nodes)
        if num_removed > 0:
            print(f"Removed {num_removed} DAG items of type(s) {command_types}:")
            if print_removed_nodes:
                for node in removed_nodes:
                    print(
                        f"{type(node).__name__}, Worker: {node.worker_rank}, Command ID: {node.command_id}"
                    )
        else:
            print(f"No nodes removed of type(s) {command_types}.")
        return num_removed

    def export_dag_json(self, output_file: str) -> None:
        # Note: The default width unit is in us, so we need to use "larger" standard durations to ensure the flow events are visible.
        default_event_width = 4000
        default_event_spacing = 1000
        stream_locs = defaultdict(int)
        trace_events = []

        borrows_start_stream = {}

        reduce_sendtensor_max_ts = defaultdict(int)
        reduce_sendtensor_events = defaultdict(list)

        for dag_item in self.control_dag:
            worker_rank = dag_item.worker_rank
            name = dag_item.command_name
            cat = dag_item.command_name.split(":")[0]
            event: Dict[str, Any] = {
                "name": name,
                "cat": cat,
                "pid": worker_rank,
                "args": {
                    "command_id": dag_item.command_id,
                    "command_type": cat,
                    "devices": dag_item.devices,
                    "control dependencies": dag_item.control_dependencies,
                },
            }

            if isinstance(dag_item, Command):
                stream_name = dag_item.stream_name
                event["ph"] = "X"
                event["tid"] = stream_name
                event["dur"] = default_event_width

                if event["cat"] in ["BorrowCreate", "BorrowLastUse"]:
                    event["ts"] = stream_locs[f"{worker_rank}_{stream_name}"]

                    borrow_id = int(event["name"].split(":")[-1])
                    borrows_start_stream[event["name"]] = stream_name

                    # Create edge
                    event_start = event.copy()

                    event_start["ph"] = "s"
                    event_start["ts"] = event["ts"] + default_event_width

                    if event["cat"] == "BorrowCreate":
                        event_start["name"] = (
                            f"BorrowCreate->BorrowFirstUse: {borrow_id}"
                        )
                        event_start["cat"] = "BorrowCreate->BorrowFirstUse"
                        event_start["id"] = (
                            f"{worker_rank}:{borrow_id}:create->firstuse"
                        )
                    elif event["cat"] == "BorrowLastUse":
                        event_start["name"] = f"BorrowLastUse->BorrowDrop: {borrow_id}"
                        event_start["cat"] = "BorrowLastUse->BorrowDrop"
                        event_start["id"] = f"{worker_rank}:{borrow_id}:lastuse->drop"
                    event_start["args"] = {"devices": dag_item.devices}
                    del event_start["dur"]

                    trace_events.append(event_start)

                if event["cat"] in ["BorrowFirstUse", "BorrowDrop"]:
                    event["ts"] = stream_locs[f"{worker_rank}_{stream_name}"]

                    borrow_id = int(event["name"].split(":")[-1])
                    start_stream_name = ""

                    if event["cat"] == "BorrowFirstUse":
                        start_stream_name = borrows_start_stream[
                            f"BorrowCreate: {borrow_id}"
                        ]
                    elif event["cat"] == "BorrowDrop":
                        start_stream_name = borrows_start_stream[
                            f"BorrowLastUse: {borrow_id}"
                        ]

                    # Create edge
                    event_end = event.copy()
                    event_end["ph"] = "f"
                    event_end["ts"] = max(
                        stream_locs[f"{worker_rank}_{start_stream_name}"],
                        stream_locs[f"{worker_rank}_{stream_name}"],
                    )

                    if event["cat"] == "BorrowFirstUse":
                        event_end["name"] = f"BorrowCreate->BorrowFirstUse: {borrow_id}"
                        event_end["cat"] = "BorrowCreate->BorrowFirstUse"
                        event_end["id"] = f"{worker_rank}:{borrow_id}:create->firstuse"
                    elif event["cat"] == "BorrowDrop":
                        event_end["name"] = f"BorrowLastUse->BorrowDrop: {borrow_id}"
                        event_end["cat"] = "BorrowLastUse->BorrowDrop"
                        event_end["id"] = f"{worker_rank}:{borrow_id}:lastuse->drop"
                    event_end["args"] = {"devices": dag_item.devices}
                    del event_end["dur"]

                    stream_locs[f"{worker_rank}_{stream_name}"] = max(
                        stream_locs[f"{worker_rank}_{start_stream_name}"],
                        stream_locs[f"{worker_rank}_{stream_name}"],
                    )
                    trace_events.append(event_end)

                if event["cat"] in ["Reduce", "SendTensor"]:
                    ts = max(
                        stream_locs[f"{worker_rank}_{stream_name}"],
                        reduce_sendtensor_max_ts[name],
                    )
                    event["ts"] = ts
                    stream_locs[f"{worker_rank}_{stream_name}"] = ts
                    reduce_sendtensor_events[name].append(
                        event
                    )  # save event for later in case we need to update
                    # update max timestamp if necessary
                    if ts > reduce_sendtensor_max_ts[name]:
                        reduce_sendtensor_max_ts[name] = ts
                    # update timestamps of all Reduce/SendTensor events with the same name
                    for e in reduce_sendtensor_events[name]:
                        if e["name"] == name and e["ts"] != ts:
                            e["ts"] = ts
                            stream_locs[f"{e['pid']}_{e['tid']}"] = (
                                reduce_sendtensor_max_ts[name]
                                + default_event_width
                                + default_event_spacing
                            )
                    # Extra SendTensor metadata
                    if event["cat"] == "SendTensor":
                        send_devices_threshold = len(dag_item.devices) // 2
                        event["args"]["send devices"] = dag_item.devices[
                            :send_devices_threshold
                        ]
                        event["args"]["recv devices"] = dag_item.devices[
                            send_devices_threshold:
                        ]

                else:
                    event["ts"] = stream_locs[f"{worker_rank}_{stream_name}"]

                stream_locs[f"{worker_rank}_{stream_name}"] += (
                    default_event_width + default_event_spacing
                )
                event["args"]["traceback"] = traceback.format_list(dag_item.traceback)
                trace_events.append(event)
            else:
                raise ValueError(f"Unknown DAG item type: {type(dag_item)}")

        with open(output_file, "w") as f:
            json.dump({"traceEvents": trace_events}, f)

    def _export_info_to_csv(
        self, info_dict: Dict[Any, Any], filename: str, info_type: str
    ) -> None:
        def _format_value_for_display(value):
            """Format a value for CSV display, handling collections."""
            if isinstance(value, (dict, List, set)):
                if not value:
                    return "None"
                return str(sorted(value))
            return str(value)

        if not info_dict:
            print(f"No {info_type} information to export.")
            return

        # Get the first value to determine if it's a NamedTuple or dict
        first_value = next(iter(info_dict.values()))
        is_namedtuple = isinstance(first_value, tuple)
        is_dataclass = hasattr(first_value, "__dataclass_fields__")

        if not (is_namedtuple or is_dataclass):
            raise ValueError(
                f"Expected NamedTuple or dataclass, got {type(first_value)}"
            )

        if is_namedtuple:
            # Use fixed order for NamedTuple headers
            keys = [
                "DataEvent",
                "command_id",
                "storage_id",
                "DTensorRef",
                "mesh_ref",
                "stream_name",
                "dims",
                "dtype",
                "size",
            ]
        else:  # is_dataclass
            keys = list(first_value.__dataclass_fields__.keys())

        def get_value(obj, key):
            if key == "DataEvent" and is_namedtuple:
                return obj.__class__.__name__[:-5]  # remove "Event" suffix
            try:
                return getattr(obj, key)
            except AttributeError:
                return ""

        widths = {key: len(key) for key in keys}

        for info in info_dict.values():
            for key in keys:
                value = get_value(info, key)
                if value is not None:
                    str_value = _format_value_for_display(value)
                    widths[key] = max(widths[key], len(str_value))

        with open(filename, "w", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            # Write header with aligned fields
            writer.writerow([key.ljust(widths[key]) for key in keys])
            for info in info_dict.values():
                row = []
                for key in keys:
                    value = get_value(info, key)
                    str_value = _format_value_for_display(value)
                    row.append(str_value.ljust(widths[key]))
                writer.writerow(row)

    def export_borrows_csv(self, filename: str) -> None:
        self._export_info_to_csv(self._control.borrows_info, filename, "borrows")

    def export_sendtensors_csv(self, filename: str) -> None:
        self._export_info_to_csv(self._control.sendtensor_info, filename, "SendTensor")

    def export_data_csv(self, filename: str) -> None:
        self._export_info_to_csv(self._data.data_dependency_info, filename, "tensor")

    def export_data_timeline_csv(self, filename: str) -> None:
        if not self.data_dag:
            print("No data dependency timeline information to export.")
            return

        # Convert list to dict with indices as keys to use _export_info_to_csv
        timeline_dict = dict(enumerate(self.data_dag))
        self._export_info_to_csv(timeline_dict, filename, "data dependency timeline")

    def export_memory_viz(self, filename: str) -> None:
        from monarch.simulator.trace import MemoryViewer

        memory_viewer = MemoryViewer()

        device_events = {}
        for event in self.data_dag:
            if not isinstance(event, (StorageCreationEvent, StorageDeletionEvent)):
                continue
            for device in event.devices:
                if device not in device_events:
                    device_events[device] = []
                device_events[device].append(event)

        for device, events in sorted(device_events.items()):
            # print(device, events) # Note: Use this to debug memory snapshot address
            memory_viewer.next_device()
            max_mem = curr_mem = 0

            for event in events:
                size = event.size if event.size is not None else 0
                if isinstance(event, StorageDeletionEvent):
                    size = -size
                curr_mem += size
                if curr_mem > max_mem:
                    max_mem = curr_mem

                traceback_info = []
                if isinstance(event, StorageCreationEvent):
                    for cmd in self.control_dag:
                        if cmd.command_id == event.command_id:
                            traceback_info = cmd.traceback
                            break
                elif isinstance(event, StorageDeletionEvent):
                    for cmd in self.data_dag:
                        if cmd.command_id == event.command_id:
                            assert isinstance(cmd, TensorDeletionEvent)
                            traceback_info = cmd.traceback
                            break

                memory_viewer.add_trace(
                    addr=event.storage_id,  # Note: this is overwritten by segment["allocated_size"] in MemoryViewer.add_trace()
                    delta=size,
                    stream=0,  # TODO: separate worker memory by stream (?)
                    traceback=traceback_info,
                )
            print(f"Device {device} max mem: {max_mem} B")

        memory_viewer.dump(filename)

    class _ControlManager:
        """
        Internal manager for control flow information in the IRGraph.

        Tracks metadata about borrows and tensor send operations across workers and streams.

        Attributes:
            borrows_info: Maps borrow IDs to their metadata (devices, streams, command IDs)
            sendtensor_info: Maps tensor IDs to send operation metadata (source/destination devices and streams)
        """

        def __init__(self):
            self.borrows_info: DefaultDict[int, BorrowInfo] = defaultdict(BorrowInfo)

            self.sendtensor_info: DefaultDict[int, SendTensorInfo] = defaultdict(
                SendTensorInfo
            )

    class _DataManager:
        """
        Internal manager for data flow information in the IRGraph.

        Tracks tensor and storage lifecycle events including creation, access, mutation, and deletion.
        Maintains mappings between tensor references, storage IDs, and their associated metadata.

        Attributes:
            data_dependency_info: Maps storage IDs to their complete lifecycle metadata
            tensorref_to_stream: Maps tensor references to their associated stream names
            tensorref_to_storageid: Maps tensor references to their underlying storage IDs
            tensorref_to_mesh_ref: Maps tensor references to their actual mesh references (mesh.ref)
            storage_to_mesh_ref: Maps storage IDs to their actual mesh references (mesh.ref)
            id_to_storageid: Maps Python object IDs to storage IDs
            storageid_to_tensorref: Maps storage IDs to their associated tensor references
            storageid_counter: Counter for generating unique storage IDs
        """

        def __init__(self):
            self.data_dependency_info: DefaultDict[int, TensorInfo] = defaultdict(
                TensorInfo
            )
            self.tensorref_to_stream: Dict[
                int, str
            ] = {}  # key = DTensorRef.ref (int); value = stream name (str)
            self.tensorref_to_storageid: Dict[
                int, int
            ] = {}  # key = DTensorRef.ref (int); value = storage id (int)
            self.tensorref_to_mesh_ref: Dict[
                int, Optional[int]
            ] = {}  # key = DTensorRef.ref (int); value = mesh.ref (int) or None
            self.storage_to_mesh_ref: Dict[
                int, Optional[int]
            ] = {}  # key = storage_id (int); value = mesh.ref (int) or None
            self.id_to_storageid: Dict[
                int, int
            ] = {}  # key = id(UntypedStorage) (int); value = storage id (int)
            self.storageid_to_tensorref: DefaultDict[int, Set[int]] = defaultdict(
                set
            )  # key = storage_id (int); value = List[DTensorRef] (List[int])
            self.storageid_counter: Iterator[int] = count()
