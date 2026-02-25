# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import logging
import pickle
import subprocess
import traceback
from typing import Any, Dict, List, Literal, Sequence, TypedDict


logger = logging.getLogger(__name__)


class TraceEvent:
    """
    Represents a trace event in the simulation.

    Args:
        start (int): The start time, in nanoseconds, of the event.
        runtime (int): The runtime, in nanoseconds, of the event.
        meta (list): A list of metadata associated with the event.
        command_id (int): The associated command id of this task.
    """

    def __init__(
        self,
        start: int,
        runtime: int,
        meta: List[str],
        command_id: int,
        traceback: Sequence[traceback.FrameSummary],
    ):
        self.start = start
        self.runtime = runtime
        self.end = start + runtime
        self.meta = meta
        self.command_id = command_id
        self.traceback = traceback

    def __repr__(self):
        return f"E(meta={self.meta}, start={self.start:.2f}, end={self.end:.1f})"


def visualize_events(worker_events):
    import pandas as pd
    import plotly.graph_objs as go

    # Convert the data to a DataFrame
    records = []
    for key, events in worker_events.items():
        for event in events:
            records.append(
                {
                    "Process": key,
                    "Event": event.meta,
                    "Start": event.start,
                    "End": event.end,
                    "Duration": event.end - event.start,
                }
            )

    df = pd.DataFrame(records)

    # Create Gantt chart using plotly.graph_objs
    fig = go.Figure()

    fw_list = [
        "#0000FF",  # Blue
        "#1E90FF",  # Dodger Blue
        "#00BFFF",  # Deep Sky Blue
        "#5F9EA0",  # Cadet Blue
        "#4682B4",  # Steel Blue
        "#87CEFA",  # Light Sky Blue
        "#6495ED",  # Cornflower Blue
        "#4169E1",  # Royal Blue
    ]
    bw_list = [
        "#FF0000",  # Red
        "#FF4500",  # Orange Red
        "#FF1493",  # Deep Pink
        "#FF69B4",  # Hot Pink
        "#DB7093",  # Pale Violet Red
        "#B22222",  # Firebrick
        "#8B0000",  # Dark Red
        "#FF6347",  # Tomato
    ]

    # Map each event to a color

    def get_color(metas):
        if "fw" in metas:
            for meta in metas:
                if meta.isdigit():
                    return fw_list[int(meta) % len(fw_list)]
        elif "bw" in metas:
            for meta in metas:
                if meta.isdigit():
                    return bw_list[int(meta) % len(fw_list)]
        return "red"

    for process in df["Process"].unique():
        process_df = df[df["Process"] == process]
        for _, row in process_df.iterrows():
            color = get_color(row["Event"])
            fig.add_trace(
                go.Bar(
                    x=[row["Duration"]],
                    y=[str(process)],
                    base=[row["Start"]],
                    orientation="h",
                    name=" ".join(row["Event"]),
                    hoverinfo="name+x",
                    marker={
                        "color": color,
                    },
                    showlegend=False,  # Hide default legend
                )
            )

    # Add custom legend
    # annotations = []
    # legend_x = 0.95
    # legend_y = 1.0

    fig.update_layout(
        title="Timeline Visualization",
        xaxis_title="Time",
        yaxis_title="Process",
        barmode="stack",
        # annotations=annotations,
        showlegend=False,  # Disable the default legend
        yaxis={"autorange": "reversed"},  # Reverse the y-axis
    )

    # Show the plot
    fig.write_html("sim.html")
    # fig.show()


def dump_process_name(trace: List[Dict[str, Any]], *, pid: int, name: str):
    trace.append(
        {
            "name": "process_name",
            "ph": "M",
            "pid": pid,
            "tid": 0,
            "args": {"name": name},
        }
    )


def _include_file(filename: str):
    if "controller/" in filename:
        return False
    return True


def _filter_traceback(tb: Sequence[traceback.FrameSummary]):
    notebook = [i for i, f in enumerate(tb) if f.name == "run_code"]
    if notebook:
        tb = tb[notebook[-1] + 1 :]  # noqa: whitespace before ':'
    filtered = [frame for frame in tb if _include_file(frame.filename)]
    filtered.reverse()
    return filtered


def _format_traceback(tb):
    return "Traceback (most recent call first)\n" + "".join(
        traceback.format_list(_filter_traceback(tb))
    )


def dump_thread_event_trace(
    trace: List[Dict[str, Any]],
    events: List[TraceEvent],
    *,
    pid: int,
    tid: int,
    name: str,
) -> float:
    trace.append(
        {
            "name": "thread_name",
            "ph": "M",
            "pid": pid,
            "tid": tid,
            "args": {"name": name},
        }
    )
    max_time = 0.0
    for event in events:
        name = " ".join(event.meta)
        trace.append(
            {
                "name": name,
                "cat": "compute",
                "ph": "X",
                "ts": event.start / 1000,
                "dur": event.runtime / 1000,
                "pid": pid,
                "tid": tid,
                "args": {
                    "External id": event.command_id + pid * 10000,
                    "correlation": event.command_id + pid * 10000,
                    "cbid": event.command_id,
                    " traceback": _format_traceback(event.traceback),
                },
                "cname": "rail_animation" if "waiting" in name else None,
            }
        )
        max_time = max(max_time, (event.start + event.runtime) / 1000)

    return max_time


def dump_memory_trace(
    trace: List[Dict[str, Any]], *, pid: int, memory: int, ts: int, name: str
) -> None:
    trace.append(
        {
            "name": name,
            "cat": "memory",
            "ph": "C",
            "ts": ts / 1000,
            "pid": pid,
            "args": {
                "allocated": memory / 10**6,
            },
        }
    )


def upload_trace(file_path) -> None:
    logger.info("Uploading the trace file to Manifold...")

    command_path = "~/fbsource/arvr/scripts/perfetto/share_trace.py"
    command = [f"{command_path} {file_path}"]
    result = subprocess.run(command, capture_output=True, text=True, shell=True)

    if result.returncode == 0:
        print(result.stdout)
    else:
        print("Failed to upload the file.")
        print(result.stdout)
        print(result.stderr)


class Frame(TypedDict):
    filename: str
    line: int
    name: str


class Block(TypedDict):
    # A piece of memory returned from the allocator, or
    # current cached but inactive.
    size: int
    requested_size: int  # size requested during malloc, may be smaller than
    # size due to rounding
    address: int
    state: Literal[
        "active_allocated",  # used by a tensor
        "active_awaiting_free",  # waiting for another stream to finish using
        # this, then it will become free
        "inactive",
    ]  # free for reuse
    frames: List[Frame]  # stack trace from where the allocation occurred


class Segment(TypedDict):
    # Segments are memory returned from a cudaMalloc call.
    # The size of reserved memory is the sum of all Segments.
    # Segments are cached and reused for future allocations.
    # If the reuse is smaller than the segment, the segment
    # is split into more then one Block.
    # empty_cache() frees Segments that are entirely inactive.
    address: int
    total_size: int  # cudaMalloc'd size of segment
    stream: int
    segment_type: Literal["small", "large"]  # 'large' (>1MB)
    allocated_size: int  # size of memory in use
    active_size: int  # size of memory in use or in active_awaiting_free state
    device: int
    blocks: List[Block]


class TraceEntry(TypedDict):
    # When `torch.cuda.memory._record_memory_history()` is enabled,
    # the snapshot will contain TraceEntry objects that record each
    # action the allocator took.
    action: Literal[
        "alloc",  # memory allocated
        "free_requested",  # the allocated received a call to free memory
        "free_completed",  # the memory that was requested to be freed is now
        # able to be used in future allocation calls
        "segment_alloc",  # the caching allocator ask cudaMalloc for more memory
        # and added it as a segment in its cache
        "segment_free",  # the caching allocator called cudaFree to return memory
        # to cuda possibly trying free up memory to
        # allocate more segments or because empty_caches was called
        "oom",  # the allocator threw an OOM exception. 'size' is
        # the requested number of bytes that did not succeed
        "snapshot",  # the allocator generated a memory snapshot
        # useful to coorelate a previously taken
        # snapshot with this trace
    ]
    addr: int  # not present for OOM
    frames: List[Frame]
    size: int
    stream: int


class Snapshot(TypedDict):
    segments: List[Segment]
    device_traces: List[List[TraceEntry]]


class MemoryViewer:
    def __init__(self) -> None:
        self.current_segments = {}
        self.snapshot: Snapshot = {"segments": [], "device_traces": []}
        self.addr_map = {}

    def next_device(self) -> None:
        self.addr_map.clear()
        self.current_segments.clear()
        self.snapshot["device_traces"].append([])

    def get_or_add_segment(self, stream: int):
        if stream in self.current_segments:
            return self.current_segments[stream]
        s: Segment = {
            "address": 0,
            "total_size": 0,
            "stream": stream,
            "segment_type": "large",
            "allocated_size": 0,
            "active_size": 0,
            "blocks": [],
            "device": len(self.snapshot["device_traces"]) - 1,
        }
        self.current_segments[stream] = s
        self.snapshot["segments"].append(s)
        return s

    def add_trace(self, addr: int, delta: int, stream: int, traceback) -> None:
        segment = self.get_or_add_segment(stream)
        if delta > 0:
            maddr = self.addr_map[addr] = segment["allocated_size"]
            segment["allocated_size"] += delta
            action: Literal["alloc", "free_requested"] = "alloc"
        else:
            maddr = self.addr_map[addr]
            action: Literal["alloc", "free_requested"] = "free_requested"

        frames: List[Frame] = [
            {"filename": frame.filename, "line": frame.lineno, "name": frame.name}
            for frame in _filter_traceback(traceback)
        ]

        trace: TraceEntry = {
            "addr": maddr,
            "frames": frames,
            "size": abs(delta),
            "stream": stream,
            "action": action,
        }
        self.snapshot["device_traces"][-1].append(trace)
        if delta < 0:
            self.snapshot["device_traces"][-1].append(
                # pyre-ignore
                {**trace, "action": "free_completed"}
            )

    def dump(self, path: str) -> None:
        for segment in self.snapshot["segments"]:
            # print(segment) # Note: Use this to debug memory snapshot address
            # TODO: modify address definition
            sz = segment["total_size"] = segment["allocated_size"]
            segment["blocks"].append(
                {
                    "address": 0,
                    "size": sz,
                    "requested_size": sz,
                    "state": "inactive",
                    "frames": [],
                }
            )

        with open(path, "wb") as fp:
            # @lint-ignore PYTHONPICKLEISBAD
            pickle.dump(self.snapshot, fp)
