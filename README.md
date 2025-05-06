# Getting started

## Installation

TODO improve

```sh
# These instructions assume you have a devserver (and need to use fwdproxy to access the internet)

# install conda and set it up
feature install genai_conda && conda-setup

# Install nightly rust toolchain
curl $(fwdproxy-config curl) --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sed 's#curl $_retry#curl $(fwdproxy-config curl) $_retry#g' | env $(fwdproxy-config --format=sh curl) sh

with-proxy rustup toolchain install nightly
rustup default nightly

# Install non-python dependencies
with-proxy conda install python=3.10
# needs cuda-toolkit-12-0 as that is the version that matches the /usr/local/cuda/ on devservers
sudo dnf install cuda-toolkit-12-0 libnccl-devel clang-devel
# install build dependencies
with-proxy pip install setuptools-rust
# install torch, can use conda or build it yourself or whatever
with-proxy pip install torch
# install other deps, see pyproject.toml for latest
with-proxy pip install pyzmq requests numpy pyre-extensions
```

## Running examples

### Basic Monarch features

In the `examples` folder under `fbcode/monarch`, there are handful of Monarch
examples. `controller/example.py` is a good starting point to understand Monarch
basic features. Run the following command to launch the example. It is also
recommended to run the following file in Bento by selecting the `monarch` Bento
kernel. In both cases, cli or Bento, it will launch Monarch processes locally.

## Debugging

If everything is hanging, set the environment
`CONTROLLER_PYSPY_REPORT_INTERVAL=10` to get a py-spy dump of the controller and
its subprocesses every 10 seconds.

Calling `pdb.set_trace()` inside a worker remote function will cause pdb to
attach to the controller process to debug the worker. Keep in mind that if there
are multiple workers, this will create multiple sequential debug sessions for
each worker.

For the rust based setup you can adjust the log level with
`RUST_LOG=<log level>` (eg. `RUST_LOG=debug`).

## Profiling

The `monarch.profiler` module provides functionality similar to
[PyTorch's Profiler](https://pytorch.org/docs/stable/profiler.html) for model
profiling. It includes `profile` and `record_function` methods. The usage is
generally the same as `torch.profiler.profile` and
`torch.profiler.record_function`, with a few modifications specific to
`monarch.profiler.profile`:

1. `monarch.profiler.profile` exclusively accepts `monarch.profiler.Schedule`, a
   dataclass that mimics `torch.profiler.schedule`.
2. The `on_trace_ready` argument in `monarch.profiler.profile` must be a string
   that specifies the directory where the worker should save the trace files.

Below is an example demonstrating how to use `monarch.profiler`:

```py
    from monarch.profiler import profile, record_function
    with profile(
        activities=[
            torch.profiler.ProfilerActivity.CPU,
            torch.profiler.ProfilerActivity.CUDA,
        ],
        on_trace_ready="./traces/",
        schedule=monarch.profilerSchedule(wait=1, warmup=1, active=2, repeat=1),
        record_shapes=True,
    ) as prof:
        with record_function("forward"):
            loss = model(batch)

        prof.step()
```

## Memory Viewer

The `monarch.memory` module provides functionality similar to
[PyTorch's Memory Snapshot and Viewer](https://pytorch.org/docs/stable/torch_cuda_memory.html)
for visualizing and analyzing memory usage in PyTorch models. It includes
`monarch.memory.dump_memory_snapshot` and `monarch.memory.record_memory_history`
methods:

1. `monarch.memory.dump_memory_snapshot`: This function wraps
   `torch.cuda.memory._dump_snapshot()` to dump memory snapshot remotely. It can
   be used to save a snapshot of the current memory usage to a file.
2. `monarch.memory.record_memory_history`: This function wraps
   `torch.cuda.memory_record_memory_history()` to allow recording memory history
   remotely. It can be used to track memory allocation and deallocation over
   time.

Both functions use `remote` to execute the corresponding remote functions
`_memory_controller_record` and `_memory_controller_dump` on the specified
device mesh.

Below is an example demonstrating how to use `monarch.memory`:

```py
    ...
    monarch.memory.record_memory_history()
    for step in range(2):
        batch = torch.randn((8, DIM))
        loss = net(batch)
        ...
    monarch.memory.dump_memory_snapshot(dir_snapshots="./snapshots/")
```
