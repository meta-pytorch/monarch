# CUDA Timer

This folder contains a lightweight CUDA timer utility and examples demonstrating its usage in GPU-accelerated programs. The CUDA Timer is designed to measure the execution time of GPU kernels using CUDA events.

## Usage
### CudaTimer API

The `CudaTimer` singleton provides a comprehensive timing interface for CUDA operations:

- `start(label)` - Begins timing a labeled operation
- `stop(label)` - Ends timing for the labeled operation
- `time(label)` - Context manager for automatic timing (recommended usage)
- `reset()` - Clears all collected timing data
- `summary()` - Returns statistical analysis of timing measurements
- `get_latest_measurement(label)` - Gets the latest measurement (in ms) for a given section
- `print_summary()` - Displays formatted timing statistics to console

### Within SPMD workloads
We provide an example of CudaTimer within SPMD workloads at [example_spmd.py](example_spmd.py).

```
import torch
from monarch.timer import CudaTimer

def main():
    if not torch.cuda.is_available():
        print("CUDA is not available. Exiting.")
        return

    device = torch.device("cuda")
    a = torch.randn(1000, 1000, device=device)
    b = torch.randn(1000, 1000, device=device)

    with CudaTimer.time("matrix_multiply"):
        result = torch.matmul(a, b)

    CudaTimer.print_summary()

```

### Within Monarch workloads
We provide an example of CudaTimer within Monarch workloads at [example_monarch.py](example_monarch.py).

```
import torch
from monarch import inspect, remote
from monarch.actor import this_host

cuda_timer_start = remote("monarch.timer.remote_cuda_timer.cuda_timer_start", propagate="inspect")
cuda_timer_stop = remote("monarch.timer.remote_cuda_timer.cuda_timer_stop", propagate="inspect")

def main():
    mesh = this_host().spawn_procs(per_host={"hosts": 1, "gpus": 1})

    with mesh.activate():
        a = torch.randn(1000, 1000, device="cuda")
        b = torch.randn(1000, 1000, device="cuda")

        cuda_timer_start()
        result = torch.matmul(a, b)
        cuda_timer_stop()

        cuda_average_ms = get_cuda_timer_average_ms()
        local_cuda_avg_ms = inspect(cuda_average_ms).item()

    print(f"average time w/ CudaTimer: {local_cuda_avg_ms:.4f} (ms)")
```
