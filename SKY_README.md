# Monarch + SkyPilot Integration

This document describes the SkyPilot integration for Monarch, which enables running Monarch actors on cloud infrastructure provisioned by SkyPilot.

## Overview

SkyPilot is a framework for running ML workloads on any cloud (AWS, GCP, Azure, Lambda, Kubernetes, etc.). The `SkyPilotJob` class in Monarch provides a seamless integration that:

1. **Provisions cloud instances** using SkyPilot's unified API
2. **Installs Monarch** (`torchmonarch` from PyPI) on remote nodes
3. **Starts Monarch workers** on each node listening for connections
4. **Connects clients** to workers using TCP for distributed actor communication

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client Machine                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    SkyPilotJob                           │   │
│  │  - Calls sky.launch() to provision cloud instances       │   │
│  │  - Configures setup commands to install torchmonarch     │   │
│  │  - Builds worker command with run_worker_loop_forever()  │   │
│  │  - Calls attach_to_workers() to create HostMesh          │   │
│  └─────────────────────────────────────────────────────────┘   │
└───────────────────────────────┬─────────────────────────────────┘
                                │ TCP connections (port 22222)
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
        ▼                       ▼                       ▼
┌───────────────┐       ┌───────────────┐       ┌───────────────┐
│   Worker 1    │       │   Worker 2    │       │   Worker N    │
│ (Cloud Node)  │       │ (Cloud Node)  │       │ (Cloud Node)  │
│               │       │               │       │               │
│ run_worker_   │       │ run_worker_   │       │ run_worker_   │
│ loop_forever()│       │ loop_forever()│       │ loop_forever()│
│               │       │               │       │               │
│ tcp://<ip>:   │       │ tcp://<ip>:   │       │ tcp://<ip>:   │
│   22222       │       │   22222       │       │   22222       │
└───────────────┘       └───────────────┘       └───────────────┘
```

## Implementation Details

### Files

- **`python/monarch/_src/job/skypilot.py`**: Core `SkyPilotJob` implementation
- **`python/monarch/job/__init__.py`**: Exports `SkyPilotJob` (with graceful ImportError handling)
- **`python/tests/test_skypilot_job.py`**: Unit tests with mocked SkyPilot
- **`python/tests/test_skypilot_integration.py`**: Integration test scaffolding
- **`python/examples/skypilot_getting_started.py`**: Example demonstrating usage

### Key Classes and Functions

#### `SkyPilotJob(JobTrait)`

Main job class that implements the Monarch `JobTrait` interface.

```python
from monarch.job import SkyPilotJob
import sky

job = SkyPilotJob(
    meshes={"trainers": 2},           # 2 nodes for "trainers" mesh
    resources=sky.Resources(
        cloud=sky.Kubernetes(),
        accelerators="H100:1",
    ),
    cluster_name="my-cluster",
    idle_minutes_to_autostop=10,
    down_on_autostop=True,
    setup_commands="pip install torchmonarch",
)

state = job.state()  # Launches cluster and returns JobState
hosts = state.trainers  # HostMesh with 2 nodes
```

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `meshes` | `Dict[str, int]` | Mesh names to node counts |
| `resources` | `sky.Resources` | SkyPilot resource specification |
| `cluster_name` | `str` | Name for the cluster |
| `monarch_port` | `int` | TCP port for workers (default: 22222) |
| `idle_minutes_to_autostop` | `int` | Auto-stop after idle minutes |
| `down_on_autostop` | `bool` | Terminate (not just stop) on autostop |
| `setup_commands` | `str` | Shell commands to run before workers start |
| `workdir` | `str` | Local directory to sync to cluster |
| `file_mounts` | `Dict[str, str]` | Additional file mounts |

### Worker Lifecycle

1. **Launch**: `sky.launch()` creates the cluster with specified resources
2. **Setup**: `setup_commands` run to install `torchmonarch`
3. **Run**: Worker command executes `run_worker_loop_forever(address, ca)`
4. **Connect**: Client calls `attach_to_workers()` to create `HostMesh`
5. **Teardown**: `sky.down()` terminates the cluster

### Environment Variables

The following environment variables control timeouts:

```python
os.environ["HYPERACTOR_HOST_SPAWN_READY_TIMEOUT"] = "300s"  # Worker spawn timeout
os.environ["HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT"] = "300s"  # Message delivery timeout
os.environ["HYPERACTOR_MESH_PROC_SPAWN_MAX_IDLE"] = "300s"  # Proc mesh spawn timeout
```

## Requirements

### Client Side
- Monarch with Rust bindings (`pip install -e .` from source)
- SkyPilot (`pip install skypilot`)
- Configured cloud credentials (`sky check`)

### Worker Side (installed via setup_commands)
- `torchmonarch` from PyPI
- **CUDA libraries** - torchmonarch requires `libcuda.so.1`
- This means workers **must run on GPU nodes**

## Usage

### Basic Example

```python
import sky
from monarch.job import SkyPilotJob
from monarch.actor import Actor, endpoint

class MyActor(Actor):
    @endpoint
    def hello(self) -> str:
        return "Hello from cloud!"

# Create job
job = SkyPilotJob(
    meshes={"workers": 2},
    resources=sky.Resources(
        cloud=sky.AWS(),
        accelerators="A100:1",
    ),
    setup_commands="pip install torchmonarch",
)

# Launch and get state
state = job.state()
hosts = state.workers

# Spawn processes and actors
procs = hosts.spawn_procs(per_host={"gpus": 1})
actors = procs.spawn("my_actors", MyActor)

# Interact with actors
results = actors.hello.call().get()
print(results)  # ["Hello from cloud!", "Hello from cloud!"]

# Cleanup
job.kill()
```

### Running the Example

```bash
# Install dependencies
pip install skypilot
pip install -e .  # Build Monarch from source

# Configure cloud credentials
sky check

# Run example
cd python/examples
python skypilot_getting_started.py \
    --cloud kubernetes \
    --num-hosts 2 \
    --accelerator "H100:1" \
    --cluster-name my-monarch-cluster
```

### Supported Clouds

- **Kubernetes**: Use `sky.Kubernetes()` with `--region` for context
- **AWS**: Use `sky.AWS()` 
- **GCP**: Use `sky.GCP()`
- **Azure**: Use `sky.Azure()`
- **Lambda Labs**: Use `sky.Lambda()`
- And others supported by SkyPilot

## Networking Considerations

### Kubernetes

When using Kubernetes, the client and workers must be in the **same Kubernetes cluster** for pod-to-pod communication. Use the `region` parameter to specify the Kubernetes context:

```python
resources=sky.Resources(
    cloud=sky.Kubernetes(),
    region="my-k8s-context",  # Must match client's cluster
)
```

### Public Clouds (AWS, GCP, Azure)

SkyPilot handles networking automatically. Workers get public IPs that clients can connect to.

### Firewall

Ensure port 22222 (or your custom `monarch_port`) is accessible:
- Kubernetes: Pod networking should handle this
- AWS: Security groups
- GCP: Firewall rules
- Azure: Network security groups

## Troubleshooting

### "libcuda.so.1: cannot open shared object file"

**Cause**: Workers are running on CPU-only nodes, but `torchmonarch` requires CUDA.

**Solution**: Request GPU nodes:
```python
resources=sky.Resources(accelerators="H100:1")
```

### "No route to host" or connection timeouts

**Cause**: Client and workers are in different networks (e.g., different Kubernetes clusters).

**Solution**: Ensure client and workers are in the same network:
- For Kubernetes: Use `region` parameter to specify the correct context
- For public clouds: Check security group / firewall rules

### "error spawning proc mesh: statuses: Timeout"

**Causes**:
1. Workers aren't listening on the expected port
2. Network connectivity issues
3. Workers crashed during startup

**Debug steps**:
1. Check SkyPilot logs: `sky logs <cluster-name>`
2. SSH into cluster: `sky ssh <cluster-name>`
3. Check if port is listening: `ss -tlnp | grep 22222`
4. Check Monarch logs: `/tmp/sky/monarch_log.log`

### Workers crash immediately

Check SkyPilot logs for the error:
```bash
sky logs <cluster-name>
```

Common issues:
- Missing CUDA libraries → use GPU nodes
- torchmonarch installation failed → check setup_commands
- Python version mismatch → ensure compatible Python version

## Testing

### Unit Tests (with mocked SkyPilot)

```bash
cd python
pytest tests/test_skypilot_job.py -v
```

### Integration Tests (requires real cloud)

```bash
cd python
pytest tests/test_skypilot_integration.py -v --cloud kubernetes
```

## Comparison with SlurmJob

| Feature | SkyPilotJob | SlurmJob |
|---------|-------------|----------|
| Cloud Support | Multi-cloud (AWS, GCP, Azure, K8s, etc.) | HPC clusters only |
| Setup | Automatic via SkyPilot | Requires Slurm installation |
| Autoscaling | Supported | Depends on cluster |
| Cost Optimization | Automatic (cheapest region) | N/A |
| Worker Discovery | Via cluster handle IPs | Via squeue hostnames |

## Future Work

- [ ] Support for spot/preemptible instances
- [ ] Multi-region deployments  
- [ ] Automatic failover on spot termination
- [ ] Integration with SkyPilot managed jobs
- [ ] Support for batch mode (client script on cluster)

