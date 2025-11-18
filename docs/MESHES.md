# Monarch Meshes: Complete Guide

## Table of Contents
1. [Introduction](#introduction)
2. [What is a Mesh?](#what-is-a-mesh)
3. [Types of Meshes](#types-of-meshes)
4. [Mesh Hierarchy](#mesh-hierarchy)
5. [Mesh Operations](#mesh-operations)
6. [Distributed Patterns](#distributed-patterns)
7. [Best Practices](#best-practices)

---

## Introduction

Meshes are the organizational backbone of Monarch. They provide a structured way to organize and manage hosts, processes, and actors across distributed systems. This document provides a comprehensive guide to understanding and using all types of meshes in Monarch.

---

## What is a Mesh?

### Definition

A **Mesh** in Monarch is a multidimensional container that organizes computational resources. Meshes can contain:
- Hosts (physical or virtual machines)
- Processes (OS-level processes)
- Actors (computation units)

### Core Properties

```mermaid
graph TD
    M[Mesh] --> D[Multidimensional]
    M --> N[Named Dimensions]
    M --> S[Sliceable]
    M --> B[Broadcast-capable]

    D --> D1["hosts: 32"]
    D --> D2["gpus: 8"]

    style M fill:#2F4F4F,stroke:#333,stroke-width:2px
    style D fill:#F8F8FF
    style N fill:#F8F8FF
    style S fill:#F8F8FF
    style B fill:#F8F8FF
```

**1. Multidimensional Structure**
- Organized along named axes (dimensions)
- Each dimension has a size
- Creates a logical coordinate system

**2. Named Dimensions**
- Dimensions have semantic names (e.g., "hosts", "gpus")
- Makes code more readable and maintainable
- Enables dimension-aware slicing

**3. Extent**
- The "shape" of a mesh
- Dictionary mapping dimension names to sizes
- Example: `{"hosts": 32, "gpus": 8}` = 256 total elements

**4. Hierarchical**
- Meshes spawn other meshes
- Forms a tree structure
- Enables resource organization

---

## Types of Meshes

Monarch provides three types of meshes, forming a hierarchy:

```mermaid
graph TD
    H[HostMesh] -->|spawn_procs| P[ProcMesh]
    P -->|spawn| A[ActorMesh]

    style H fill:#e1f5ff,stroke:#0288d1,stroke-width:2px
    style P fill:#fff4e1,stroke:#f57c00,stroke-width:2px
    style A fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
```

### 1. HostMesh

**Purpose:** Represents a collection of compute hosts (machines/nodes).

**Characteristics:**
- Top-level mesh
- Represents physical/virtual machines
- Spawns processes on hosts
- Manages resource allocation

**Structure Diagram:**

```mermaid
graph LR
    subgraph HostMesh
        H1[Host 1<br/>Node 1]
        H2[Host 2<br/>Node 2]
        H3[Host 3<br/>Node 3]
        HN[Host N<br/>Node N]
    end

    style H1 fill:#e1f5ff
    style H2 fill:#e1f5ff
    style H3 fill:#e1f5ff
    style HN fill:#e1f5ff
```

**Common Operations:**
```python
from monarch.actor import hosts_from_config, this_host

# Get hosts from scheduler config
hosts = hosts_from_config("MONARCH_HOSTS")
print(hosts.extent)  # {"hosts": 32}

# Or use current host for development
host = this_host()
print(host.extent)  # {"hosts": 1} or unity

# Spawn processes on hosts
procs = hosts.spawn_procs(per_host={"gpus": 8})
```

**Dimension Examples:**
```python
# Single dimension
hosts = HostMesh(extent={"hosts": 16})

# Multiple dimensions (rare for HostMesh)
hosts = HostMesh(extent={"racks": 4, "hosts_per_rack": 8})
```

### 2. ProcMesh

**Purpose:** A distributed mesh of OS processes for actor computation.

**Characteristics:**
- Middle layer in hierarchy
- One process typically per GPU/CPU core
- Spawns actors
- Can activate for distributed tensors

**Structure Diagram:**

```mermaid
graph TB
    subgraph ProcMesh
        subgraph Host1[Host 1]
            P1[Process<br/>GPU 0]
            P2[Process<br/>GPU 1]
            P3[Process<br/>GPU 2]
            P4[Process<br/>GPU 3]
        end

        subgraph Host2[Host 2]
            P5[Process<br/>GPU 0]
            P6[Process<br/>GPU 1]
            P7[Process<br/>GPU 2]
            P8[Process<br/>GPU 3]
        end
    end

    style P1 fill:#fff4e1
    style P2 fill:#fff4e1
    style P3 fill:#fff4e1
    style P4 fill:#fff4e1
    style P5 fill:#fff4e1
    style P6 fill:#fff4e1
    style P7 fill:#fff4e1
    style P8 fill:#fff4e1
```

**Common Operations:**
```python
# Create from host mesh
hosts = this_host()
procs = hosts.spawn_procs(per_host={"gpus": 8})
print(procs.extent)  # {"gpus": 8}

# Multiple hosts
hosts = hosts_from_config("MONARCH_HOSTS")  # 32 hosts
procs = hosts.spawn_procs(per_host={"gpus": 8})
print(procs.extent)  # {"hosts": 32, "gpus": 8}

# Spawn actors on processes
actors = procs.spawn("actors", MyActor, init_args)

# Activate for distributed tensors
with procs.activate():
    tensor = torch.rand(100, 100)  # Distributed
```

**Dimension Examples:**
```python
# Single host, multiple GPUs
procs = this_host().spawn_procs(per_host={"gpus": 8})
# extent: {"gpus": 8}

# Multiple hosts, multiple GPUs per host
procs = hosts.spawn_procs(per_host={"gpus": 8})
# extent: {"hosts": 32, "gpus": 8}

# Custom dimensions
procs = hosts.spawn_procs(per_host={"workers": 16})
# extent: {"hosts": 32, "workers": 16}
```

### 3. ActorMesh

**Purpose:** A collection of actor instances organized in a mesh.

**Characteristics:**
- Bottom layer in hierarchy
- Contains actual actor instances
- Inherits dimensions from ProcMesh
- Supports all messaging adverbs

**Structure Diagram:**

```mermaid
graph TB
    subgraph ActorMesh
        subgraph Proc1[Process 1]
            A1[Actor<br/>Instance]
        end
        subgraph Proc2[Process 2]
            A2[Actor<br/>Instance]
        end
        subgraph Proc3[Process 3]
            A3[Actor<br/>Instance]
        end
        subgraph Proc4[Process 4]
            A4[Actor<br/>Instance]
        end
        subgraph Proc5[Process 5]
            A5[Actor<br/>Instance]
        end
        subgraph Proc6[Process 6]
            A6[Actor<br/>Instance]
        end
        subgraph Proc7[Process 7]
            A7[Actor<br/>Instance]
        end
        subgraph Proc8[Process 8]
            A8[Actor<br/>Instance]
        end
    end

    style A1 fill:#e8f5e9
    style A2 fill:#e8f5e9
    style A3 fill:#e8f5e9
    style A4 fill:#e8f5e9
    style A5 fill:#e8f5e9
    style A6 fill:#e8f5e9
    style A7 fill:#e8f5e9
    style A8 fill:#e8f5e9
```

**Common Operations:**
```python
# Create from ProcMesh
actors = procs.spawn("actors", MyActor, init_args)
print(type(actors))  # ActorMesh
print(actors.extent)  # Same as procs.extent

# Call endpoints
results = actors.method.call(args).get()

# Broadcast
actors.method.broadcast(args)

# Slice
subset = actors.slice(gpus=slice(0, 4))
```

**Dimension Examples:**
```python
# Inherits from ProcMesh
procs = this_host().spawn_procs(per_host={"gpus": 8})
actors = procs.spawn("actors", MyActor)
# actors.extent: {"gpus": 8}

# Multi-dimensional
procs = hosts.spawn_procs(per_host={"gpus": 8})
actors = procs.spawn("actors", MyActor)
# actors.extent: {"hosts": 32, "gpus": 8}
```

---

## Mesh Hierarchy

### Complete Hierarchy

```mermaid
graph TB
    subgraph Level1[Level 1: Hosts]
        H1[HostMesh<br/>32 hosts]
    end

    subgraph Level2[Level 2: Processes]
        P1[ProcMesh<br/>32 hosts × 8 gpus<br/>= 256 processes]
    end

    subgraph Level3[Level 3: Actors]
        A1[ActorMesh: Trainers<br/>256 instances]
        A2[ActorMesh: DataLoaders<br/>256 instances]
        A3[ActorMesh: Evaluators<br/>256 instances]
    end

    H1 -->|spawn_procs| P1
    P1 -->|spawn| A1
    P1 -->|spawn| A2
    P1 -->|spawn| A3

    style H1 fill:#e1f5ff,stroke:#0288d1,stroke-width:2px
    style P1 fill:#fff4e1,stroke:#f57c00,stroke-width:2px
    style A1 fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style A2 fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style A3 fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
```

### Spawning Flow

```mermaid
sequenceDiagram
    participant User
    participant HostMesh
    participant ProcMesh
    participant ActorMesh

    User->>HostMesh: hosts_from_config()
    Note over HostMesh: Create host mesh

    User->>HostMesh: spawn_procs(per_host=...)
    HostMesh->>ProcMesh: Create processes
    Note over ProcMesh: Start OS processes

    User->>ProcMesh: spawn("name", ActorClass, args)
    ProcMesh->>ActorMesh: Create actors
    Note over ActorMesh: Instantiate actors

    ActorMesh-->>User: Return ActorMesh
```

### Dimension Inheritance

```python
# Dimensions flow down the hierarchy

# HostMesh
hosts = hosts_from_config("MONARCH_HOSTS")
# extent: {"hosts": 32}

# ProcMesh inherits and adds
procs = hosts.spawn_procs(per_host={"gpus": 8})
# extent: {"hosts": 32, "gpus": 8}

# ActorMesh inherits exactly
trainers = procs.spawn("trainers", Trainer)
# extent: {"hosts": 32, "gpus": 8}

dataloaders = procs.spawn("dataloaders", DataLoader)
# extent: {"hosts": 32, "gpus": 8}
```

**Dimension Flow Diagram:**

```mermaid
graph LR
    H["HostMesh<br/>{hosts: 32}"]
    P["ProcMesh<br/>{hosts: 32, gpus: 8}"]
    A1["ActorMesh 1<br/>{hosts: 32, gpus: 8}"]
    A2["ActorMesh 2<br/>{hosts: 32, gpus: 8}"]

    H -->|+ per_host dims| P
    P -->|inherit| A1
    P -->|inherit| A2

    style H fill:#e1f5ff
    style P fill:#fff4e1
    style A1 fill:#e8f5e9
    style A2 fill:#e8f5e9
```

---

## Mesh Operations

### 1. Slicing

Slicing allows selecting subsets of a mesh along dimensions.

#### Single Index Slicing

```python
actors = procs.spawn("actors", MyActor)
# extent: {"hosts": 32, "gpus": 8}

# Select specific GPU
first_gpu = actors.slice(gpus=0)
# extent: {"hosts": 32, "gpus": 1}

# Select specific host
first_host = actors.slice(hosts=0)
# extent: {"hosts": 1, "gpus": 8}

# Select specific point
specific = actors.slice(hosts=5, gpus=3)
# extent: {"hosts": 1, "gpus": 1}  (single actor)
```

#### Range Slicing

```python
# Python slice notation
first_four_gpus = actors.slice(gpus=slice(0, 4))
# extent: {"hosts": 32, "gpus": 4}

# First two hosts
first_two_hosts = actors.slice(hosts=slice(0, 2))
# extent: {"hosts": 2, "gpus": 8}

# Combined
subset = actors.slice(hosts=slice(0, 2), gpus=slice(0, 4))
# extent: {"hosts": 2, "gpus": 4}
```

#### Slicing Visualization

```mermaid
graph TB
    subgraph Original["Original Mesh (4 hosts × 4 GPUs)"]
        direction LR
        subgraph H0[Host 0]
            H0G0[0,0]
            H0G1[0,1]
            H0G2[0,2]
            H0G3[0,3]
        end
        subgraph H1[Host 1]
            H1G0[1,0]
            H1G1[1,1]
            H1G2[1,2]
            H1G3[1,3]
        end
        subgraph H2[Host 2]
            H2G0[2,0]
            H2G1[2,1]
            H2G2[2,2]
            H2G3[2,3]
        end
        subgraph H3[Host 3]
            H3G0[3,0]
            H3G1[3,1]
            H3G2[3,2]
            H3G3[3,3]
        end
    end

    subgraph Sliced["Sliced: hosts=0:2, gpus=0:2"]
        S00[0,0]
        S01[0,1]
        S10[1,0]
        S11[1,1]
    end

    H0G0 -.->|selected| S00
    H0G1 -.->|selected| S01
    H1G0 -.->|selected| S10
    H1G1 -.->|selected| S11

    style S00 fill:#6a906d
    style S01 fill:#6a906d
    style S10 fill:#6a906d
    style S11 fill:#6a906d
```

### 2. Broadcasting

Sending messages to all elements in a mesh.

```python
# Broadcast to entire mesh
actors.method.call(args).get()

# Broadcast to slice
actors.slice(gpus=slice(0, 4)).method.call(args).get()

# Fire-and-forget broadcast
actors.method.broadcast(args)
```

**Broadcast Flow:**

```mermaid
sequenceDiagram
    participant Client
    participant Mesh
    participant A1 as Actor [0,0]
    participant A2 as Actor [0,1]
    participant A3 as Actor [1,0]
    participant A4 as Actor [1,1]

    Client->>Mesh: method.call(args)

    par Broadcast to all
        Mesh->>A1: message
        Mesh->>A2: message
        Mesh->>A3: message
        Mesh->>A4: message
    end

    par Collect responses
        A1-->>Mesh: result
        A2-->>Mesh: result
        A3-->>Mesh: result
        A4-->>Mesh: result
    end

    Mesh-->>Client: [all results]
```

### 3. Point-to-Point Selection

```python
# Get single actor
actor = actors.slice(hosts=0, gpus=0)

# Call single actor
result = actor.method.call_one(args).get()

# Use in actor initialization
class Client(Actor):
    def __init__(self, servers: ActorMesh):
        # Get corresponding server for my rank
        rank = context().actor_instance.rank
        self.server = servers.slice(**rank)
```

### 4. Extent and Shape

```python
# Get mesh extent
print(actors.extent)
# Output: {"hosts": 32, "gpus": 8}

# Get total size
total = 1
for size in actors.extent.values():
    total *= size
print(f"Total actors: {total}")  # 256

# Check dimensions
if "gpus" in actors.extent:
    num_gpus = actors.extent["gpus"]
```

---

## Distributed Patterns

### Pattern 1: Simple Data Parallel

All actors process different data independently.

```python
class Trainer(Actor):
    @endpoint
    def train_step(self, global_step: int):
        # Each actor has its own data
        batch = self.get_local_batch()
        loss = self.model(batch)
        return loss

# All actors train in parallel
trainers = procs.spawn("trainers", Trainer)
losses = trainers.train_step.call(step=0).get()
print(f"Average loss: {sum(losses) / len(losses)}")
```

**Pattern Diagram:**

```mermaid
graph TB
    subgraph Data Parallel
        D1[Data Shard 1] --> T1[Trainer 1]
        D2[Data Shard 2] --> T2[Trainer 2]
        D3[Data Shard 3] --> T3[Trainer 3]
        D4[Data Shard 4] --> T4[Trainer 4]

        T1 --> L1[Loss 1]
        T2 --> L2[Loss 2]
        T3 --> L3[Loss 3]
        T4 --> L4[Loss 4]
    end

    style T1 fill:#2e5e4a
    style T2 fill:#2e5e4a
    style T3 fill:#2e5e4a
    style T4 fill:#2e5e4a
```

### Pattern 2: Parameter Server

Central server stores parameters, workers pull and push.

```python
class ParameterServer(Actor):
    def __init__(self):
        self.params = {}

    @endpoint
    def get_params(self):
        return self.params

    @endpoint
    def update_params(self, gradients):
        # Apply gradients
        for k, grad in gradients.items():
            self.params[k] -= 0.01 * grad

class Worker(Actor):
    def __init__(self, ps: ParameterServer):
        # Each worker connects to PS
        self.ps = ps.slice(gpus=0)  # Single PS

    @endpoint
    def train_step(self):
        # Pull params
        params = self.ps.get_params.call_one().get()

        # Train
        gradients = self.compute_gradients(params)

        # Push gradients
        self.ps.update_params.call_one(gradients)

# 1 parameter server
ps_proc = this_host().spawn_procs(per_host={"gpus": 1})
ps = ps_proc.spawn("ps", ParameterServer)

# 8 workers
worker_procs = this_host().spawn_procs(per_host={"gpus": 8})
workers = worker_procs.spawn("workers", Worker, ps)

# Train
workers.train_step.call()
```

**Parameter Server Diagram:**

```mermaid
graph TB
    PS[Parameter Server]

    W1[Worker 1]
    W2[Worker 2]
    W3[Worker 3]
    W4[Worker 4]

    PS <-->|get/update params| W1
    PS <-->|get/update params| W2
    PS <-->|get/update params| W3
    PS <-->|get/update params| W4

    style PS fill:#ff6b6b
    style W1 fill:#2e5e4a
    style W2 fill:#2e5e4a
    style W3 fill:#2e5e4a
    style W4 fill:#2e5e4a
```

### Pattern 3: Pipeline Parallel

Stages process data sequentially.

```python
class Stage1(Actor):
    @endpoint
    def forward(self, input):
        return self.layer1(input)

class Stage2(Actor):
    def __init__(self, stage1: Stage1):
        rank = context().actor_instance.rank
        self.prev_stage = stage1.slice(**rank)

    @endpoint
    def forward(self, input):
        # Get output from previous stage
        x = self.prev_stage.forward.call_one(input).get()
        return self.layer2(x)

class Stage3(Actor):
    def __init__(self, stage2: Stage2):
        rank = context().actor_instance.rank
        self.prev_stage = stage2.slice(**rank)

    @endpoint
    def forward(self, input):
        x = self.prev_stage.forward.call_one(input).get()
        return self.layer3(x)

# Create pipeline
stage1 = procs.spawn("stage1", Stage1)
stage2 = procs.spawn("stage2", Stage2, stage1)
stage3 = procs.spawn("stage3", Stage3, stage2)

# Run pipeline
results = stage3.forward.call(input_data).get()
```

**Pipeline Diagram:**

```mermaid
graph LR
    I[Input] --> S1[Stage 1]
    S1 --> S2[Stage 2]
    S2 --> S3[Stage 3]
    S3 --> O[Output]

    style S1 fill:#2e5e4a
    style S2 fill:#3e9876
    style S3 fill:#2e5e4a
```

### Pattern 4: Hierarchical Communication

Groups communicate internally, then aggregate.

```python
class Worker(Actor):
    @endpoint
    def compute(self):
        return self.local_result

class GroupLeader(Actor):
    def __init__(self, workers: ActorMesh):
        # Get workers in my group
        rank = context().actor_instance.rank
        self.workers = workers.slice(hosts=rank["hosts"])

    @endpoint
    def aggregate(self):
        # Collect from workers in group
        results = self.workers.compute.call().get()
        return sum(results) / len(results)

# Workers on all GPUs
workers = procs.spawn("workers", Worker)

# One leader per host
leader_procs = hosts.spawn_procs(per_host={"gpus": 1})
leaders = leader_procs.spawn("leaders", GroupLeader, workers)

# Two-level aggregation
group_results = leaders.aggregate.call().get()
final_result = sum(group_results) / len(group_results)
```

**Hierarchical Diagram:**

```mermaid
graph TB
    subgraph Host 1
        L1[Leader 1]
        L1 --> W1[Worker 1]
        L1 --> W2[Worker 2]
    end

    subgraph Host 2
        L2[Leader 2]
        L2 --> W3[Worker 3]
        L2 --> W4[Worker 4]
    end

    L1 --> F[Final Aggregation]
    L2 --> F

    style L1 fill:#ff6b6b
    style L2 fill:#ff6b6b
    style W1 fill:#2e5e4a
    style W2 fill:#2e5e4a
    style W3 fill:#2e5e4a
    style W4 fill:#2e5e4a
    style F fill:#3e9876
```

---

## Best Practices

### 1. Mesh Design

✅ **DO:**
- Use semantic dimension names ("hosts", "gpus", "workers")
- Keep meshes aligned when actors communicate
- Document mesh extent expectations
- Use consistent naming across codebase

❌ **DON'T:**
- Use generic names ("dim1", "dim2")
- Create mismatched meshes for communicating actors
- Hardcode extent values
- Mix different mesh organizations

```python
# ✅ Good: Clear, semantic names
procs = hosts.spawn_procs(per_host={"gpus": 8})
trainers = procs.spawn("trainers", Trainer)
dataloaders = procs.spawn("dataloaders", DataLoader)
# Both have same extent: {"hosts": N, "gpus": 8}

# ❌ Bad: Mismatched meshes
procs1 = hosts.spawn_procs(per_host={"gpus": 8})
procs2 = hosts.spawn_procs(per_host={"workers": 16})
trainers = procs1.spawn("trainers", Trainer)
dataloaders = procs2.spawn("dataloaders", DataLoader)
# Different extents - hard to coordinate!
```

### 2. Slicing Patterns

```python
# ✅ Use slicing for subset operations
first_rank = actors.slice(gpus=0)
subset = actors.slice(gpus=slice(0, 4))

# ✅ Use rank-based selection in actors
class Worker(Actor):
    def __init__(self, servers: ActorMesh):
        rank = context().actor_instance.rank
        self.server = servers.slice(**rank)

# ❌ Don't manually track indices
class BadWorker(Actor):
    def __init__(self, servers: list):
        self.server_idx = compute_index()  # Error-prone!
        self.server = servers[self.server_idx]
```

### 3. Dimension Naming

```python
# ✅ Good: Descriptive names
{
    "hosts": 32,
    "gpus_per_host": 8,
    "replicas": 4
}

# ✅ Good: Standard names
{
    "hosts": 32,
    "gpus": 8
}

# ❌ Bad: Ambiguous
{
    "dim1": 32,
    "dim2": 8
}

# ❌ Bad: Confusing
{
    "x": 32,
    "y": 8
}
```

### 4. Extent Validation

```python
class MyActor(Actor):
    @endpoint
    def validate_mesh(self, other_actors: ActorMesh):
        # ✅ Validate extent matches
        my_extent = context().actor_instance.mesh_extent
        other_extent = other_actors.extent

        if my_extent != other_extent:
            raise ValueError(
                f"Mesh extent mismatch: {my_extent} != {other_extent}"
            )
```

### 5. Resource Management

```python
# ✅ Clean shutdown
def cleanup():
    # Stop actors gracefully
    actors.shutdown.broadcast()

    # Wait for completion
    time.sleep(1)

    # Clean up processes
    procs.terminate()

# ✅ Handle failures
try:
    result = actors.compute.call().get()
except MeshFailure as e:
    print(f"Mesh failure: {e}")
    # Handle recovery
```

### 6. Testing Meshes

```python
import pytest
from monarch.actor import this_host

@pytest.fixture
def test_mesh():
    # Create small mesh for testing
    procs = this_host().spawn_procs(per_host={"gpus": 2})
    yield procs
    # Cleanup
    procs.terminate()

def test_broadcast(test_mesh):
    actors = test_mesh.spawn("test", TestActor)
    results = actors.test_method.call(42).get()
    assert len(results) == 2
    assert all(r == 42 for r in results)

def test_slicing(test_mesh):
    actors = test_mesh.spawn("test", TestActor)

    # Test single slice
    single = actors.slice(gpus=0)
    result = single.test_method.call_one(10).get()
    assert result == 10
```

---

## Summary

### Key Takeaways

1. **Three Mesh Types**: HostMesh → ProcMesh → ActorMesh
2. **Hierarchical**: Meshes spawn other meshes
3. **Multidimensional**: Named dimensions with extents
4. **Sliceable**: Select subsets along dimensions
5. **Broadcast-capable**: Send to all or subset
6. **Dimension Inheritance**: Child meshes inherit parent dimensions

### Mesh Type Comparison

| Feature | HostMesh | ProcMesh | ActorMesh |
|---------|----------|----------|-----------|
| **Contains** | Hosts/Machines | OS Processes | Actor Instances |
| **Spawns** | ProcMesh | ActorMesh | Nothing |
| **Dimensions** | Host-level | Host + Process | Inherited |
| **Messaging** | No | Limited | Full |
| **Slicing** | Yes | Yes | Yes |
| **Activation** | No | Yes (tensors) | No |

### Hierarchy Recap

```mermaid
graph TB
    HM[HostMesh<br/>Machines] -->|spawn_procs| PM[ProcMesh<br/>Processes]
    PM -->|spawn| AM1[ActorMesh<br/>Actor Type 1]
    PM -->|spawn| AM2[ActorMesh<br/>Actor Type 2]

    style HM fill:#3e9876,stroke:#0288d1,stroke-width:2px
    style PM fill:#6a906d,stroke:#f57c00,stroke-width:2px
    style AM1 fill:#2e5e4a,stroke:#388e3c,stroke-width:2px
    style AM2 fill:#2e5e4a,stroke:#388e3c,stroke-width:2px
```

---

## Quick Reference Card

### Creating Meshes

```python
# HostMesh
hosts = hosts_from_config("MONARCH_HOSTS")
host = this_host()

# ProcMesh
procs = hosts.spawn_procs(per_host={"gpus": 8})

# ActorMesh
actors = procs.spawn("actors", MyActor, args)
```

### Slicing

```python
# Single index
actor = actors.slice(gpus=0)

# Range
subset = actors.slice(gpus=slice(0, 4))

# Multiple dimensions
region = actors.slice(hosts=1, gpus=slice(0, 2))
```

### Messaging

```python
# Call all and collect
results = actors.method.call(args).get()

# Call one
result = actor.method.call_one(args).get()

# Broadcast (no wait)
actors.method.broadcast(args)
```

### Common Patterns

```python
# Get corresponding actor in __init__
class MyActor(Actor):
    def __init__(self, others: ActorMesh):
        rank = context().actor_instance.rank
        self.other = others.slice(**rank)

# Validate extent
assert actors.extent == expected_extent

# Get total size
total = prod(actors.extent.values())
```


<!-- ### Next Steps

- Read [Actor Concepts](./ACTORS.md) for actor details
- Review [Overview](./MONARCH_OVERVIEW.md) for system architecture
- Explore [Examples](../examples/) for patterns
- Check [API Reference](./api/) for complete API -->
