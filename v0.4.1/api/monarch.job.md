# monarch.job

The `monarch.job` module provides a declarative interface for managing
distributed job resources. Jobs abstract away the details of different
schedulers (SLURM, local execution, etc.) and provide a unified way to
allocate hosts and create HostMesh objects.

# Job Model

A job object comprises a declarative specification and optionally the job's
*state*. The `apply()` operation applies the job's specification to the
scheduler, creating or updating the job as required. Once applied, you can
query the job's `state()` to get the allocated HostMesh objects.

Example:

```
from monarch.job import SlurmJob

# Create a job specification
job = SlurmJob(
 meshes={"trainers": 4, "dataloaders": 2},
 partition="gpu",
 time_limit="01:00:00",
)

# Get the state (applies the job if needed)
state = job.state()

# Access host meshes by name
trainer_hosts = state.trainers
dataloader_hosts = state.dataloaders
```

# Job State

*class*monarch.job.JobState(*hosts*)[[source]](../_modules/monarch/_src/job/job_state.html#JobState)

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

Container for the current state of a job.

Provides access to the HostMesh objects for each mesh requested in the job
specification. Each mesh is accessible as an attribute.

Example:

```
state = job.state()
state.trainers # HostMesh for the "trainers" mesh
state.dataloaders # HostMesh for the "dataloaders" mesh
```

__init__(*hosts*)[[source]](../_modules/monarch/_src/job/job_state.html#JobState.__init__)

# Job Base Class

All job implementations inherit from `JobTrait`, which defines the core
interface for job lifecycle management.

*class*monarch.job.JobTrait[[source]](../_modules/monarch/_src/job/job.html#JobTrait)

Bases: [`ABC`](https://docs.python.org/3/library/abc.html#abc.ABC)

A job object represents a specification and set of machines that can be
used to create monarch HostMeshes and run actors on them.

A job object comprises a declarative specification for the job and
optionally the job's *state*. The `apply()` operation applies the job's
specification to the scheduler, creating or updating the job as
required. If the job exists and there are no changes in its
specification, `apply()` is a no-op. Once applied, we can query the
job's *state*. The state of the job contains the set of hosts currently
allocated, arranged into the requested host meshes. Conceptually, the
state can be retrieved directly from the scheduler, but we may also
cache snapshots of the state locally.

The state is the interface to the job consumed by Monarch: Monarch
bootstraps host meshes from the state alone, and is not concerned with
any other aspect of the job.

Conceptually, dynamic jobs (e.g., to enable consistently fast restarts,
elasticity, etc.) can simply poll the state for changes. In practice,
notification mechanisms would be developed so that polling isn't
required. The model allows for late resolution of some parts of the
job's *specification*. For example, a job that does not specify a
name may instead resolve the name on the first `apply()`. In this way,
jobs can also be "templates". But the model also supports having the job
refer to a *specific instance* by including the resolved job name in the
specification itself.

Note

Subclasses must NOT set `_status` directly. The `state()` method
manages status transitions and pickle caching. If a subclass
pre-emptively sets `_status = "running"`, the `state()` method
will skip the cache dump, breaking job persistence. Instead, let
`apply()` set the status after `_create()` returns.

enable_telemetry(*config=None*, ***kwargs*)[[source]](../_modules/monarch/_src/job/job.html#JobTrait.enable_telemetry)

Configure automatic telemetry startup on the next `state()` call.

Parameters:

**config** (*TelemetryConfig**|**None*) - A `TelemetryConfig` instance. If omitted, one is
constructed from *kwargs* (forwarded to `TelemetryConfig`).

Returns:

`self`, for chaining.

Return type:

*JobTrait*

enable_admin(*config=None*, ***kwargs*)[[source]](../_modules/monarch/_src/job/job.html#JobTrait.enable_admin)

Configure automatic mesh admin agent startup on the next `state()` call.

Parameters:

**config** (*MeshAdminConfig**|**None*) - A `MeshAdminConfig` instance. If omitted, one is
constructed from *kwargs*.

Returns:

`self`, for chaining.

Return type:

*JobTrait*

apply(*client_script=None*)[[source]](../_modules/monarch/_src/job/job.html#JobTrait.apply)

Request the job as specified is brought into existence or modified to the current specification/
The worker machines launched in the job should call run_worker_forever to join the job.

Calling apply when the job as specified has already been applied is a no-op.

If client_script is not None, then creating the job arranges for the job to run train.py as the client.

Implementation note: To batch launch the job, we will first write .monarch/job_state.pkl with a Job
that instructs the client to connect to the job that it is running in.
Then we will schedule the job including that .monarch/job_state.pkl.
When the client calls .state(), it will find the .monarch/job_state.pkl and connect to it.

*property*apply_id*: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)*

A UUID identifying the current allocation of this job.

Generated fresh each time `apply()` creates a new allocation.
`None` if the job has not been applied yet. When a job is loaded
from a cached file, the original `apply_id` is preserved.

*property*active*: [bool](https://docs.python.org/3/library/functions.html#bool)*

state(*cached_path='.monarch/job_state.pkl'*)[[source]](../_modules/monarch/_src/job/job.html#JobTrait.state)

Connect to the job and return its state with all configured mounts applied.

See `_connect()` for the connection logic. After connecting, all
mount configs registered via `remote_mount()` and gather mount
configs registered via `gather_mount()` are applied before returning.

dump(*filename*)[[source]](../_modules/monarch/_src/job/job.html#JobTrait.dump)

Save job to a file, following any symlink at *filename*.

If *filename* is a symlink, writes to the symlink target rather than
replacing the link itself. Creates the target's parent directory if
it does not yet exist.

dumps()[[source]](../_modules/monarch/_src/job/job.html#JobTrait.dumps)

kill()[[source]](../_modules/monarch/_src/job/job.html#JobTrait.kill)

remote_mount(*source*, *mntpoint=None*, *meshes=None*, *python_exe='.venv/bin/python'*, ***kwargs*)[[source]](../_modules/monarch/_src/job/job.html#JobTrait.remote_mount)

Declare a local directory to be mounted on workers via FUSE.

This is configuration-only -- no mount is established immediately.
The mount is applied (and re-applied on reconnect) on the next call
to `state()`.

Parameters:

- **source** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) - Local directory path to mount.
- **mntpoint** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)*|**None*) - Mount point on workers. Defaults to `source`.
- **meshes** ([*List*](https://docs.python.org/3/library/typing.html#typing.List)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]**|**None*) - Names of meshes to mount on. `None` means all meshes
returned by `state()`.
- **python_exe** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)*|**None*) - Path to the Python executable relative to the mount
point, used to set `python_executable` on the returned mesh.
Set to `None` to skip. Defaults to `".venv/bin/python"`.
- ****kwargs** ([*Any*](https://docs.python.org/3/library/typing.html#typing.Any)) - Forwarded to `remotemount()`.

gather_mount(*remote_mount_point*, *local_mount_point*, *meshes=None*)[[source]](../_modules/monarch/_src/job/job.html#JobTrait.gather_mount)

Declare a remote directory to be mounted locally via gather mount.

This is configuration-only -- no mount is established immediately.
The mount is applied (and re-applied on reconnect) on the next call
to `state()`.

Parameters:

- **remote_mount_point** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) - Path on workers to expose. The token
`$SUBDIR` is replaced with each host's mesh-coordinate key
(e.g. `hosts_0`).
- **local_mount_point** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) - Local path where the remote directory will be
mounted.
- **meshes** ([*List*](https://docs.python.org/3/library/typing.html#typing.List)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]**|**None*) - Names of meshes to gather from. `None` means all meshes
returned by `state()`.

*abstract*can_run(*spec*)[[source]](../_modules/monarch/_src/job/job.html#JobTrait.can_run)

Is this job capable of running the job spec? This is used to check if a
cached job can be used to run spec instead of creating a new reserveration.

It is also used by the batch run infrastructure to indicate that the batch job can certainly run itself.

# Job Implementations

## LocalJob

*class*monarch.job.LocalJob(*hosts=('hosts',)*)[[source]](../_modules/monarch/_src/job/job.html#LocalJob)

Bases: `JobTrait`

Job that runs on the local host.

This job calls `this_host()` for each host mesh requested. It serves as a
stand-in in configuration so a job can be switched between remote and local
execution by changing the job configuration.

can_run(*spec*)[[source]](../_modules/monarch/_src/job/job.html#LocalJob.can_run)

Local jobs are the same regardless of what was saved, so just
use the spec, which has the correct 'hosts' sequence.

*property*process

## ProcessJob

*class*monarch.job.ProcessJob(*meshes*, *env=None*)[[source]](../_modules/monarch/_src/job/process.html#ProcessJob)

Bases: `JobTrait`

Job where each host is a local subprocess communicating over IPC.

Suitable for local testing of multi-host scenarios without SSH or a
scheduler. Each host runs `run_worker_loop_forever` in a child
process, listening on a Unix socket.

Example:

```
job = ProcessJob({"trainers": 2, "dataloaders": 1})
state = job.state(cached_path=None)
state.trainers # HostMesh with 2 hosts
state.dataloaders # HostMesh with 1 host
```

can_run(*spec*)[[source]](../_modules/monarch/_src/job/process.html#ProcessJob.can_run)

Is this job capable of running the job spec? This is used to check if a
cached job can be used to run spec instead of creating a new reserveration.

It is also used by the batch run infrastructure to indicate that the batch job can certainly run itself.

## SlurmJob

*class*monarch.job.SlurmJob(*meshes*, *python_exe='python'*, *slurm_args=()*, *monarch_port=22222*, *job_name='monarch_job'*, *ntasks_per_node=1*, *time_limit=None*, *partition=None*, *log_dir=None*, *exclusive=True*, *gpus_per_node=None*, *cpus_per_task=None*, *mem=None*, *job_start_timeout=None*)[[source]](../_modules/monarch/_src/job/slurm.html#SlurmJob)

Bases: `JobTrait`

A job scheduler that uses SLURM command line tools to schedule jobs.

This implementation:
1. Uses sbatch to submit SLURM jobs that start monarch workers
2. Queries job status with squeue to get allocated hostnames
3. Uses the hostnames to connect to the started workers

add_mesh(*name*, *num_nodes*)[[source]](../_modules/monarch/_src/job/slurm.html#SlurmJob.add_mesh)

can_run(*spec*)[[source]](../_modules/monarch/_src/job/slurm.html#SlurmJob.can_run)

Check if this job can run the given spec.

share_node(*tasks_per_node*, *gpus_per_task*, *partition*)[[source]](../_modules/monarch/_src/job/slurm.html#SlurmJob.share_node)

Share a node with other jobs.

## KubernetesJob

*class*monarch.job.kubernetes.KubernetesJob(*namespace*, *timeout=None*)[[source]](../_modules/monarch/_src/job/kubernetes.html#KubernetesJob)

Bases: `JobTrait`

Job implementation for Kubernetes that discovers and connects to pods.

Supports two modes:

*Pre-provisioned* - connect to pre-provisioned pods discovered via label
selectors. Compatible with the MonarchMesh operator, third-party
schedulers, or manually created pods. Used when `image_spec` or `pod_spec`
is not specified in `add_mesh`.

*Provisioning* - create MonarchMesh CRDs via the K8s API so the
pre-installed operator provisions StatefulSets and Services
automatically. Pass `image_spec` or `pod_spec` (a `V1PodSpec`) to
`add_mesh` to enable provisioning for that
mesh. If the MonarchMesh CRD
already exists, it is patched instead
of created.

add_mesh(*name*, *num_replicas*, *label_selector=None*, *pod_rank_label='apps.kubernetes.io/pod-index'*, *image_spec=None*, *port=26600*, *pod_spec=None*, *labels=None*)[[source]](../_modules/monarch/_src/job/kubernetes.html#KubernetesJob.add_mesh)

Add a mesh specification.

In *attach-only* mode (default), meshes are discovered by label
selector. In *provisioning* mode (`image_spec` or `pod_spec`
supplied), a MonarchMesh CRD is created so the operator can
provision the pods.

Parameters:

- **name** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) - Name of the mesh. Must follow RFC 1123 DNS label standard and Monarch hostname restriction:
* At most 63 characters
* only lowercase alphanumeric characters
* must start with an alphabetic character,
* and end with an alphanumeric character.
- **num_replicas** ([*int*](https://docs.python.org/3/library/functions.html#int)) - Number of pod replicas (expects all ranks 0 to num_replicas-1)
- **label_selector** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)*|**None*) - Custom label selector for pod discovery. Cannot be set when provisioning.
- **pod_rank_label** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) - Label key containing the pod rank. Cannot be customized when provisioning.
- **image_spec** (*ImageSpec**|**None*) - `ImageSpec` with container image and optional resources for simple provisioning.
Mutually exclusive with `pod_spec`.
- **port** ([*int*](https://docs.python.org/3/library/functions.html#int)) - Monarch worker port (default: 26600).
- **pod_spec** (*V1PodSpec**|**None*) - `V1PodSpec` for advanced provisioning (e.g. custom volumes, sidecars).
Mutually exclusive with `image_spec`.
- **labels** ([*dict*](https://docs.python.org/3/library/stdtypes.html#dict)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*,*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]**|**None*) - Optional labels to apply to the MonarchMesh CRD metadata.
Only used when provisioning (`image_spec` or `pod_spec` supplied).

Raises:

[**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) - On invalid name or conflicting parameters.

can_run(*spec*)[[source]](../_modules/monarch/_src/job/kubernetes.html#KubernetesJob.can_run)

Check if this job can run the given spec.

Verifies that:
1. The spec is a KubernetesJob with matching configuration
2. The required pods are available and ready

Parameters:

**spec** (*JobTrait*) - JobTrait specification to check

Returns:

True if this job matches the spec and all required pods are available

Return type:

[bool](https://docs.python.org/3/library/functions.html#bool)

# Serialization

Jobs can be serialized and deserialized for persistence and caching.

monarch.job.job_load(*filename='.monarch/job_state.pkl'*)[[source]](../_modules/monarch/_src/job/job.html#job_load)

Load a job from a file.

Parameters:

**filename** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) - Path to the pickled job file, typically from `JobTrait.dump()`.
Defaults to `.monarch/job_state.pkl`.

Returns:

The deserialized job object.

Return type:

*JobTrait*

monarch.job.job_loads(*data*)[[source]](../_modules/monarch/_src/job/job.html#job_loads)

Deserialize a job from bytes.

Parameters:

**data** ([*bytes*](https://docs.python.org/3/library/stdtypes.html#bytes)) - Pickled job bytes, typically from `JobTrait.dumps()`.

Returns:

The deserialized job object.

Return type:

*JobTrait*

# SPMD Jobs

The `monarch.job.spmd` submodule provides job primitives for launching
torchrun-style SPMD training over Monarch. It parses torchrun arguments from
an AppDef and executes the training script across the mesh.

monarch.job.spmd.serve(*appdef*, *scheduler='mast_conda'*, *scheduler_cfg=None*)[[source]](../_modules/monarch/_src/job/spmd.html#serve)

Launch SPMD job using an AppDef or a single-node torchrun command.

This function launches monarch workers, then allows running SPMD training
via run_spmd().

Assumptions:

- When using an AppDef, the role's entrypoint is a script (e.g.,
"workspace/entrypoint.sh") that sets up the environment (activates
conda, sets WORKSPACE_DIR, etc.) and runs its arguments.
- The role's args contains a torchrun command with the training script,
e.g., ["torchrun", "-nnodes=1", "-m", "train", "-lr", "0.001"].
- The role's workspace defines which files to upload to workers.
- When using a command list, it should be a torchrun command, e.g.,
["torchrun", "-nproc-per-node=4", "-standalone", "train.py"].

Note

When passing a command list, only single-node torchrun is supported
(`--standalone` or `--nnodes=1`). For multi-node training, use an
`AppDef` with a scheduler that manages node allocation.

Parameters:

- **appdef** (*AppDef**|*[*List*](https://docs.python.org/3/library/typing.html#typing.List)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]*) - Either a torchx `AppDef` instance, or a torchrun command as
a list of strings (e.g., `["torchrun", "--nproc-per-node=4",
"train.py"]`). When a list is provided, the first element is the
entrypoint and the rest are arguments.
- **scheduler** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) - Scheduler name (e.g., 'mast_conda', 'local_cwd')
- **scheduler_cfg** ([*Dict*](https://docs.python.org/3/library/typing.html#typing.Dict)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*,*[*Any*](https://docs.python.org/3/library/typing.html#typing.Any)*]**|**None*) - Scheduler configuration dict

Returns:

SPMDJob instance

Raises:

[**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) - If command list specifies multi-node (-nnodes > 1).

Return type:

*SPMDJob*

Example

Using a torchrun command list (single-node only):

```
from monarch.job.spmd import serve

job = serve(
 ["torchrun", "--nproc-per-node=4", "--standalone", "train.py"],
 scheduler="local_cwd",
)
job.run_spmd()
```

Using an AppDef (supports multi-node):

```
from monarch.job.spmd import serve
from torchx import specs

app = specs.AppDef(
 name="my-training",
 roles=[
 specs.Role(
 name="trainer",
 image="my_workspace:latest",
 entrypoint="workspace/entrypoint.sh",
 args=["torchrun", "--nnodes=2", "--nproc-per-node=8",
 "-m", "train"],
 num_replicas=2,
 resource=specs.resource(h="gtt_any"),
 ),
 ],
)
job = serve(
 app,
 scheduler="mast_conda",
 scheduler_cfg={
 "hpcClusterUuid": "MastGenAICluster",
 "hpcIdentity": "my_identity",
 "localityConstraints": ["region", "pci"],
 },
)
job.run_spmd()
```

*class*monarch.job.spmd.SPMDJob(*handle*, *scheduler*, *workspace=None*, *original_roles=None*)[[source]](../_modules/monarch/_src/job/spmd.html#SPMDJob)

Bases: `JobTrait`

SPMD (Single Program Multiple Data) job that uses torchx directly.

This job type wraps a torchx Runner and job handle, providing monarch job tracking.

can_run(*spec*)[[source]](../_modules/monarch/_src/job/spmd.html#SPMDJob.can_run)

Is this job capable of running the job spec? This is used to check if a
cached job can be used to run spec instead of creating a new reserveration.

It is also used by the batch run infrastructure to indicate that the batch job can certainly run itself.

run_spmd()[[source]](../_modules/monarch/_src/job/spmd.html#SPMDJob.run_spmd)