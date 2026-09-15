# Chrysalis scale benchmark

`chrysalis-scale` is a pure-Rust MAST benchmark for the Chrysalis nameserver and
stream data path. The `flat` comparison topology connects every process to the
root:

```text
root
  <- child
  <- child
  <- ...
```

Every logical node runs in its own OS process with a fresh Meta leaf certificate
issued by `chrysalis-identity-meta`. In the `flat` baseline, every process has
its own UDP socket. The root waits until its nameserver contains all expected
process entries, records full-join latency, and then opens one new application
stream to every child. A stream starts with an operation byte. The initial
benchmark uses the echo operation, followed by one frame:

```text
[operation: u8][payload length: u32 big-endian][payload bytes]
```

The request payload is one byte. Each child echoes the frame, waits until the
reply is acknowledged, and exits. The root prints a machine-readable `RESULT`
line containing join duration, total echo duration, echo throughput, and mean
and maximum per-child latency.

The elected root also prints a bare join token:

```text
udp://[<root-ipv6>]:26600?authority=<pid>
```

Use it to inspect the live benchmark namespace:

```bash
fbcode/monarch/bin/chrysalis --identity=meta ps '<join-token>'
```

MAST tasks supervise many logical node processes. By default, each task hosts
up to 100 processes, starts at most eight concurrently, and uses the
`task-head` topology:

```text
root / task-0 head
  <- leaf
  <- leaf
  <- task head
       <- leaf
       <- leaf
```

Each task supervisor hosts its head node and launches every remaining rank as a
child process. A head uses UDP to communicate with the root and a Unix datagram
carrier to communicate with its local leaves. Leaves bind only Unix datagram
sockets; they do not create or process UDP sockets. The root is also the root
task's head, so it uses UDP externally and Unix datagrams for its local leaves.
MAST reserves each task's whole host, and the process rejects a placement whose
deduplicated hostname count does not equal its task count.

The root is level 0. Leaves attached directly to the root are level 1. Heads on
other tasks are level 1, and their leaves are level 2. The supervisor bounds
concurrent worker startup with `--identity-concurrency`, waits for each process
to join its parent, propagates worker failures, and terminates the remaining
workers when the task exits.

## Generate the ablation

The default MAST command builds an optimized Rust binary, uploads one ephemeral
fbpkg, and writes job specifications for 1K, 10K, and 100K nodes:

```bash
buck run fbcode//monarch/chrysalis-scale:chrysalis-scale -- mast
```

Add `--launch` to submit all three jobs:

```bash
buck run fbcode//monarch/chrysalis-scale:chrysalis-scale -- mast --launch
```

After each job is fully placed, the launcher prints an address-only connect
string and its stable resolver URL:

```text
[mast] connect: udp://[2401:db00:...]:26600
[mast] resolver: mast://<job-name>
```

It can be passed directly to the Chrysalis CLI. The Meta certificate still
authenticates the peer and supplies its PID; the address-only form simply does
not pin that PID before connecting.

Reuse an existing package to avoid rebuilding and uploading:

```bash
buck run fbcode//monarch/chrysalis-scale:chrysalis-scale -- mast \
  --package monarch_additional_packages:<ephemeral-id> \
  --launch
```

Useful options include `--nodes 1000,10000,100000`, `--topology task-head`,
`--region atn`, `--nodes-per-task 100`, `--identity-concurrency 8`,
`--concurrency 1024`, `--ram-mb 2048`, `--cpus 4`, `--port 26600`,
`--opec-tag opec-only`, and `--print-spec`.

## Placement constraint

Every node process obtains one Meta leaf certificate from the task's delegated
CAT material. A task on the first advertised hostname claims the fixed
`chrysalis_root` UDP port and becomes the root. In `flat` mode, children bind
kernel-assigned ephemeral UDP ports and publish their actual bound addresses.
In `task-head` mode, only the task head binds UDP; its leaf processes bind
filesystem-named Unix datagram sockets in a per-task runtime directory. The head
combines both carriers in one socket set, advertises only its UDP address
upstream, and forwards descendant traffic over the local Unix carrier.
Every process publishes `client`, `rank`, `task`, `role`, `level`, and `topology`
labels. Roles are `root`, `head`, or `leaf`, so `chrysalis ps` can expose the
logical placement without consulting the benchmark database.
`MastGenAICluster` defaults to dedicated capacity; the classic CPU pools
default to OPEC-only capacity. Use `--opec-tag` to override that choice.
MAST validates the physical task count against regional task slots before the
application starts.

The benchmark reads the complete host vector from the Tupperware user
metadata named by `TW_USER_METADATA_FILE_PATH` and
`TW_USER_METADATA_HOSTNAMES_LIST_KEY`. It falls back to
`MAST_HPC_TASK_GROUP_HOSTNAMES` for older environments. The benchmark sorts and
deduplicates the vector, requires exactly one hostname per task, and uses the
first hostname to locate the root. The launcher derives the same host from the
post-placement MAST task status.

## Results

Query the root's unique result marker:

```bash
lg mast:<job-name> --pattern 'joined|RESULT|ERROR' \
  --stream stdout --print-all
```

The final line has this shape:

```text
[root] RESULT {"event":"chrysalis_scale_result","nodes":1000,"tasks":10,"nodes_per_task":100,...}
```

## Persistent experiments

Persistent mode gives every logical node a file-backed cr-sqlite replica. Each
node waits for full node convergence, serves benchmark streams, and executes rows
from the replicated `experiments` table that name its PID. A node runs its
experiments serially; different nodes may run experiments concurrently. `count`
selects that many peer nodes, and `size` is the request payload size in bytes.
Requests and responses are streamed in bounded chunks, so the payload does not
need to fit in a QUIC flow-control window or an in-memory frame.

Launch one persistent job and use the address-only connect string printed after
placement:

```bash
buck run fbcode//monarch/chrysalis-scale:chrysalis-scale -- mast \
  --nodes 1000 --topology task-head --persist --launch
```

Use the scale utility to inspect the nodes registered by the job:

```bash
JOIN='mast://<job-name>'
chrysalis-scale nodes "$JOIN" list
chrysalis-scale nodes "$JOIN" show <pid>
```

The resolver selects the placed root task, a matching wildcard UDP carrier,
and the Meta identity provider. Direct PID and address join tokens remain
supported.

The replicated `nodes` table contains each logical node's PID, rank, MAST task
ID and handle, hostname, advertised address, root role, configured job size,
nodes per task, parent PID, topology level, and start time. Rank is the primary
key, so restarting a logical rank replaces its stale registration.

Add and inspect experiments through the same replicated database:

```bash
chrysalis-scale experiments "$JOIN" list
chrysalis-scale experiments "$JOIN" add 999-64k <pid> 999 65536
chrysalis-scale experiments "$JOIN" add-targeted leaf-pair-64m \
  <source-pid> 67108864 <target-pid>
chrysalis-scale experiments "$JOIN" add-targeted leaf-delivery-64m \
  <source-pid> 67108864 <target-pid> --kind delivery
chrysalis-scale experiments "$JOIN" show 999-64k
```

Experiment names are globally unique. A new experiment starts as `pending`, its
target node atomically changes it to `processing` before opening echo streams,
and commits `done` with its result. Execution success or failure remains in the
corresponding `results.status` and `results.error` columns. The `add` command
waits until the target claims the experiment before disconnecting.

`add` selects the first `count` peers in rank order. `add-targeted` instead
stores an ordered, explicit PID set in the replicated `experiment_targets`
table and contacts exactly those processes. Both commands default to `echo` and
accept `--kind delivery`.

An echo stream returns the full request payload. A delivery stream sends its
payload, closes the sender's stream half, and waits for the receiver to reply
with the received byte count as a big-endian `u64`. The measured delivery is
complete only after the sender validates that receipt. `payload_mib_per_second`
therefore counts two payload traversals for echo and one for delivery.

Persistent nodes also echo unframed streams whose first byte is not a reserved
benchmark operation. This makes `chrysalis cat <pid>` useful for interactive
connectivity checks without affecting measured experiment traffic.

Before measurement, the source completes an untimed one-byte operation of the
selected kind with every target. This establishes and pools the QUIC
connection. The reported `warmup_seconds` records that phase, while
`operation_seconds` begins immediately before the measured streams are opened.

Each result also snapshots the source's application QUIC datagram I/O during
the measured interval. `transmit_calls`, `transmit_datagrams`,
`transmit_bytes`, and `transmit_blocked` describe submissions to the Chrysalis
datagram adapter; the corresponding receive fields describe acknowledgements
and responses. `mean_transmit_bytes` and `transmit_datagrams_per_second` make
packetization and syscall pressure visible. These counters exclude the warmup
and link-local nameserver traffic.

For arbitrary SQL, you can still attach an in-memory replica and REPL:

```bash
chrysalis --identity=meta --carrier 'udp://[::]:0' --cluster "$JOIN" sqlite
```

The `nodes`, `experiments`, and `results` definitions arrive over the
replication link.
