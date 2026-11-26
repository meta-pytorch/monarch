# Slurm checks for Chrysalis

Before running Chrysalis's microVM demos on a Slurm cluster, use these checks:

| Script | What it checks |
|---|---|
| [run_udp.sh](run_udp.sh) | Two nodes of one Slurm job can exchange UDP packets, trying IPv6 first and IPv4 if needed. |
| [kvm-run-check.sh](kvm-run-check.sh) | A non-root job user can run a KVM guest. |

Both checks run on Linux with the system's `python3` (3.11 or later). Run them from this source directory; no build is needed. The KVM check requires x86-64.

## Check UDP between two nodes

### What the cluster must provide

Both compute nodes must have access to:

- Your writable home directory, shared and mounted at the same path on both nodes.
- This directory's Python files, readable at the same path on both nodes.

The cluster must provide these mounts; Slurm does not create them. The workers use the shared home to tell each other their addresses, then send UDP packets directly between the nodes.

### Run the check

From this directory, submit one job for two nodes. Choose the partition, account and nodes for your cluster; add `-q <qos>` if needed:

```sh
sbatch -p <partition> -A <account> -w <node1>,<node2> \
    "$PWD/run_udp.sh" "$PWD"
```

`run_udp.sh` first runs on one of the allocated nodes. It creates a fresh directory under `~/chr-slurm-checks/<job-id>/`, then starts the Python program that runs the checks. That program uses `srun` to start one worker on each node for IPv6. If that step fails, it starts a new step with both workers testing IPv4. Each family gets its own directory. The job allows 20 minutes for both attempts and startup delays.

The second argument tells the script where the Python files live. Slurm runs its own saved copy of the shell script, so looking beside that copy would not find them.

### What the workers do

Each worker opens a UDP socket on the network interface its node normally uses for the family being tested. It asks Linux to choose an unused port, then writes its IP address and port to a file in that family's shared directory. IPv6 needs one usable global or private address on an unambiguous default-route interface; link-local addresses are not supported. If a node has no usable IPv6 configuration, that attempt fails and the IPv4 attempt follows.

Each waits up to five minutes for the other worker's file. Once it knows the other worker's address, it sends three requests and answers the other worker's three. Each must receive all three requests and all three replies within 90 seconds. Both workers must pass. Missing messages are not retried, and duplicates do not increase the counts. The result covers this one exchange.

Stopping the job signals the launcher, which stops the current step without starting a fallback. A stop received during cleanup is remembered and acted on after cleanup finishes. Each step also has a seven-minute limit. If the step times out or the program is stopped, the program stops the local `srun` process group, waiting up to ten seconds for `srun` to exit.

### Find the results

The script prints the run's directory: `~/chr-slurm-checks/<job-id>/attempt.*`. It keeps that directory after success, failure or cancellation. `summary.json` records which family passed and the exit status of every attempted step. It is written when the attempts finish, so a cancelled run may have logs without a summary.

The `ipv6/` directory holds the first attempt. `ipv4/` exists only if a fallback was attempted. In each directory, the workers are numbered `0` and `1`; that number replaces `<slot>` in these filenames:

- `ready-<slot>.json`: the worker's node, user ID, IP address and UDP port.
- `result-<slot>.json`: a successful result, including the addresses, message counts and elapsed times.
- `failure-<slot>.json`: a worker's failure phase and error, when it could save them.
- `worker-<slot>.out`: the worker's `PASS` message.
- `worker-<slot>.err`: progress, Python version and any failure.

Check the job with `sacct -j <job-id>`. Success requires the job and the successful family's step to complete with exit 0, and both of that step's workers to print `PASS` with 3/3 requests and 3/3 replies. An IPv6 failure followed by IPv4 success is reported as `PASS IPv4 fallback; IPv6 failed`. The failed step's logs and status remain in the report. If IPv6 passes, IPv4 is reported as not tested. If both fail, the job fails.

### Read a failure

Read both workers' logs for the family that failed before drawing a conclusion about the network.

| What you see | What it means or what to check |
|---|---|
| The job is pending | The workers have not started. Check the allocation, account, QoS and available nodes. |
| `FAIL discovery` | The worker could not find or open a usable address for this family. Check its route, interface and socket error. |
| `FAIL readiness: no ready-<slot>.json` | The worker could not see the other worker's file. Check the shared mounts, then both logs: the other worker may have started late, failed, or written a file that the filesystem has not shown yet. UDP has not been tested by this worker. |
| `FAIL exchange: timed out`, with the other worker still waiting for its file | The other worker had not learned where to send packets. Check file visibility first. |
| `FAIL exchange: timed out`, with both workers exchanging packets | The exchange failed. Keep both logs and check the job state, routing and network policy. The timeout alone does not tell you why messages are missing. |
| `FAIL exchange: frame from ...` | A packet for this run arrived from a different IP address or UDP port than the other worker advertised. |
| `FAIL result` | The exchange finished, but saving its result failed. Check the shared storage error in the log. |
| A worker log ends with `terminated by signal 15`, and the other worker logged `FAIL` | Slurm stops the remaining worker when its peer fails. A step status of 143 alone does not mean the whole job was cancelled; check the launcher log and the fallback result. |
| A step's status is 124 | The step exceeded the launcher's seven-minute limit. Check both worker logs and the Slurm job state. |
| One worker printed `PASS`, but its step failed | That family did not pass. Check the other worker's log and the summary for the fallback's result. |

## Check that a job can run a KVM guest

Run this from the source directory in a one-node job:

```sh
srun -p <partition> -A <account> -w <node> -N1 -n1 -c 1 --mem=1G -t 5 \
    bash "$PWD/kvm-run-check.sh"
```

`kvm-run-check.sh` refuses to run as root. It opens `/dev/kvm`, checks API version 12, and creates a tiny VM with one virtual CPU and 64 KiB of memory. The guest executes one halt instruction (`HLT`). The check passes only if `KVM_RUN` returns the expected halt result, `KVM_EXIT_HLT`.

Success prints `PASS` and exits 0. Failure exits non-zero and leaves diagnostics in the job's output. The script changes no kernel setting.
