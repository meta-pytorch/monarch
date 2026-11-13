# Run Sealed Postcard Across Firecracker MicroVMs

This walkthrough runs the same three roles as
[`separate-processes.md`](separate-processes.md), but Alice and Bob each run
inside their own Firecracker microVM and Relay runs on the host. The roles are
the unchanged Rust `sealed-postcard` binary; only their addresses differ.
Alice sends Bob the postcard and checks his acknowledgement, then opens 32 more
streams that Bob echoes. Relay forwards every encrypted datagram between the
two VMs and never sees the postcard's plaintext.

## What Runs Where

```text
  VM A                         host                                     VM B
  Alice                        Relay (namespace root)                   Bob
  dials native:2:5001  ---->   listens on unix:<run>/a/v.sock_5001
                               listens on unix:<run>/b/v.sock_5001  <----  dials native:2:5001
```

The VMs have no network interface: their only link is vsock to the host.
Inside a guest that link is vsock; on the host, Firecracker represents each
VM's vsock as Unix sockets beside the VM's `v.sock`. When Alice or Bob dials the
host (CID 2) on port 5001, that VM's Firecracker connects them to `v.sock_5001`
beside the VM's `v.sock`, where Relay listens.

A Python program on the host, `sealed_postcard_microvms.py`, starts the three
roles. It boots each VM with a small init agent that receives one role's
command, runs it, forwards signals to it and reports its exit. The three Rust
programs are the roles; the two Firecracker processes and their agents are only
how two of them get a VM.

## Prerequisites

- A Linux devserver with this checkout at `~/fbsource` and read and write
  access to `/dev/kvm`. No root access or host network setup is needed.
- tmux.

## Build the Inputs

Buck builds the guest image, the Rust example, the demo program, the
Firecracker binary and the guest kernel. The first build of the kernel and
image takes several minutes.

```sh
cd ~/fbsource
buck2 build @fbcode//mode/dev-nosan \
  fbcode//monarch/chrysalis/crates/chrysalis:sealed_postcard_guest_rootfs \
  fbcode//monarch/chrysalis/crates/chrysalis:sealed-postcard \
  fbcode//monarch/chrysalis/python/examples:sealed_postcard_microvms \
  fbcode//cse/firecracker:fcexe \
  fbcode//cse/firecracker:firecracker.kernel.vmlinux \
  --show-full-output > /tmp/sealed-postcard-outputs

output() { awk -v target="$1" '$1 == target { print $2 }' /tmp/sealed-postcard-outputs; }
export CHR_FC_ROOTFS=$(output fbcode//monarch/chrysalis/crates/chrysalis:sealed_postcard_guest_rootfs)
export SEALED_POSTCARD=$(output fbcode//monarch/chrysalis/crates/chrysalis:sealed-postcard)
export SEALED_POSTCARD_MICROVMS=$(output fbcode//monarch/chrysalis/python/examples:sealed_postcard_microvms)
export CHR_FC_FIRECRACKER=$(output fbcode//cse/firecracker:fcexe)/firecracker
export CHR_FC_KERNEL=$(output fbcode//cse/firecracker:firecracker.kernel.vmlinux)
```

`--show-full-output` prints each target with the absolute path of its output.
The `fcexe` output is a directory, and the Firecracker binary is the
`firecracker` file inside it.

## Start the Run

Create a fresh, short run directory. It holds the VMs' sockets, so its paths
must fit the operating system's limit for a Unix socket path. Then create a
tmux session with four panes: one each following Alice's VM console, Relay's
log and Bob's VM console, and a control pane that runs the demo.

```sh
sealed_postcard_dir=$(mktemp -d /tmp/chr-fc-XXXXXX)
session="sealed-postcard-${sealed_postcard_dir##*-}"

tmux new-session -d -s "$session" -n microvms \
  -e "CHR_FC_ROOTFS=$CHR_FC_ROOTFS" \
  -e "SEALED_POSTCARD=$SEALED_POSTCARD" \
  -e "CHR_FC_FIRECRACKER=$CHR_FC_FIRECRACKER" \
  -e "CHR_FC_KERNEL=$CHR_FC_KERNEL" \
  -e "sealed_postcard_dir=$sealed_postcard_dir" \
  "tail -n +1 -F $sealed_postcard_dir/a/console.log"
tmux set-option -t "$session" remain-on-exit on
tmux set-option -t "$session" pane-border-status top
tmux split-window -t "$session:microvms" "tail -n +1 -F $sealed_postcard_dir/relay.log"
tmux split-window -t "$session:microvms" "tail -n +1 -F $sealed_postcard_dir/b/console.log"
tmux split-window -t "$session:microvms" \
  "$SEALED_POSTCARD_MICROVMS --run-dir $sealed_postcard_dir"
tmux select-layout -t "$session:microvms" tiled
tmux select-pane -t "$session:microvms.0" -T 'Alice: VM A console'
tmux select-pane -t "$session:microvms.1" -T 'Relay: host'
tmux select-pane -t "$session:microvms.2" -T 'Bob: VM B console'
tmux select-pane -t "$session:microvms.3" -T 'control'
tmux attach-session -t "$session"
```

The `-e` options give the session exactly the paths built above, so a tmux
server started earlier can't supply stale ones. `tail -F` waits for a log that
doesn't exist yet, and `remain-on-exit` keeps the control pane's output on
screen after the demo exits.

## What Happens

The control pane reports each stage as the demo:

1. runs `sealed-postcard identities --out "$sealed_postcard_dir/ids"`, which
   writes one private identity bundle per role and prints their PIDs. There is
   no separate step to create identities;
2. starts Relay, the namespace root, on the host:

   ```sh
   sealed-postcard relay \
     --listen "unix:$sealed_postcard_dir/a/v.sock_5001" \
     --listen "unix:$sealed_postcard_dir/b/v.sock_5001"
   ```

3. boots VM A, whose agent runs Alice:

   ```sh
   sealed-postcard alice --parent-pid <relay's PID> --parent native:2:5001 \
     --bob-pid <bob's PID>
   ```

   CID 2 is the host, so Alice dials Relay;
4. boots VM B, whose agent runs Bob:

   ```sh
   sealed-postcard bob --parent-pid <relay's PID> --parent native:2:5001 \
     --alice-pid <alice's PID>
   ```

   CID 2 is the host, so Bob dials Relay;
5. waits for Alice to finish the exchange and exit successfully. Alice prints
   nothing of her own; her result is her exit status, which the control pane
   reports. VM A then shuts down;
6. sends Bob SIGTERM through his VM's agent. Bob prints his report in his pane
   and exits, and VM B shuts down:

   ```text
   bob served 33 streams, all from alice: true
   ```

7. stops Relay with SIGTERM. Relay prints its report in its pane:

   ```text
   relay observed 123 datagrams, postcard plaintext seen: false, log complete: true
   ```

The roles start without waiting for each other: Alice and Bob keep retrying
their parent link to Relay, and Alice waits up to 120 seconds for Bob. Each role
receives only its own bundle. The demo succeeds only if Alice and Bob exit successfully,
both reports appear, and Relay exits successfully, which it does only when it
recorded every datagram it forwarded and none contained the postcard's
plaintext. The control pane ends with:

```text
sealed-postcard microVMs: succeeded; logs and VM specs are kept in /tmp/chr-fc-ab12cd
```

At that point every process the demo started has exited. The run directory
stays so you can read the logs; that doesn't mean a VM is still running.

## Interrupt a Run

Press Ctrl-C in the control pane to stop a run early. The demo releases every
process it started, within bounded waits, and exits unsuccessfully, keeping the
logs. Closing the control pane or sending the demo SIGTERM does the same.
Further interrupts are ignored, so they can't cut that cleanup short. If a
process couldn't be confirmed stopped, the demo says so and keeps the run
directory.

`kill -9` of the demo is not a clean shutdown: it can leave Relay or a VM
running. To check for anything still running from this run:

```sh
pgrep -af "$sealed_postcard_dir"
```

Run this after closing the session, because the `tail` panes also name the run
directory. Don't delete the run directory while it lists a VM or Relay; stop
those processes first.

## Clean Up

After the control pane's final line, close this run's session. Then delete the
run directory, which also removes the identity bundles' private keys, but only
if nothing from the run is still running:

```sh
tmux kill-session -t "$session"
pgrep -af "$sealed_postcard_dir"; pgrep_status=$?
if [ "$pgrep_status" -eq 1 ]; then
  rm -rf "$sealed_postcard_dir"
else
  echo "kept $sealed_postcard_dir: stop the processes listed above first"
fi
```

`pgrep` exits with status 1 only when it finds no process. If it lists a VM or
Relay, or fails, the directory stays: stop those processes, as described under
"Interrupt a Run", before deleting it.

## The Automated Test

The same program is the core of an automated test, which runs the exchange and
two checks of how it releases the identity generator:

```sh
buck test @fbcode//mode/dev-nosan \
  fbcode//monarch/chrysalis/python/tests:test_sealed_postcard_rust_microvms \
  -- --print-passing-details
```

On a host with usable KVM it reports three passing tests and no skips. Without
KVM the microVM test is skipped and the other two still run.

## Buck Only

Buck builds the guest image, Firecracker, the kernel, the Rust example and the
demo program, and runs the automated test. The tmux demo runs that program
against those Buck outputs. Cargo still builds and runs the Rust example as before, but it
doesn't build the image or run anything in a VM.
