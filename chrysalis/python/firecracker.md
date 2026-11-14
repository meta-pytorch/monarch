# Run Python programs in Firecracker microVMs

The [hello-world tutorial](README.md) and its separate-processes sections run Python Chrysalis nodes on one host. This walkthrough puts them in Firecracker microVMs. First it runs the Python sealed-postcard roles with Alice and Bob each in their own microVM and Relay on the host, as the [Rust walkthrough](../crates/chrysalis/examples/sealed-postcard/firecracker.md) does with the Rust roles. Then it runs a program of your own in a microVM, using `hello_world.py` as the example.

## What runs where

```text
  VM A                         host                                     VM B
  Alice                        Relay (namespace root)                   Bob
  dials native:2:5001  ---->   listens on unix:<run>/a/v.sock_5001
                               listens on unix:<run>/b/v.sock_5001  <----  dials native:2:5001
```

Alice, Relay and Bob are the modes of [`examples/sealed_postcard.py`](examples/sealed_postcard.py), the same program the separate-processes section runs. The VMs have no network interface: their only link is vsock to the host. When Alice or Bob dials the host (CID 2) on port 5001, that VM's Firecracker connects them to `v.sock_5001` beside the VM's `v.sock`, where Relay listens.

A Python program on the host, [`examples/sealed_postcard_microvms.py`](examples/sealed_postcard_microvms.py), starts the three roles. It boots each VM with a small init agent that receives one role's command, runs it, forwards signals to it and reports its exit.

## How a Python program gets into a VM

A guest image comes from one Buck macro, `python_guest_rootfs` in [`../firecracker/guest_rootfs.bzl`](../firecracker/guest_rootfs.bzl). This is the sealed-postcard image, from [`examples/BUCK`](examples/BUCK):

```python
python_guest_rootfs(
    name = "sealed_postcard_python_guest_rootfs",
    program = ":sealed_postcard",
)
```

The image is read-only. It holds CSE's Firecracker root filesystem, the init agent, platform010's runtime and CPython, and the program at `/usr/local/bin/guest-program`. The program must be a `python_binary` with `package_style = "outplace"`, which copies its dependencies beside it so they can go into the image; the default dev-mode packaging links into `buck-out`, which a VM can't see. The host runs the same build for the `identities` mode and Relay.

## Prerequisites

- A Linux devserver with this checkout at `~/fbsource` and read and write access to `/dev/kvm`. No root access or host network setup is needed.
- The host Python `/usr/local/fbcode/platform010/bin/python3.12`, to run a program of your own. Use that path, not `python3`, which may be an older system Python on a devserver.
- tmux.

## Build the inputs

Buck builds the guest image, the program, the demo program, the Firecracker binary and the guest kernel. The first build of the kernel and image takes several minutes.

```sh
cd ~/fbsource
buck2 build @fbcode//mode/dev-nosan \
  fbcode//monarch/chrysalis/python/examples:sealed_postcard_python_guest_rootfs \
  fbcode//monarch/chrysalis/python/examples:sealed_postcard \
  fbcode//monarch/chrysalis/python/examples:sealed_postcard_microvms \
  fbcode//cse/firecracker:fcexe \
  fbcode//cse/firecracker:firecracker.kernel.vmlinux \
  --show-full-output > /tmp/sealed-postcard-py-outputs

output() { awk -v target="$1" '$1 == target { print $2 }' /tmp/sealed-postcard-py-outputs; }
export CHR_FC_PYTHON_ROOTFS=$(output fbcode//monarch/chrysalis/python/examples:sealed_postcard_python_guest_rootfs)
export SEALED_POSTCARD_PYTHON=$(output fbcode//monarch/chrysalis/python/examples:sealed_postcard)
export SEALED_POSTCARD_MICROVMS=$(output fbcode//monarch/chrysalis/python/examples:sealed_postcard_microvms)
export CHR_FC_FIRECRACKER=$(output fbcode//cse/firecracker:fcexe)/firecracker
export CHR_FC_KERNEL=$(output fbcode//cse/firecracker:firecracker.kernel.vmlinux)
```

`--show-full-output` prints each target with the absolute path of its output. The `fcexe` output is a directory, and the Firecracker binary is the `firecracker` file inside it.

## Start the run

Create a fresh, short run directory. It holds the VMs' sockets, so its paths must fit the operating system's limit for a Unix socket path. Then create a tmux session with four panes: one each following Alice's VM console, Relay's log and Bob's VM console, and a control pane that runs the demo with `--language python`.

```sh
sealed_postcard_dir=$(mktemp -d /tmp/chr-fc-XXXXXX)
session="sealed-postcard-py-${sealed_postcard_dir##*-}"

tmux new-session -d -s "$session" -n microvms \
  -e "CHR_FC_PYTHON_ROOTFS=$CHR_FC_PYTHON_ROOTFS" \
  -e "SEALED_POSTCARD_PYTHON=$SEALED_POSTCARD_PYTHON" \
  -e "CHR_FC_FIRECRACKER=$CHR_FC_FIRECRACKER" \
  -e "CHR_FC_KERNEL=$CHR_FC_KERNEL" \
  -e "sealed_postcard_dir=$sealed_postcard_dir" \
  "tail -n +1 -F $sealed_postcard_dir/a/console.log"
tmux set-option -t "$session" remain-on-exit on
tmux set-option -t "$session" pane-border-status top
tmux split-window -t "$session:microvms" "tail -n +1 -F $sealed_postcard_dir/relay.log"
tmux split-window -t "$session:microvms" "tail -n +1 -F $sealed_postcard_dir/b/console.log"
tmux split-window -t "$session:microvms" \
  "$SEALED_POSTCARD_MICROVMS --language python --run-dir $sealed_postcard_dir"
tmux select-layout -t "$session:microvms" tiled
tmux select-pane -t "$session:microvms.0" -T 'Alice: VM A console'
tmux select-pane -t "$session:microvms.1" -T 'Relay: host'
tmux select-pane -t "$session:microvms.2" -T 'Bob: VM B console'
tmux select-pane -t "$session:microvms.3" -T 'control'
tmux attach-session -t "$session"
```

The `-e` options give the session exactly the paths built above, so a tmux server started earlier can't supply stale ones. `tail -F` waits for a log that doesn't exist yet, and `remain-on-exit` keeps the control pane's output on screen after the demo exits.

## What happens

The control pane reports each stage as the demo:

1. runs `sealed_postcard identities --out "$sealed_postcard_dir/ids"`, which writes one private identity bundle per role and prints their PIDs;
2. starts Relay, the namespace root, on the host:

   ```sh
   sealed_postcard relay \
     --listen "unix:$sealed_postcard_dir/a/v.sock_5001" \
     --listen "unix:$sealed_postcard_dir/b/v.sock_5001"
   ```

   Relay's pane shows the address of each listener:

   ```text
   relay address unix:/tmp/chr-fc-ab12cd/a/v.sock_5001
   relay address unix:/tmp/chr-fc-ab12cd/b/v.sock_5001
   ```

3. boots VM A, whose agent runs Alice:

   ```sh
   sealed_postcard alice --parent-pid <relay's PID> --parent native:2:5001 \
     --bob-pid <bob's PID>
   ```

   CID 2 is the host, so Alice dials Relay;
4. boots VM B, whose agent runs Bob:

   ```sh
   sealed_postcard bob --parent-pid <relay's PID> --parent native:2:5001 \
     --alice-pid <alice's PID>
   ```

5. waits for Alice to finish the exchange and exit successfully. Alice prints nothing of her own; her result is her exit status, which the control pane reports. VM A then shuts down;
6. sends Bob SIGTERM through his VM's agent. Bob prints his report in his pane and exits, and VM B shuts down:

   ```text
   bob served 33 streams, all from alice: true
   ```

7. stops Relay with SIGTERM.

The roles start without waiting for each other: Alice and Bob keep retrying their parent link to Relay, and Alice waits up to 120 seconds for Bob. Each role receives only its own bundle. Relay forwards their datagrams but can't read the postcard, because the connection between Alice and Bob is encrypted end to end. The Python Relay keeps no record of what it forwards; the Rust walkthrough's Relay records its traffic and checks it for the postcard's plaintext. The demo succeeds only if Alice, Bob and Relay exit successfully and Bob's report appears. The control pane ends with:

```text
sealed-postcard microVMs: succeeded; logs and VM specs are kept in /tmp/chr-fc-ab12cd
```

At that point every process the demo started has exited. The run directory stays so you can read the logs; that doesn't mean a VM is still running.

## Interrupt a run

Press Ctrl-C in the control pane to stop a run early. The demo releases every process it started, within bounded waits, and exits unsuccessfully, keeping the logs. Closing the control pane or sending the demo SIGTERM does the same. Further interrupts are ignored, so they can't cut that cleanup short. If a process couldn't be confirmed stopped, the demo says so and keeps the run directory.

`kill -9` of the demo is not a clean shutdown: it can leave Relay or a VM running. To check for anything still running from this run, after closing the session, because the `tail` panes also name the run directory:

```sh
pgrep -af "$sealed_postcard_dir"
```

## Clean up

After the control pane's final line, close this run's session. Then delete the run directory, which also removes the identity bundles' private keys, but only if nothing from the run is still running:

```sh
tmux kill-session -t "$session"
pgrep -af "$sealed_postcard_dir"; pgrep_status=$?
if [ "$pgrep_status" -eq 1 ]; then
  rm -rf "$sealed_postcard_dir"
else
  echo "kept $sealed_postcard_dir: stop the processes listed above first"
fi
```

`pgrep` exits with status 1 only when it finds no process.

## Run your own program in a VM

Any Python program built with Buck can run in a microVM the same way. The example is `hello_world.py` from the hello-world tutorial, unchanged: its three nodes talk over loopback UDP, which inside a VM is the VM's own loopback.

The program is a `python_binary` with `package_style = "outplace"`, from [`examples/BUCK`](examples/BUCK):

```python
python_binary(
    name = "hello_world",
    srcs = ["hello_world.py"],
    main_function = "monarch.chrysalis.python.examples.hello_world.main",
    package_style = "outplace",
    deps = [
        "//monarch/chrysalis/python/chrysalis/_rust_bindings:_rust_bindings",
    ],
)
```

Its image is one macro call in `examples/BUCK`. In a BUCK file of your own, it looks like this, with the macro and the program named by their full labels:

```python
load("//monarch/chrysalis/firecracker:guest_rootfs.bzl", "python_guest_rootfs")

python_guest_rootfs(
    name = "hello_world_guest_rootfs",
    program = "//monarch/chrysalis/python/examples:hello_world",
)
```

Build the image, Firecracker and the kernel, then run the program with [`../firecracker/python_guest.py`](../firecracker/python_guest.py). It boots one VM from the image, runs the program with the arguments you give, starting with its name, prints the guest console and exits with the program's exit status:

```sh
cd ~/fbsource
buck2 build @fbcode//mode/dev-nosan \
  fbcode//monarch/chrysalis/python/examples:hello_world_guest_rootfs \
  fbcode//cse/firecracker:fcexe \
  fbcode//cse/firecracker:firecracker.kernel.vmlinux \
  --show-full-output > /tmp/hello-world-outputs

output() { awk -v target="$1" '$1 == target { print $2 }' /tmp/hello-world-outputs; }
rootfs=$(output fbcode//monarch/chrysalis/python/examples:hello_world_guest_rootfs)
export CHR_FC_FIRECRACKER=$(output fbcode//cse/firecracker:fcexe)/firecracker
export CHR_FC_KERNEL=$(output fbcode//cse/firecracker:firecracker.kernel.vmlinux)

/usr/local/fbcode/platform010/bin/python3.12 \
  fbcode/monarch/chrysalis/firecracker/python_guest.py "$rootfs" hello_world
```

Firecracker and the guest kernel print a few lines of their own, some of which read like errors (`ACPI BIOS Error`, `PCI: Fatal`, i8042); they are normal for a Firecracker guest. Among them is hello world's output. The PIDs and ports change each time:

```text
alice 46591982e72d44e2355c899587bd39b6 udp://127.0.0.1:41673
relay c14d6fa5d91dee1d8159288f96cc5ba9 udp://127.0.0.1:34560
bob   c5acbf12247707f6935961364ad5ff33 udp://127.0.0.1:50920
bob received b'wish you were here' from alice
alice received b"we're just two lost souls swimming in a fish bowl, year after year" from bob
```

The guest gives a program a read-only root filesystem, loopback and no network interface, an empty environment and `/` as its working directory; it gets only the arguments on the command line. So hello world's UDP never leaves the VM. Ctrl-C, SIGTERM or SIGHUP stops the run and releases the VM.

`python_guest.py` runs one program in one VM. Programs in several VMs that talk to each other need a Relay on the host, as the sealed-postcard run above has; `examples/sealed_postcard_microvms.py` is the worked example of the host side.

## The automated tests

Two tests run what this walkthrough shows:

```sh
buck test @fbcode//mode/dev-nosan \
  fbcode//monarch/chrysalis/python/tests:test_sealed_postcard_python_microvms
buck test @fbcode//mode/dev-nosan \
  fbcode//monarch/chrysalis/python/tests:test_hello_world_microvm
```

The first runs the sealed-postcard exchange across two microVMs; the second runs hello world in a microVM through `python_guest.py`'s command line. On a host with usable KVM each reports one passing test. Without KVM each skips its test.
