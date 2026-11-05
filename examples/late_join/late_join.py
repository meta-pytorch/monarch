# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Connect a new client to a trainer that is already running.

This program has two roles, run in separate processes:

- `trainer` runs a simulated training loop in an ordinary thread. Monarch
  does not start or supervise that loop. Each step updates a step count and
  loss using the current learning rate.
- `client` connects later, reads the trainer's state, changes its learning
  rate by sending a function that runs inside the trainer, reads the state
  again, and exits. The trainer keeps running.

The trainer spawns a `Probe` actor on its own proc and publishes it with
`publish()`, writing the token string to a file. A client reads the file and
calls `connect(token, Probe)` to get a reference to that same probe. Clients
do not own the trainer or become its controller, and connecting does not add
actors to the probe's mesh.

`Probe.run(fn)` calls `fn` on the trainer's live state, in the trainer's
process, with the trainer's authority. Use trusted tokens on a trusted
network.
"""

import argparse
import sys
import threading
import time
from typing import Callable, cast, TypeVar

import monarch.actor
from monarch._src.actor.actor_mesh import ActorMesh
from monarch._src.actor.token import connect, publish, PublishedToken
from monarch.actor import Actor, enable_transport, endpoint, MeshFailure, this_proc

TResult = TypeVar("TResult")

# Delay between simulated training steps, in seconds.
STEP_INTERVAL = 0.2
# Maximum time the client waits for the connection reply or a probe reply.
REPLY_TIMEOUT = 10.0


def say(line: str) -> None:
    """Print one line at once, even when stdout is a file."""
    print(line, flush=True)


class TrainerState:
    """State of the simulated training loop, shared with the probe."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        # Number of simulated training steps completed.
        self.step = 0
        # Simulated loss, multiplied by `1.0 - learning_rate` at each step.
        self.loss = 1.0
        # Rate used by the training loop; clients can change it through `Probe`.
        self.learning_rate = 0.01

    def set_learning_rate(self, learning_rate: float) -> float:
        """Use `learning_rate` for later steps and return the previous rate."""
        with self.lock:
            previous, self.learning_rate = self.learning_rate, learning_rate
        say(f"trainer: learning rate changed from {previous} to {learning_rate}")
        return previous


# Spawn arguments are copied, so the probe reaches the live state through this
# process-wide object rather than through an argument.
STATE = TrainerState()


class Probe(Actor):
    """The actor clients contact to inspect and adjust the running trainer."""

    @endpoint
    def snapshot(self) -> tuple[int, float, float]:
        """Return the step, loss and learning rate at one instant."""
        with STATE.lock:
            return STATE.step, STATE.loss, STATE.learning_rate

    @endpoint
    def run(self, fn: Callable[[TrainerState], TResult]) -> TResult:
        """Call `fn` on the live state in the trainer's process."""
        return fn(STATE)


def train_forever() -> None:
    while True:
        # Sleep without holding the lock, so the probe can serve clients
        # between steps. Each step uses the latest learning rate.
        time.sleep(STEP_INTERVAL)
        with STATE.lock:
            STATE.step += 1
            STATE.loss *= 1.0 - STATE.learning_rate
            step, loss, rate = STATE.step, STATE.loss, STATE.learning_rate
        if step % 25 == 0:
            say(f"trainer: step {step} loss {loss:.4f} learning rate {rate}")


def report_peers(published: PublishedToken) -> None:
    while True:
        say(f"trainer: client {published.next_peer().get()} connected")


def log_fault(failure: MeshFailure) -> None:
    """Log a probe failure instead of ending the trainer process."""
    say(f"trainer: probe event: {failure}")


def run_trainer(token_file: str) -> None:
    monarch.actor.unhandled_fault_hook = log_fault
    threading.Thread(target=train_forever, daemon=True).start()

    # `this_proc()` is this process, so the probe shares `STATE` with the loop.
    probe = this_proc().spawn("probe", Probe)
    probe.initialized.get()
    published = publish(probe)
    with open(token_file, "w") as f:
        f.write(published.token + "\n")
    say(f"trainer: token written to {token_file}")

    threading.Thread(target=report_peers, args=(published,), daemon=True).start()
    threading.Event().wait()


def run_client(token_file: str, learning_rate: float) -> None:
    with open(token_file) as f:
        token = f.read().strip()
    probe = connect(token, Probe, timeout=REPLY_TIMEOUT)
    # The reference names the probe's mesh locally; this sends no message.
    say(f"client: connected; probe is {cast(ActorMesh, probe)._name.get()}")

    step, loss, rate = probe.snapshot.call_one().get(timeout=REPLY_TIMEOUT)
    say(f"client: step {step} loss {loss:.4f} learning rate {rate}")

    # The function runs in the trainer, on its live state.
    previous = probe.run.call_one(
        lambda state: state.set_learning_rate(learning_rate)
    ).get(timeout=REPLY_TIMEOUT)
    say(f"client: learning rate changed from {previous} to {learning_rate}")

    # Training continued during these calls, so the step and loss may differ.
    step, loss, rate = probe.snapshot.call_one().get(timeout=REPLY_TIMEOUT)
    say(f"client: step {step} loss {loss:.4f} learning rate {rate}")


def main() -> None:
    parser = argparse.ArgumentParser(prog="late_join")
    parser.add_argument("role", choices=["trainer", "client"])
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    args = parser.parse_args()

    # Plaintext TCP on a non-loopback address resolved from the hostname, so
    # clients on this machine or another host can reach the trainer. Between
    # regions or datacenters, use an encrypted transport such as
    # enable_transport("metatls-hostname") instead.
    enable_transport("tcp")
    try:
        if args.role == "trainer":
            run_trainer(args.token_file)
        else:
            run_client(args.token_file, args.learning_rate)
    except Exception as e:
        print(f"{args.role}: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
