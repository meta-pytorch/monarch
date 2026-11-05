# Connect a Python client to a running trainer

This is the Python version of `hyperactor_remote/example/late_join.rs`, and it runs the same way: a trainer and clients as separate processes on one machine, in two tmux panes. Start the trainer first, let it make progress, then start a client. The client connects, reads the training state, changes the learning rate, reads the state again, and exits. The trainer keeps running, and another client can connect later.

The training loop is an ordinary thread. The trainer spawns a `Probe` actor on its own proc, publishes it with `publish()`, and writes the token string to a file. A client calls `connect(token, Probe)` and gets a reference to that probe; it then calls the probe's endpoints directly. Connecting does not create actors or add them to the probe's mesh.

## Sending a function

The client changes the learning rate by sending a function, not a fixed message:

```python
probe.run.call_one(lambda state: state.set_learning_rate(rate))
```

`Probe.run(fn)` calls `fn` on the trainer's live state, in the trainer's process. Any function a client sends runs there with the trainer's authority. Use trusted tokens on a trusted network; even in this single-host procedure, the TCP listeners are not restricted to local clients.

## Embedding the probe in a real trainer

Two parts of `late_join.py` carry over to a real training script. The trainer's state lives in a process-wide object, because spawn arguments are copied: a probe constructed with the state would get a copy. And the trainer installs a `monarch.actor.unhandled_fault_hook` that logs a probe failure, because the default hook ends the process.

## Automated test

The shell test is the one the Rust example uses. It runs the trainer and two clients directly, without tmux, checks both learning-rate changes, and checks that a client fails with exit status 1 on the old token after the trainer exits.

```sh
cd ~/fbsource
buck2 test @fbcode//mode/dev-nosan fbcode//monarch/examples/late_join:late_join_test
```

## 1. Build the program

Use a host with an fbsource checkout, Buck2, and tmux installed. Start in a terminal outside tmux:

```sh
cd ~/fbsource
buck2 build @fbcode//mode/dev-nosan fbcode//monarch/examples/late_join:late_join
```

Both processes listen on a non-loopback TCP address resolved from the machine's hostname. Check its resolution with `getent ahosts "$(hostname)"`.

## 2. Create the tmux session

```sh
late_join_dir=$(mktemp -d /tmp/monarch-late-join.XXXXXX)
export LATE_JOIN_TOKEN="$late_join_dir/probe.token"

trainer_pane=$(tmux new-session -d -P -F '#{pane_id}' \
  -s monarch-late-join -n demo -c "$PWD" \
  -e "LATE_JOIN_TOKEN=$LATE_JOIN_TOKEN")

client_pane=$(tmux split-window -h -P -F '#{pane_id}' \
  -t "$trainer_pane" -c "$PWD" \
  -e "LATE_JOIN_TOKEN=$LATE_JOIN_TOKEN")

tmux select-pane -t "$trainer_pane" -T trainer
tmux select-pane -t "$client_pane" -T client
tmux set-option -w -t monarch-late-join:demo pane-border-status top
tmux set-option -w -t monarch-late-join:demo pane-border-format '#{pane_title}'
tmux select-pane -t "$trainer_pane"
tmux attach-session -t monarch-late-join
```

The trainer pane is on the left and the client pane on the right. With tmux's default key bindings, press `Ctrl-b`, then `o`, to switch panes.

## 3. Start the trainer in the left pane

```sh
buck2 run @fbcode//mode/dev-nosan fbcode//monarch/examples/late_join:late_join -- \
  trainer --token-file "$LATE_JOIN_TOKEN"
```

Wait for `trainer: token written to ...`, then at least one `trainer: step ...` line, printed every 25 steps (about every five seconds). The trainer starts with learning rate `0.01`.

## 4. Connect from the right pane

```sh
test -s "$LATE_JOIN_TOKEN" && \
buck2 run @fbcode//mode/dev-nosan fbcode//monarch/examples/late_join:late_join -- \
  client --token-file "$LATE_JOIN_TOKEN" --learning-rate 0.05
echo "client exit status: $?"
```

The client prints `client: connected; probe is ...` with the probe mesh's ID, a state report with learning rate `0.01`, `client: learning rate changed from 0.01 to 0.05`, and a state report with `0.05`, then exits with status `0`. The trainer pane logs `trainer: client ... connected` and `trainer: learning rate changed from 0.01 to 0.05`, and keeps printing progress. Step counts and loss values depend on when you run the client.

## 5. Connect again without restarting the trainer

```sh
buck2 run @fbcode//mode/dev-nosan fbcode//monarch/examples/late_join:late_join -- \
  client --token-file "$LATE_JOIN_TOKEN" --learning-rate 0.02
echo "client exit status: $?"
```

This client reads `0.05`, changes it to `0.02`, reads `0.02`, and exits with status `0`. The token accepts any number of clients, and a client's exit does not stop the trainer.

## 6. Stop the trainer and check the old token

Press `Ctrl-C` in the trainer pane and wait for its prompt. Then, in the client pane:

```sh
buck2 run @fbcode//mode/dev-nosan fbcode//monarch/examples/late_join:late_join -- \
  client --token-file "$LATE_JOIN_TOKEN" --learning-rate 0.03
echo "client exit status: $?"
```

The client times out waiting for an answer and exits with status `1`. A new trainer writes a new token; clients must use that one.

## 7. Close the demo session

Detach with `Ctrl-b`, then `d`. Back in the original terminal:

```sh
tmux kill-session -t monarch-late-join
rm -- "$LATE_JOIN_TOKEN"
rmdir -- "$late_join_dir"
```
