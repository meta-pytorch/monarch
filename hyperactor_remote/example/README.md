# Run the late-join example on one host

This procedure runs a trainer and a client as separate processes on the same machine, in two tmux panes. Start the trainer first, let it make progress, then start a client. The client joins, reads the training state, changes the learning rate, reads the state again, and exits. The trainer keeps running, and another client can join later.

The training loop is an ordinary Tokio task. The trainer also embeds a Monarch actor runtime containing `ProbeHost`, `Probe`, and a rendezvous actor. The token file tells the client where to send its join request. The rendezvous actor gives the client a reference to `Probe`; the client then sends its state queries and learning-rate change directly to `Probe`.

## Automated test

The shell test runs the trainer and two clients directly, without tmux. It checks both learning-rate changes and that a client cannot join through the old token after the trainer exits. Client waits and trainer cleanup are bounded, and a failing run prints its captured logs.

```sh
cd ~/fbsource
buck2 test @fbcode//mode/dev-nosan \
  fbcode//monarch/hyperactor_remote:hyperactor-remote-example-late-join-test
```

For the interactive version, follow the tmux procedure below.

## 1. Build the program

Use a host with an fbsource checkout, Buck2, and tmux installed. Start in a terminal outside tmux:

```sh
cd ~/fbsource
tmux -V
buck2 build @fbcode//mode/dev-nosan \
  fbcode//monarch/hyperactor_remote:hyperactor-remote-example-late-join
```

Although both processes run on one host, the program uses `TcpMode::Hostname`, not loopback. The machine's hostname must resolve to a non-loopback, non-link-local address that the machine can bind and reach. Check its resolution with:

```sh
hostname
getent ahosts "$(hostname)"
```

If the program reports that the hostname has no routable address, fix the host's name resolution before continuing.

## 2. Create the tmux session

Run the following in the same terminal. Use an unused session name; these commands create `monarch-late-join` and do not replace an existing session.

The temporary directory gives this run a fresh token path. Both pane shells receive that path as `LATE_JOIN_TOKEN`. Capturing pane IDs avoids depending on whether your tmux configuration numbers panes from zero or one.

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

The trainer pane is on the left, and the client pane is on the right. With tmux's default key bindings, press `Ctrl-b`, release both keys, then press `o` to switch panes. Use your configured prefix instead if you changed it.

## 3. Start the trainer in the left pane

```sh
buck2 run @fbcode//mode/dev-nosan \
  fbcode//monarch/hyperactor_remote:hyperactor-remote-example-late-join -- \
  trainer --token-file "$LATE_JOIN_TOKEN"
```

Leave this command running. Wait for `trainer: token written to ...`, then at least one `trainer: step ...` progress line. Progress is printed every 25 steps, approximately every five seconds. This establishes that training was already running before the client joined.

The trainer starts with learning rate `0.01`. The token file contains the rendezvous actor's reference; it is not a checkpoint of the training state.

## 4. Join from the right pane

Switch to the client pane. Check that the trainer has written the token, then run the first client:

```sh
test -s "$LATE_JOIN_TOKEN" && \
buck2 run @fbcode//mode/dev-nosan \
  fbcode//monarch/hyperactor_remote:hyperactor-remote-example-late-join -- \
  client --token-file "$LATE_JOIN_TOKEN" --learning-rate 0.05
echo "client exit status: $?"
```

Check that:

- The client prints `client: connected; probe is ...`.
- Its first state report shows learning rate `0.01`.
- It prints `client: learning rate changed from 0.01 to 0.05`.
- Its second state report shows learning rate `0.05`.
- It exits with status `0`, returning to the shell prompt.
- The trainer pane logs `trainer: client ... connected` and continues printing progress.

Step counts and loss values depend on when you run the client; they are not fixed expected values. The join notification and client output can appear in either order across the panes: the rendezvous actor does not wait for `ProbeHost` to finish logging before replying to the client.

## 5. Join again without restarting the trainer

In the same client pane, start another client using the same token:

```sh
buck2 run @fbcode//mode/dev-nosan \
  fbcode//monarch/hyperactor_remote:hyperactor-remote-example-late-join -- \
  client --token-file "$LATE_JOIN_TOKEN" --learning-rate 0.02
echo "client exit status: $?"
```

This client should read learning rate `0.05`, change it to `0.02`, then read `0.02` and exit with status `0`. The trainer logs another join and keeps running. This demonstrates that the token accepts multiple clients and that a client's exit does not stop the trainer.

## 6. Stop the trainer and check the old token

Switch to the trainer pane and press `Ctrl-C`. Wait for its shell prompt to return. The training task, probe, and rendezvous actor have now stopped; the token file is still present, but its actor reference no longer points to a running rendezvous actor.

In the client pane, try the old token:

```sh
buck2 run @fbcode//mode/dev-nosan \
  fbcode//monarch/hyperactor_remote:hyperactor-remote-example-late-join -- \
  client --token-file "$LATE_JOIN_TOKEN" --learning-rate 0.03
echo "client exit status: $?"
```

This attempt must not join successfully. Expect a nonzero exit status; an undeliverable-message error or a join timeout may be reported. Do not rely on one exact failure message. Starting another trainer creates new actors and writes a new token; clients must use that new token.

## 7. Close the demo session

Detach with `Ctrl-b`, then `d`. Back in the original terminal, after the trainer and clients have stopped:

```sh
tmux kill-session -t monarch-late-join
rm -- "$LATE_JOIN_TOKEN"
rmdir -- "$late_join_dir"
```

This closes only the demo session and removes its token file and temporary directory. No other tmux sessions are affected.

This example demonstrates late attachment, not authentication. Even in this single-host procedure, its TCP listeners are not restricted to local clients. Use a trusted network; a client holding the token can inspect the trainer and change its learning rate.
