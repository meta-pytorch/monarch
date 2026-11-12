# Run Sealed Postcard as Three Processes

This walkthrough starts after the identity bundles and `pids` file have been
created as described in [`README.md`](README.md).

## Create Three Panes

```sh
tmux new-session -d -s sealed-postcard -n demo \
  -e "sealed_postcard_dir=$sealed_postcard_dir" \
  -e "alice_pid=$alice_pid" \
  -e "relay_pid=$relay_pid" \
  -e "bob_pid=$bob_pid" \
  -c "$HOME/fbsource/fbcode/monarch/chrysalis/crates/chrysalis/examples/sealed-postcard"
tmux split-window -h -t sealed-postcard:demo
tmux split-window -v -t sealed-postcard:demo.0
tmux select-layout -t sealed-postcard:demo tiled
tmux select-pane -t sealed-postcard:demo.0 -T 'Bob'
tmux select-pane -t sealed-postcard:demo.1 -T 'Relay'
tmux select-pane -t sealed-postcard:demo.2 -T 'Alice'
tmux select-pane -t sealed-postcard:demo.0
tmux attach-session -t sealed-postcard
```

The roles can start in any order, because Alice and Bob retry their parent
links until Relay is listening. This walkthrough starts Bob, then Relay, the
namespace root, then Alice, who runs the exchange.

## Start Bob

In the Bob pane, run the command below. See the [Bob example](README.md#bob)
for what each argument means.

```sh
SEALED_POSTCARD_IDENTITY=$(cat "$sealed_postcard_dir/ids/bob.pem") \
  cargo run --example sealed-postcard -- bob \
    --parent-pid "$relay_pid" \
    --parent "unix:$sealed_postcard_dir/r" \
    --alice-pid "$alice_pid"
```

Bob waits for Relay and then serves Alice's streams.

## Start Relay

In the Relay pane, run the command below. See the
[Relay example](README.md#relay) for what each argument means.

```sh
SEALED_POSTCARD_IDENTITY=$(cat "$sealed_postcard_dir/ids/relay.pem") \
  cargo run --example sealed-postcard -- relay \
    --listen "unix:$sealed_postcard_dir/r"
```

Relay listens for Alice and Bob, and Bob's retrying parent link connects.

## Start Alice

In the Alice pane, run the command below. See the
[Alice example](README.md#alice) for what each argument means.

```sh
SEALED_POSTCARD_IDENTITY=$(cat "$sealed_postcard_dir/ids/alice.pem") \
  cargo run --example sealed-postcard -- alice \
    --parent-pid "$relay_pid" \
    --parent "unix:$sealed_postcard_dir/r" \
    --bob-pid "$bob_pid"
```

Alice joins Relay, sends the postcard, checks Bob's acknowledgement, opens 32
numbered streams, checks their echoes, and exits.

## Stop Bob and Relay

Press Ctrl-C in Bob's pane. He shuts down and joins his Chrysalis node, then
prints:

```text
bob served 33 streams, all from alice: true
```

Press Ctrl-C in Relay's pane. It drains its final traffic observations, shuts
down and joins its node, then prints a line like:

```text
relay observed 123 datagrams, postcard plaintext seen: false, log complete: true
```

## How It Works

Each process loads its own certificate and private key from one PEM bundle.
Relay is the namespace root, and Alice and Bob are its children. The processes
use `VsockDatagramSocket` over direct Unix streams. Alice and Bob communicate
through end-to-end encrypted QUIC streams while Relay forwards the carrier
datagrams without terminating their connection.
