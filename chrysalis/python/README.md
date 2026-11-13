# Chrysalis in Python

These bindings are experimental, private, and subject to change. They are not part of Chrysalis's public API.

This tutorial runs three Chrysalis nodes in one Python process. Relay is the root of a process namespace, and Alice and Bob join Relay. Alice knows Bob's certificate-derived process ID, finds him through the namespace, opens an authenticated stream through Relay, sends a postcard, and receives Bob's reply.

The nodes share a Python process, but they do not exchange packets through Python objects or in-memory queues. Each node binds its own UDP socket on `127.0.0.1`, and Chrysalis sends real QUIC packets between those sockets.

The runnable source is [`examples/hello_world.py`](examples/hello_world.py).

## Run it

From `~/fbsource`:

```sh
buck2 run @fbcode//mode/dev-nosan fbcode//monarch/chrysalis/python/examples:hello_world
```

One run prints output like this. The PIDs and UDP ports change each time:

```text
alice 46591982e72d44e2355c899587bd39b6 udp://127.0.0.1:41673
relay c14d6fa5d91dee1d8159288f96cc5ba9 udp://127.0.0.1:34560
bob   c5acbf12247707f6935961364ad5ff33 udp://127.0.0.1:50920
bob received b'wish you were here' from alice
alice received b"we're just two lost souls swimming in a fish bowl, year after year" from bob
```

## What happens

### Create three identities

The program creates three PEM identity bundles signed by one new test certificate authority, then loads them:

```python
alice, relay, bob = (
    Identity.from_pem(bundle) for bundle in generate_test_identities(3)
)
```

Each identity contains a certificate and private key. The certificate determines the node's `Pid`, a complete 32-digit process identifier. Because one test certificate authority signs all three certificates, the nodes trust one another for this run.

`generate_test_identities` is for examples and tests. It does not load production credentials.

### Build the namespace tree

The three nodes form this parent-child tree:

```text
Relay
├── Alice
└── Bob
```

The program constructs that tree directly:

```python
relay_node = Node(relay)
alice_node = Node(alice, parent=(relay.pid, relay_node.addresses[0]))
bob_node = Node(bob, parent=(relay.pid, relay_node.addresses[0]))
```

`Node(relay)` has no parent, so Relay becomes the namespace root. With no carrier argument, each node binds a new `UdpCarrier` on `127.0.0.1` and asks the operating system to choose an available port.

The `parent` tuple contains two different facts: the PID the parent must authenticate as, and the UDP address where the child contacts it. `Node.addresses` reports the actual address after the operating system chooses the port. Alice and Bob each receive Relay's PID and address.

Joining is asynchronous. Bob's namespace entry takes a moment to reach Relay, and Alice finds Bob by asking Relay, so Alice retries `dial` until Bob is visible or the deadline passes:

```python
async def dial_when_visible(node: Node, pid: Pid, within: float = 10.0) -> Stream:
    """Open a stream to `pid` once `node` can see it.

    A new node's PID takes a moment to reach the rest of the namespace, and
    `dial` fails until it does.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while True:
        try:
            return await node.dial(pid)
        except ChrysalisError:
            if loop.time() > deadline:
                raise
            await asyncio.sleep(0.05)
```

Alice dials Bob by PID, not by Bob's UDP port. Alice resolves Bob through her parent, Relay, and the next hop toward Bob is Relay. Relay forwards Bob-addressed packets rather than terminating Alice's application connection.

### Exchange bytes on one stream

Bob waits for an incoming stream:

```python
source, stream = await bob_node.accept()
postcard = await read_to_end(stream)
sender = "alice" if source == alice.pid else str(source)
print(f"bob received {postcard!r} from {sender}")
await stream.write(REPLY)
await stream.finish()
```

`accept` returns both the new bidirectional `Stream` and the PID that the peer authenticated as during mutual TLS. Bob therefore compares a certificate-backed PID, not a sender name supplied in the postcard.

Alice opens the other end of that stream and sends the postcard:

```python
stream = await dial_when_visible(alice_node, bob.pid)
await stream.write(POSTCARD)
await stream.finish()
print(f"alice received {await read_to_end(stream)!r} from bob")
```

`write` waits until Chrysalis accepts all the bytes. `finish` says that this side will send no more bytes; it does not close the stream's receive side. Each side calls `read` until it returns `b""`, which means the peer finished sending. A connection failure before that point raises `ChrysalisError` instead of looking like a normal end of stream.

Relay sees and forwards encrypted QUIC packets addressed to Bob. It does not receive the application `Stream` and does not parse the postcard.

### Close every node

The program always shuts down all three nodes, whether the exchange succeeds or raises:

```python
finally:
    await asyncio.gather(*(node.close() for node in nodes))
```

`close` starts node shutdown and waits for its background work to finish.

## Run the roles as separate processes

[`examples/sealed_postcard.py`](examples/sealed_postcard.py) runs Alice, Relay, and Bob as three processes in the same tree. Alice sends Bob a postcard through Relay and checks his acknowledgement, then opens 32 numbered streams that Bob echoes. Bob checks that every stream came from Alice. Its modes and arguments are those of the [Rust `sealed-postcard` example's role modes](../crates/chrysalis/examples/sealed-postcard/README.md#run-the-roles-as-separate-processes).

Each role uses a `VsockCarrier`, which frames Chrysalis datagrams onto byte streams. Relay listens on the addresses it is given, and Alice and Bob only connect to their parent:

```python
node = Node(identity, carrier=VsockCarrier(listen=listen))  # Relay
node = Node(identity, carrier=VsockCarrier(), parent=parent)  # Alice and Bob
```

Here every address is `unix:<path>`, the carrier's Unix-stream mode on one host. Inside a Firecracker microVM, the same carrier takes vsock addresses, `native:<cid>:<port>`.

From `~/fbsource`, build the program, create a short run directory so its socket path fits the operating system's limit, and write one identity bundle per role:

```sh
sealed_postcard=$(buck2 build @fbcode//mode/dev-nosan \
  fbcode//monarch/chrysalis/python/examples:sealed_postcard \
  --show-full-output | awk '{ print $2 }')
sealed_postcard_dir=$(mktemp -d /tmp/sealed-postcard-py.XXXXXX)

"$sealed_postcard" identities --out "$sealed_postcard_dir/ids" > "$sealed_postcard_dir/pids"
alice_pid=$(awk '$1 == "alice" { print $2 }' "$sealed_postcard_dir/pids")
relay_pid=$(awk '$1 == "relay" { print $2 }' "$sealed_postcard_dir/pids")
bob_pid=$(awk '$1 == "bob" { print $2 }' "$sealed_postcard_dir/pids")
```

Each bundle file is readable only by its owner. Start Relay and Bob in the background, then run Alice:

```sh
SEALED_POSTCARD_IDENTITY=$(cat "$sealed_postcard_dir/ids/relay.pem") \
  "$sealed_postcard" relay --listen "unix:$sealed_postcard_dir/r" &
relay_process=$!

SEALED_POSTCARD_IDENTITY=$(cat "$sealed_postcard_dir/ids/bob.pem") \
  "$sealed_postcard" bob --parent-pid "$relay_pid" \
  --parent "unix:$sealed_postcard_dir/r" --alice-pid "$alice_pid" &
bob_process=$!

SEALED_POSTCARD_IDENTITY=$(cat "$sealed_postcard_dir/ids/alice.pem") \
  "$sealed_postcard" alice --parent-pid "$relay_pid" \
  --parent "unix:$sealed_postcard_dir/r" --bob-pid "$bob_pid" && echo "alice: done"
```

Alice exits successfully after both exchanges. Stop Bob, who prints his report and exits successfully only if he served all 33 streams and Alice opened every one:

```sh
kill -TERM "$bob_process"; wait "$bob_process"
```

```text
bob served 33 streams, all from alice: true
```

Then stop Relay and delete the run directory, which removes the private keys:

```sh
kill -TERM "$relay_process"; wait "$relay_process"
rm -rf "$sealed_postcard_dir"
```

## Alice's child, Carol

Alice can serve a child of her own. With `--carol`, she starts Carol, a second node in her process, and Carol joins her over loopback UDP:

```text
        relay
       /     \
   unix:     unix:
     /         \
  alice        bob
    |
   udp
    |
  carol
```

Alice's carrier is then a pair, a `VsockCarrier` and a `UdpCarrier`. The first carrier of a pair faces the node's parent and is the address it advertises; children may join over either:

```python
node = Node(identity, carrier=(VsockCarrier(), UdpCarrier()), parent=parent)  # Alice
child = Node(carol, parent=(identity.pid, address))  # Carol, at Alice's UDP address
```

After Alice's exchanges, Carol sends Bob her own postcard. It travels from Carol to Alice to Relay to Bob. Alice forwards it but can't read it: Carol's connection to Bob is encrypted end to end, as Alice's is through Relay. Bob checks that it came from Carol's PID. Inside a Firecracker microVM, Alice and Carol would share Alice's VM, Alice's `unix:` link to Relay would be vsock, and Carol's UDP would be the VM's own loopback.

Continuing from the commands above, with a new run directory, write the identities with Carol's, then start Relay, Bob with Carol's PID, and Alice with Carol's bundle:

```sh
sealed_postcard_dir=$(mktemp -d /tmp/sealed-postcard-py.XXXXXX)

"$sealed_postcard" identities --out "$sealed_postcard_dir/ids" --carol > "$sealed_postcard_dir/pids"
alice_pid=$(awk '$1 == "alice" { print $2 }' "$sealed_postcard_dir/pids")
relay_pid=$(awk '$1 == "relay" { print $2 }' "$sealed_postcard_dir/pids")
bob_pid=$(awk '$1 == "bob" { print $2 }' "$sealed_postcard_dir/pids")
carol_pid=$(awk '$1 == "carol" { print $2 }' "$sealed_postcard_dir/pids")

SEALED_POSTCARD_IDENTITY=$(cat "$sealed_postcard_dir/ids/relay.pem") \
  "$sealed_postcard" relay --listen "unix:$sealed_postcard_dir/r" &
relay_process=$!

SEALED_POSTCARD_IDENTITY=$(cat "$sealed_postcard_dir/ids/bob.pem") \
  "$sealed_postcard" bob --parent-pid "$relay_pid" \
  --parent "unix:$sealed_postcard_dir/r" --alice-pid "$alice_pid" \
  --carol-pid "$carol_pid" &
bob_process=$!

SEALED_POSTCARD_IDENTITY=$(cat "$sealed_postcard_dir/ids/alice.pem") \
SEALED_POSTCARD_CAROL_IDENTITY=$(cat "$sealed_postcard_dir/ids/carol.pem") \
  "$sealed_postcard" alice --parent-pid "$relay_pid" \
  --parent "unix:$sealed_postcard_dir/r" --bob-pid "$bob_pid" --carol && echo "alice: done"
```

Stop Bob. He reports Alice's streams as before, then Carol's postcard, and exits successfully only if both are right:

```sh
kill -TERM "$bob_process"; wait "$bob_process"
```

```text
bob served 33 streams, all from alice: true
bob got postcards from carol: 1
```

Then stop Relay and delete the run directory:

```sh
kill -TERM "$relay_process"; wait "$relay_process"
rm -rf "$sealed_postcard_dir"
```

## The API used here

- `generate_test_identities(count)` returns PEM identity bundles signed by one new test certificate authority.
- `Identity.from_pem(bundle)` loads one node's certificate, private key, and trust roots. `Identity.pid` is the `Pid` derived from its certificate.
- `Pid(value)` parses a complete 32-digit hexadecimal process ID. PIDs compare and hash by value.
- `UdpCarrier(bind="127.0.0.1:0")` describes a UDP binding. The default address limits it to loopback, and port `0` asks the operating system to choose an available port. `Node` creates this carrier when none is supplied.
- `VsockCarrier(listen=...)` frames datagrams onto byte streams. It listens at each `unix:<path>` or `native:<cid>:<port>` address in `listen`; with none, the node only connects to its parent.
- `Node(identity, carrier=..., parent=...)` starts a namespace root or joins one parent. `carrier` is one carrier, or a pair of one `UdpCarrier` and one `VsockCarrier` in either order, whose first carrier faces the parent. `addresses` reports its bound UDP address or its vsock listen addresses, a pair's first carrier's addresses coming first, `dial(pid)` opens a stream to a visible PID, `accept()` waits for a peer to open one, and `close()` shuts the node down.
- `Stream.write(data)` sends all the bytes, `finish()` closes the send side, and `read(max_bytes)` reads the next available bytes or returns `b""` after the peer finishes.
- `ChrysalisError` reports node and transport failures. Invalid input raises `ValueError`, and a wrong type or out-of-range integer raises `TypeError` or `OverflowError`.

The complete experimental signatures are in [`chrysalis/_rust_bindings/__init__.pyi`](chrysalis/_rust_bindings/__init__.pyi).

## Limitations

- The bindings are private and experimental. Importing `chrysalis._rust_bindings` does not establish a public or stable Python API.
- `generate_test_identities` creates test credentials whose certificates name `localhost`. The current binding uses that fixed TLS server name.
- A node binds one carrier, or one UDP and one vsock carrier as a pair, which joins its parent over its first carrier.
- A UDP node advertises the address it binds. It cannot bind an unspecified address such as `0.0.0.0` while advertising a different reachable address.
