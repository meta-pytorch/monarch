# chrysalis-transport-quiche

This crate drives quiche connections over the runtime-neutral packet slots in
`chrysalis-transport-uring`. Application threads interact through the bounded submission and
completion queues in `chrysalis-transport-core`.

Cargo builds use Meta's CMake distribution for quiche's vendored BoringSSL:

```bash
CMAKE="$FBSOURCE/fbcode/third-party-buck/platform010/build/cmake/bin/cmake" \
cargo test --manifest-path fbcode/monarch/chrysalis-transport-quiche/Cargo.toml
```

The adapter currently provides:

- client and server handshakes over one unconnected, multi-peer UDP driver;
- CID-based connection routing and source-CID refresh;
- direct quiche packet generation into paced UDP GSO slots;
- bounded send, receive, FIN, and discard submissions;
- caller-owned receive buffers and acknowledgement-based send completion; and
- connection and incoming-stream lifecycle completions.

Callers provide configured `quiche::Config` values and endpoint certificate material. The crate
derives the local PID from that certificate while leaving certificate issuance to an optional
identity provider.
