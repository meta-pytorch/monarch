# TCP Transport

> transport: **tcp** — length-prefixed frames over `tokio::net::TcpStream`; uses `seq`/`ack` for exactly-once delivery into the server’s queue; reconnects with exponential backoff.

**address syntax:**
`tcp:HOST:PORT` (supports IPv4/IPv6)

**framing:**
- 8-byte big-endian length prefix
- followed by exactly that many payload bytes

**dial / serve:**
- `tcp::dial::<M>(addr: SocketAddr) -> NetTx<M>`
- `tcp::serve::<M>(addr: SocketAddr).await -> (ChannelAddr, NetRx<M>)`

**notes:**
- Disables Nagle’s algorithm (`set_nodelay(true)`) to reduce latency for small messages.
- Shares common framing/ack semantics with Unix and MetaTLS.
