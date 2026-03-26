# Unix Transport

> transport: **unix** — Unix domain sockets; same framing/ack semantics as TCP.

**address syntax:**
- `unix:/path/to/socket` (filesystem path)
- `unix:@name` (Linux abstract namespace)

**dial / serve:**
- `unix::dial::<M>(addr: SocketAddr) -> NetTx<M>`
- `unix::serve::<M>(addr: SocketAddr).await -> (ChannelAddr, NetRx<M>)`

**notes:**
- Abstract sockets (`@name`) are Linux-only; on other platforms they are mapped to filesystem paths.
- Shares common framing/ack semantics with TCP and MetaTLS.
