# Local Transport

> transport: **local** — implements `Tx/Rx` directly (in-process `tokio::sync::mpsc`). no network framing or acks.

**address syntax:**
`local:<id>` — serving with `local:0` chooses any available id.

**dial / serve:**
- `ChannelAddr::Local(id)`
- `serve_local::<M>() -> (ChannelAddr, ChannelRx<M>)`
- or `serve::<M>("local:0".parse()?).await`

**notes:**
- `Tx::send` completes after local enqueue (oneshot dropped).
