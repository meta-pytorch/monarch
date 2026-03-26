# Sim Transport

> transport: **sim** — simulation transport for testing; mirrors channel semantics without real sockets.

**address syntax:**
`sim:<inner-addr>` (wraps a concrete inner transport address)

**dial / serve:**
- `ChannelAddr::Sim(inner)`
- `sim::dial::<M>(inner: ChannelAddr) -> NetTx<M>`
- `sim::serve::<M>(inner: ChannelAddr).await -> (ChannelAddr, NetRx<M>)`

**notes:**
- Used only in tests and simulations.
- Wraps an underlying concrete transport (e.g., `local`, `tcp`) to mimic end-to-end semantics without real network I/O.
