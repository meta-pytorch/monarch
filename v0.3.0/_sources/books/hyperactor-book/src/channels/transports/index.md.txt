# Transports

Channels abstract over multiple underlying transports. Each transport implements the same `Tx<M>` / `Rx<M>` traits, but differs in how messages are carried.

All transports share the same **framing and acknowledgement semantics** (length-prefixed frames, `seq`/`ack` for exactly-once delivery), except the **Local** transport which uses a plain in-process `mpsc` and does not involve network acks.

tcp, unix, and metatls are built on the shared **net** stack (`NetTx` / `NetRx`) which handles outbox management, seq/ack, retransmission, and reconnection with backoff. local and sim implement the traits directly.

Available transports:

- [Local](local.md) — in-process only
- [TCP](tcp.md) — length-prefixed over sockets
- [Unix](unix.md) — Unix domain sockets
- [MetaTLS](metatls.md) — TCP wrapped in TLS
- [Sim](sim.md) — simulation for testing

_see also_: [tx/rx api](../channels/tx_rx.md), [frames](../channels/frames.md)
