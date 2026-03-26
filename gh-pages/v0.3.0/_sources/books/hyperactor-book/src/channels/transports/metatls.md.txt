# MetaTLS Transport

> transport: **metatls** — TCP wrapped in TLS (`tokio-rustls`), with the same framing/ack semantics as TCP.

**address syntax:**
`metatls:HOST:PORT`

**dial / serve:**
- `ChannelAddr::MetaTls(host, port)`
- `metatls::dial::<M>(host, port) -> NetTx<M>`
- `metatls::serve::<M>(host, port).await -> (ChannelAddr, NetRx<M>)`

**notes:**
- Uses Meta cert/key plumbing (`/var/facebook/x509_identities/server.pem`, env vars like `THRIFT_TLS_*`).
- See code for details on TLS config and environment integration.
