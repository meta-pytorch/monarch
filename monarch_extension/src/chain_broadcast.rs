/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Pipelined-chain bulk broadcast over hyperactor's channel transport.
//!
//! A leader streams a large block down an ordered chain of workers --
//! `leader -> worker[0] -> worker[1] -> ... -> worker[N-1]` -- where each
//! interior worker forwards every chunk to its successor the instant it lands
//! (temporal pipelining), so the source egresses ~one copy regardless of N.
//! This is the bandwidth-optimal large-block broadcast (NCCL's ring, MPI's
//! pipelined chain); it replaces a single-leader O(N) fan-out.
//!
//! Unlike a bolt-on transport, this reuses [`hyperactor::channel`]: connections
//! are persistent channel streams (the process default transport -- metatls in
//! cluster, for Meta mutual-TLS identity with no cert paths to read -- or any
//! transport `serve` is given), framing and reconnect live in the channel layer,
//! and addresses are exchanged as plain strings. The only new machinery here is the
//! chunk relay ([`forward`]) and a byte-counter recv-completion signal
//! ([`new_ctx`]/[`ctx_wait_into`]).
//!
//! The Python surface mirrors the C transport it supersedes so the remotemount
//! orchestration is unchanged: `serve` (bind a listener), `connect` (dial the
//! head/successor), `send_block` (prime the pipeline at the source), `forward`
//! (relay + deliver at each worker), and `new_ctx`/`ctx_wait_into`/`ctx_close`.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelRx;
use hyperactor::channel::ChannelTx;
use hyperactor::channel::Rx;
use hyperactor::channel::Tx;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use serde::Deserialize;
use serde::Serialize;
use serde_multipart::Part;
use typeuri::Named;

/// Default number of parallel TLS streams per hop, to fill the NIC (as the C ctcp
/// transport did). Delivery is offset-addressed, so the out-of-order arrival that
/// multi-stream allows is fine. Tradeoff vs the single ordered stream: the
/// unordered path does NOT retransmit on reconnect, so a mid-transfer connection
/// drop can lose a chunk (surfacing as a `ctx_wait_into` timeout, then a re-fault);
/// acceptable on a stable in-cluster fabric. `connect`/`forward` take the count as
/// an argument (this is only the default) so the caller can tune it per run without
/// a rebuild -- more streams fill a fatter cross-DC pipe, fewer reduce per-stream
/// ramp overhead on a short chain.
const NUM_CBC_STREAMS: usize = 32;

/// One frame of a block on the wire: a slice of the block plus enough header to
/// place it (`offset`) and to know when the block is complete (`total`). `tag`
/// is the block id, carried through for debugging / future multiplexing. `data`
/// is a `serde_multipart::Part` so the channel carries the payload out-of-band
/// (zero-copy) rather than bincode-encoding it byte by byte -- and forwarding it
/// (`Chunk::clone`) is a refcount bump, not a memcpy.
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
struct Chunk {
    tag: u64,
    offset: u64,
    total: u64,
    data: Part,
}

/// Recv-completion state, mirroring the byte-counter ctx of the C transport it
/// replaces. The relay stores each landed chunk as an `(offset, Bytes)` fragment
/// (a refcount bump + a push -- NO memcpy on the forwarding hot path); the block
/// is assembled from the fragments once, in `wait_into`, off that path. A running
/// `received` counter lets `wait` know when the whole block has landed. Delivery
/// is synchronous (one block in flight per mount), so the fragments are drained
/// and reused across blocks.
struct Ctx {
    inner: Mutex<CtxInner>,
    cond: Condvar,
}

struct CtxInner {
    fragments: Vec<(usize, Bytes)>,
    /// Offsets already delivered for the current block, so a duplicate chunk (a
    /// reconnect re-send on the unordered multi-stream path) is dropped instead of
    /// double-counting `received` and completing `wait` on a hole. Cleared when a
    /// block is drained or abandoned.
    seen: HashSet<usize>,
    received: u64,
    closed: bool,
}

impl Ctx {
    fn new(_block_size: usize) -> Self {
        Self {
            inner: Mutex::new(CtxInner {
                fragments: Vec::new(),
                seen: HashSet::new(),
                received: 0,
                closed: false,
            }),
            cond: Condvar::new(),
        }
    }

    /// Stash a landed chunk as an `(offset, Bytes)` fragment (zero-copy: a Bytes
    /// refcount bump + a Vec push, no memcpy) and bump the received counter,
    /// waking any parked `wait_into`. The actual assembly happens in `wait_into`,
    /// so the relay can forward the next chunk immediately.
    fn deliver(&self, chunk: &Chunk) {
        let bytes = chunk.data.to_bytes();
        let off = chunk.offset as usize;
        let len = bytes.len();
        let mut inner = self.inner.lock().expect("ctx mutex poisoned");
        if !inner.seen.insert(off) {
            // Duplicate offset (a reconnect re-sent a chunk on the unordered
            // multi-stream path). Counting it again would push `received` past the
            // block size while a real gap stays unfilled, so `wait` would complete
            // on a hole -- drop it; the first copy already landed.
            tracing::warn!(
                offset = off,
                "chain_broadcast: dropping duplicate chunk at already-received offset"
            );
            return;
        }
        inner.fragments.push((off, bytes));
        inner.received += len as u64;
        drop(inner);
        self.cond.notify_all();
    }

    fn mark_closed(&self) {
        self.inner.lock().expect("ctx mutex poisoned").closed = true;
        self.cond.notify_all();
    }

    /// Block until `want` bytes of the current block have landed, then assemble
    /// them directly into `dst` (draining the counter by `want`) and return
    /// `true`. `false` means the predecessor closed before the block completed,
    /// or the timeout elapsed. `dst` must be at least `want` bytes and is
    /// expected pre-zeroed: a gap left by a malformed sender reads as zero
    /// without this re-zeroing, keeping the assemble to one copy per chunk
    /// straight into the caller's buffer, with no intermediate block.
    fn wait_into(&self, dst: &mut [u8], want: u64, timeout_ms: i64) -> bool {
        let mut inner = self.inner.lock().expect("ctx mutex poisoned");
        let deadline =
            (timeout_ms >= 0).then(|| Instant::now() + Duration::from_millis(timeout_ms as u64));
        while inner.received < want && !inner.closed {
            match deadline {
                None => inner = self.cond.wait(inner).expect("ctx cond poisoned"),
                Some(dl) => {
                    let now = Instant::now();
                    if now >= dl {
                        break;
                    }
                    let (guard, _) = self
                        .cond
                        .wait_timeout(inner, dl - now)
                        .expect("ctx cond poisoned");
                    inner = guard;
                }
            }
        }
        if inner.received < want {
            // Timed out, or the predecessor closed mid-block. Drop the partial
            // fragments and reset the counter so the ctx (reused for the mount's life)
            // is clean for the next block; the design does not retransmit, so the
            // caller re-faults to trigger a fresh delivery.
            inner.fragments.clear();
            inner.seen.clear();
            inner.received = 0;
            return false;
        }
        // Assemble the block from its fragments -- one copy per chunk, done ONCE
        // here (off the relay's forwarding path, not per hop) and straight into
        // `dst`. Drain so the ctx is clean for the next block. A fragment that
        // would overrun the buffer (a stray frame) is skipped rather than
        // panicking.
        debug_assert!(
            want as usize <= dst.len(),
            "wait_into: dst ({} bytes) is smaller than want ({want})",
            dst.len()
        );
        let cap = (want as usize).min(dst.len());
        let frags = std::mem::take(&mut inner.fragments);
        inner.seen.clear();
        for (off, b) in frags {
            let end = off.saturating_add(b.len());
            if end <= cap {
                dst[off..end].copy_from_slice(&b);
            } else {
                tracing::warn!(
                    offset = off,
                    len = b.len(),
                    cap = cap,
                    "chain_broadcast: dropping out-of-range fragment that would overrun the block"
                );
            }
        }
        inner.received -= want;
        true
    }
}

#[pyclass(
    name = "ChainCtx",
    module = "monarch._rust_bindings.monarch_extension.chain_broadcast"
)]
struct PyCtx {
    ctx: Arc<Ctx>,
}

/// A bound listener for the predecessor to dial. Holds the receive end until
/// [`forward`] consumes it to run the relay.
#[pyclass(
    name = "ChainServer",
    module = "monarch._rust_bindings.monarch_extension.chain_broadcast"
)]
struct PyChainServer {
    #[pyo3(get)]
    addr: String,
    rx: Option<ChannelRx<Chunk>>,
}

/// A persistent connection to a successor (or the chain head), reused for every
/// block.
#[pyclass(
    name = "ChainSession",
    module = "monarch._rust_bindings.monarch_extension.chain_broadcast"
)]
struct PyChainSession {
    tx: Arc<ChannelTx<Chunk>>,
}

fn parse_addr(addr: &str) -> PyResult<ChannelAddr> {
    addr.parse()
        .map_err(|e| PyRuntimeError::new_err(format!("bad channel addr {addr}: {e}")))
}

/// Create a recv-completion ctx sized to a single block.
#[pyfunction]
fn new_ctx(block_size: usize) -> PyCtx {
    PyCtx {
        ctx: Arc::new(Ctx::new(block_size)),
    }
}

/// Bind a listener for the predecessor to dial. Returns the concrete bound
/// address (with its ephemeral port) as a string to distribute, plus the server
/// handle that [`forward`] consumes.
///
/// `bind_addr` chooses the transport. `None` defers to monarch's process-wide
/// default transport -- the same `default_transport` /
/// `HYPERACTOR_MESH_DEFAULT_TRANSPORT` knob every other serve site uses -- so the
/// chain listener binds whatever the rest of the worker's channels bind (metatls
/// in cluster: mutual-TLS identity, no cert paths to distribute). A caller without
/// Meta TLS infra (OSS, tests) flips that knob to e.g. tcp, or passes an explicit
/// address string here, e.g. `"tcp![::]:0"`; `connect` and `forward` dial whatever
/// transport the resulting address encodes, so no other knob is needed.
#[pyfunction]
#[pyo3(signature = (bind_addr=None))]
fn serve(bind_addr: Option<String>) -> PyResult<PyChainServer> {
    let bind = match bind_addr {
        Some(a) => parse_addr(&a)?,
        None => hyperactor_mesh::transport::default_bind_spec().binding_addr(),
    };
    // channel::serve spawns its listener task, so it must run inside the shared
    // tokio runtime -- and on the SAME runtime as the relay task in `forward`, so
    // the listener and its consumer are never split across runtimes.
    let _guard = monarch_hyperactor::runtime::get_tokio_runtime().enter();
    let (addr, rx) = channel::serve::<Chunk>(bind)
        .map_err(|e| PyRuntimeError::new_err(format!("serve failed: {e}")))?;
    Ok(PyChainServer {
        addr: addr.to_string(),
        rx: Some(rx),
    })
}

/// Dial a successor (or the chain head). The connection is persistent and reused
/// for every block; dialing is lazy, so the peer need not be serving yet.
/// `num_streams` is the number of parallel streams to open (defaults to
/// [`NUM_CBC_STREAMS`]); the caller picks it at runtime to tune the pipe.
#[pyfunction]
#[pyo3(signature = (addr, num_streams = NUM_CBC_STREAMS))]
fn connect(addr: &str, num_streams: usize) -> PyResult<PyChainSession> {
    // unordered::dial opens `num_streams` parallel streams to fill the NIC; it
    // spawns sender tasks, so run it on the shared runtime.
    let _guard = monarch_hyperactor::runtime::get_tokio_runtime().enter();
    let tx = channel::unordered::dial::<Chunk>(parse_addr(addr)?, num_streams.max(1))
        .map_err(|e| PyRuntimeError::new_err(format!("dial {addr} failed: {e}")))?;
    Ok(PyChainSession { tx: Arc::new(tx) })
}

/// Stripe `buf` into `chunk_size`-byte frames and post them to the peer. The chain
/// source calls this to prime the pipeline; interior nodes forward via the relay
/// in [`forward`]. Returns the number of bytes sent.
#[pyfunction]
fn send_block(
    py: Python<'_>,
    session: &PyChainSession,
    data: &Bound<'_, PyBytes>,
    chunk_size: usize,
    tag: u64,
) -> PyResult<usize> {
    // Wrap the Python bytes as a shared Bytes with no payload copy: the wrapper
    // pins the PyBytes alive for the lifetime of every chunk sliced from it, so
    // the source egresses the block without ever copying it. This reuses the
    // buffers module's zero-copy PyBytes view (the same conversion
    // `Buffer::take_part` uses); each chunk is then a zero-copy slice of it
    // wrapped as a Part.
    let owned = monarch_hyperactor::buffers::py_bytes_to_bytes(data.clone().unbind());
    let total = owned.len();
    let chunk_size = chunk_size.max(1);
    let tx = session.tx.clone();
    py.detach(|| {
        let mut offset = 0usize;
        while offset < total {
            let end = (offset + chunk_size).min(total);
            tx.post(Chunk {
                tag,
                offset: offset as u64,
                total: total as u64,
                data: Part::from(owned.slice(offset..end)),
            });
            offset = end;
        }
    });
    Ok(total)
}

/// Run the chain relay for this node: consume chunks from the bound listener
/// (`server`), forward each to the successor the instant it lands (if any), and
/// deliver it into the local recv ctx. Spawns on the shared tokio runtime and
/// returns immediately; the relay runs until the predecessor closes the
/// connection. A node with no successor is the chain tail (recv + deliver only).
/// `num_streams` (defaults to [`NUM_CBC_STREAMS`]) is how many parallel streams
/// this node opens to its successor, tuned at runtime to match the pipe.
#[pyfunction]
#[pyo3(signature = (server, successor, ctx, num_streams = NUM_CBC_STREAMS))]
fn forward(
    server: &mut PyChainServer,
    successor: Option<String>,
    ctx: &PyCtx,
    num_streams: usize,
) -> PyResult<()> {
    let mut rx = server
        .rx
        .take()
        .ok_or_else(|| PyRuntimeError::new_err("forward: server already consumed"))?;
    let ctx = ctx.ctx.clone();
    let runtime = monarch_hyperactor::runtime::get_tokio_runtime();
    // Dial the successor up front (if any) so a malformed address or a failed dial
    // surfaces here as an error, instead of silently collapsing this node into the
    // chain tail and stranding every node downstream. `None` = this node is the tail
    // (recv + deliver only). Dialing is lazy, so the successor need not be serving yet.
    let succ_tx: Option<ChannelTx<Chunk>> = match successor {
        None => None,
        Some(addr) => {
            let parsed = parse_addr(&addr)?;
            let _guard = runtime.enter();
            let tx =
                channel::unordered::dial::<Chunk>(parsed, num_streams.max(1)).map_err(|e| {
                    PyRuntimeError::new_err(format!("forward: dial successor {addr} failed: {e}"))
                })?;
            Some(tx)
        }
    };
    runtime.spawn(async move {
        while let Ok(chunk) = rx.recv().await {
            // Forward first so the successor starts receiving while we write
            // locally -- the temporal pipeline that makes the chain fast.
            if let Some(tx) = &succ_tx {
                tx.post(chunk.clone());
            }
            ctx.deliver(&chunk);
        }
        ctx.mark_closed();
    });
    Ok(())
}

/// Block until `want` bytes of the current block have landed locally, then
/// assemble them straight into the caller's buffer at `dst_addr` (`dst_len`
/// bytes) -- the mount's `block_ptr` staging buffer -- draining the counter by
/// `want`. Writing directly into that buffer avoids materialising an
/// intermediate `bytes` and the extra copy from it into the mount buffer that a
/// return-by-value would cost; `receive_block` then freezes the buffer into the
/// served block with no further copy.
#[pyfunction]
fn ctx_wait_into(
    py: Python<'_>,
    ctx: &PyCtx,
    dst_addr: usize,
    dst_len: usize,
    want: u64,
    timeout_ms: i64,
) -> PyResult<()> {
    let c = ctx.ctx.clone();
    let ok = py.detach(|| {
        // SAFETY: `dst_addr`/`dst_len` are the return of `block_ptr`, i.e. the
        // mount's staging `BytesMut` for this block -- a live, non-null,
        // `AVAILABILITY_BLOCK_SIZE`-byte allocation that is never grown (so the
        // pointer stays valid) and that nothing else reads or writes between
        // `block_ptr` and `receive_block`, because delivery is one block in
        // flight per mount (block_ptr -> ctx_wait_into -> receive_block runs
        // sequentially on one actor task). So this slice is the unique writer for
        // the buffer over its whole lifetime here, is valid for `dst_len` bytes,
        // and `u8` needs no alignment; `wait_into` never writes past `dst.len()`.
        let dst = unsafe { std::slice::from_raw_parts_mut(dst_addr as *mut u8, dst_len) };
        c.wait_into(dst, want, timeout_ms)
    });
    if ok {
        Ok(())
    } else {
        Err(PyRuntimeError::new_err(format!(
            "ctx_wait_into: did not receive {want} bytes (predecessor closed or timed out)"
        )))
    }
}

/// Wake any parked `wait_into` and mark the ctx closed (mount teardown).
#[pyfunction]
fn ctx_close(ctx: &PyCtx) {
    ctx.ctx.mark_closed();
}

macro_rules! add_fn {
    ($module:expr, $f:ident) => {{
        let f = wrap_pyfunction!($f, $module)?;
        f.setattr(
            "__module__",
            "monarch._rust_bindings.monarch_extension.chain_broadcast",
        )?;
        $module.add_function(f)?;
    }};
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyCtx>()?;
    module.add_class::<PyChainServer>()?;
    module.add_class::<PyChainSession>()?;
    add_fn!(module, new_ctx);
    add_fn!(module, serve);
    add_fn!(module, connect);
    add_fn!(module, send_block);
    add_fn!(module, forward);
    add_fn!(module, ctx_wait_into);
    add_fn!(module, ctx_close);
    Ok(())
}

#[cfg(test)]
mod tests {
    use hyperactor::channel::ChannelTransport;
    use hyperactor::channel::TcpMode;

    use super::*;

    /// A two-hop chain over TCP (metatls needs Meta TLS infra not present in a
    /// unit test): source -> relay -> tail. The relay forwards each chunk to the
    /// tail as it lands; both the relay's and the tail's ctx must reconstruct the
    /// exact block, proving the transport + byte-counter completion end to end.
    #[test]
    fn chain_relays_and_delivers_block() {
        let rt = tokio::runtime::Runtime::new().expect("build tokio runtime");
        rt.block_on(async {
            let block: Vec<u8> = (0..(3 * 1024 * 1024u32)).map(|i| (i % 251) as u8).collect();
            let block_size = block.len();
            let any_tcp = || ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Localhost));

            // Tail: serve, relay into its ctx with no successor.
            let (tail_addr, mut tail_rx) = channel::serve::<Chunk>(any_tcp()).expect("tail serve");
            let tail_ctx = Arc::new(Ctx::new(block_size));
            {
                let tail_ctx = tail_ctx.clone();
                tokio::spawn(async move {
                    while let Ok(chunk) = tail_rx.recv().await {
                        tail_ctx.deliver(&chunk);
                    }
                    tail_ctx.mark_closed();
                });
            }

            // Relay: serve, forward each chunk to the tail as it lands.
            let (relay_addr, mut relay_rx) =
                channel::serve::<Chunk>(any_tcp()).expect("relay serve");
            let relay_ctx = Arc::new(Ctx::new(block_size));
            {
                let relay_ctx = relay_ctx.clone();
                let tail_tx = channel::dial::<Chunk>(tail_addr).expect("dial tail");
                tokio::spawn(async move {
                    while let Ok(chunk) = relay_rx.recv().await {
                        tail_tx.post(chunk.clone());
                        relay_ctx.deliver(&chunk);
                    }
                    relay_ctx.mark_closed();
                });
            }

            // Source: stripe the block into 512 KiB chunks to the relay head.
            let head_tx = channel::dial::<Chunk>(relay_addr).expect("dial relay");
            let chunk = 512 * 1024usize;
            let mut offset = 0usize;
            while offset < block_size {
                let end = (offset + chunk).min(block_size);
                head_tx.post(Chunk {
                    tag: 0,
                    offset: offset as u64,
                    total: block_size as u64,
                    data: Part::from(block[offset..end].to_vec()),
                });
                offset = end;
            }

            let mut relay_got = vec![0u8; block_size];
            assert!(
                relay_ctx.wait_into(&mut relay_got, block_size as u64, 30_000),
                "relay received the whole block"
            );
            let mut tail_got = vec![0u8; block_size];
            assert!(
                tail_ctx.wait_into(&mut tail_got, block_size as u64, 30_000),
                "tail received the whole block via the relay"
            );
            assert_eq!(relay_got, block, "relay reconstructs the source block");
            assert_eq!(
                tail_got, block,
                "tail reconstructs the block forwarded through the relay"
            );
        });
    }

    /// Many distinct blocks streamed over ONE persistent connection (the real
    /// remotemount flow: `send_block` reuses one Tx, `ctx_wait` drains one block
    /// per call), each verified byte-identical. This is the multi-block path the
    /// single-block test above does not cover -- the case that failed on MAST.
    #[test]
    fn many_blocks_over_persistent_channel_are_byte_identical() {
        let rt = tokio::runtime::Runtime::new().expect("build tokio runtime");
        rt.block_on(async {
            let block_size = 16 * 1024 * 1024usize;
            let chunk = 4 * 1024 * 1024usize;
            let any_tcp = ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Localhost));

            let (addr, mut rx) = channel::serve::<Chunk>(any_tcp).expect("serve");
            let ctx = Arc::new(Ctx::new(block_size));
            {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    while let Ok(c) = rx.recv().await {
                        ctx.deliver(&c);
                    }
                    ctx.mark_closed();
                });
            }

            // ONE persistent Tx, reused for every block (as LeaderActor does).
            let tx = channel::dial::<Chunk>(addr).expect("dial");
            for b in 0..6u64 {
                // A distinct pattern per block so a stale/mixed buffer is caught.
                let block: Vec<u8> = (0..block_size)
                    .map(|i| {
                        ((i as u64).wrapping_mul(31).wrapping_add(b.wrapping_mul(97)) % 251) as u8
                    })
                    .collect();
                let mut off = 0usize;
                while off < block_size {
                    let end = (off + chunk).min(block_size);
                    tx.post(Chunk {
                        tag: b,
                        offset: off as u64,
                        total: block_size as u64,
                        data: Part::from(block[off..end].to_vec()),
                    });
                    off = end;
                }
                let mut got = vec![0u8; block_size];
                assert!(
                    ctx.wait_into(&mut got, block_size as u64, 30_000),
                    "block {b} not received"
                );
                assert_eq!(
                    got, block,
                    "block {b} byte-identical over the persistent channel"
                );
            }
        });
    }

    /// `wait` drains exactly one block's worth per call and returns a fresh owned
    /// block each time, so a later block is never contaminated by an earlier one
    /// -- the synchronous one-block-in-flight pattern remotemount uses (the leader
    /// waits for every worker's `ctx_wait` of block N before sourcing block N+1).
    #[test]
    fn ctx_wait_drains_each_block_without_contamination() {
        let ctx = Ctx::new(16);

        ctx.deliver(&Chunk {
            tag: 0,
            offset: 0,
            total: 16,
            data: Part::from(vec![1u8; 16]),
        });
        let mut first = vec![0u8; 16];
        assert!(ctx.wait_into(&mut first, 16, 0), "block 0 ready");
        assert_eq!(first, vec![1u8; 16], "first wait drains block 0");

        // Block 1 gets a fresh owned block after block 0 was drained (counter back to 0).
        ctx.deliver(&Chunk {
            tag: 1,
            offset: 0,
            total: 16,
            data: Part::from(vec![2u8; 16]),
        });
        let mut second = vec![0u8; 16];
        assert!(ctx.wait_into(&mut second, 16, 0), "block 1 ready");
        assert_eq!(
            second,
            vec![2u8; 16],
            "second wait drains block 1 independently of block 0"
        );
    }
}
