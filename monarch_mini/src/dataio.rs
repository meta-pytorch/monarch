/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Data-plane I/O for message parts, above the wire [`crate::framing`]: reading and
//! writing each part in whichever form fits — a small part inline on the control stream,
//! a large part read/written straight into a shared-memory slab (or a heap buffer with no
//! slab), and a *large* part optionally **striped** across several parallel data streams
//! of the connection to fill a fat cross-region pipe. [`crate::framing`] frames the
//! header and drives these per part; the shared-memory and striping policy — *when* to
//! stripe, opening the data streams, and holding a message back until its shards land —
//! lives here, exposed as two objects framing/the transport drive directly:
//!
//! - [`PartWriter`] — [`write_message_parts`](PartWriter::write_message_parts) writes a
//!   message's parts, each inline on the control stream or striped across the data streams.
//! - [`PartReader`] — [`read_message_parts`](PartReader::read_message_parts) reads them
//!   back, each part returned before its bytes land and its shards tracked for completion.
//! - [`read_messages`] — the receive loop: reads frames, and delivers assembled messages
//!   to the command loop **in order** (a straggling large message holds back only what
//!   genuinely follows it) while reading ahead.
//!
//! A single stream is one throughput-bound flow (crypto core-bound, and cross-region
//! cwnd/RTT-bound to one flow's share of the link), so striping a large message across
//! several data streams gives the parallel flows that fill a fat pipe, while small
//! messages stay inline on the control stream. See `PARALLEL_DATA_STREAMS_DESIGN.md`.
//!
//! ## Fixed stream indices (direction by parity)
//!
//! The control stream is index 0 and the heartbeat stream index 1 (see
//! [`crate::net_transport`]). Data streams take higher indices, with **direction
//! encoded by parity** so the two directions' index spaces never overlap regardless of
//! how many streams each opens: `td` (toward-dialer) at even indices `2, 4, …` (the
//! acceptor writes, the dialer reads) and `ta` (toward-acceptor) at odd indices
//! `3, 5, …` (the dialer writes, the acceptor reads). Shard `k` of a direction is at
//! `base + 2k`; both ends name the same fixed index, so their halves pair with no
//! negotiation. Data streams carry **unframed** shard bytes — the control-stream
//! descriptor ([`crate::framing::WirePart::Striped`]) fully determines the split.
//!
//! ## Streams and tasks
//!
//! Each object holds a clone of the (`Send`) connection handle and opens the data
//! streams it needs itself, lazily on the first large message — blocking the control
//! stream for that one setup is fine (unlike the heartbeat stream, the control stream
//! has no deadline). Each open spawns one per-stream I/O task ([`shard_reader_task`] /
//! [`shard_writer_task`]) via `tokio::spawn`; the halves are `Send`, so this composes
//! with the data runtime the control coroutines may run on. There are no other tasks —
//! no per-connection coordinator — and nothing exists until a connection carries a
//! large message, and only for the direction(s) that do.
//!
//! ## The unsafe: shards write straight into the destination
//!
//! A striped part is read **zero-copy** into its final buffer: the reader allocates the
//! destination once (a shared-memory slab block, or a plain heap `Vec` when there is no
//! slab) and hands each per-stream reader a raw `*mut u8` into its shard's sub-range.
//! This is a deliberately localized `unsafe` (we do not add a bespoke [`MsgPart`]
//! constructor for it): the destination buffer is owned by the assembled [`MsgPart`],
//! which [`read_messages`]'s buffer holds until *every* shard has landed, so the pointers
//! stay valid for the whole of the (disjoint) writes.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::framing::WirePart;
use crate::msg::MsgPart;
use crate::net::Net;
use crate::net::NetConn;
use crate::shm::MapperHandle;
use crate::shm::SHM_THRESHOLD;
use crate::shm::ShmClient;
use crate::shm::ShmClientSlot;

/// The send half of a connection's stream, for the protocol `N`.
type ConnSend<N> = <<N as Net>::Conn as NetConn>::Send;
/// The receive half of a connection's stream, for the protocol `N`.
type ConnRecv<N> = <<N as Net>::Conn as NetConn>::Recv;

/// Send priority of a data stream (quic; tcp ignores it): the same low priority as the
/// control stream, below the heartbeat's, so a beat still packs ahead of bulk data.
const PRIORITY_DATA: i32 = 0;

/// Default size at/above which a message part is striped, when `MM_NET_LARGE_MSG_BYTES`
/// is unset. 1 MiB — comfortably above the shared-memory inline threshold
/// ([`crate::shm::SHM_THRESHOLD`]), so mid-size slab parts stay inline and only bulk
/// payloads take the parallel path.
const DEFAULT_LARGE_MSG_BYTES: u64 = 1 << 20;

/// Number of data streams per striping direction (sender-side width). `0` (unset)
/// disables striping entirely: every part stays inline and the transport behaves
/// exactly as before. A *sender* knob — the receiver follows the wire descriptor — so
/// the two ends need not agree.
pub(crate) fn n_data_streams() -> usize {
    std::env::var("MM_NET_N_DATA_STREAMS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0)
}

/// Size at/above which a message part is striped (see [`DEFAULT_LARGE_MSG_BYTES`]).
fn large_msg_bytes() -> u64 {
    std::env::var("MM_NET_LARGE_MSG_BYTES")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_LARGE_MSG_BYTES)
}

/// The first data-stream index for a task that opened its connection by **dialing**
/// (`dialed`) vs **accepting**, and in the given direction — `write` (toward the peer)
/// or `read` (from the peer). Direction is encoded by parity: `ta` (odd) flows
/// dialer→acceptor, `td` (even) acceptor→dialer, and shard `k` is at `base + 2k`, so the
/// two ends of one stream name the same index. A one-directional gateway side channel
/// (dialer writes, acceptor reads) needs no special case: both land on `ta` (base 3).
fn stream_base(dialed: bool, write: bool) -> usize {
    // toward-acceptor `ta` (odd, 3) when dialer-writing or acceptor-reading; else `td`.
    if dialed == write { 3 } else { 2 }
}

/// Split `len` bytes evenly across `n` shards; the first `len % n` shards get one extra
/// byte. For a striped part (`len >= large_msg_bytes`, `n` small) every shard is
/// comfortably non-empty.
fn even_split(len: u64, n: usize) -> Vec<u64> {
    let n = n as u64;
    let base = len / n;
    let rem = len % n;
    (0..n).map(|i| base + u64::from(i < rem)).collect()
}

/// A raw pointer to a shard's destination sub-range, made `Send` so it can ride to a
/// per-stream reader task. Sound because the owning [`MsgPart`] keeps the buffer alive
/// past every write and each pointer names a disjoint range (see the module docs).
struct ShardDst(*mut u8);
// SAFETY: see the module docs — the destination buffer outlives the writes and the
// ranges are disjoint, so moving the raw pointer to the reader task is sound.
unsafe impl Send for ShardDst {}

/// One shard-read instruction to a per-stream reader: fill `[dst, dst+len)` from the
/// stream, then acknowledge success or failure to the owning message's [`Batch`].
struct ShardRead {
    dst: ShardDst,
    len: usize,
    done: oneshot::Sender<io::Result<()>>,
}

/// A batch of in-flight shard reads and their completion acknowledgements. A
/// [`PartReader`] accumulates a message's striped-part acknowledgements into the current
/// batch; [`PartReader::sync`] then awaits all of them and rotates in a fresh batch.
/// Waiting never stops at the first error because sibling readers may still be writing
/// through raw pointers into the message's destination.
struct Batch {
    done: Vec<oneshot::Receiver<io::Result<()>>>,
}

impl Batch {
    fn new() -> Self {
        Self { done: Vec::new() }
    }
}

/// Await every dispatched shard, retaining the first failure until all sibling reads
/// have either completed or discarded their queued instruction.
async fn wait_for_shards(done: Vec<oneshot::Receiver<io::Result<()>>>) -> io::Result<()> {
    let mut first_error = None;
    for completion in done {
        let result = completion
            .await
            .unwrap_or_else(|_| Err(io::Error::other("data stream closed")));
        match result {
            Err(error) if first_error.is_none() => first_error = Some(error),
            _ => {}
        }
    }
    first_error.map_or(Ok(()), Err)
}

/// Keep a destination owner alive while already-dispatched shards finish after a
/// higher-level operation has failed. The original operation's error takes precedence,
/// so callers intentionally ignore the shard result after the lifetime barrier.
async fn wait_for_shards_before_drop<T>(
    keep_alive: T,
    done: Vec<oneshot::Receiver<io::Result<()>>>,
) -> io::Result<()> {
    let result = wait_for_shards(done).await;
    drop(keep_alive);
    result
}

/// One shard-write job to a per-stream writer: write `part`'s `[offset, offset+len)`
/// onto the stream. The whole part is shared (`Arc`) across the streams that carry its
/// shards; no bytes are copied.
struct ShardWrite {
    part: Arc<MsgPart>,
    offset: usize,
    len: usize,
}

/// Read shards off one data stream in order, each straight into its instruction's
/// destination sub-range, acknowledging success or failure as each finishes. A read
/// error ends the task; dropping queued instructions closes their acknowledgement
/// channels so their message waiters also observe failure.
async fn shard_reader_task<N: Net>(
    mut recv: ConnRecv<N>,
    mut instrs: mpsc::UnboundedReceiver<ShardRead>,
) {
    while let Some(instr) = instrs.recv().await {
        // SAFETY: `dst` points at `len` bytes of the destination buffer owned by the
        // still-undelivered `MsgPart` (held in the delivery gate until every shard
        // lands); this task is the sole writer of this disjoint sub-range. See the
        // module docs on the localized unsafe.
        let buf = unsafe { std::slice::from_raw_parts_mut(instr.dst.0, instr.len) };
        let result = recv.read_exact(buf).await.map(|_| ());
        let failed = result.is_err();
        let _ = instr.done.send(result);
        if failed {
            return;
        }
    }
}

/// Write shards onto one data stream in the order the message writer enqueued them
/// (per-stream FIFO by message). Ends when the striper drops its sender or a write
/// fails (finishing the stream so the peer's reader sees EOF and severs).
async fn shard_writer_task<N: Net>(
    mut send: ConnSend<N>,
    mut jobs: mpsc::UnboundedReceiver<ShardWrite>,
) {
    while let Some(job) = jobs.recv().await {
        let bytes = &job.part.as_bytes()[job.offset..job.offset + job.len];
        if send.write_all(bytes).await.is_err() || send.flush().await.is_err() {
            return;
        }
    }
    let _ = send.shutdown().await;
}

/// Writes a message's parts to the control stream (inline) or across the connection's
/// data streams (striped), the send-side mirror of [`PartReader`]. It decides the split
/// ([`Self::plan`], for the header framing writes) and writes each part
/// ([`Self::write`]) — a small part inline, a large part striped across the data streams
/// (opened lazily off the connection handle). When striping is disabled (`n == 0`)
/// [`Self::plan`] marks every part inline and the wire is byte-for-byte the non-striped
/// path. Striped writes are fire-and-forget (per-stream FIFO handles ordering), so —
/// unlike [`PartReader`] — there is no completion to await.
pub(crate) struct PartWriter<N: Net> {
    conn: N::Conn,
    base: usize,
    n: usize,
    threshold: u64,
    writers: Vec<mpsc::UnboundedSender<ShardWrite>>,
}

impl<N: Net> PartWriter<N> {
    /// A part writer over this side's outbound data streams. `dialed` (did we open this
    /// connection by dialing?) picks the write direction's stream indices.
    pub(crate) fn new(conn: N::Conn, dialed: bool) -> Self {
        Self {
            conn,
            base: stream_base(dialed, true),
            n: n_data_streams(),
            threshold: large_msg_bytes(),
            writers: Vec::new(),
        }
    }

    /// Write a message's parts onto `control` after its routing header (written by
    /// [`crate::framing`]): the per-part plan as a header, then each part — inline bytes
    /// onto `control`, or striped across the data streams — and flush. The whole
    /// message-part write path for both pathways (they differ only in the routing header
    /// framing writes first). Striped shards are enqueued (they flow asynchronously)
    /// before the flush; the peer reads the header, opens its data streams, and pairs
    /// them, so nothing blocks here.
    pub(crate) async fn write_message_parts<W: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        control: &mut W,
        parts: Vec<MsgPart>,
    ) -> io::Result<()> {
        let plan: Vec<WirePart> = parts
            .iter()
            .map(|part| self.plan(part.len() as u64))
            .collect();
        crate::framing::write_header(control, &plan).await?;
        for (part, wire_part) in parts.into_iter().zip(&plan) {
            self.write(control, part, wire_part).await?;
        }
        control.flush().await
    }

    /// The wire descriptor for a part of `len` bytes: inline, or striped with its
    /// per-shard byte lengths. The plan goes in the header before any part bytes.
    fn plan(&self, len: u64) -> WirePart {
        if self.n > 0 && len >= self.threshold {
            WirePart::Striped {
                shard_lens: even_split(len, self.n),
            }
        } else {
            WirePart::Inline { len }
        }
    }

    /// Write one part per its `wire_part` descriptor: an inline part's bytes stream onto
    /// `control`; a striped part's shards are handed to the data streams' per-stream
    /// writers (opened lazily). Striped shards go out asynchronously, so this returns
    /// once they are enqueued.
    async fn write<W: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        control: &mut W,
        part: MsgPart,
        wire_part: &WirePart,
    ) -> io::Result<()> {
        let WirePart::Striped { shard_lens } = wire_part else {
            control.write_all(part.as_bytes()).await?;
            return Ok(());
        };
        self.ensure(shard_lens.len()).await?;
        let part = Arc::new(part);
        let mut offset = 0usize;
        for (i, &shard_len) in shard_lens.iter().enumerate() {
            let len = shard_len as usize;
            self.writers[i]
                .send(ShardWrite {
                    part: Arc::clone(&part),
                    offset,
                    len,
                })
                .map_err(|_| io::Error::other("data stream writer closed"))?;
            offset += len;
        }
        Ok(())
    }

    /// Ensure `n` per-stream writers are open, opening stream `base + 2i` for each new
    /// index and spawning its [`shard_writer_task`].
    async fn ensure(&mut self, n: usize) -> io::Result<()> {
        while self.writers.len() < n {
            let index = self.base + 2 * self.writers.len();
            let (send, _recv) = self.conn.stream(index, PRIORITY_DATA).await?;
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(shard_writer_task::<N>(send, rx));
            self.writers.push(tx);
        }
        Ok(())
    }
}

/// Reads a message's parts off the control stream, returning each [`MsgPart`] before its
/// bytes have landed. A small part is read inline (into the slab, or a heap buffer when
/// there is no slab); a large part is striped across the connection's data streams
/// (opened lazily off the handle), its shards read straight into the destination. It
/// carries the shared-memory context and accumulates the in-flight shards into the
/// current [`Batch`]; [`Self::sync`] hands back a future for that batch's completion and
/// starts a fresh one, so the reader can deliver each message once its shards land.
pub(crate) struct PartReader<N: Net> {
    conn: N::Conn,
    base: usize,
    mapper: MapperHandle,
    client: ShmClientSlot,
    readers: Vec<mpsc::UnboundedSender<ShardRead>>,
    batch: Batch,
}

impl<N: Net> PartReader<N> {
    /// A part reader over this side's inbound data streams, landing striped parts in the
    /// slab via `mapper`/`client`. `dialed` (did we open this connection by dialing?)
    /// picks the read direction's stream indices.
    pub(crate) fn new(
        conn: N::Conn,
        dialed: bool,
        mapper: MapperHandle,
        client: ShmClientSlot,
    ) -> Self {
        Self {
            conn,
            base: stream_base(dialed, false),
            mapper,
            client,
            readers: Vec::new(),
            batch: Batch::new(),
        }
    }

    /// Read a message's parts off `control` after its routing header (read by
    /// [`crate::framing`]): the per-part plan header, then each part — inline off
    /// `control`, or striped from the data streams. The whole message-part read path for
    /// both pathways. Striped parts come back before their bytes land, accumulated into
    /// the current batch; the reader loop calls [`Self::sync`] afterward to gate delivery.
    pub(crate) async fn read_message_parts<R: AsyncRead + Unpin>(
        &mut self,
        control: &mut R,
    ) -> io::Result<Vec<MsgPart>> {
        let plan: Vec<WirePart> = crate::framing::read_header(control).await?;
        let mut out = Vec::with_capacity(plan.len());
        for wire_part in plan {
            match self.read(control, wire_part).await {
                Ok(part) => out.push(part),
                Err(error) => {
                    let Batch { done } = std::mem::replace(&mut self.batch, Batch::new());
                    let _ = wait_for_shards_before_drop(out, done).await;
                    return Err(error);
                }
            }
        }
        Ok(out)
    }

    /// Read one part off `control`, per its wire descriptor. Returns before a striped
    /// part's bytes land — they fill from the data streams into the returned part's
    /// destination, tracked by the current batch (see [`Self::sync`]).
    async fn read<R: AsyncRead + Unpin>(
        &mut self,
        control: &mut R,
        part: WirePart,
    ) -> io::Result<MsgPart> {
        match part {
            WirePart::Inline { len } => {
                read_inline(control, len, self.mapper.clone(), self.client()).await
            }
            WirePart::Striped { shard_lens } => self.read_striped(&shard_lens).await,
        }
    }

    /// A future that resolves once every shard read since the last `sync` has landed
    /// (`Ok`) or a data stream died (`Err`), rotating in a fresh batch for the next
    /// message's parts — or `None` for a message with no striped parts (the common case),
    /// which is already complete and needs no waiting or allocation. Only a genuinely
    /// striped message produces (and boxes) a future.
    fn sync(&mut self) -> Option<Done> {
        if self.batch.done.is_empty() {
            return None;
        }
        let Batch { done } = std::mem::replace(&mut self.batch, Batch::new());
        Some(Box::pin(wait_for_shards(done)))
    }

    /// Allocate the destination for a striped part and hand each shard's sub-range to its
    /// per-stream reader, adding this part's shards to the current batch. Returns the
    /// [`MsgPart`] (its bytes fill in as the shards land).
    async fn read_striped(&mut self, shard_lens: &[u64]) -> io::Result<MsgPart> {
        self.ensure(shard_lens.len()).await?;
        let total: u64 = shard_lens.iter().sum();
        let (part, base) = alloc_dst(self.mapper.clone(), self.client(), total).await?;

        let mut done = Vec::with_capacity(shard_lens.len());
        let mut offset = 0usize;
        for (i, &shard_len) in shard_lens.iter().enumerate() {
            let len = shard_len as usize;
            // SAFETY: `base` is a `total`-byte destination and `offset + len <= total`,
            // so `base + offset` starts a distinct in-bounds `len`-byte range.
            let dst = ShardDst(unsafe { base.add(offset) });
            let (done_tx, done_rx) = oneshot::channel();
            if self.readers[i]
                .send(ShardRead {
                    dst,
                    len,
                    done: done_tx,
                })
                .is_err()
            {
                let error = io::Error::other("data stream reader closed");
                let _ = wait_for_shards_before_drop(part, done).await;
                return Err(error);
            }
            done.push(done_rx);
            offset += len;
        }
        self.batch.done.extend(done);
        Ok(part)
    }

    /// Snapshot the owning actor's gateway client (`None` until learned).
    fn client(&self) -> Option<ShmClient> {
        *self.client.lock().expect("shm client slot mutex poisoned")
    }

    /// Ensure `n` per-stream readers are open, opening stream `base + 2i` for each new
    /// index and spawning its [`shard_reader_task`].
    async fn ensure(&mut self, n: usize) -> io::Result<()> {
        while self.readers.len() < n {
            let index = self.base + 2 * self.readers.len();
            let (_send, recv) = self.conn.stream(index, PRIORITY_DATA).await?;
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(shard_reader_task::<N>(recv, rx));
            self.readers.push(tx);
        }
        Ok(())
    }
}

/// Why [`read_messages`] stopped.
pub(crate) enum ReadStop {
    /// The control stream ended or errored (the caller severs, folding in `io::Error`).
    Ended(io::Error),
    /// A data stream died mid-message, so a striped message can no longer complete and
    /// order can't skip it (the caller severs).
    DataStreamDied,
    /// `on_message` returned `false` — its command sink is gone; stop quietly.
    Delivered,
}

/// A striped message's shard-completion future ([`PartReader::sync`]), boxed so the
/// buffer can hold it: `Ok` once every shard landed, `Err` if a data stream died. The
/// buffer pairs each message with an `Option<Done>` — `None` for the common message with
/// no striped parts (already complete), so only a striped one ever allocates a future.
type Done = Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;

/// Reads and delivers a pathway's messages, driven by [`Self::read_messages`]. Each
/// pathway (a join/serve connection, a gateway side channel) is one implementation,
/// supplying how to decode a control frame ([`Self::read_frame`]) and where a ready
/// message goes ([`Self::on_message`]); the loop and the striping/ordering are shared.
///
/// Making the pathway an implementation (rather than a generic closure) keeps
/// [`Self::read_frame`]'s future *concrete per impl*, so its `Send`-ness — needed to run
/// the loop on the data runtime — is checked structurally instead of as a higher-ranked
/// bound the compiler can't yet discharge; that is why this is a trait and not two
/// closures.
pub(crate) trait MessageReader<N: Net>: Send {
    /// The delivered message type (`ConnectionCommand` / `SideChannelMessage`).
    type Item: Send;

    /// Decode the next control frame off `control`, reading its parts through `parts`.
    /// Not cancel-safe (a frame is several reads), so [`Self::read_messages`] drives one
    /// to completion before starting the next.
    fn read_frame(
        control: &mut ConnRecv<N>,
        parts: &mut PartReader<N>,
    ) -> impl Future<Output = io::Result<Self::Item>> + Send;

    /// Deliver one in-order message. Returns `false` if its sink is gone (stop the loop).
    fn on_message(&mut self, item: Self::Item) -> bool;

    /// The whole receive loop. Read a frame, then hand it to [`Self::on_message`] **in
    /// order** — but read ahead while a large message's shards land, delivering a message
    /// only once its batch ([`PartReader::sync`]) completes, so a straggler only delays
    /// what genuinely follows it. A message with no striped parts completes at once, so
    /// with nothing striped nothing is ever held.
    fn read_messages(
        &mut self,
        mut control: ConnRecv<N>,
        mut parts: PartReader<N>,
    ) -> impl Future<Output = ReadStop> + Send {
        async move {
            // In-order, each with its shards' completion future — `None` when nothing was
            // striped. Empty in the steady state; only a striped straggler and whatever
            // arrives behind it ever land here.
            let mut buffer: VecDeque<(Self::Item, Option<Done>)> = VecDeque::new();
            loop {
                // The latency path: nothing buffered ⇒ nothing to wait on, so just read
                // the next frame directly — no completion race, no buffer round trip.
                let frame = if buffer.is_empty() {
                    Self::read_frame(&mut control, &mut parts).await
                } else {
                    // Something striped is still landing: read ahead while delivering any
                    // already-complete leading messages. The read future is kept
                    // un-cancelled (a frame is several reads) until it yields.
                    let read = Self::read_frame(&mut control, &mut parts);
                    tokio::pin!(read);
                    loop {
                        tokio::select! {
                            frame = &mut read => break frame,
                            done = await_front(&mut buffer) => match done {
                                Err(_) => {
                                    buffer.pop_front().expect("front just completed");
                                    drain_buffer(&mut buffer).await;
                                    return ReadStop::DataStreamDied;
                                }
                                Ok(()) => {
                                    let (item, _) =
                                        buffer.pop_front().expect("front just completed");
                                    if !self.on_message(item) {
                                        drain_buffer(&mut buffer).await;
                                        return ReadStop::Delivered;
                                    }
                                }
                            },
                        }
                    }
                };
                let item = match frame {
                    Ok(item) => item,
                    Err(err) => {
                        drain_buffer(&mut buffer).await;
                        return ReadStop::Ended(err);
                    }
                };
                match parts.sync() {
                    // The latency path continues: no shards to wait on and nothing ahead
                    // of it — deliver straight through, never touching the buffer.
                    None if buffer.is_empty() => {
                        if !self.on_message(item) {
                            return ReadStop::Delivered;
                        }
                    }
                    // Striped (must wait), or something ahead of it is still landing
                    // (deliver in order): buffer it behind the rest.
                    done => buffer.push_back((item, done)),
                }
            }
        }
    }
}

/// Await the front buffered message's shard completion — pending forever when the buffer
/// is empty, so the reader only reads. `Ok` ⇒ its shards all landed, or it had none
/// (deliver it); `Err` ⇒ a data stream died.
async fn await_front<Item>(buffer: &mut VecDeque<(Item, Option<Done>)>) -> io::Result<()> {
    match buffer.front_mut() {
        None => std::future::pending().await,
        Some((_, None)) => Ok(()),
        Some((_, Some(done))) => done.as_mut().await,
    }
}

/// Wait out every buffered message's shard sync before dropping its destination parts.
/// Errors are irrelevant once the reader has already decided to stop, but each `Done`
/// must reach completion because its shard tasks may still hold raw destination pointers.
async fn drain_buffer<Item>(buffer: &mut VecDeque<(Item, Option<Done>)>) {
    while let Some((_, done)) = buffer.front_mut() {
        if let Some(done) = done {
            let _ = done.as_mut().await;
        }
        buffer.pop_front();
    }
}

/// Read an inline part of `len` bytes off `control` into its destination. A part `>=
/// SHM_THRESHOLD` whose actor has learned its gateway [`ShmClient`] lands straight in a
/// freshly allocated slab block (the only copy is the unavoidable kernel-to-userspace
/// one, and a later unix hop forwards the descriptor without recopying); everything else
/// lands in an owned heap buffer. A free function (not a `&self` method) so the read
/// future stays `Send` without requiring the connection handle to be `Sync`.
async fn read_inline<R: AsyncRead + Unpin>(
    control: &mut R,
    len: u64,
    mapper: MapperHandle,
    client: Option<ShmClient>,
) -> io::Result<MsgPart> {
    // The slab only pays off for a large part, so filter the client out below the
    // threshold; `alloc_dst` then gives a heap buffer for the small/no-slab case.
    let (part, dst) = alloc_dst(mapper, client.filter(|_| len >= SHM_THRESHOLD), len).await?;
    // SAFETY: `dst` is `len` writable bytes of the destination buffer owned by `part`
    // (slab mapping or heap `Vec`), valid past this read; this is the sole writer.
    let buf = unsafe { std::slice::from_raw_parts_mut(dst, len as usize) };
    control.read_exact(buf).await?;
    Ok(part)
}

/// Allocate a `total`-byte destination for a message part and return it plus a raw
/// pointer to its start. With a gateway [`ShmClient`] it is a slab block (read
/// zero-copy, ready to relay across a later unix hop by descriptor); without one it is
/// a plain heap buffer. The pointer stays valid for the returned [`MsgPart`]'s lifetime
/// (the slab mapping is context-lived; the heap `Vec`'s buffer never moves once boxed).
async fn alloc_dst(
    mapper: MapperHandle,
    client: Option<ShmClient>,
    total: u64,
) -> io::Result<(MsgPart, *mut u8)> {
    let Some(client) = client else {
        // No slab: assemble into a plain heap buffer. Take the raw pointer from the
        // freshly-allocated `Vec` before handing it to `MsgPart::from_bytes` — moving
        // the `Vec` into the part does not move its heap allocation, so the pointer
        // stays valid, and this side owns the part exclusively until delivery.
        let mut buf: Vec<u8> = Vec::with_capacity(total as usize);
        // SAFETY: the allocation above reserves capacity for `total` bytes, so growing
        // the length to `total` names only those bytes. The buffer is exposed solely as
        // a write destination (a raw `*mut u8`); every byte is filled by the
        // `read_exact`/shard reads that follow before any is read, so its uninitialized
        // contents are never observed. Skipping the zero-fill avoids wasted work.
        // (`clippy::uninit_vec` flags this pattern generically; the read-before-use
        // invariant above is exactly what makes it sound here.)
        #[allow(clippy::uninit_vec)]
        unsafe {
            buf.set_len(total as usize)
        };
        let ptr = buf.as_mut_ptr();
        return Ok((MsgPart::from_bytes(buf), ptr));
    };
    let (offset, token) = client.allocate(total).await?;
    let ptr = {
        let mut guard = mapper.lock().expect("shm mapper mutex poisoned");
        // SAFETY: `offset` was just granted for `total` bytes against `client`'s slab,
        // so the file covers it and the mapper can map the range.
        unsafe { guard.map(client.slab_fd(), offset, total as usize)? }
    };
    Ok((
        MsgPart::new_shm(mapper, client.slab_fd(), token, offset, total),
        ptr,
    ))
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[tokio::test]
    async fn shard_failure_waits_for_sibling_completion() {
        let (failed_tx, failed_rx) = oneshot::channel();
        let (sibling_tx, sibling_rx) = oneshot::channel();
        let waiter = tokio::spawn(wait_for_shards(vec![failed_rx, sibling_rx]));

        failed_tx
            .send(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "injected shard failure",
            )))
            .expect("failed shard should acknowledge");
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "the batch must wait for a sibling after the first failure"
        );

        sibling_tx
            .send(Ok(()))
            .expect("sibling shard should acknowledge");
        let error = waiter
            .await
            .expect("batch waiter should finish")
            .expect_err("the first shard failure should be returned");
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    struct DropNotice(Option<oneshot::Sender<()>>);

    impl Drop for DropNotice {
        fn drop(&mut self) {
            if let Some(notice) = self.0.take() {
                let _ = notice.send(());
            }
        }
    }

    #[tokio::test]
    async fn failed_dispatch_keeps_destination_until_dispatched_shards_finish() {
        let (shard_tx, shard_rx) = oneshot::channel();
        let (drop_tx, mut drop_rx) = oneshot::channel();
        let waiter = tokio::spawn(wait_for_shards_before_drop(
            DropNotice(Some(drop_tx)),
            vec![shard_rx],
        ));

        tokio::task::yield_now().await;
        assert!(
            matches!(drop_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "destination must remain alive while a shard is active"
        );
        shard_tx
            .send(Ok(()))
            .expect("dispatched shard should acknowledge");
        waiter
            .await
            .expect("dispatch waiter should finish")
            .expect("dispatched shard should finish successfully");
        drop_rx
            .await
            .expect("destination should drop after the shard finishes");
    }

    #[tokio::test]
    async fn buffered_drain_waits_for_syncs_after_front_failure() {
        let (failed_tx, failed_rx) = oneshot::channel();
        let (pending_tx, pending_rx) = oneshot::channel();
        let (drop_tx, mut drop_rx) = oneshot::channel();
        failed_tx
            .send(Err(io::Error::other("injected front failure")))
            .expect("front shard should acknowledge failure");

        let mut buffer = VecDeque::new();
        buffer.push_back((
            DropNotice(None),
            Some(Box::pin(wait_for_shards(vec![failed_rx])) as Done),
        ));
        buffer.push_back((
            DropNotice(Some(drop_tx)),
            Some(Box::pin(wait_for_shards(vec![pending_rx])) as Done),
        ));
        let waiter = tokio::spawn(async move {
            drain_buffer(&mut buffer).await;
            assert!(
                buffer.is_empty(),
                "drain should consume every buffered item"
            );
        });

        tokio::task::yield_now().await;
        assert!(
            matches!(drop_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "a later destination must remain alive after the front sync fails"
        );
        pending_tx
            .send(Ok(()))
            .expect("later shard should acknowledge");
        waiter.await.expect("buffer drain should finish");
        drop_rx
            .await
            .expect("later destination should drop after its shard finishes");
    }
}
