/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Read-only FUSE filesystem backed by packed metadata and byte chunks.
//!
//! Replaces the Python fusepy-based ChunkedFS with a Rust implementation
//! using the `fuse3` crate (libfuse3, async/tokio). Exposed to Python
//! via PyO3.
//!
//! Block bytes live in a positional table `blocks: Vec<Option<Bytes>>` indexed by
//! block id (`global_offset / block`); a `None` entry is a not-yet-delivered block.
//! Each block has a paired `tokio::sync::Notify` in `block_notifys`. A FUSE read
//! that touches a `None` block fires `fault_callback` (briefly acquiring the GIL) to
//! signal the client and then parks on that block's `Notify`; the client materialises
//! the block into a mount-owned buffer (`block_ptr`, wrapped as a memoryview and
//! written via RDMA), then calls `receive_block`, which stores `Some(bytes)` at the
//! slot (a zero-copy freeze of that buffer) and wakes the block's waiters. The parked
//! read then re-reads. There is no on-disk cache.
//!
//! Staleness is per FILE, not per block: a file whose source diverged under the
//! freshness fence cannot be reproduced, so the client garbage-fills that file's
//! bytes in the delivered block and flips the file's `stale` bit, shipped alongside
//! the bytes in `receive_block`'s stale list. A read of that file returns EIO, and
//! co-located valid files sharing the same 64
//! MiB block still serve. The bit is cleared for free on `refresh` (the metadata is
//! rebuilt).
//!
//! `PyMountHandle::refresh` atomically swaps the metadata behind the `RwLock` and
//! grows the (append-only) block table for new tail blocks. All FUSE methods take a
//! read lock, so reads run concurrently and are only briefly blocked during the
//! write-lock swap; a read's bytes are gathered (one copy) from the touched blocks
//! under that read lock.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use bytes::BytesMut;
use fuse3::Errno;
use fuse3::FileType;
use fuse3::MountOptions;
use fuse3::Result as FuseResult;
use fuse3::path::prelude::*;
use futures::stream;
use monarch_gil::GilSite;
use monarch_gil::monarch_with_gil_blocking;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tracing::warn;

const DEFAULT_TTL_MS: u64 = 200;
const AVAILABILITY_BLOCK_SIZE: usize = 64 * 1024 * 1024;

enum FsEntry {
    Dir {
        attr: FileAttr,
        children: Vec<String>,
    },
    File {
        attr: FileAttr,
        global_offset: usize,
        file_len: usize,
        /// Set from ``receive_block``'s stale list when the client could not reproduce this file's
        /// bytes (its source diverged under the fence), so the delivered block holds
        /// garbage there: a read of this file returns EIO. Rebuilt false on every
        /// ``refresh`` (the metadata table is recreated from scratch).
        stale: bool,
    },
    Symlink {
        attr: FileAttr,
        link_target: OsString,
    },
}

impl FsEntry {
    fn attr(&self) -> &FileAttr {
        match self {
            FsEntry::Dir { attr, .. } => attr,
            FsEntry::File { attr, .. } => attr,
            FsEntry::Symlink { attr, .. } => attr,
        }
    }
}

/// All filesystem state — metadata and chunk data — protected by a single RwLock.
///
/// Keeping them together ensures every FUSE read sees a fully consistent
/// snapshot: no reader can observe new chunk data paired with old file
/// offsets/sizes (or vice versa), because `refresh` applies both under
/// one write-lock acquisition.
struct FsState {
    metadata: HashMap<OsString, FsEntry>,
    /// Positional block table indexed by block id (= global_offset / block size);
    /// ``None`` is not-yet-delivered, ``Some(bytes)`` is delivered. Sized to the block
    /// count at mount and grown (append-only) on refresh. Staleness lives per file
    /// (``FsEntry::File::stale``), not per block, so one diverged file does not poison
    /// the co-located files that share its 64 MiB block.
    blocks: Vec<Option<Bytes>>,
    /// One ``Notify`` per block, parallel to ``blocks`` (same length). A read that
    /// finds ``blocks[id] == None`` parks on ``block_notifys[id]``; ``receive_block``
    /// wakes it after storing the bytes. In an ``Arc`` so a read can clone it out
    /// under the read lock and await it after releasing the lock.
    block_notifys: Vec<Arc<Notify>>,
    /// Total concatenated layout size.
    total_size: usize,
    /// Mount-owned writable staging buffers, parallel to ``blocks`` and keyed by the same
    /// block id: ``reserved[id]`` is the buffer block ``id`` is delivered into, or ``None``
    /// when no delivery is outstanding for it. ``block_ptr`` provisions ``Some(zeroed
    /// BLOCK_SIZE)`` on demand and hands the client its address (wrapped as a memoryview
    /// and written via RDMA / staging); ``receive_block`` ``take``s it and freezes it
    /// (zero-copy) into ``blocks[id]``, leaving the slot ``None`` again. Grown with
    /// ``None`` (no eager allocation), so only outstanding blocks hold a 64 MiB buffer.
    reserved: Vec<Option<BytesMut>>,
}

impl FsState {
    fn lookup_entry(&self, path: &OsStr) -> Option<&FsEntry> {
        self.metadata.get(path)
    }

    /// Grow the block table and its parallel per-block notifys to `num_blocks`,
    /// appending empty (`None`) slots. Append-only -- never shrinks -- and keeps
    /// the invariant `blocks.len() == block_notifys.len()`.
    fn grow_to(&mut self, num_blocks: usize) {
        if num_blocks > self.blocks.len() {
            self.blocks.resize(num_blocks, None);
        }
        while self.block_notifys.len() < self.blocks.len() {
            self.block_notifys.push(Arc::new(Notify::new()));
        }
    }

    /// Store a delivered block's ``bytes`` at its slot, set the ``stale`` bit on any
    /// files whose source diverged under the fence, and return the block's ``Notify``
    /// (so the caller can wake parked reads after releasing the lock).
    fn install_block(&mut self, block_id: usize, bytes: Bytes, stale: Vec<String>) -> Arc<Notify> {
        // Defensive: a delivery that races ahead of the refresh that grew the table
        // still lands (the block id is bounded by the layout size).
        self.grow_to(block_id + 1);
        self.blocks[block_id] = Some(bytes);
        for path in stale {
            if let Some(FsEntry::File { stale, .. }) = self.metadata.get_mut(OsStr::new(&path)) {
                *stale = true;
            }
        }
        self.block_notifys[block_id].clone()
    }

    /// The staging-buffer slot for block ``block_id``, growing the array with ``None`` to
    /// cover it if needed (no eager allocation -- only outstanding blocks hold a buffer).
    /// ``block_ptr`` calls this then ``get_or_insert_with`` to provision the buffer;
    /// ``receive_block`` calls it then ``take`` to consume it, leaving the slot ``None``
    /// so a later re-delivery (e.g. a refresh) re-provisions cleanly.
    fn reserved_slot(&mut self, block_id: usize) -> &mut Option<BytesMut> {
        if block_id >= self.reserved.len() {
            self.reserved.resize_with(block_id + 1, || None);
        }
        &mut self.reserved[block_id]
    }

    /// Commit block ``block_id``: freeze its staged buffer into ``blocks`` (zero-copy) and
    /// return its ``Notify`` so the caller can wake parked reads. With no staged buffer, an
    /// already-served block is a duplicate/stray commit -> no-op (``None``), never
    /// clobbering the served bytes with zeros; a never-delivered block lands a zeroed
    /// buffer rather than panicking.
    fn commit_block(&mut self, block_id: usize, stale: Vec<String>) -> Option<Arc<Notify>> {
        match self.reserved_slot(block_id).take() {
            Some(buf) => Some(self.install_block(block_id, buf.freeze(), stale)),
            None if self.blocks.get(block_id).is_some_and(|b| b.is_some()) => None,
            None => Some(self.install_block(
                block_id,
                BytesMut::zeroed(AVAILABILITY_BLOCK_SIZE).freeze(),
                stale,
            )),
        }
    }
}

/// Gather a FUSE read from the positional block table, under the state read lock.
/// The read spans at most two 64 MiB blocks; copy each touched block's bytes into
/// one buffer, zero-padding a short tail block or (defensively) a block the read
/// handler's presence check should have excluded.
fn read_blocks(
    blocks: &[Option<Bytes>],
    global_offset: usize,
    file_len: usize,
    offset: u64,
    size: u32,
) -> Bytes {
    let offset = offset as usize;
    if offset >= file_len {
        return Bytes::new();
    }
    let len = std::cmp::min(size as usize, file_len - offset);
    if len == 0 {
        return Bytes::new();
    }
    let start = global_offset + offset;
    let end = start + len;

    let mut out: Vec<u8> = Vec::with_capacity(len);
    let mut pos = start;
    while pos < end {
        let block_id = pos / AVAILABILITY_BLOCK_SIZE;
        let block_base = block_id * AVAILABILITY_BLOCK_SIZE;
        let chunk_end = std::cmp::min(block_base + AVAILABILITY_BLOCK_SIZE, end);
        let want = chunk_end - pos;
        let within = pos - block_base;
        match blocks.get(block_id) {
            Some(Some(b)) => {
                // Copy what the block actually holds; a short (tail) block leaves
                // the remainder zero-padded.
                let take = want.min(b.len().saturating_sub(within));
                out.extend_from_slice(&b[within..within + take]);
                if take < want {
                    out.resize(out.len() + (want - take), 0);
                }
            }
            // Not-yet-delivered (the read handler gathers only once every touched
            // block is present, so this is defensive) or out-of-range -> zero-pad.
            _ => out.resize(out.len() + want, 0),
        }
        pos = chunk_end;
    }
    Bytes::from(out)
}

/// The first not-yet-delivered (``None``) block in the touched range, or ``None`` if
/// every touched block is present and the read can gather. Factored out so it is
/// unit-testable. enumerate() before take/skip so the index stays the absolute block
/// id, not a range-relative one.
fn first_missing_block(
    blocks: &[Option<Bytes>],
    first_block: usize,
    last_block: usize,
) -> Option<usize> {
    blocks
        .iter()
        .enumerate()
        .take(last_block + 1)
        .skip(first_block)
        .find(|(_, slot)| slot.is_none())
        .map(|(b, _)| b)
}

fn join_path(parent: &OsStr, name: &OsStr) -> OsString {
    let parent_s = parent.to_string_lossy();
    let name_s = name.to_string_lossy();
    if parent_s == "/" {
        OsString::from(format!("/{name_s}"))
    } else {
        OsString::from(format!("{parent_s}/{name_s}"))
    }
}

fn parent_path(path: &OsStr) -> OsString {
    let s = path.to_string_lossy();
    match s.rfind('/') {
        Some(0) | None => OsString::from("/"),
        Some(i) => OsString::from(&s[..i]),
    }
}

fn f64_to_system_time(ts: f64) -> SystemTime {
    if ts >= 0.0 {
        UNIX_EPOCH + Duration::from_secs_f64(ts)
    } else {
        UNIX_EPOCH
    }
}

fn required_key<'py, T: pyo3::FromPyObject<'py>>(
    dict: &Bound<'py, PyDict>,
    key: &str,
) -> PyResult<T> {
    dict.get_item(key)?
        .ok_or_else(|| PyRuntimeError::new_err(format!("missing required attr key: {key}")))?
        .extract()
}

fn extract_attr(dict: &Bound<'_, PyDict>, kind: FileType) -> PyResult<FileAttr> {
    let st_size: u64 = required_key(dict, "st_size")?;
    Ok(FileAttr {
        size: st_size,
        blocks: st_size.div_ceil(512),
        atime: f64_to_system_time(required_key(dict, "st_atime")?),
        mtime: f64_to_system_time(required_key(dict, "st_mtime")?),
        ctime: f64_to_system_time(required_key(dict, "st_ctime")?),
        kind,
        perm: (required_key::<u32>(dict, "st_mode")? & 0o7777) as u16,
        nlink: required_key(dict, "st_nlink")?,
        uid: required_key(dict, "st_uid")?,
        gid: required_key(dict, "st_gid")?,
        rdev: 0,
        blksize: 4096,
        #[cfg(target_os = "macos")]
        crtime: std::time::SystemTime::UNIX_EPOCH,
        #[cfg(target_os = "macos")]
        flags: 0,
    })
}

fn extract_metadata(dict: &Bound<'_, PyDict>) -> PyResult<HashMap<OsString, FsEntry>> {
    let mut entries = HashMap::with_capacity(dict.len());
    for (key, value) in dict.iter() {
        let path: String = key.extract()?;
        let entry_dict: &Bound<'_, PyDict> = value.downcast()?;
        let attr_obj = entry_dict.get_item("attr")?.ok_or_else(|| {
            PyRuntimeError::new_err(format!("missing 'attr' key for path: {path}"))
        })?;
        let attr_dict: &Bound<'_, PyDict> = attr_obj.downcast()?;

        let fs_entry = if let Some(link_target) = entry_dict.get_item("link_target")? {
            let target: String = link_target.extract()?;
            FsEntry::Symlink {
                attr: extract_attr(attr_dict, FileType::Symlink)?,
                link_target: OsString::from(target),
            }
        } else if let Some(children_obj) = entry_dict.get_item("children")? {
            let children: Vec<String> = children_obj.extract()?;
            FsEntry::Dir {
                attr: extract_attr(attr_dict, FileType::Directory)?,
                children,
            }
        } else {
            let global_offset: usize = entry_dict
                .get_item("global_offset")?
                .map(|v| v.extract())
                .transpose()?
                .unwrap_or(0);
            let file_len: usize = entry_dict
                .get_item("file_len")?
                .map(|v| v.extract())
                .transpose()?
                .unwrap_or(0);
            FsEntry::File {
                attr: extract_attr(attr_dict, FileType::RegularFile)?,
                global_offset,
                file_len,
                stale: false,
            }
        };
        entries.insert(OsString::from(path), fs_entry);
    }
    Ok(entries)
}

// --- FUSE filesystem ---

struct ChunkedFuseFs {
    state: Arc<RwLock<FsState>>,
    ttl: Duration,
    /// Called (briefly acquiring the GIL) from the read handler when a read faults
    /// a missing block, so the client is signalled to materialise it.
    fault_callback: Py<PyAny>,
}

impl PathFilesystem for ChunkedFuseFs {
    type DirEntryStream<'a> = stream::Iter<std::vec::IntoIter<FuseResult<DirectoryEntry>>>;
    type DirEntryPlusStream<'a> = stream::Iter<std::vec::IntoIter<FuseResult<DirectoryEntryPlus>>>;

    async fn init(&self, _req: Request) -> FuseResult<ReplyInit> {
        Ok(ReplyInit {
            max_write: NonZeroU32::new(16 * 1024).expect("16KB is non-zero"),
        })
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> FuseResult<ReplyEntry> {
        let path = join_path(parent, name);
        let state = self.state.read().unwrap();
        let entry = state.lookup_entry(&path).ok_or_else(Errno::new_not_exist)?;
        Ok(ReplyEntry {
            ttl: self.ttl,
            attr: *entry.attr(),
        })
    }

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _flags: u32,
    ) -> FuseResult<ReplyAttr> {
        let path = path.ok_or_else(Errno::new_not_exist)?;
        let state = self.state.read().unwrap();
        let entry = state.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
        Ok(ReplyAttr {
            ttl: self.ttl,
            attr: *entry.attr(),
        })
    }

    async fn readlink(&self, _req: Request, path: &OsStr) -> FuseResult<ReplyData> {
        let state = self.state.read().unwrap();
        let entry = state.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
        match entry {
            FsEntry::Symlink { link_target, .. } => Ok(ReplyData {
                data: Bytes::copy_from_slice(link_target.as_encoded_bytes()),
            }),
            _ => Err(libc::EINVAL.into()),
        }
    }

    async fn open(&self, _req: Request, path: &OsStr, flags: u32) -> FuseResult<ReplyOpen> {
        let state = self.state.read().unwrap();
        let entry = state.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
        if matches!(entry, FsEntry::Dir { .. }) {
            return Err(Errno::new_is_dir());
        }
        Ok(ReplyOpen { fh: 0, flags })
    }

    async fn read(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> FuseResult<ReplyData> {
        let path = path.ok_or_else(Errno::new_not_exist)?;
        loop {
            // Resolve the file and find the first touched block that is not yet
            // present, under the read lock. If every touched block is present,
            // gather and return without dropping the lock so no refresh can swap
            // state mid-read; otherwise take that block's notify to wait on.
            let (block, notify) = {
                let state = self.state.read().unwrap();
                let entry = state.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
                let FsEntry::File {
                    global_offset,
                    file_len,
                    stale,
                    ..
                } = entry
                else {
                    return Err(libc::EINVAL.into());
                };
                // This file's source diverged under the fence and could not be
                // reproduced -- its bytes in the delivered block are garbage. EIO
                // this file; co-located valid files (each checked via its own entry)
                // still gather from the same block.
                if *stale {
                    return Err(libc::EIO.into());
                }
                let (global_offset, file_len) = (*global_offset, *file_len);
                let offset_usize = offset as usize;
                if offset_usize >= file_len {
                    return Ok(ReplyData { data: Bytes::new() });
                }
                let len = (size as usize).min(file_len - offset_usize);
                if len == 0 {
                    return Ok(ReplyData { data: Bytes::new() });
                }
                let start = global_offset + offset_usize;
                let first_block = start / AVAILABILITY_BLOCK_SIZE;
                let last_block = (start + len - 1) / AVAILABILITY_BLOCK_SIZE;
                match first_missing_block(&state.blocks, first_block, last_block) {
                    None => {
                        return Ok(ReplyData {
                            data: read_blocks(&state.blocks, global_offset, file_len, offset, size),
                        });
                    }
                    Some(b) => (b, state.block_notifys[b].clone()),
                }
            };

            // The block is missing. Register interest before requesting it, so a
            // delivery landing between the check above and the await below cannot be
            // lost -- a lost wakeup would wedge the read in uninterruptible sleep. If
            // it was delivered in that window, re-loop and re-decide (which also
            // re-checks the file's stale bit); otherwise signal the client (briefly
            // acquiring the GIL) to materialise it, then wait for `receive_block` to
            // wake us and re-decide.
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.read().unwrap().blocks[block].is_some() {
                continue;
            }
            monarch_with_gil_blocking(GilSite::EndpointDispatch, |py| {
                if let Err(e) = self.fault_callback.bind(py).call1((block,)) {
                    warn!("chunked_fuse fault callback failed: {e}");
                }
            });
            notified.await;
        }
    }

    async fn opendir(&self, _req: Request, path: &OsStr, flags: u32) -> FuseResult<ReplyOpen> {
        let state = self.state.read().unwrap();
        let entry = state.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
        if !matches!(entry, FsEntry::Dir { .. }) {
            return Err(Errno::new_is_not_dir());
        }
        Ok(ReplyOpen { fh: 0, flags })
    }

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        parent: &'a OsStr,
        _fh: u64,
        offset: i64,
    ) -> FuseResult<ReplyDirectory<Self::DirEntryStream<'a>>> {
        let state = self.state.read().unwrap();
        let entry = state
            .lookup_entry(parent)
            .ok_or_else(Errno::new_not_exist)?;
        let children = match entry {
            FsEntry::Dir { children, .. } => children,
            _ => return Err(Errno::new_is_not_dir()),
        };

        let offset = offset as u64;
        let mut entries: Vec<FuseResult<DirectoryEntry>> = Vec::new();
        let mut idx: u64 = 1;

        if offset < idx {
            entries.push(Ok(DirectoryEntry {
                kind: FileType::Directory,
                name: OsString::from("."),
                offset: idx as i64,
            }));
        }
        idx += 1;

        if offset < idx {
            entries.push(Ok(DirectoryEntry {
                kind: FileType::Directory,
                name: OsString::from(".."),
                offset: idx as i64,
            }));
        }
        idx += 1;

        for child_name in children {
            if offset < idx {
                let child_path = join_path(parent, OsStr::new(child_name));
                if let Some(child_entry) = state.lookup_entry(&child_path) {
                    entries.push(Ok(DirectoryEntry {
                        kind: child_entry.attr().kind,
                        name: OsString::from(child_name),
                        offset: idx as i64,
                    }));
                }
            }
            idx += 1;
        }

        Ok(ReplyDirectory {
            entries: stream::iter(entries),
        })
    }

    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        parent: &'a OsStr,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> FuseResult<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {
        let state = self.state.read().unwrap();
        let entry = state
            .lookup_entry(parent)
            .ok_or_else(Errno::new_not_exist)?;
        let children = match entry {
            FsEntry::Dir { children, .. } => children,
            _ => return Err(Errno::new_is_not_dir()),
        };

        let ttl = self.ttl;
        let mut entries: Vec<FuseResult<DirectoryEntryPlus>> = Vec::new();
        let mut idx: u64 = 1;

        if offset < idx {
            entries.push(Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: OsString::from("."),
                offset: idx as i64,
                attr: *entry.attr(),
                entry_ttl: ttl,
                attr_ttl: ttl,
            }));
        }
        idx += 1;

        if offset < idx {
            let parent_key = parent_path(parent);
            let dotdot_attr = match state.lookup_entry(&parent_key) {
                Some(e) => *e.attr(),
                None => {
                    warn!(
                        "readdirplus: parent path {:?} not found in metadata, \
                         falling back to current dir attrs",
                        parent_key
                    );
                    *entry.attr()
                }
            };
            entries.push(Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: OsString::from(".."),
                offset: idx as i64,
                attr: dotdot_attr,
                entry_ttl: ttl,
                attr_ttl: ttl,
            }));
        }
        idx += 1;

        for child_name in children {
            if offset < idx {
                let child_path = join_path(parent, OsStr::new(child_name));
                if let Some(child_entry) = state.lookup_entry(&child_path) {
                    entries.push(Ok(DirectoryEntryPlus {
                        kind: child_entry.attr().kind,
                        name: OsString::from(child_name),
                        offset: idx as i64,
                        attr: *child_entry.attr(),
                        entry_ttl: ttl,
                        attr_ttl: ttl,
                    }));
                }
            }
            idx += 1;
        }

        Ok(ReplyDirectoryPlus {
            entries: stream::iter(entries),
        })
    }

    async fn access(&self, _req: Request, path: &OsStr, _mask: u32) -> FuseResult<()> {
        let state = self.state.read().unwrap();
        if state.lookup_entry(path).is_some() {
            Ok(())
        } else {
            Err(Errno::new_not_exist())
        }
    }

    async fn release(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> FuseResult<()> {
        Ok(())
    }

    async fn flush(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: u64,
        _lock_owner: u64,
    ) -> FuseResult<()> {
        Ok(())
    }

    async fn fsync(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: u64,
        _datasync: bool,
    ) -> FuseResult<()> {
        Ok(())
    }
}

// --- PyO3 bindings ---

/// Handle to a running FUSE mount. Call `unmount()` to stop it.
#[pyclass(
    name = "FuseMountHandle",
    module = "monarch._rust_bindings.monarch_extension.chunked_fuse"
)]
struct PyMountHandle {
    unmount_tx: Option<oneshot::Sender<()>>,
    mount_point: String,
    state: Arc<RwLock<FsState>>,
}

#[pymethods]
impl PyMountHandle {
    /// Unmount the FUSE filesystem.
    ///
    /// Cleanup of the background FUSE session task is asynchronous: this
    /// method returns once `fusermount3 -u` completes, but the tokio task
    /// may still be winding down.
    fn unmount(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.unmount_tx.take().is_none() {
            return Ok(());
        }
        let mount_point = self.mount_point.clone();
        py.detach(|| {
            match std::process::Command::new("fusermount3")
                .arg("-u")
                .arg(&mount_point)
                .output()
            {
                Ok(o) if !o.status.success() => {
                    let stderr = String::from_utf8_lossy(&o.stderr);
                    Err(PyRuntimeError::new_err(format!(
                        "fusermount3 -u {mount_point} failed: {stderr}"
                    )))
                }
                Err(e) => Err(PyRuntimeError::new_err(format!(
                    "failed to run fusermount3: {e}"
                ))),
                _ => Ok(()),
            }
        })
    }

    /// Atomically swap filesystem metadata + sizing.
    ///
    /// Acquires the write lock once, so no FUSE read can observe a mix of
    /// old metadata with new chunk data (or vice versa).
    ///
    /// Append-only: unchanged block_ids keep their in-memory bytes, and any
    /// appended blocks fault in on demand after the swap -- so refresh only swaps
    /// the tree + sizing, never the block contents. (A defrag, which reassigns
    /// block_ids, re-mounts rather than refreshing.)
    ///
    /// Sleeps 3× the FUSE TTL after the lock is released so kernel
    /// attr/dentry caches expire before returning.
    fn refresh(
        &self,
        py: Python<'_>,
        metadata: &Bound<'_, PyDict>,
        new_total_size: usize,
    ) -> PyResult<()> {
        let new_metadata = extract_metadata(metadata)?;
        {
            let mut state = self.state.write().unwrap();
            state.metadata = new_metadata;
            state.total_size = new_total_size;
            // Append-only: grow the block table (and its notifys) for the new tail
            // blocks; existing ids keep their delivered bytes. A shrink never happens
            // here -- a defrag re-mounts instead of refreshing. New blocks fault in
            // on demand, so no parked reader needs waking here.
            let num_blocks = new_total_size.div_ceil(AVAILABILITY_BLOCK_SIZE);
            state.grow_to(num_blocks);
        }
        // Wait for kernel FUSE attr/page caches (TTL=200ms) to expire.
        let settle = Duration::from_millis(DEFAULT_TTL_MS * 3);
        py.detach(|| {
            #[allow(clippy::disallowed_methods)]
            std::thread::sleep(settle);
            Ok(())
        })
    }

    /// The stable heap address of block ``block_id``'s staging buffer, provisioning a
    /// zeroed ``BLOCK_SIZE`` buffer for it if none is outstanding. The client wraps this
    /// address as a writable memoryview (e.g. via ``ctypes``), writes the block into it
    /// (RDMA read / staged copy), then commits it with ``receive_block``. Valid for the
    /// buffer's life: the allocation does not move, and ``receive_block``'s freeze is
    /// zero-copy (the same address carries into the served ``Bytes``).
    fn block_ptr(&self, block_id: usize) -> usize {
        let mut state = self.state.write().unwrap();
        state
            .reserved_slot(block_id)
            .get_or_insert_with(|| BytesMut::zeroed(AVAILABILITY_BLOCK_SIZE))
            .as_ptr() as usize
    }

    /// Commit block ``block_id``'s staging buffer with NO copy: ``take`` the ``BytesMut``
    /// (already written via its ``block_ptr`` memoryview) and freeze it into ``blocks``,
    /// mark diverged files stale, and wake parked reads. If no buffer is outstanding: a
    /// duplicate/stray commit of an already-served block is a no-op -- it must not clobber
    /// the served bytes with zeros; a call for a never-delivered block (no preceding
    /// ``block_ptr``, nothing served) lands a zeroed buffer rather than panicking. A
    /// legitimate re-delivery (e.g. after a refresh) always provisions a fresh buffer via
    /// ``block_ptr`` first, so it takes the normal path and overwrites cleanly. The freeze
    /// hands the buffer's allocation to ``blocks`` and leaves the staging slot ``None``, so
    /// the memoryview the client still holds aliases the served bytes -- the client must
    /// not write it again after committing. ``stale`` carries the vpaths whose source
    /// diverged under the fence (their bytes here are garbage) so their reads EIO while
    /// co-located fresh files serve; the bit is sticky until a ``refresh`` rebuilds the
    /// metadata.
    fn receive_block(&self, block_id: usize, stale: Vec<String>) {
        // Commit under the write lock, then wake parked reads after releasing it. A no-op
        // commit (a duplicate/stray call) returns no notify, so there is nothing to wake.
        let notify = self.state.write().unwrap().commit_block(block_id, stale);
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }
}

/// Mount a read-only FUSE filesystem serving blocks from memory.
///
/// Args:
///     metadata: dict mapping paths to entry dicts (as produced by
///         the layout builder), including each file's `global_offset`.
///     total_size: total concatenated layout size (for block-count bounds).
///     mount_point: path to mount the filesystem.
/// Returns a FuseMountHandle. Call handle.unmount() to unmount, handle.refresh() to
/// swap metadata, or block_ptr + receive_block to deliver a block into memory (write
/// it into block_ptr's buffer, then receive_block to freeze it in and serve it).
#[pyfunction]
fn mount_chunked_fuse(
    py: Python<'_>,
    metadata: &Bound<'_, PyDict>,
    total_size: usize,
    mount_point: String,
    fault_callback: Py<PyAny>,
) -> PyResult<PyMountHandle> {
    let ttl_ms = DEFAULT_TTL_MS;
    if !mount_point.starts_with('/') {
        return Err(PyRuntimeError::new_err(
            "mount_point must be an absolute path",
        ));
    }

    let metadata = extract_metadata(metadata)?;

    // The block table is sized to the layout's block count, all ``None``, with a
    // paired ``Notify`` per block; blocks are delivered into their slots on demand
    // via receive_block (open's code prefill + read faults). There is no warm-load.
    let num_blocks = total_size.div_ceil(AVAILABILITY_BLOCK_SIZE);
    let state = Arc::new(RwLock::new(FsState {
        metadata,
        blocks: vec![None; num_blocks],
        block_notifys: (0..num_blocks).map(|_| Arc::new(Notify::new())).collect(),
        total_size,
        reserved: Vec::new(),
    }));

    let fs = ChunkedFuseFs {
        state: state.clone(),
        ttl: Duration::from_millis(ttl_ms),
        fault_callback,
    };

    let mount_path = mount_point.clone();
    let (unmount_tx, unmount_rx) = oneshot::channel::<()>();
    let (mount_result_tx, mount_result_rx) = oneshot::channel::<Result<(), String>>();

    // The FUSE session task runs on the shared tokio runtime. We do not
    // retain the JoinHandle because the task's lifetime is governed by
    // fusermount3 -u (called in unmount()). If the Python interpreter
    // exits without calling unmount(), the runtime's atexit shutdown will
    // drop the task. This is acceptable: the kernel automatically cleans
    // up the FUSE mount when the process exits.
    let runtime = monarch_hyperactor::runtime::get_tokio_runtime();
    runtime.spawn(async move {
        let mut opts = MountOptions::default();
        opts.read_only(true).force_readdir_plus(true).nonempty(true);

        let mount_result = fuse3::path::Session::new(opts)
            .mount_with_unprivileged(fs, &mount_path)
            .await;

        match mount_result {
            Ok(mount_handle) => {
                let _ = mount_result_tx.send(Ok(()));
                // Run the FUSE session until unmount() calls fusermount3 -u,
                // which causes the session future to complete.
                if let Err(e) = mount_handle.await {
                    warn!("fuse session error: {e}");
                }
            }
            Err(e) => {
                warn!("fuse mount failed: {e}");
                let _ = mount_result_tx.send(Err(format!("{e}")));
            }
        }
        drop(unmount_rx);
    });

    // Poll until mount appears in /proc/mounts or the task signals
    // failure. Releases the GIL while waiting. We use blocking
    // std::thread::sleep because this is a synchronous PyO3 context.
    //
    // NOTE: Polling /proc/mounts has an inherent TOCTOU race: the mount
    // could theoretically appear and disappear between our check and the
    // caller's first filesystem access. In practice this doesn't happen
    // because nothing else unmounts the path, but inotify on /proc/mounts
    // would be a more robust alternative if needed.
    #[allow(clippy::disallowed_methods)]
    py.detach(|| {
        let start = std::time::Instant::now();
        let mut mount_result_rx = Some(mount_result_rx);
        while start.elapsed() < Duration::from_secs(50) {
            // Check if the mount task reported an early failure.
            if let Some(rx) = mount_result_rx.as_mut() {
                match rx.try_recv() {
                    Ok(Err(e)) => {
                        return Err(PyRuntimeError::new_err(format!("fuse mount failed: {e}")));
                    }
                    Ok(Ok(())) => {
                        // Mount succeeded; stop checking the channel but
                        // still wait for /proc/mounts visibility.
                        mount_result_rx = None;
                    }
                    Err(oneshot::error::TryRecvError::Closed) => {
                        return Err(PyRuntimeError::new_err(
                            "fuse mount task exited without reporting status \
                             (possible panic, tokio runtime shutdown, or \
                             task cancellation)",
                        ));
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {}
                }
            }

            // Check /proc/mounts directly (authoritative source).
            if let Ok(mounts) = std::fs::read_to_string("/proc/mounts") {
                for line in mounts.lines() {
                    if let Some(mp) = line.split_whitespace().nth(1)
                        && mp == mount_point
                    {
                        return Ok(());
                    }
                }
            }

            std::thread::sleep(Duration::from_millis(100));
        }
        Err(PyRuntimeError::new_err("timed out waiting for FUSE mount"))
    })?;

    Ok(PyMountHandle {
        unmount_tx: Some(unmount_tx),
        mount_point,
        state,
    })
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyMountHandle>()?;
    let f = wrap_pyfunction!(mount_chunked_fuse, module)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_extension.chunked_fuse",
    )?;
    module.add_function(f)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_state() -> FsState {
        FsState {
            metadata: HashMap::new(),
            blocks: Vec::new(),
            block_notifys: Vec::new(),
            total_size: 0,
            reserved: Vec::new(),
        }
    }

    /// Model ``receive_block`` for one block: grow the table (blocks + notifys) and
    /// store its bytes at the block's slot.
    fn mark_ready(state: &mut FsState, block_id: usize, bytes: &[u8]) {
        state.grow_to(block_id + 1);
        state.blocks[block_id] = Some(Bytes::copy_from_slice(bytes));
    }

    /// Gather a byte range via the production read helper, exactly as the FUSE
    /// ``read`` handler does once every touched block is present. The blocks are
    /// laid out at ``global_offset`` 0.
    fn serve(state: &FsState, file_len: usize, offset: u64, size: u32) -> Vec<u8> {
        read_blocks(&state.blocks, 0, file_len, offset, size).to_vec()
    }

    #[test]
    fn receive_stores_block_with_paired_notify() {
        let mut state = new_state();
        mark_ready(&mut state, 0, &[7u8; 100]);

        let Some(b) = &state.blocks[0] else {
            panic!("block 0 should hold bytes after mark_ready");
        };
        assert_eq!(b.len(), 100, "the block holds the received bytes");
        assert_eq!(&b[..], &[7u8; 100], "the stored bytes match");
        assert_eq!(
            state.block_notifys.len(),
            state.blocks.len(),
            "growing the table keeps one notify per block"
        );
    }

    #[test]
    fn serve_from_block() {
        let mut state = new_state();
        let bytes: Vec<u8> = (0..200u32).map(|i| (i * 3) as u8).collect();
        mark_ready(&mut state, 0, &bytes);

        let got = serve(&state, bytes.len(), 10, 20);

        assert_eq!(got, bytes[10..30], "serve returns the requested slice");
    }

    #[test]
    fn serve_straddles_block_boundary() {
        // A read crossing the 64 MiB boundary must pull from block 0 and block 1.
        // Block 0's last 4 bytes are 0xAA; block 1's first 4 are 0xBB. The 8-byte
        // read centered on the boundary stitches them.
        let mut state = new_state();
        let mut b0 = vec![0u8; AVAILABILITY_BLOCK_SIZE];
        for x in b0.iter_mut().skip(AVAILABILITY_BLOCK_SIZE - 4) {
            *x = 0xAA;
        }
        let b1 = vec![0xBBu8; 8];
        mark_ready(&mut state, 0, &b0);
        mark_ready(&mut state, 1, &b1);

        let file_len = AVAILABILITY_BLOCK_SIZE + b1.len();
        let got = serve(&state, file_len, (AVAILABILITY_BLOCK_SIZE - 4) as u64, 8);

        assert_eq!(
            got,
            vec![0xAA, 0xAA, 0xAA, 0xAA, 0xBB, 0xBB, 0xBB, 0xBB],
            "the straddling read stitches block 0's tail and block 1's head"
        );
    }

    #[test]
    fn first_missing_none_when_all_present() {
        let mut state = new_state();
        mark_ready(&mut state, 0, &[0u8; 10]);
        mark_ready(&mut state, 1, &[0u8; 10]);
        assert_eq!(
            first_missing_block(&state.blocks, 0, 1),
            None,
            "all touched blocks present -> gather (no fault)"
        );
    }

    #[test]
    fn first_missing_finds_first_gap() {
        let mut state = new_state();
        state.grow_to(3);
        mark_ready(&mut state, 0, &[0u8; 10]);
        // blocks 1 and 2 are not delivered; the read faults the first one.
        assert_eq!(
            first_missing_block(&state.blocks, 0, 2),
            Some(1),
            "faults the first not-yet-delivered touched block"
        );
    }

    #[test]
    fn install_block_serves_frozen_buffer_zero_copy() {
        // A reserved BytesMut, written then frozen via install_block, serves the exact
        // bytes AND shares the same allocation -- proving the freeze is zero-copy (no
        // memcpy from the RDMA/staged buffer into the served Bytes).
        let mut state = new_state();
        let payload: Vec<u8> = (0..64u8).collect();
        let mut buf = BytesMut::zeroed(payload.len());
        buf.copy_from_slice(&payload);
        let ptr_before = buf.as_ptr();

        let _notify = state.install_block(0, std::mem::take(&mut buf).freeze(), Vec::new());

        let Some(b) = &state.blocks[0] else {
            panic!("block 0 should hold bytes after install_block");
        };
        assert_eq!(
            &b[..],
            &payload[..],
            "the served block returns exactly what was written"
        );
        assert_eq!(
            b.as_ptr(),
            ptr_before,
            "freeze is zero-copy: the served Bytes reuses the BytesMut allocation"
        );
    }

    #[test]
    fn commit_block_duplicate_is_noop_and_keeps_served_bytes() {
        // A stray/duplicate commit with nothing staged must be a no-op (return no Notify)
        // and must NOT overwrite the already-served block with a zeroed buffer.
        let mut state = new_state();

        // First delivery: provision the staging buffer (as block_ptr does), write a
        // recognizable head, then commit -- freezing it into blocks[0].
        {
            let buf = state
                .reserved_slot(0)
                .get_or_insert_with(|| BytesMut::zeroed(AVAILABILITY_BLOCK_SIZE));
            buf[..4].copy_from_slice(&[7u8; 4]);
        }
        assert!(
            state.commit_block(0, Vec::new()).is_some(),
            "first commit serves the block and returns its notify"
        );
        assert_eq!(
            state.blocks[0].as_ref().map(|b| b[..4].to_vec()),
            Some(vec![7u8; 4]),
            "block 0 serves the written bytes"
        );

        // Duplicate commit: block_ptr was not called again, so nothing is staged and the
        // block is already served -- it must no-op and leave the served bytes intact.
        assert!(
            state.commit_block(0, Vec::new()).is_none(),
            "duplicate commit is a no-op (no waiters to wake)"
        );
        assert_eq!(
            state.blocks[0].as_ref().map(|b| b[..4].to_vec()),
            Some(vec![7u8; 4]),
            "duplicate commit must not clobber the served bytes with zeros"
        );
    }

    #[test]
    fn reserved_slot_grows_with_none_and_take_consumes() {
        // block_ptr provisions a block's staging buffer on demand; receive_block takes it,
        // leaving the slot None so a re-delivery (e.g. a refresh) re-provisions cleanly.
        // Growth uses None placeholders -- a high block id must NOT eagerly allocate a
        // 64 MiB buffer for every lower slot.
        let mut state = new_state();

        assert_eq!(
            state
                .reserved_slot(0)
                .get_or_insert_with(|| BytesMut::zeroed(AVAILABILITY_BLOCK_SIZE))
                .len(),
            AVAILABILITY_BLOCK_SIZE,
            "block_ptr provisions an empty slot to a full-size writable buffer"
        );

        let taken = state.reserved_slot(0).take();
        assert!(taken.is_some(), "receive_block takes the staging buffer");
        assert!(
            state.reserved[0].is_none(),
            "the committed slot is left None (re-delivery re-provisions)"
        );

        let _ = state.reserved_slot(5);
        assert_eq!(state.reserved.len(), 6, "the array grows to cover block 5");
        assert!(
            state.reserved[3].is_none(),
            "intermediate slots stay None, not 64 MiB buffers"
        );
    }
}
