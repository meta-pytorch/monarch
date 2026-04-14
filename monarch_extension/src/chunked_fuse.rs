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
//! Supports live refresh: `PyMountHandle::refresh` atomically swaps
//! the filesystem metadata behind a `RwLock` without unmounting. Chunk
//! data lives in a separate `Arc<Mutex<Vec<u8>>>` flat buffer that can be
//! updated in-place via `update_chunk_range` without a full swap.
//! All FUSE methods acquire a read lock on metadata, so reads are
//! lock-free relative to each other and only briefly blocked during
//! the write-lock swap.

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
use fuse3::Errno;
use fuse3::FileType;
use fuse3::MountOptions;
use fuse3::Result as FuseResult;
use fuse3::path::prelude::*;
use futures::stream;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::oneshot;
use tracing::warn;

const DEFAULT_TTL_MS: u64 = 200;

enum FsEntry {
    Dir {
        attr: FileAttr,
        children: Vec<String>,
    },
    File {
        attr: FileAttr,
        global_offset: usize,
        file_len: usize,
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
    chunks: Vec<u8>,
    chunk_size: usize,
}

impl FsState {
    fn lookup_entry(&self, path: &OsStr) -> Option<&FsEntry> {
        self.metadata.get(path)
    }
}

/// Read chunk data from the flat buffer.
///
/// Reads `len` bytes starting at `start` from `buf`. Returns an owned
/// `Bytes` copy — the copy is small (one FUSE read, up to 128KB) and
/// avoids holding the lock across an await point.
fn read_chunk_data(
    buf: &[u8],
    chunk_size: usize,
    global_offset: usize,
    file_len: usize,
    offset: u64,
    size: u32,
) -> Result<Bytes, libc::c_int> {
    let offset = offset as usize;
    if offset >= file_len {
        return Ok(Bytes::new());
    }
    let len = std::cmp::min(size as usize, file_len - offset);
    let start = global_offset + offset;
    let end = start + len;

    if end > buf.len() {
        warn!(
            "read out of bounds: start={start} end={end} buf_len={} \
             global_offset={global_offset} file_len={file_len} offset={offset}",
            buf.len()
        );
        // Return what we have rather than an error, to be lenient.
        let available = buf.len().saturating_sub(start);
        if available == 0 {
            return Ok(Bytes::new());
        }
        return Ok(Bytes::copy_from_slice(&buf[start..start + available]));
    }

    // Single contiguous read from the flat buffer — no chunk-boundary logic needed.
    let _ = chunk_size; // kept in FsMetadata for future use
    Ok(Bytes::copy_from_slice(&buf[start..end]))
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
            }
        };
        entries.insert(OsString::from(path), fs_entry);
    }
    Ok(entries)
}

/// Build the flat chunk buffer by concatenating all Python chunk buffers.
///
/// Called once at mount time. For subsequent refreshes, `update_chunk_range`
/// is used to write only dirty blocks in-place.
fn build_flat_buffer(chunks: Vec<pyo3::buffer::PyBuffer<u8>>) -> Vec<u8> {
    let total: usize = chunks.iter().map(|b| b.len_bytes()).sum();
    let mut out = Vec::with_capacity(total);
    for buf in &chunks {
        // SAFETY: buf is a live PyBuffer and we hold the GIL.
        let slice =
            unsafe { std::slice::from_raw_parts(buf.buf_ptr() as *const u8, buf.len_bytes()) };
        out.extend_from_slice(slice);
    }
    out
}

// --- FUSE filesystem ---

struct ChunkedFuseFs {
    state: Arc<RwLock<FsState>>,
    ttl: Duration,
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
        // Hold the read lock for both the metadata lookup and the data copy.
        // This ensures no refresh() can swap metadata or patch chunks between
        // the two operations, eliminating the TOCTOU race.
        let state = self.state.read().unwrap();
        let entry = state.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
        match entry {
            FsEntry::File {
                global_offset,
                file_len,
                ..
            } => {
                let result = read_chunk_data(
                    &state.chunks,
                    state.chunk_size,
                    *global_offset,
                    *file_len,
                    offset,
                    size,
                )
                .map_err(Errno::from)?;
                Ok(ReplyData { data: result })
            }
            _ => Err(libc::EINVAL.into()),
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

    /// Atomically apply dirty chunk patches and swap filesystem metadata.
    ///
    /// Acquires the write lock once, so no FUSE read can observe a mix of
    /// old metadata with new chunk data (or vice versa).
    ///
    /// `chunk_buf` is the full Python mmap (memoryview). `dirty_ranges` is a
    /// list of `(offset, length)` byte ranges within that buffer to copy into
    /// the flat chunk store. Pass `[(0, total_size)]` for an initial full copy.
    ///
    /// Sleeps 3× the FUSE TTL after the lock is released so kernel
    /// attr/dentry caches expire before returning.
    fn refresh(
        &self,
        py: Python<'_>,
        metadata: &Bound<'_, PyDict>,
        chunk_buf: pyo3::buffer::PyBuffer<u8>,
        dirty_ranges: Vec<(usize, usize)>,
        new_total_size: usize,
        chunk_size: usize,
    ) -> PyResult<()> {
        let new_metadata = extract_metadata(metadata)?;
        // SAFETY: chunk_buf is a live PyBuffer; we hold the GIL throughout
        // the write-lock scope below, so the buffer remains valid.
        let src = unsafe {
            std::slice::from_raw_parts(chunk_buf.buf_ptr() as *const u8, chunk_buf.len_bytes())
        };
        {
            let mut state = self.state.write().unwrap();
            state.chunks.resize(new_total_size, 0);
            for (offset, length) in &dirty_ranges {
                state.chunks[*offset..*offset + *length]
                    .copy_from_slice(&src[*offset..*offset + *length]);
            }
            state.metadata = new_metadata;
            state.chunk_size = chunk_size;
        }
        // Wait for kernel FUSE attr/page caches (TTL=200ms) to expire.
        let settle = Duration::from_millis(DEFAULT_TTL_MS * 3);
        py.detach(|| {
            #[allow(clippy::disallowed_methods)]
            std::thread::sleep(settle);
            Ok(())
        })
    }
}

/// Mount a read-only FUSE filesystem from packed metadata and chunks.
///
/// Args:
///     metadata: dict mapping paths to entry dicts (as produced by
///         `pack_directory_chunked`).
///     chunks: list of memoryview/bytes chunks (used only at initial mount;
///         copied once into a flat Rust-owned buffer).
///     chunk_size: size of each chunk in bytes.
///     mount_point: path to mount the filesystem.
/// Returns a FuseMountHandle. Call handle.unmount() to unmount,
/// handle.refresh() to atomically swap metadata, or
/// handle.update_chunk_range() to write dirty blocks in-place.
#[pyfunction]
fn mount_chunked_fuse(
    py: Python<'_>,
    metadata: &Bound<'_, PyDict>,
    chunks: Vec<pyo3::buffer::PyBuffer<u8>>,
    chunk_size: usize,
    mount_point: String,
) -> PyResult<PyMountHandle> {
    let ttl_ms = DEFAULT_TTL_MS;
    if chunk_size == 0 {
        return Err(PyRuntimeError::new_err("chunk_size must be > 0"));
    }
    if !mount_point.starts_with('/') {
        return Err(PyRuntimeError::new_err(
            "mount_point must be an absolute path",
        ));
    }

    let metadata = extract_metadata(metadata)?;

    // Build the initial flat buffer by concatenating all chunk buffers.
    // Python passes [] at mount time; the first refresh() call populates it.
    let flat_buf = build_flat_buffer(chunks);

    let state = Arc::new(RwLock::new(FsState {
        metadata,
        chunks: flat_buf,
        chunk_size,
    }));

    let fs = ChunkedFuseFs {
        state: state.clone(),
        ttl: Duration::from_millis(ttl_ms),
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
                    if let Some(mp) = line.split_whitespace().nth(1) {
                        if mp == mount_point {
                            return Ok(());
                        }
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
