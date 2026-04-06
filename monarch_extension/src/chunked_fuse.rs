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
//! the filesystem data (metadata + chunks) behind a `RwLock` without
//! unmounting. All FUSE methods acquire a read lock, so reads are
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

/// Snapshot of filesystem data that can be atomically swapped.
struct FsData {
    metadata: HashMap<OsString, FsEntry>,
    chunks: Vec<Bytes>,
    chunk_size: usize,
}

impl FsData {
    fn lookup_entry(&self, path: &OsStr) -> Option<&FsEntry> {
        self.metadata.get(path)
    }

    fn read_data(
        &self,
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

        let chunk_size = self.chunk_size;
        let start_chunk = start / chunk_size;
        let end_chunk = (end.saturating_sub(1)) / chunk_size;

        if start_chunk == end_chunk && start_chunk < self.chunks.len() {
            let chunk_offset = start % chunk_size;
            Ok(self.chunks[start_chunk].slice(chunk_offset..chunk_offset + len))
        } else {
            let mut buf = Vec::with_capacity(len);
            let mut pos = start;
            while pos < end {
                let ci = pos / chunk_size;
                if ci >= self.chunks.len() {
                    break;
                }
                let off_in_chunk = pos % chunk_size;
                if off_in_chunk >= self.chunks[ci].len() {
                    break;
                }
                let avail = self.chunks[ci].len() - off_in_chunk;
                let take = std::cmp::min(avail, end - pos);
                buf.extend_from_slice(&self.chunks[ci][off_in_chunk..off_in_chunk + take]);
                pos += take;
            }
            if buf.len() != len {
                warn!(
                    "multi-chunk read: expected {len} bytes but assembled {}, \
                     chunks may be truncated or corrupted",
                    buf.len()
                );
                return Err(libc::EIO);
            }
            Ok(Bytes::from(buf))
        }
    }
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

/// Copy Python buffers into owned `Bytes` objects.
///
/// We intentionally avoid zero-copy (`Bytes::from_owner` wrapping
/// `PyBuffer`) because `PyBuffer::drop` calls `PyBuffer_Release` which
/// requires the GIL. If a FUSE task is still alive when the tokio
/// runtime shuts down via atexit, the `PyBuffer` would be dropped on a
/// tokio worker thread during interpreter finalization, causing a
/// segfault.
fn copy_py_buffers(chunks: Vec<pyo3::buffer::PyBuffer<u8>>) -> Vec<Bytes> {
    chunks
        .into_iter()
        .map(|buf| {
            // SAFETY: buf is a live PyBuffer and we hold the GIL.
            // buf_ptr() is valid for len_bytes() bytes for the lifetime
            // of `buf`. Bytes::copy_from_slice performs a full memcpy
            // before this closure ends and `buf` is dropped.
            let slice =
                unsafe { std::slice::from_raw_parts(buf.buf_ptr() as *const u8, buf.len_bytes()) };
            Bytes::copy_from_slice(slice)
        })
        .collect()
}

// --- FUSE filesystem ---

struct ChunkedFuseFs {
    data: Arc<RwLock<FsData>>,
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
        let data = self.data.read().unwrap();
        let entry = data.lookup_entry(&path).ok_or_else(Errno::new_not_exist)?;
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
        let data = self.data.read().unwrap();
        let entry = data.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
        Ok(ReplyAttr {
            ttl: self.ttl,
            attr: *entry.attr(),
        })
    }

    async fn readlink(&self, _req: Request, path: &OsStr) -> FuseResult<ReplyData> {
        let data = self.data.read().unwrap();
        let entry = data.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
        match entry {
            FsEntry::Symlink { link_target, .. } => Ok(ReplyData {
                data: Bytes::copy_from_slice(link_target.as_encoded_bytes()),
            }),
            _ => Err(libc::EINVAL.into()),
        }
    }

    async fn open(&self, _req: Request, path: &OsStr, flags: u32) -> FuseResult<ReplyOpen> {
        let data = self.data.read().unwrap();
        let entry = data.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
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
        let data = self.data.read().unwrap();
        let entry = data.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
        match entry {
            FsEntry::File {
                global_offset,
                file_len,
                ..
            } => {
                let result = data
                    .read_data(*global_offset, *file_len, offset, size)
                    .map_err(Errno::from)?;
                Ok(ReplyData { data: result })
            }
            _ => Err(libc::EINVAL.into()),
        }
    }

    async fn opendir(&self, _req: Request, path: &OsStr, flags: u32) -> FuseResult<ReplyOpen> {
        let data = self.data.read().unwrap();
        let entry = data.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
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
        let data = self.data.read().unwrap();
        let entry = data.lookup_entry(parent).ok_or_else(Errno::new_not_exist)?;
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
                if let Some(child_entry) = data.lookup_entry(&child_path) {
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
        let data = self.data.read().unwrap();
        let entry = data.lookup_entry(parent).ok_or_else(Errno::new_not_exist)?;
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
            let dotdot_attr = match data.lookup_entry(&parent_key) {
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
                if let Some(child_entry) = data.lookup_entry(&child_path) {
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
        let data = self.data.read().unwrap();
        if data.lookup_entry(path).is_some() {
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
    data: Arc<RwLock<FsData>>,
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

    /// Atomically replace filesystem data (metadata + chunks) without
    /// unmounting. Ongoing reads see either the old or new data, never
    /// a partial mix. Sleeps 3x the FUSE TTL after swapping to ensure
    /// kernel caches have expired before returning.
    fn refresh(
        &self,
        py: Python<'_>,
        metadata: &Bound<'_, PyDict>,
        chunks: Vec<pyo3::buffer::PyBuffer<u8>>,
        chunk_size: usize,
    ) -> PyResult<()> {
        let metadata = extract_metadata(metadata)?;
        let chunks = copy_py_buffers(chunks);
        let new_data = FsData {
            metadata,
            chunks,
            chunk_size,
        };
        let mut guard = self.data.write().unwrap();
        *guard = new_data;
        drop(guard);
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
///     chunks: list of memoryview/bytes chunks.
///     chunk_size: size of each chunk in bytes.
///     mount_point: path to mount the filesystem.
/// Returns a FuseMountHandle. Call handle.unmount() to unmount, or
/// handle.refresh() to atomically swap the data.
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
    let chunks = copy_py_buffers(chunks);

    let data = Arc::new(RwLock::new(FsData {
        metadata,
        chunks,
        chunk_size,
    }));

    let fs = ChunkedFuseFs {
        data: data.clone(),
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
        data,
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
