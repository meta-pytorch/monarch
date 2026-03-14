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

use std::collections::HashMap;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::num::NonZeroU32;
use std::path::Path;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use fuse3::Errno;
use fuse3::FileType;
use fuse3::MountOptions;
use fuse3::Result as FuseResult;
use fuse3::path::prelude::*;
use futures_util::stream;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use serde::Deserialize;
use tokio::sync::oneshot;
use tracing::warn;

const TTL: Duration = Duration::from_secs(3600);

// --- Metadata deserialization from Python JSON ---

#[derive(Deserialize)]
struct PyFsEntry {
    attr: PyAttr,
    #[serde(default)]
    children: Option<Vec<String>>,
    #[serde(default)]
    link_target: Option<String>,
    #[serde(default)]
    global_offset: Option<usize>,
    #[serde(default)]
    file_len: Option<usize>,
}

#[derive(Deserialize)]
struct PyAttr {
    st_mode: u32,
    st_nlink: u32,
    st_uid: u32,
    st_gid: u32,
    st_size: u64,
    st_atime: f64,
    st_mtime: f64,
    st_ctime: f64,
}

// --- Internal filesystem types ---

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

fn f64_to_system_time(ts: f64) -> SystemTime {
    if ts >= 0.0 {
        UNIX_EPOCH + Duration::from_secs_f64(ts)
    } else {
        UNIX_EPOCH
    }
}

fn convert_attr(py: &PyAttr, kind: FileType) -> FileAttr {
    FileAttr {
        size: py.st_size,
        blocks: py.st_size.div_ceil(512),
        atime: f64_to_system_time(py.st_atime),
        mtime: f64_to_system_time(py.st_mtime),
        ctime: f64_to_system_time(py.st_ctime),
        kind,
        perm: (py.st_mode & 0o7777) as u16,
        nlink: py.st_nlink,
        uid: py.st_uid,
        gid: py.st_gid,
        rdev: 0,
        blksize: 4096,
    }
}

fn parse_metadata(raw: HashMap<String, PyFsEntry>) -> HashMap<OsString, FsEntry> {
    let mut entries = HashMap::with_capacity(raw.len());
    for (path, entry) in raw {
        let fs_entry = if let Some(link_target) = entry.link_target {
            FsEntry::Symlink {
                attr: convert_attr(&entry.attr, FileType::Symlink),
                link_target: OsString::from(link_target),
            }
        } else if let Some(children) = entry.children {
            FsEntry::Dir {
                attr: convert_attr(&entry.attr, FileType::Directory),
                children,
            }
        } else {
            FsEntry::File {
                attr: convert_attr(&entry.attr, FileType::RegularFile),
                global_offset: entry.global_offset.unwrap_or(0),
                file_len: entry.file_len.unwrap_or(0),
            }
        };
        entries.insert(OsString::from(path), fs_entry);
    }
    entries
}

// --- FUSE filesystem ---

struct ChunkedFuseFs {
    metadata: HashMap<OsString, FsEntry>,
    chunks: Vec<Bytes>,
    chunk_size: usize,
}

impl ChunkedFuseFs {
    fn lookup_entry(&self, path: &OsStr) -> Option<&FsEntry> {
        self.metadata.get(path)
    }

    fn read_data(&self, global_offset: usize, file_len: usize, offset: u64, size: u32) -> Bytes {
        let offset = offset as usize;
        if offset >= file_len {
            return Bytes::new();
        }
        let len = std::cmp::min(size as usize, file_len - offset);
        let start = global_offset + offset;
        let end = start + len;

        let chunk_size = self.chunk_size;
        let start_chunk = start / chunk_size;
        let end_chunk = (end.saturating_sub(1)) / chunk_size;

        if start_chunk == end_chunk && start_chunk < self.chunks.len() {
            // Fast path: single chunk
            let chunk_offset = start % chunk_size;
            self.chunks[start_chunk].slice(chunk_offset..chunk_offset + len)
        } else {
            // Multi-chunk: assemble
            let mut buf = Vec::with_capacity(len);
            let mut pos = start;
            while pos < end {
                let ci = pos / chunk_size;
                if ci >= self.chunks.len() {
                    break;
                }
                let off_in_chunk = pos % chunk_size;
                let avail = self.chunks[ci].len() - off_in_chunk;
                let take = std::cmp::min(avail, end - pos);
                buf.extend_from_slice(&self.chunks[ci][off_in_chunk..off_in_chunk + take]);
                pos += take;
            }
            Bytes::from(buf)
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
        let path = Self::join_path(parent, name);
        let entry = self.lookup_entry(&path).ok_or_else(Errno::new_not_exist)?;
        Ok(ReplyEntry {
            ttl: TTL,
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
        let entry = self.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
        Ok(ReplyAttr {
            ttl: TTL,
            attr: *entry.attr(),
        })
    }

    async fn readlink(&self, _req: Request, path: &OsStr) -> FuseResult<ReplyData> {
        let entry = self.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
        match entry {
            FsEntry::Symlink { link_target, .. } => Ok(ReplyData {
                data: Bytes::copy_from_slice(link_target.as_encoded_bytes()),
            }),
            _ => Err(libc::EINVAL.into()),
        }
    }

    async fn open(&self, _req: Request, path: &OsStr, flags: u32) -> FuseResult<ReplyOpen> {
        let entry = self.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
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
        let entry = self.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
        match entry {
            FsEntry::File {
                global_offset,
                file_len,
                ..
            } => Ok(ReplyData {
                data: self.read_data(*global_offset, *file_len, offset, size),
            }),
            _ => Err(libc::EISDIR.into()),
        }
    }

    async fn opendir(&self, _req: Request, path: &OsStr, flags: u32) -> FuseResult<ReplyOpen> {
        let entry = self.lookup_entry(path).ok_or_else(Errno::new_not_exist)?;
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
        let entry = self.lookup_entry(parent).ok_or_else(Errno::new_not_exist)?;
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
                let child_path = Self::join_path(parent, OsStr::new(child_name));
                if let Some(child_entry) = self.lookup_entry(&child_path) {
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
        let entry = self.lookup_entry(parent).ok_or_else(Errno::new_not_exist)?;
        let children = match entry {
            FsEntry::Dir { children, .. } => children,
            _ => return Err(Errno::new_is_not_dir()),
        };

        let mut entries: Vec<FuseResult<DirectoryEntryPlus>> = Vec::new();
        let mut idx: u64 = 1;

        // "." entry
        if offset < idx {
            entries.push(Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: OsString::from("."),
                offset: idx as i64,
                attr: *entry.attr(),
                entry_ttl: TTL,
                attr_ttl: TTL,
            }));
        }
        idx += 1;

        // ".." entry
        if offset < idx {
            // Use parent's own attr for ".." (simplification)
            entries.push(Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: OsString::from(".."),
                offset: idx as i64,
                attr: *entry.attr(),
                entry_ttl: TTL,
                attr_ttl: TTL,
            }));
        }
        idx += 1;

        for child_name in children {
            if offset < idx {
                let child_path = Self::join_path(parent, OsStr::new(child_name));
                if let Some(child_entry) = self.lookup_entry(&child_path) {
                    entries.push(Ok(DirectoryEntryPlus {
                        kind: child_entry.attr().kind,
                        name: OsString::from(child_name),
                        offset: idx as i64,
                        attr: *child_entry.attr(),
                        entry_ttl: TTL,
                        attr_ttl: TTL,
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
        if self.lookup_entry(path).is_some() {
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
}

#[pymethods]
impl PyMountHandle {
    fn unmount(&mut self) -> PyResult<()> {
        if let Some(tx) = self.unmount_tx.take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}

/// Mount a read-only FUSE filesystem from packed metadata and chunks.
///
/// Args:
///     metadata_json: JSON string of the metadata dict (path -> entry).
///     chunks: list of memoryview/bytes chunks.
///     chunk_size: size of each chunk in bytes.
///     mount_point: path to mount the filesystem.
///
/// Returns a FuseMountHandle. Call handle.unmount() to unmount.
#[pyfunction]
fn mount_chunked_fuse(
    py: Python<'_>,
    metadata_json: String,
    chunks: Vec<pyo3::buffer::PyBuffer<u8>>,
    chunk_size: usize,
    mount_point: String,
) -> PyResult<PyMountHandle> {
    if chunk_size == 0 {
        return Err(PyRuntimeError::new_err("chunk_size must be > 0"));
    }

    let raw_meta: HashMap<String, PyFsEntry> = serde_json::from_str(&metadata_json)
        .map_err(|e| PyRuntimeError::new_err(format!("failed to parse metadata: {e}")))?;
    let metadata = parse_metadata(raw_meta);
    // Zero-copy: wrap each Python buffer in a Bytes that keeps the
    // Python object alive. The PyBuffer holds a reference to the
    // underlying Python memoryview/mmap.
    struct PyBufOwner(pyo3::buffer::PyBuffer<u8>);
    // SAFETY: PyBuffer is Send — it holds a raw pointer to a Python buffer
    // object, but we only access it via buf_ptr()/len_bytes() which are safe
    // after the GIL is released. The buffer lifetime is tied to the Python object.
    unsafe impl Send for PyBufOwner {}
    // SAFETY: PyBufOwner is Sync because the underlying buffer data is immutable
    // (read-only FUSE mount) and PyBuffer's buf_ptr()/len_bytes() are safe to
    // call from any thread once the GIL is released.
    unsafe impl Sync for PyBufOwner {}
    impl AsRef<[u8]> for PyBufOwner {
        fn as_ref(&self) -> &[u8] {
            // SAFETY: buf_ptr() returns a valid pointer to the buffer data, and
            // len_bytes() returns its length. The PyBuffer keeps the Python object
            // alive, guaranteeing the pointer remains valid for the lifetime of self.
            unsafe { std::slice::from_raw_parts(self.0.buf_ptr() as *const u8, self.0.len_bytes()) }
        }
    }
    let chunks: Vec<Bytes> = chunks
        .into_iter()
        .map(|buf| Bytes::from_owner(PyBufOwner(buf)))
        .collect();

    let fs = ChunkedFuseFs {
        metadata,
        chunks,
        chunk_size,
    };

    let mount_path = mount_point.clone();
    let (unmount_tx, unmount_rx) = oneshot::channel::<()>();

    let runtime = monarch_hyperactor::runtime::get_tokio_runtime();
    runtime.spawn(async move {
        let mut opts = MountOptions::default();
        opts.read_only(true).force_readdir_plus(true);

        let mount_result = fuse3::path::Session::new(opts)
            .mount_with_unprivileged(fs, &mount_path)
            .await;

        match mount_result {
            Ok(mount_handle) => {
                // Wait for either unmount signal or FUSE session end.
                tokio::pin!(mount_handle);
                tokio::select! {
                    _ = unmount_rx => {
                        // unmount() consumes mount_handle, but it's pinned.
                        // Use fusermount3 -u instead.
                        let _ = tokio::process::Command::new("fusermount3")
                            .arg("-u")
                            .arg(&mount_path)
                            .output()
                            .await;
                    }
                    result = &mut mount_handle => {
                        if let Err(e) = result {
                            warn!("fuse session error: {e}");
                        }
                    }
                }
            }
            Err(e) => {
                warn!("fuse mount failed: {e}");
            }
        }
    });

    // Poll until mount appears (release GIL while waiting).
    // We use blocking std::thread::sleep here because this runs in a
    // synchronous PyO3 context — the async Clock::sleep is not available.
    #[allow(clippy::disallowed_methods)]
    py.detach(|| {
        let start = std::time::Instant::now();
        let mount_path = Path::new(&mount_point);
        while start.elapsed() < Duration::from_secs(50) {
            if mount_path.exists() && mount_path.is_dir() {
                // Check if it's actually a mount point by reading /proc/mounts
                if let Ok(mounts) = std::fs::read_to_string("/proc/mounts") {
                    if mounts.contains(&mount_point) {
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
