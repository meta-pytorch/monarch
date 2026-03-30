/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Read-only FUSE filesystem backed by a Python `GatherClientActor`.
//!
//! Every FUSE operation (getattr, readdir, read, …) calls the actor via:
//!   1. Briefly acquire the GIL to call `actor.endpoint.call_one(args)`
//!      and extract the inner `PyPythonTask` Rust future.
//!   2. Release the GIL entirely.
//!   3. `.await` the Rust future on the Tokio runtime — the actor's asyncio
//!      event loop runs the endpoint (acquiring the GIL as needed) and sends
//!      the result back via Tokio channels, so no GIL is held while we wait.
//!   4. Re-acquire the GIL to decode the `Py<PyAny>` result.
//!
//! No blocking thread pool (`spawn_blocking`) is needed.

use std::ffi::OsStr;
use std::ffi::OsString;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use fuse3::Errno;
use fuse3::FileType;
use fuse3::MountOptions;
use fuse3::Result as FuseResult;
use fuse3::path::prelude::*;
use futures::Future;
use futures::stream;
use monarch_hyperactor::pytokio::PyPythonTask;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::oneshot;
use tracing::warn;

const TTL: Duration = Duration::ZERO;

// ── Helpers for extracting and awaiting actor futures ────────────────────────

/// The concrete future type returned by [`take_actor_future`].
type ActorFuture = Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send + 'static>>;

/// Extract the inner Rust `Future` from the Python `Future` object returned
/// by `actor.endpoint.call_one(args)`.
///
/// Python layout:
/// ```text
/// Future._status  →  _Unawaited(coro=PyPythonTask)
/// ```
/// `PyPythonTask.take_task()` consumes the task and returns the raw future.
fn take_actor_future(py_future: Bound<'_, PyAny>) -> PyResult<ActorFuture> {
    let coro: Bound<'_, PyPythonTask> = py_future
        .getattr("_status")?
        .getattr("coro")?
        .downcast_into()?;
    coro.borrow_mut().take_task()
}

/// Call `actor.<endpoint>.call_one(<args>)` while holding the GIL, extract
/// the inner Rust future, then release the GIL and return the future.
///
/// The returned future can be `.await`ed on the Tokio runtime without holding
/// the GIL.  The actor's event loop will acquire the GIL to run the endpoint
/// function, but this does not conflict because we have already released it.
fn start_actor_call<'py, A>(
    actor: &Bound<'py, PyAny>,
    endpoint: &str,
    args: A,
) -> PyResult<ActorFuture>
where
    A: pyo3::call::PyCallArgs<'py>,
{
    take_actor_future(actor.getattr(endpoint)?.call_method1("call_one", args)?)
}

// ── Python result decoders ────────────────────────────────────────────────────

fn f64_to_system_time(ts: f64) -> SystemTime {
    if ts >= 0.0 {
        UNIX_EPOCH + Duration::from_secs_f64(ts)
    } else {
        UNIX_EPOCH
    }
}

fn file_type_from_mode(mode: u32) -> FileType {
    match mode & 0o170000 {
        0o040000 => FileType::Directory,
        0o120000 => FileType::Symlink,
        _ => FileType::RegularFile,
    }
}

fn extract_stat_attr(dict: &Bound<'_, PyDict>) -> PyResult<FileAttr> {
    fn get<'py, T: FromPyObject<'py>>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<T> {
        dict.get_item(key)?
            .ok_or_else(|| PyRuntimeError::new_err(format!("missing stat key: {key}")))?
            .extract()
    }
    let mode: u32 = get(dict, "st_mode")?;
    let size: u64 = get(dict, "st_size")?;
    Ok(FileAttr {
        size,
        blocks: size.div_ceil(512),
        atime: f64_to_system_time(get(dict, "st_atime")?),
        mtime: f64_to_system_time(get(dict, "st_mtime")?),
        ctime: f64_to_system_time(get(dict, "st_ctime")?),
        kind: file_type_from_mode(mode),
        perm: (mode & 0o7777) as u16,
        nlink: get(dict, "st_nlink")?,
        uid: get(dict, "st_uid")?,
        gid: get(dict, "st_gid")?,
        rdev: 0,
        blksize: 4096,
    })
}

/// Decode `actor.getattr_path.call_one(path).get()` result.
/// Returns `Ok(Some(attr))`, `Ok(None)` for errno, or `Err` for Python errors.
fn decode_getattr(result: Bound<'_, PyAny>) -> PyResult<FuseResult<FileAttr>> {
    if let Ok(errno) = result.extract::<libc::c_int>() {
        return Ok(Err(Errno::from(errno)));
    }
    Ok(Ok(extract_stat_attr(result.downcast::<PyDict>()?)?))
}

fn decode_readdir(result: Bound<'_, PyAny>) -> PyResult<FuseResult<Vec<String>>> {
    if let Ok(errno) = result.extract::<libc::c_int>() {
        return Ok(Err(Errno::from(errno)));
    }
    Ok(Ok(result.extract::<Vec<String>>()?))
}

fn decode_read(result: Bound<'_, PyAny>) -> PyResult<FuseResult<Bytes>> {
    if let Ok(errno) = result.extract::<libc::c_int>() {
        return Ok(Err(Errno::from(errno)));
    }
    Ok(Ok(Bytes::from(result.extract::<Vec<u8>>()?)))
}

// ── Per-operation async helpers ───────────────────────────────────────────────

/// Maps Python errors and FUSE errors to a single `FuseResult<T>`.
fn py_err_to_fuse(e: PyErr) -> Errno {
    warn!("gather_fuse actor error: {e}");
    Errno::from(libc::EIO)
}

async fn do_getattr(actor: &Arc<Py<PyAny>>, path: String) -> FuseResult<FileAttr> {
    let fut = Python::attach(|py| start_actor_call(actor.bind(py), "getattr_path", (&path,)))
        .map_err(py_err_to_fuse)?;
    let raw = fut.await.map_err(py_err_to_fuse)?;
    Python::attach(|py| decode_getattr(raw.bind(py).clone())).map_err(py_err_to_fuse)?
}

async fn do_readdir(actor: &Arc<Py<PyAny>>, path: String) -> FuseResult<Vec<String>> {
    let fut = Python::attach(|py| start_actor_call(actor.bind(py), "readdir_path", (&path,)))
        .map_err(py_err_to_fuse)?;
    let raw = fut.await.map_err(py_err_to_fuse)?;
    Python::attach(|py| decode_readdir(raw.bind(py).clone())).map_err(py_err_to_fuse)?
}

async fn do_read(
    actor: &Arc<Py<PyAny>>,
    path: String,
    size: u32,
    offset: u64,
) -> FuseResult<Bytes> {
    let fut =
        Python::attach(|py| start_actor_call(actor.bind(py), "read_path", (&path, size, offset)))
            .map_err(py_err_to_fuse)?;
    let raw = fut.await.map_err(py_err_to_fuse)?;
    Python::attach(|py| decode_read(raw.bind(py).clone())).map_err(py_err_to_fuse)?
}

// ── Path utilities ─────────────────────────────────────────────────────────────

fn join_path(parent: &OsStr, name: &OsStr) -> String {
    let p = parent.to_string_lossy();
    let n = name.to_string_lossy();
    if p == "/" {
        format!("/{n}")
    } else {
        format!("{p}/{n}")
    }
}

// ── FUSE filesystem ───────────────────────────────────────────────────────────

struct GatherMountFs {
    client_actor: Arc<Py<PyAny>>,
}

impl PathFilesystem for GatherMountFs {
    type DirEntryStream<'a> = stream::Iter<std::vec::IntoIter<FuseResult<DirectoryEntry>>>;
    type DirEntryPlusStream<'a> = stream::Iter<std::vec::IntoIter<FuseResult<DirectoryEntryPlus>>>;

    async fn init(&self, _req: Request) -> FuseResult<ReplyInit> {
        Ok(ReplyInit {
            max_write: NonZeroU32::new(16 * 1024).expect("non-zero"),
        })
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> FuseResult<ReplyEntry> {
        let path = join_path(parent, name);
        let attr = do_getattr(&self.client_actor, path).await?;
        Ok(ReplyEntry { ttl: TTL, attr })
    }

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _flags: u32,
    ) -> FuseResult<ReplyAttr> {
        let path_str = path
            .ok_or_else(Errno::new_not_exist)?
            .to_string_lossy()
            .into_owned();
        let attr = do_getattr(&self.client_actor, path_str).await?;
        Ok(ReplyAttr { ttl: TTL, attr })
    }

    async fn open(&self, _req: Request, _path: &OsStr, flags: u32) -> FuseResult<ReplyOpen> {
        let write_flags =
            (libc::O_WRONLY | libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC | libc::O_APPEND) as u32;
        if flags & write_flags != 0 {
            return Err(Errno::from(libc::EACCES));
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
        let path_str = path
            .ok_or_else(Errno::new_not_exist)?
            .to_string_lossy()
            .into_owned();
        let data = do_read(&self.client_actor, path_str, size, offset).await?;
        Ok(ReplyData { data })
    }

    async fn opendir(&self, _req: Request, _path: &OsStr, flags: u32) -> FuseResult<ReplyOpen> {
        Ok(ReplyOpen { fh: 0, flags })
    }

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        parent: &'a OsStr,
        _fh: u64,
        offset: i64,
    ) -> FuseResult<ReplyDirectory<Self::DirEntryStream<'a>>> {
        let path_str = parent.to_string_lossy().into_owned();
        let names = do_readdir(&self.client_actor, path_str).await?;

        let mut entries: Vec<FuseResult<DirectoryEntry>> = Vec::new();
        let mut idx: i64 = 1;

        if offset < idx {
            entries.push(Ok(DirectoryEntry {
                kind: FileType::Directory,
                name: OsString::from("."),
                offset: idx,
            }));
        }
        idx += 1;

        if offset < idx {
            entries.push(Ok(DirectoryEntry {
                kind: FileType::Directory,
                name: OsString::from(".."),
                offset: idx,
            }));
        }
        idx += 1;

        for name in names {
            if offset < idx {
                entries.push(Ok(DirectoryEntry {
                    kind: FileType::RegularFile, // hint; kernel calls getattr for truth
                    name: OsString::from(&name),
                    offset: idx,
                }));
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
        let path_str = parent.to_string_lossy().into_owned();

        // Fetch parent attr and child names concurrently, then attrs for children.
        let parent_attr = do_getattr(&self.client_actor, path_str.clone())
            .await
            .unwrap_or(FileAttr {
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o555,
                nlink: 2,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 4096,
            });

        let names = do_readdir(&self.client_actor, path_str.clone()).await?;

        // Fetch attrs for each child (cached by GatherClientActor after first access).
        let mut children: Vec<(String, Option<FileAttr>)> = Vec::with_capacity(names.len());
        for name in names {
            let child_path = if path_str == "/" {
                format!("/{name}")
            } else {
                format!("{path_str}/{name}")
            };
            let attr = do_getattr(&self.client_actor, child_path).await.ok();
            children.push((name, attr));
        }

        let mut entries: Vec<FuseResult<DirectoryEntryPlus>> = Vec::new();
        let mut idx: u64 = 1;

        if offset < idx {
            entries.push(Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: OsString::from("."),
                offset: idx as i64,
                attr: parent_attr,
                entry_ttl: TTL,
                attr_ttl: TTL,
            }));
        }
        idx += 1;

        if offset < idx {
            entries.push(Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: OsString::from(".."),
                offset: idx as i64,
                attr: parent_attr,
                entry_ttl: TTL,
                attr_ttl: TTL,
            }));
        }
        idx += 1;

        for (name, maybe_attr) in children {
            if offset < idx {
                if let Some(attr) = maybe_attr {
                    entries.push(Ok(DirectoryEntryPlus {
                        kind: attr.kind,
                        name: OsString::from(&name),
                        offset: idx as i64,
                        attr,
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

    async fn access(&self, _req: Request, _path: &OsStr, mask: u32) -> FuseResult<()> {
        if mask & libc::W_OK as u32 != 0 {
            return Err(Errno::from(libc::EACCES));
        }
        Ok(())
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

// ── PyO3 bindings ─────────────────────────────────────────────────────────────

/// Handle to a running gather_mount FUSE session.  Call `unmount()` to stop.
#[pyclass(
    name = "GatherMountHandle",
    module = "monarch._rust_bindings.monarch_extension.gather_fuse"
)]
struct PyGatherMountHandle {
    unmount_tx: Option<oneshot::Sender<()>>,
    mount_point: String,
}

#[pymethods]
impl PyGatherMountHandle {
    /// Unmount the FUSE filesystem using `fusermount3 -uz` (lazy unmount).
    fn unmount(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.unmount_tx.take().is_none() {
            return Ok(());
        }
        let mount_point = self.mount_point.clone();
        py.detach(|| {
            for cmd in &["fusermount3", "fusermount"] {
                match std::process::Command::new(cmd)
                    .args(["-uz", &mount_point])
                    .output()
                {
                    Ok(o) if o.status.success() => return Ok(()),
                    Ok(_) if *cmd == "fusermount3" => continue,
                    Ok(o) => {
                        let stderr = String::from_utf8_lossy(&o.stderr);
                        return Err(PyRuntimeError::new_err(format!(
                            "{cmd} -uz {mount_point} failed: {stderr}"
                        )));
                    }
                    Err(_) if *cmd == "fusermount3" => continue,
                    Err(e) => {
                        return Err(PyRuntimeError::new_err(format!("failed to run {cmd}: {e}")));
                    }
                }
            }
            Ok(())
        })
    }
}

/// Mount a read-only gather_mount FUSE filesystem.
///
/// Args:
///     client_actor: the `GatherClientActor` Python object.
///     mount_point: absolute path where the filesystem will be mounted.
///
/// Returns a `GatherMountHandle`.  Call `handle.unmount()` to unmount.
#[pyfunction]
fn mount_gather_fuse(
    py: Python<'_>,
    client_actor: Py<PyAny>,
    mount_point: String,
) -> PyResult<PyGatherMountHandle> {
    if !mount_point.starts_with('/') {
        return Err(PyRuntimeError::new_err(
            "mount_point must be an absolute path",
        ));
    }

    let fs = GatherMountFs {
        client_actor: Arc::new(client_actor),
    };

    let mount_path = mount_point.clone();
    let (unmount_tx, unmount_rx) = oneshot::channel::<()>();
    let (ready_tx, ready_rx) = oneshot::channel::<Result<(), String>>();

    let runtime = monarch_hyperactor::runtime::get_tokio_runtime();
    runtime.spawn(async move {
        let mut opts = MountOptions::default();
        opts.read_only(true).force_readdir_plus(true);

        match fuse3::path::Session::new(opts)
            .mount_with_unprivileged(fs, &mount_path)
            .await
        {
            Ok(handle) => {
                let _ = ready_tx.send(Ok(()));
                if let Err(e) = handle.await {
                    warn!("gather_fuse session error: {e}");
                }
            }
            Err(e) => {
                warn!("gather_fuse mount failed: {e}");
                let _ = ready_tx.send(Err(format!("{e}")));
            }
        }
        drop(unmount_rx);
    });

    #[allow(clippy::disallowed_methods)]
    py.detach(|| {
        let start = std::time::Instant::now();
        let mut ready_rx = Some(ready_rx);
        while start.elapsed() < Duration::from_secs(50) {
            if let Some(rx) = ready_rx.as_mut() {
                match rx.try_recv() {
                    Ok(Err(e)) => {
                        return Err(PyRuntimeError::new_err(format!(
                            "gather_fuse mount failed: {e}"
                        )));
                    }
                    Ok(Ok(())) => ready_rx = None,
                    Err(oneshot::error::TryRecvError::Closed) => {
                        return Err(PyRuntimeError::new_err(
                            "gather_fuse mount task exited without reporting status",
                        ));
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {}
                }
            }

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
        Err(PyRuntimeError::new_err(
            "timed out waiting for gather_fuse mount",
        ))
    })?;

    Ok(PyGatherMountHandle {
        unmount_tx: Some(unmount_tx),
        mount_point,
    })
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyGatherMountHandle>()?;
    let f = wrap_pyfunction!(mount_gather_fuse, module)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_extension.gather_fuse",
    )?;
    module.add_function(f)?;
    Ok(())
}
