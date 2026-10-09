/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! macOS read-only NFSv3 adapter for the same actor endpoints as readonly_fuse.
//! Only the protocol and mount lifecycle live here; the gather actor owns all
//! remote filesystem discovery, caching and invalidation.

use std::collections::HashMap;
use std::io;
use std::net::TcpListener as StdTcpListener;
use std::pin::Pin;
use std::process::Command;
use std::process::ExitStatus;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use futures::Future;
use monarch_gil::GilSite;
use monarch_gil::monarch_with_gil_blocking;
use monarch_hyperactor::pytokio::PyPythonTask;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;

const LISTEN_ADDRESS: &str = "127.0.0.1:0";
const ROOT_FILE_ID: u64 = 1;
const FILE_SYSTEM_ID: u64 = 1;
const NFS_BLOCK_SIZE: u64 = 512;
const MAX_DIRECTORY_SNAPSHOTS: usize = 64;
const XDR_ALIGNMENT: usize = 4;
const RPC_RECORD_LAST_FRAGMENT: u32 = 1 << 31;
const MAX_RPC_REQUEST_BYTES: usize = 1024 * 1024;
const UNMOUNT_TIMEOUT: Duration = Duration::from_secs(5);

fn wait_command(command: &mut Command, timeout: Duration) -> io::Result<ExitStatus> {
    let mut child = command.spawn()?;
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            // The child may exit between try_wait and kill; always reap it.
            let _ = child.kill();
            child.wait()?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("unmount did not finish within {timeout:?}"),
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

// ONC RPC and NFSv3 wire assignments (RFCs 5531, 1833, and 1813).
const RPC_VERSION: u32 = 2;
const RPC_CALL: u32 = 0;
const RPC_REPLY: u32 = 1;
const RPC_MSG_ACCEPTED: u32 = 0;
const RPC_SUCCESS: u32 = 0;
const RPC_PROG_UNAVAIL: u32 = 1;
const RPC_PROG_MISMATCH: u32 = 2;
const RPC_PROC_UNAVAIL: u32 = 3;
const AUTH_NULL: u32 = 0;
const AUTH_SYS: u32 = 1;
const IPPROTO_TCP: u32 = 6;
const NFS_PROGRAM: u32 = 100003;
const MOUNT_PROGRAM: u32 = 100005;
const PORTMAP_PROGRAM: u32 = 100000;
const NFS_VERSION: u32 = 3;
const MOUNT_VERSION: u32 = 3;
const PORTMAP_VERSION: u32 = 2;

const NULL_PROC: u32 = 0;
const MOUNT_PROC_MNT: u32 = 1;
const MOUNT_PROC_UMNT: u32 = 3;
const MOUNT_PROC_EXPORT: u32 = 5;
const PORTMAP_PROC_GETPORT: u32 = 3;
const MOUNT_OK: u32 = 0;
const MOUNT_ERR_NOENT: u32 = 2;
const MOUNT_AUTH_FLAVOR_COUNT: u32 = 1;
const NFS_PROC_GETATTR: u32 = 1;
const NFS_PROC_LOOKUP: u32 = 3;
const NFS_PROC_ACCESS: u32 = 4;
const NFS_PROC_READ: u32 = 6;
const NFS_PROC_READDIR: u32 = 16;
const NFS_PROC_READDIRPLUS: u32 = 17;
const NFS_PROC_FSSTAT: u32 = 18;
const NFS_PROC_FSINFO: u32 = 19;
const NFS_PROC_PATHCONF: u32 = 20;

const NFS_OK: u32 = 0;
const NFS_ERR_NOENT: u32 = 2;
const NFS_ERR_NOTDIR: u32 = 20;
const NFS_ERR_ISDIR: u32 = 21;
const NFS_ERR_STALE: u32 = 70;
const NFS_ERR_IO: u32 = 5;
const NFS_ERR_BAD_COOKIE: u32 = 10003;
const NFS_TYPE_FILE: u32 = 1;
const NFS_TYPE_DIRECTORY: u32 = 2;
const NFS_TYPE_SYMLINK: u32 = 5;
const POSIX_FILE_TYPE_MASK: u32 = 0o170000;
const POSIX_DIRECTORY: u32 = 0o040000;
const POSIX_SYMLINK: u32 = 0o120000;
const NFS_ACCESS_READ: u32 = 0x01;
const NFS_ACCESS_LOOKUP: u32 = 0x02;
const NFS_ACCESS_EXECUTE: u32 = 0x20;
const POSIX_EXECUTE_BITS: u32 = 0o111;
const NFS_READ_BUFFER_SIZE: u32 = 65536;
const NFS_DIRECTORY_REPLY_OVERHEAD: usize = 128;
const NFS_DIRECTORY_ENTRY_OVERHEAD: usize = 128;
const NFS_MAX_FILE_SIZE: u64 = 1 << 40;
const FSINFO_TRANSFER_SIZE_FIELDS: usize = 7; // rtmax/rtpref/rtmult, wtmax/wtpref/wtmult, dtpref
const FSINFO_TIME_DELTA_NANOS: u32 = 1;
const FSSTAT_TOTAL_BYTES: u64 = 1 << 30;
const FSSTAT_AVAILABLE_BYTES: u64 = 1 << 29;
const FSSTAT_TOTAL_FILES: u64 = 1024;
const FSSTAT_AVAILABLE_FILES: u64 = 1022;
const MAX_LINK_COUNT: u32 = 1024;
const MAX_FILE_NAME_LENGTH: u32 = 255;

#[derive(Clone)]
struct Stat {
    mode: u32,
    size: u64,
    nlink: u32,
    uid: u32,
    gid: u32,
    atime: f64,
    mtime: f64,
    ctime: f64,
}

impl Stat {
    fn is_dir(&self) -> bool {
        self.mode & POSIX_FILE_TYPE_MASK == POSIX_DIRECTORY
    }
}

fn stat_field<'py, T: for<'a> FromPyObject<'a, 'py>>(
    dict: &Bound<'py, PyDict>,
    key: &str,
) -> Result<T, u32> {
    dict.get_item(key)
        .ok()
        .flatten()
        .ok_or(NFS_ERR_IO)?
        .extract()
        .map_err(|_| NFS_ERR_IO)
}

#[derive(Default)]
struct Paths {
    by_id: HashMap<u64, String>,
    by_path: HashMap<String, u64>,
    next_id: u64,
}

struct NfsServer {
    actor: Py<PyAny>,
    paths: Mutex<Paths>,
    directory_snapshots: Mutex<DirectorySnapshots>,
    port: u16,
}

#[derive(Default)]
struct DirectorySnapshots {
    next_verifier: u64,
    slots: Vec<DirectorySnapshot>,
}

#[derive(Clone)]
struct DirectorySnapshot {
    directory_id: u64,
    verifier: u64,
    entries: Arc<Vec<String>>,
}

impl DirectorySnapshots {
    fn insert(&mut self, id: u64, entries: Vec<String>) -> DirectorySnapshot {
        let verifier = self.next_verifier.wrapping_add(1).max(1);
        self.next_verifier = verifier;
        let snapshot = DirectorySnapshot {
            directory_id: id,
            verifier,
            entries: Arc::new(entries),
        };
        // Verifier 1 belongs to slot 0; each subsequent verifier advances a slot.
        let slot = ((verifier - 1) % MAX_DIRECTORY_SNAPSHOTS as u64) as usize;
        if self.slots.len() < MAX_DIRECTORY_SNAPSHOTS {
            self.slots.push(snapshot.clone());
        } else {
            self.slots[slot] = snapshot.clone();
        }
        snapshot
    }

    fn get(&self, id: u64, verifier: u64) -> Option<DirectorySnapshot> {
        let slot = ((verifier.checked_sub(1)?) % MAX_DIRECTORY_SNAPSHOTS as u64) as usize;
        self.slots
            .get(slot)
            .filter(|snapshot| snapshot.directory_id == id && snapshot.verifier == verifier)
            .cloned()
    }
}

enum ActorQuery<'a> {
    Getattr(&'a str),
    Readdir(&'a str),
    Read(&'a str, u32, u64),
}

impl NfsServer {
    fn new(actor: Py<PyAny>, port: u16) -> Self {
        let mut paths = Paths::default();
        paths.by_id.insert(ROOT_FILE_ID, "/".into());
        paths.by_path.insert("/".into(), ROOT_FILE_ID);
        paths.next_id = ROOT_FILE_ID + 1;
        Self {
            actor,
            paths: Mutex::new(paths),
            directory_snapshots: Mutex::new(DirectorySnapshots::default()),
            port,
        }
    }

    fn path(&self, id: u64) -> Result<String, u32> {
        self.paths
            .lock()
            .expect("NFS path table lock poisoned")
            .by_id
            .get(&id)
            .cloned()
            .ok_or(NFS_ERR_STALE)
    }

    fn id(&self, path: String) -> u64 {
        let mut paths = self.paths.lock().expect("NFS path table lock poisoned");
        if let Some(&id) = paths.by_path.get(&path) {
            return id;
        }
        let id = paths.next_id;
        paths.next_id += 1;
        paths.by_id.insert(id, path.clone());
        paths.by_path.insert(path, id);
        id
    }

    fn snapshot_directory(&self, id: u64, entries: Vec<String>) -> DirectorySnapshot {
        self.directory_snapshots
            .lock()
            .expect("NFS directory snapshot lock poisoned")
            .insert(id, entries)
    }

    fn directory_snapshot(&self, id: u64, verifier: u64) -> Option<DirectorySnapshot> {
        self.directory_snapshots
            .lock()
            .expect("NFS directory snapshot lock poisoned")
            .get(id, verifier)
    }

    async fn call(&self, query: ActorQuery<'_>) -> Result<Py<PyAny>, u32> {
        // The GIL must be released before awaiting the actor's Python task.
        let future: Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send>> =
            monarch_with_gil_blocking(GilSite::EndpointDispatch, |py| {
                let actor = self.actor.bind(py);
                let call = match query {
                    ActorQuery::Getattr(path) => actor
                        .getattr("getattr_path")?
                        .call_method1("call_one", (path,))?,
                    ActorQuery::Readdir(path) => actor
                        .getattr("readdir_path")?
                        .call_method1("call_one", (path,))?,
                    ActorQuery::Read(path, count, offset) => actor
                        .getattr("read_path")?
                        .call_method1("call_one", (path, count, offset))?,
                };
                let task: Bound<'_, PyPythonTask> =
                    call.call_method0("_take_inner")?.cast_into()?;
                task.borrow_mut().take_task()
            })
            .map_err(|e| {
                tracing::warn!("NFS actor call: {e}");
                NFS_ERR_IO
            })?;
        future.await.map_err(|e| {
            tracing::warn!("NFS actor reply: {e}");
            NFS_ERR_IO
        })
    }

    async fn getattr(&self, id: u64) -> Result<Stat, u32> {
        let path = self.path(id)?;
        let result = self.call(ActorQuery::Getattr(&path)).await?;
        monarch_with_gil_blocking(GilSite::ReplyConvert, |py| {
            let value = result.bind(py);
            if let Ok(errno) = value.extract::<u32>() {
                return Err(errno);
            }
            let dict = value.cast::<PyDict>().map_err(|_| NFS_ERR_IO)?;
            Ok(Stat {
                mode: stat_field(dict, "st_mode")?,
                size: stat_field(dict, "st_size")?,
                nlink: stat_field(dict, "st_nlink")?,
                uid: stat_field(dict, "st_uid")?,
                gid: stat_field(dict, "st_gid")?,
                atime: stat_field(dict, "st_atime")?,
                mtime: stat_field(dict, "st_mtime")?,
                ctime: stat_field(dict, "st_ctime")?,
            })
        })
    }

    async fn readdir(&self, path: &str) -> Result<Vec<String>, u32> {
        let result = self.call(ActorQuery::Readdir(path)).await?;
        monarch_with_gil_blocking(GilSite::ReplyConvert, |py| {
            let value = result.bind(py);
            if let Ok(errno) = value.extract::<u32>() {
                return Err(errno);
            }
            value.extract().map_err(|_| NFS_ERR_IO)
        })
    }

    async fn read(&self, path: &str, count: u32, offset: u64) -> Result<Vec<u8>, u32> {
        let result = self.call(ActorQuery::Read(path, count, offset)).await?;
        monarch_with_gil_blocking(GilSite::ReplyConvert, |py| {
            let value = result.bind(py);
            if let Ok(errno) = value.extract::<u32>() {
                return Err(errno);
            }
            value.extract().map_err(|_| NFS_ERR_IO)
        })
    }
}

struct Input<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Input<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(len)?;
        let bytes = self.data.get(self.pos..end)?;
        self.pos = end;
        Some(bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    fn opaque(&mut self) -> Option<&'a [u8]> {
        let len = self.u32()? as usize;
        let padded = len.checked_add(XDR_ALIGNMENT - 1)? & !(XDR_ALIGNMENT - 1);
        Some(&self.take(padded)?[..len])
    }

    fn handle(&mut self) -> Option<u64> {
        let bytes = self.opaque()?;
        Some(u64::from_be_bytes(bytes.try_into().ok()?))
    }

    fn auth(&mut self) -> Option<()> {
        self.u32()?; // credential flavor
        self.opaque()?; // opaque credential body
        Some(())
    }
}

#[derive(Default)]
struct Output(Vec<u8>);

impl Output {
    fn u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }

    fn bytes(&mut self, data: &[u8]) {
        self.0.extend_from_slice(data);
    }

    fn opaque(&mut self, data: &[u8]) {
        self.u32(data.len() as u32);
        self.bytes(data);
        self.0
            .resize(self.0.len().div_ceil(XDR_ALIGNMENT) * XDR_ALIGNMENT, 0);
    }

    fn string(&mut self, value: &str) {
        self.opaque(value.as_bytes());
    }

    fn handle(&mut self, id: u64) {
        self.opaque(&id.to_be_bytes());
    }

    fn attr(&mut self, id: u64, stat: &Stat) {
        self.u32(match stat.mode & POSIX_FILE_TYPE_MASK {
            POSIX_DIRECTORY => NFS_TYPE_DIRECTORY,
            POSIX_SYMLINK => NFS_TYPE_SYMLINK,
            _ => NFS_TYPE_FILE,
        });
        self.u32(stat.mode & 0o7777);
        self.u32(stat.nlink);
        self.u32(stat.uid);
        self.u32(stat.gid);
        self.u64(stat.size);
        self.u64(stat.size.div_ceil(NFS_BLOCK_SIZE) * NFS_BLOCK_SIZE);
        self.u32(0); // rdev specdata1
        self.u32(0); // rdev specdata2
        self.u64(FILE_SYSTEM_ID);
        self.u64(id); // fileid
        for time in [stat.atime, stat.mtime, stat.ctime] {
            let time = time.max(0.0);
            self.u32(time as u32);
            self.u32((time.fract() * 1e9) as u32);
        }
    }

    fn post_attr(&mut self, id: u64, stat: Option<&Stat>) {
        self.bool(stat.is_some());
        if let Some(stat) = stat {
            self.attr(id, stat);
        }
    }

    fn bool(&mut self, value: bool) {
        self.u32(u32::from(value));
    }
}

// The response includes the RPC accepted-reply prefix. An unknown procedure
// gets PROC_UNAVAIL; a malformed request is dropped in this minimal server.
async fn reply(request: &[u8], server: &NfsServer) -> Option<Vec<u8>> {
    let mut input = Input::new(request);
    let xid = input.u32()?;
    if input.u32()? != RPC_CALL || input.u32()? != RPC_VERSION {
        return None; // not an ONC RPC v2 CALL
    }
    let program = input.u32()?;
    let version = input.u32()?;
    let procedure = input.u32()?;
    input.auth()?;
    input.auth()?;

    let mut out = Output::default();
    out.u32(xid);
    out.u32(RPC_REPLY);
    out.u32(RPC_MSG_ACCEPTED);
    out.u32(AUTH_NULL); // verifier flavor
    out.u32(0); // empty verifier
    let acceptable_version = match program {
        NFS_PROGRAM => version == NFS_VERSION,
        MOUNT_PROGRAM => version == MOUNT_VERSION,
        PORTMAP_PROGRAM => version == PORTMAP_VERSION,
        _ => false,
    };
    if !acceptable_version {
        if program == NFS_PROGRAM || program == MOUNT_PROGRAM || program == PORTMAP_PROGRAM {
            out.u32(RPC_PROG_MISMATCH);
            let supported = if program == PORTMAP_PROGRAM {
                PORTMAP_VERSION
            } else {
                NFS_VERSION
            };
            out.u32(supported); // lowest supported version
            out.u32(supported); // highest supported version
        } else {
            out.u32(RPC_PROG_UNAVAIL);
        }
        return Some(out.0);
    }

    let mut body = Output::default();
    let implemented = match (program, procedure) {
        (NFS_PROGRAM | MOUNT_PROGRAM | PORTMAP_PROGRAM, NULL_PROC) => true,
        (MOUNT_PROGRAM, MOUNT_PROC_MNT) => {
            if input.opaque()? == b"/" {
                body.u32(MOUNT_OK);
                body.handle(ROOT_FILE_ID);
                body.u32(MOUNT_AUTH_FLAVOR_COUNT);
                body.u32(AUTH_SYS);
            } else {
                body.u32(MOUNT_ERR_NOENT);
            }
            true
        }
        (MOUNT_PROGRAM, MOUNT_PROC_UMNT) => true, // UMNT is stateless
        (MOUNT_PROGRAM, MOUNT_PROC_EXPORT) => {
            body.bool(true); // one export
            body.string("/");
            body.bool(false); // no group restriction
            body.bool(false); // end of export list
            true
        }
        (PORTMAP_PROGRAM, PORTMAP_PROC_GETPORT) => {
            let wanted = input.u32()?;
            let vers = input.u32()?;
            let proto = input.u32()?;
            input.u32()?; // caller's port
            body.u32(
                if proto == IPPROTO_TCP
                    && ((wanted == NFS_PROGRAM && vers == NFS_VERSION)
                        || (wanted == MOUNT_PROGRAM && vers == MOUNT_VERSION))
                {
                    u32::from(server.port)
                } else {
                    0
                },
            );
            true
        }
        (NFS_PROGRAM, NFS_PROC_GETATTR) => {
            let id = input.handle()?;
            let stat = server.getattr(id).await;
            body.u32(stat.as_ref().map(|_| NFS_OK).unwrap_or_else(|e| *e));
            if let Ok(stat) = stat {
                body.attr(id, &stat);
            }
            true
        }
        (NFS_PROGRAM, NFS_PROC_LOOKUP) => {
            let parent = input.handle()?;
            let name = std::str::from_utf8(input.opaque()?).ok()?;
            let parent_stat = server.getattr(parent).await;
            let found: Result<(u64, Stat), u32> = async {
                let stat = parent_stat.as_ref().map_err(|e| *e)?;
                if !stat.is_dir() {
                    return Err(NFS_ERR_NOTDIR);
                }
                let path = server.path(parent)?;
                let child = match name {
                    "." => path,
                    ".." => path
                        .rsplit_once('/')
                        .map(|(p, _)| if p.is_empty() { "/" } else { p })
                        .unwrap_or("/")
                        .into(),
                    _ if name.is_empty() || name.contains('/') || name == ".." => {
                        return Err(NFS_ERR_NOENT);
                    }
                    _ if path == "/" => format!("/{name}"),
                    _ => format!("{path}/{name}"),
                };
                let id = server.id(child);
                server.getattr(id).await.map(|attr| (id, attr))
            }
            .await;
            body.u32(found.as_ref().map(|_| NFS_OK).unwrap_or_else(|e| *e));
            if let Ok((id, stat)) = found {
                body.handle(id);
                body.post_attr(id, Some(&stat));
            }
            body.post_attr(parent, parent_stat.as_ref().ok());
            true
        }
        (NFS_PROGRAM, NFS_PROC_ACCESS) => {
            let id = input.handle()?;
            let requested = input.u32()?;
            let stat = server.getattr(id).await;
            body.u32(stat.as_ref().map(|_| NFS_OK).unwrap_or_else(|e| *e));
            if let Ok(stat) = stat {
                body.post_attr(id, Some(&stat));
                let permitted = if stat.is_dir() {
                    NFS_ACCESS_READ | NFS_ACCESS_LOOKUP | NFS_ACCESS_EXECUTE
                } else {
                    NFS_ACCESS_READ
                        | if stat.mode & POSIX_EXECUTE_BITS != 0 {
                            NFS_ACCESS_EXECUTE
                        } else {
                            0
                        }
                };
                body.u32(requested & permitted);
            } else {
                body.bool(false);
            }
            true
        }
        (NFS_PROGRAM, NFS_PROC_READ) => {
            let id = input.handle()?;
            let offset = input.u64()?;
            let count = input.u32()?.min(NFS_READ_BUFFER_SIZE);
            let stat = server.getattr(id).await;
            let data: Result<Vec<u8>, u32> = async {
                let stat = stat.as_ref().map_err(|e| *e)?;
                if stat.is_dir() {
                    return Err(NFS_ERR_ISDIR);
                }
                server.read(&server.path(id)?, count, offset).await
            }
            .await;
            body.u32(data.as_ref().map(|_| NFS_OK).unwrap_or_else(|e| *e));
            body.post_attr(id, stat.as_ref().ok());
            if let Ok(bytes) = data {
                body.u32(bytes.len() as u32);
                body.bool(
                    stat.as_ref()
                        .is_ok_and(|s| offset.saturating_add(bytes.len() as u64) >= s.size),
                );
                body.opaque(&bytes);
            }
            true
        }
        (NFS_PROGRAM, NFS_PROC_READDIR | NFS_PROC_READDIRPLUS) => {
            let plus = procedure == NFS_PROC_READDIRPLUS;
            let id = input.handle()?;
            let cookie = input.u64()?;
            let cookie_verifier = input.u64()?;
            let count = input.u32()?; // count / dircount
            let maxcount = if plus { input.u32()? } else { count };
            let stat = server.getattr(id).await;
            let snapshot: Result<DirectorySnapshot, u32> = async {
                let stat = stat.as_ref().map_err(|e| *e)?;
                if !stat.is_dir() {
                    return Err(NFS_ERR_NOTDIR);
                }
                if cookie != 0 {
                    // A missing snapshot means its cookie verifier was evicted;
                    // the client must restart this directory listing.
                    let snapshot = server
                        .directory_snapshot(id, cookie_verifier)
                        .ok_or(NFS_ERR_BAD_COOKIE)?;
                    if cookie > snapshot.entries.len() as u64 {
                        return Err(NFS_ERR_BAD_COOKIE);
                    }
                    return Ok(snapshot);
                }
                let mut entries = server.readdir(&server.path(id)?).await?;
                entries.retain(|name| name != "." && name != ".." && !name.contains('/'));
                Ok(server.snapshot_directory(id, entries))
            }
            .await;
            body.u32(snapshot.as_ref().map(|_| NFS_OK).unwrap_or_else(|e| *e));
            body.post_attr(id, stat.as_ref().ok());
            if let Ok(snapshot) = snapshot {
                body.u64(snapshot.verifier);
                let entries = &snapshot.entries;
                let path = server.path(id).ok()?;
                // Keep each response below the client's requested byte count.
                let limit = (maxcount as usize)
                    .saturating_sub(NFS_DIRECTORY_REPLY_OVERHEAD)
                    .min(NFS_READ_BUFFER_SIZE as usize);
                let mut last = cookie;
                for (index, name) in entries.iter().enumerate().skip(cookie as usize) {
                    if body.0.len() + NFS_DIRECTORY_ENTRY_OVERHEAD + name.len() > limit
                        && last != cookie
                    {
                        break;
                    }
                    let child = if path == "/" {
                        format!("/{name}")
                    } else {
                        format!("{path}/{name}")
                    };
                    let child_id = server.id(child);
                    let child_stat = if plus {
                        server.getattr(child_id).await.ok()
                    } else {
                        None
                    };
                    body.bool(true); // entry follows
                    body.u64(child_id);
                    body.string(name);
                    last = index as u64 + 1;
                    body.u64(last);
                    if plus {
                        body.post_attr(child_id, child_stat.as_ref());
                        body.bool(true); // handle follows
                        body.handle(child_id);
                    }
                }
                body.bool(false); // end of entries
                body.bool(last as usize >= entries.len()); // EOF
            }
            true
        }
        (NFS_PROGRAM, NFS_PROC_FSSTAT | NFS_PROC_FSINFO | NFS_PROC_PATHCONF) => {
            let id = input.handle()?;
            let stat = server.getattr(id).await;
            body.u32(stat.as_ref().map(|_| NFS_OK).unwrap_or_else(|e| *e));
            body.post_attr(id, stat.as_ref().ok());
            if stat.is_ok() {
                match procedure {
                    NFS_PROC_FSSTAT => {
                        // total/free/available bytes, files and invarsec
                        for value in [
                            FSSTAT_TOTAL_BYTES,
                            FSSTAT_AVAILABLE_BYTES,
                            FSSTAT_AVAILABLE_BYTES,
                            FSSTAT_TOTAL_FILES,
                            FSSTAT_AVAILABLE_FILES,
                            FSSTAT_AVAILABLE_FILES,
                        ] {
                            body.u64(value);
                        }
                        body.u32(0);
                    }
                    NFS_PROC_FSINFO => {
                        for _ in 0..FSINFO_TRANSFER_SIZE_FIELDS {
                            body.u32(NFS_READ_BUFFER_SIZE); // read/write preferred sizes
                        }
                        body.u64(NFS_MAX_FILE_SIZE);
                        body.u32(0); // time delta seconds
                        body.u32(FSINFO_TIME_DELTA_NANOS);
                        body.u32(0); // no optional properties
                    }
                    _ => {
                        body.u32(MAX_LINK_COUNT);
                        body.u32(MAX_FILE_NAME_LENGTH);
                        body.bool(true); // no-trunc
                        body.bool(true); // chown restricted
                        body.bool(false); // case insensitive
                        body.bool(true); // case preserving
                    }
                }
            }
            true
        }
        _ => false,
    };
    out.u32(if implemented {
        RPC_SUCCESS
    } else {
        RPC_PROC_UNAVAIL
    });
    out.bytes(&body.0);
    Some(out.0)
}

async fn serve(mut stream: TcpStream, server: &NfsServer) -> io::Result<()> {
    loop {
        // ONC RPC over TCP uses record-marking. Combine fragments until the
        // high bit of the length header announces the final fragment.
        let mut request = Vec::new();
        loop {
            let mut header = [0; 4];
            match stream.read_exact(&mut header).await {
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e),
                Ok(_) => {}
            }
            let fragment = u32::from_be_bytes(header);
            let len = (fragment & !RPC_RECORD_LAST_FRAGMENT) as usize;
            if request.len().saturating_add(len) > MAX_RPC_REQUEST_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RPC request too large",
                ));
            }
            let start = request.len();
            request.resize(start + len, 0);
            stream.read_exact(&mut request[start..]).await?;
            if fragment & RPC_RECORD_LAST_FRAGMENT != 0 {
                break;
            }
        }
        if let Some(response) = reply(&request, server).await {
            let size = response.len() as u32 | RPC_RECORD_LAST_FRAGMENT;
            stream.write_all(&size.to_be_bytes()).await?;
            stream.write_all(&response).await?;
        }
    }
}

#[pyclass]
struct NfsMount {
    mountpoint: String,
    listener: Option<JoinHandle<()>>,
}

#[pymethods]
impl NfsMount {
    #[pyo3(signature = (timeout=5.0))]
    fn unmount(&mut self, py: Python<'_>, timeout: f64) -> PyResult<()> {
        let timeout = Duration::try_from_secs_f64(timeout)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?
            .min(UNMOUNT_TIMEOUT);
        let status = py
            .detach(|| wait_command(Command::new("/sbin/umount").arg(&self.mountpoint), timeout))
            .map_err(|e| pyo3::exceptions::PyOSError::new_err(e.to_string()))?;
        if !status.success() {
            return Err(pyo3::exceptions::PyOSError::new_err(format!(
                "umount {} exited with {status}",
                self.mountpoint
            )));
        }
        self.stop();
        Ok(())
    }
}

impl NfsMount {
    fn stop(&mut self) {
        if let Some(listener) = self.listener.take() {
            listener.abort();
        }
    }
}

impl Drop for NfsMount {
    fn drop(&mut self) {
        // Never implicitly unmount: other users of the mount may still exist.
        self.stop();
    }
}

#[pyfunction]
fn mount_read_only_nfs(py: Python<'_>, actor: Py<PyAny>, mountpoint: String) -> PyResult<NfsMount> {
    let listener = StdTcpListener::bind(LISTEN_ADDRESS)?;
    let port = listener.local_addr()?.port();
    listener.set_nonblocking(true)?;
    let runtime = monarch_hyperactor::runtime::get_tokio_runtime();
    let listener = {
        let _enter = runtime.enter();
        TcpListener::from_std(listener)?
    };
    let server = Arc::new(NfsServer::new(actor, port));
    let handle = runtime.spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => match accepted {
                Ok((stream, _peer)) => {
                    let server = server.clone();
                    connections.spawn(async move {
                        if let Err(e) = serve(stream, &server).await {
                            tracing::warn!("NFS client disconnected: {e}");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("NFS accept error: {e}");
                    break;
                }
                },
                finished = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(e)) = finished {
                        tracing::warn!("NFS connection task failed: {e}");
                    }
                }
            }
        }
    });
    let mut mount = NfsMount {
        mountpoint,
        listener: Some(handle),
    };
    let options = format!(
        "ro,vers=3,tcp,port={port},mountport={port},noresvport,nolocks,retrycnt=0,actimeo=0"
    );
    // mount_nfs can call back into the actor during mount (GETATTR/FSINFO).
    // Release the GIL while waiting for the external process to complete.
    let mounted = py.detach(|| {
        Command::new("/sbin/mount_nfs")
            .args(["-o", &options, "127.0.0.1:/", &mount.mountpoint])
            .output()
    });
    match mounted {
        Ok(output) if output.status.success() => Ok(mount),
        Ok(output) => {
            mount.stop();
            Err(pyo3::exceptions::PyOSError::new_err(format!(
                "mount_nfs exited with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )))
        }
        Err(e) => {
            mount.stop();
            Err(pyo3::exceptions::PyOSError::new_err(format!(
                "mount_nfs: {e}"
            )))
        }
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NfsMount>()?;
    module.add_function(wrap_pyfunction!(mount_read_only_nfs, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unmount_command_timeout_reaps_child() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "exec sleep 60"]);
        let start = Instant::now();
        let error = wait_command(&mut command, Duration::from_millis(20)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn unmount_command_preserves_failure_status() {
        let status = wait_command(
            Command::new("/bin/sh").args(["-c", "exit 7"]),
            Duration::from_secs(2),
        )
        .unwrap();
        assert_eq!(status.code(), Some(7));
    }
}
