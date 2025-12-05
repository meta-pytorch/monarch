/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use anyhow::ensure;
use async_tempfile::TempFile;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use filetime::FileTime;
use futures::SinkExt;
use futures::StreamExt;
use futures::try_join;
use globset::Glob;
use globset::GlobSet;
use globset::GlobSetBuilder;
use ignore::DirEntry;
use ignore::WalkBuilder;
use ignore::WalkState;
use itertools::Itertools;
use memmap2::MmapMut;
use serde::Deserialize;
use serde::Serialize;
use tokio::fs;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;
use tokio_util::codec::LengthDelimitedCodec;

use crate::diff::CondaFingerprint;
use crate::replace::ReplacerBuilder;

#[derive(Eq, PartialEq)]
enum Origin {
    Src,
    Dst,
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
enum FileTypeInfo {
    Directory,
    File(bool),
    Symlink,
}

impl FileTypeInfo {
    fn same(&self, other: &FileTypeInfo) -> bool {
        match (self, other) {
            (FileTypeInfo::Directory, FileTypeInfo::Directory) => true,
            (FileTypeInfo::File(_), FileTypeInfo::File(_)) => true,
            (FileTypeInfo::Symlink, FileTypeInfo::Symlink) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Metadata {
    mtime: SystemTime,
    ftype: FileTypeInfo,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Receive {
    File { executable: bool },
    Symlink,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Action {
    /// Delete a path.
    Delete { directory: bool },
    /// Create a directory.
    Directory,
    /// Receive the path contents from the sender.
    Receive(SystemTime, Receive),
}

#[derive(Debug, Serialize, Deserialize)]
struct FileSectionHeader {
    num: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileHeader {
    path: PathBuf,
    symlink: bool,
}

#[derive(Debug, Serialize, Deserialize)]
enum FileContents {
    Symlink(PathBuf),
    File(u64),
}

#[derive(Debug, Serialize, Deserialize)]
struct FileContentsHeader {
    path: PathBuf,
    contents: FileContents,
}

#[derive(Debug, Serialize, Deserialize)]
enum FileList {
    Entry(PathBuf, Metadata),
    Done,
}

struct ActionsBuilder {
    ignores: Option<GlobSet>,
    state: DashMap<PathBuf, (Origin, Metadata)>,
    actions: DashMap<PathBuf, Action>,
    mtime_comparator: Box<dyn Fn(&SystemTime, &SystemTime) -> Ordering + Send + Sync + 'static>,
}

impl ActionsBuilder {
    fn new_with(
        ignores: Option<GlobSet>,
        mtime_comparator: Box<dyn Fn(&SystemTime, &SystemTime) -> Ordering + Send + Sync + 'static>,
    ) -> Self {
        Self {
            ignores,
            state: DashMap::new(),
            actions: DashMap::new(),
            mtime_comparator,
        }
    }

    fn process(&self, origin: Origin, path: PathBuf, metadata: Metadata) -> Result<()> {
        match self.state.entry(path) {
            Entry::Occupied(val) => {
                let (path, (existing_origin, existing_metadata)) = val.remove_entry();
                if let Some(ignores) = &self.ignores {
                    if ignores.is_match(path.as_path()) {
                        return Ok(());
                    }
                }
                ensure!(existing_origin != origin);
                let (src, dst) = match origin {
                    Origin::Dst => (existing_metadata, metadata),
                    Origin::Src => (metadata, existing_metadata),
                };
                if src.ftype == FileTypeInfo::Directory && dst.ftype == FileTypeInfo::Directory {
                    // --omit-dir-times
                } else {
                    match (self.mtime_comparator)(&src.mtime, &dst.mtime) {
                        Ordering::Equal => {
                            ensure!(
                                src.ftype.same(&dst.ftype),
                                "{}: {:?} != {:?}",
                                path.display(),
                                dst,
                                src
                            );
                        }
                        Ordering::Greater | Ordering::Less => {
                            self.actions.insert(
                                path,
                                match src.ftype {
                                    FileTypeInfo::File(executable) => {
                                        Action::Receive(src.mtime, Receive::File { executable })
                                    }
                                    FileTypeInfo::Symlink => {
                                        Action::Receive(src.mtime, Receive::Symlink)
                                    }
                                    FileTypeInfo::Directory => Action::Directory,
                                },
                            );
                        }
                    }
                }
            }
            Entry::Vacant(entry) => {
                entry.insert((origin, metadata));
            }
        }
        Ok(())
    }

    fn process_src(&self, path: PathBuf, metadata: Metadata) -> Result<()> {
        self.process(Origin::Src, path, metadata)
    }

    fn process_dst(&self, path: PathBuf, metadata: Metadata) -> Result<()> {
        self.process(Origin::Dst, path, metadata)
    }

    fn into_actions(self) -> HashMap<PathBuf, Action> {
        let mut actions: HashMap<_, _> = self.actions.into_iter().collect();
        for (path, (origin, metadata)) in self.state.into_iter() {
            match origin {
                Origin::Src => {
                    if let Some(ignores) = &self.ignores {
                        if ignores.is_match(path.as_path()) {
                            continue;
                        }
                    }
                    actions.insert(
                        path,
                        match metadata.ftype {
                            FileTypeInfo::File(executable) => {
                                Action::Receive(metadata.mtime, Receive::File { executable })
                            }
                            FileTypeInfo::Directory => Action::Directory,
                            FileTypeInfo::Symlink => {
                                Action::Receive(metadata.mtime, Receive::Symlink)
                            }
                        },
                    );
                }
                Origin::Dst => {
                    actions.insert(
                        path,
                        Action::Delete {
                            directory: matches!(metadata.ftype, FileTypeInfo::Directory),
                        },
                    );
                }
            }
        }
        actions
    }
}

fn walk_dir<
    E: Into<anyhow::Error>,
    F: Fn(PathBuf, Metadata) -> Result<(), E> + Sync + Send + 'static,
>(
    src: PathBuf,
    callback: F,
) -> Result<()> {
    let (error_tx, error_rx) = channel();

    let src_handle = src.clone();
    let handle_ent = move |entry: DirEntry| -> Result<()> {
        let metadata = entry.metadata()?;
        callback(
            entry
                .path()
                .strip_prefix(src_handle.clone())
                .context("sub path")?
                .to_path_buf(),
            Metadata {
                mtime: UNIX_EPOCH
                    + Duration::new(
                        metadata.mtime().try_into()?,
                        metadata.mtime_nsec().try_into()?,
                    ),
                ftype: if metadata.file_type().is_file() {
                    let mode = metadata.permissions().mode();
                    FileTypeInfo::File(mode & 0o100 != 0)
                } else if metadata.file_type().is_dir() {
                    FileTypeInfo::Directory
                } else if metadata.file_type().is_symlink() {
                    FileTypeInfo::Symlink
                } else {
                    bail!("unexpected file type")
                },
            },
        )
        .map_err(Into::into)?;
        Ok(())
    };

    WalkBuilder::new(src)
        .standard_filters(true)
        .same_file_system(true)
        .build_parallel()
        .run(|| {
            Box::new(|ent| match ent.map_err(Into::into).and_then(&handle_ent) {
                Ok(()) => WalkState::Continue,
                Err(err) => {
                    error_tx.clone().send(err).unwrap();
                    WalkState::Quit
                }
            })
        });

    match error_rx.try_recv() {
        Ok(err) => Err(err),
        _ => Ok(()),
    }
}

pub async fn sender(
    src: &Path,
    from_receiver: impl AsyncRead + Unpin,
    to_receiver: impl AsyncWrite + Unpin,
) -> Result<()> {
    let mut to_receiver = FramedWrite::new(to_receiver, LengthDelimitedCodec::new());
    let mut from_receiver = FramedRead::new(from_receiver, LengthDelimitedCodec::new());

    let (ent_tx, mut ent_rx) = tokio::sync::mpsc::unbounded_channel();
    let src_clone = src.to_path_buf();
    try_join!(
        async {
            tokio::task::spawn_blocking(move || {
                walk_dir(src_clone.clone(), move |path, ent| ent_tx.send((path, ent)))
            })
            .await?
        },
        async {
            // Send conda env fingerprint
            let src_env = CondaFingerprint::from_env(src).await?;
            to_receiver
                .send(bincode::serialize(&src_env)?.into())
                .await
                .context("sending src conda fingerprint")?;
            to_receiver.flush().await?;

            // Send file lists.
            while let Some((path, metadata)) = ent_rx.recv().await {
                to_receiver
                    .send(bincode::serialize(&FileList::Entry(path, metadata))?.into())
                    .await
                    .context("sending file ent")?;
            }
            to_receiver
                .send(bincode::serialize(&FileList::Done)?.into())
                .await
                .context("sending file list end")?;
            to_receiver.flush().await?;

            anyhow::Ok(())
        },
    )?;

    // Convert back to raw stream to send file header + contents.
    to_receiver.flush().await?;
    let mut to_receiver = to_receiver.into_inner();

    let hdr: FileSectionHeader =
        bincode::deserialize(&from_receiver.next().await.context("header")??)?;
    for _ in 0..hdr.num {
        let FileHeader { path, symlink } =
            bincode::deserialize(&from_receiver.next().await.context("signature")??)?;
        let fpath = src.join(&path);
        if symlink {
            let header = FileContentsHeader {
                path,
                contents: FileContents::Symlink(fs::read_link(&fpath).await?),
            };
            let header = bincode::serialize(&header)?;
            to_receiver.write_all(&header.len().to_le_bytes()).await?;
            to_receiver
                .write_all(&header)
                .await
                .context("sending sig header")?;
        } else {
            let mut base = fs::File::open(src.join(&path)).await?;
            let header = FileContentsHeader {
                path,
                contents: FileContents::File(base.metadata().await?.len()),
            };
            let header = bincode::serialize(&header)?;
            to_receiver.write_all(&header.len().to_le_bytes()).await?;
            to_receiver
                .write_all(&header)
                .await
                .context("sending sig header")?;
            tokio::io::copy(&mut base, &mut to_receiver).await?;
        }
    }
    to_receiver.flush().await?;

    Ok(())
}

async fn persist(tmp: TempFile, path: &Path) -> Result<(), std::io::Error> {
    // Atomic rename the temp file into its final location.
    match fs::rename(tmp.file_path(), &path).await {
        Err(err) if err.kind() == ErrorKind::IsADirectory => {
            async {
                fs::remove_dir(&path).await?;
                fs::rename(tmp.file_path(), &path).await
            }
            .await
        }
        other => other,
    }?;
    tmp.drop_async().await;
    Ok(())
}

/// Helper function to set the FileTime for every file, symlink, and directory in a directory tree
async fn set_mtime(path: &Path, mtime: SystemTime) -> Result<(), std::io::Error> {
    let mtime = FileTime::from_system_time(mtime);
    filetime::set_symlink_file_times(path, mtime.clone(), mtime)?;
    Ok(())
}

async fn make_executable(path: &Path) -> Result<(), std::io::Error> {
    let metadata = fs::metadata(path).await?;
    let mut permissions = metadata.permissions();
    let mode = permissions.mode();
    permissions.set_mode(mode | 0o111);
    fs::set_permissions(path, permissions).await?;
    Ok(())
}

fn is_binary(buf: &[u8]) -> bool {
    // If any null byte is seen, treat as binary
    if buf.iter().contains(&0) {
        return true;
    }

    // Count non-printable characters (excluding common control chars)
    let non_print = buf
        .iter()
        .filter(|&&b| !(b == b'\n' || b == b'\r' || b == b'\t' || (0x20..=0x7E).contains(&b)))
        .count();

    // If more than 30%, consider binary
    non_print * 100 > buf.len() * 30
}

pub async fn receiver(
    dst: &Path,
    from_sender: impl AsyncRead + Unpin,
    to_sender: impl AsyncWrite + Unpin,
    replacement_paths: HashMap<PathBuf, PathBuf>,
) -> Result<HashMap<PathBuf, Action>> {
    let mut to_sender = FramedWrite::new(to_sender, LengthDelimitedCodec::new());
    let mut from_sender = FramedRead::new(from_sender, LengthDelimitedCodec::new());

    // Get the conda env fingerprint for the src and dst, and use that to create a
    // comparator we can use to compare the mtimes between them.
    let dst_env = CondaFingerprint::from_env(dst).await?;
    let src_env: CondaFingerprint =
        bincode::deserialize(&from_sender.next().await.context("fingerprint")??)?;
    let comparator = CondaFingerprint::mtime_comparator(&src_env, &dst_env)?;
    let ignores = GlobSetBuilder::new()
        .add(Glob::new("**/*.pyc")?)
        .add(Glob::new("**/__pycache__/")?)
        .add(Glob::new("**/__pycache__/**/*")?)
        .build()?;
    let actions_builder = Arc::new(ActionsBuilder::new_with(Some(ignores), comparator));

    // Process file lists from src/dst.
    try_join!(
        // Walk destination to grab file list.
        async {
            let dst = dst.to_path_buf();
            let actions_builder = actions_builder.clone();
            tokio::task::spawn_blocking(move || {
                walk_dir(dst, move |path, ent| {
                    actions_builder
                        .process_dst(path.clone(), ent)
                        .with_context(|| format!("{}", path.display()))
                })
            })
            .await??;
            anyhow::Ok(())
        },
        // Process file list sent from sender.
        async {
            while let FileList::Entry(path, metadata) =
                bincode::deserialize(&from_sender.next().await.context("file list")??)?
            {
                actions_builder
                    .process_src(path.clone(), metadata)
                    .with_context(|| format!("{}", path.display()))?;
            }
            anyhow::Ok(())
        }
    )?;
    let actions = Arc::into_inner(actions_builder)
        .expect("should be done")
        .into_actions();

    // Demultiplex FS actions.
    let mut dirs = BTreeSet::new();
    let mut deletions = BTreeMap::new();
    let mut files = HashMap::new();
    for (path, action) in actions.iter() {
        let path = path.clone();
        match action {
            Action::Directory => {
                dirs.insert(path);
            }
            Action::Receive(mtime, recv) => {
                files.insert(path, (*mtime, recv));
            }
            Action::Delete { directory } => {
                deletions.insert(path, *directory);
            }
        }
    }

    try_join!(
        async {
            // Process deletions first.
            for (path, is_dir) in deletions.into_iter().rev() {
                let fpath = dst.join(path);
                if is_dir {
                    fs::remove_dir(&fpath).await
                } else {
                    fs::remove_file(&fpath).await
                }
                .with_context(|| format!("deleting {}", fpath.display()))?;
            }

            // Then create dirs.
            for path in dirs.into_iter() {
                let fpath = dst.join(path);
                match fs::remove_file(&fpath).await {
                    Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
                    other => other,
                }
                .with_context(|| format!("clearing path {}", fpath.display()))?;
                fs::create_dir(&fpath)
                    .await
                    .with_context(|| format!("creating dir {}", fpath.display()))?;
            }

            // Build a prefix path replacer.
            let replacer = {
                let mut builder = ReplacerBuilder::new();

                // Add the conda src/dst prefixes.
                let src_prefix = src_env.pack_meta.history.last_prefix()?;
                let dst_prefix = dst_env.pack_meta.history.last_prefix()?;
                if src_prefix != dst_prefix {
                    builder.add(src_prefix, dst_prefix)?;
                }

                // Add custom replacements passed in.
                for (src, dst) in replacement_paths.iter() {
                    if src != dst {
                        builder.add(src, dst)?;
                    }
                }

                builder.build_if_non_empty()?
            };

            // Then pull file data and create files.
            let mut from_sender = from_sender.into_inner();
            for _ in 0..files.len() {
                // Read a file header.
                let len = from_sender.read_u64_le().await?;
                let mut buf = vec![0u8; len as usize];
                from_sender.read_exact(&mut buf).await?;
                let FileContentsHeader { path, contents } =
                    bincode::deserialize(&buf).context("delta header")?;
                let fpath = dst.join(&path);
                match (contents, files.get(&path).context("missing file")?) {
                    // Read file contents and write to a tempfile.
                    (FileContents::File(len), (mtime, Receive::File { executable })) => {
                        let mut dst_tmp =
                            TempFile::new_in(fpath.parent().context("parent")?).await?;
                        let mut reader = (&mut from_sender).take(len);

                        // Copy the file contents.
                        if let Some(ref replacer) = replacer {
                            // We do different copies dependending on whether the file is binary or not.
                            let mut buf = vec![0; 4096];
                            let len = reader.read(&mut buf[..]).await?;
                            buf.truncate(len);
                            if is_binary(&buf) {
                                dst_tmp.write_all(&buf).await?;
                                tokio::io::copy(&mut reader, &mut dst_tmp).await?;

                                // For binary files, replace prefixes.
                                // SAFETY: use mmap for fast in-place prefix replacement
                                let mut mmap = unsafe { MmapMut::map_mut(&*dst_tmp)? };
                                replacer.replace_inplace_padded(&mut mmap)?;
                            } else {
                                reader.read_to_end(&mut buf).await?;
                                replacer.replace_inplace(&mut buf);
                                dst_tmp.write_all(&buf).await?;
                            }
                        } else {
                            tokio::io::copy(&mut reader, &mut dst_tmp).await?;
                        }

                        if *executable {
                            make_executable(dst_tmp.file_path()).await?;
                        }
                        persist(dst_tmp, &fpath).await?;
                        set_mtime(&fpath, *mtime).await?;
                    }
                    (FileContents::Symlink(mut target), (mtime, Receive::Symlink)) => {
                        if let Some(ref replacer) = replacer {
                            target = replacer.replace_path(target);
                        }
                        fs::symlink(target, &fpath).await?;
                        set_mtime(&fpath, *mtime).await?;
                    }
                    _ => bail!("unexpected file contents"),
                }
            }
            anyhow::Ok(())
        },
        async {
            to_sender
                .send(bincode::serialize(&FileSectionHeader { num: files.len() })?.into())
                .await
                .context("sending sig section header")?;
            for (path, (_, recv)) in files.iter() {
                to_sender
                    .send(
                        bincode::serialize(&FileHeader {
                            path: path.clone(),
                            symlink: matches!(recv, Receive::Symlink),
                        })?
                        .into(),
                    )
                    .await
                    .context("sending sig header")?;
            }
            to_sender.flush().await?;
            anyhow::Ok(())
        },
    )?;

    Ok(actions)
}

pub async fn sync(src: &Path, dst: &Path) -> Result<HashMap<PathBuf, Action>> {
    // Receiver -> Sender
    let (recv, send) = tokio::io::duplex(5 * 1024 * 1024);
    let (from_receiver, to_receiver) = tokio::io::split(recv);
    let (from_sender, to_sender) = tokio::io::split(send);
    let (actions, ()) = try_join!(
        receiver(dst, from_sender, to_sender, HashMap::new()),
        sender(src, from_receiver, to_receiver),
    )?;
    Ok(actions)
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use std::collections::HashMap;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::path::PathBuf;
    use std::time::Duration;
    use std::time::SystemTime;

    use anyhow::Result;
    use rattler_conda_types::package::FileMode;
    use tempfile::TempDir;
    use tokio::fs;

    use super::Action;
    use super::make_executable;
    use super::set_mtime;
    use super::sync;
    use crate::pack_meta_history::History;
    use crate::pack_meta_history::HistoryRecord;
    use crate::pack_meta_history::Offset;
    use crate::pack_meta_history::OffsetRecord;
    use crate::pack_meta_history::Offsets;
    use crate::sync::Receive;

    /// Helper function to create a basic conda environment structure
    async fn setup_conda_env<P: AsRef<Path>>(
        dirpath: P,
        mtime: SystemTime,
        prefix: Option<&str>,
    ) -> Result<P> {
        let env_path = dirpath.as_ref();

        // Create the basic directory structure
        fs::create_dir_all(&env_path).await?;
        fs::create_dir(&env_path.join("conda-meta")).await?;
        fs::create_dir(&env_path.join("pack-meta")).await?;

        // Create a basic conda-meta file to establish fingerprint
        add_file(
            env_path,
            "conda-meta/history",
            "==> 2023-01-01 00:00:00 <==\npackage install actions\n",
            mtime,
            false,
        )
        .await?;

        // Create a basic package record
        add_file(
            env_path,
            "conda-meta/package-1.0-0.json",
            r#"{
                "name": "package",
                "version": "1.0",
                "build": "0",
                "build_number": 0,
                "paths_data": {
                    "paths": [
                        {
                            "path": "bin/test-file",
                            "path_type": "hardlink",
                            "size_in_bytes": 10,
                            "mode": "text"
                        }
                    ]
                },
                "repodata_record": {
                    "package_record": {
                        "timestamp": 1672531200
                    }
                }
            }"#,
            mtime,
            false,
        )
        .await?;

        // Create offsets.jsonl
        let offsets = Offsets {
            entries: vec![OffsetRecord {
                path: PathBuf::from("bin/test-file"),
                mode: FileMode::Text,
                offsets: vec![Offset {
                    start: 0,
                    len: 10,
                    contents: None,
                }],
            }],
        };
        add_file(
            env_path,
            "pack-meta/offsets.jsonl",
            &offsets.to_str()?,
            mtime,
            false,
        )
        .await?;

        // Create the actual file referenced in the metadata
        fs::create_dir(env_path.join("bin")).await?;
        add_file(env_path, "bin/test-file", "test data\n", mtime, false).await?;

        // Create a file that was prefix-updated after the package was installed.
        let window = (
            mtime + Duration::from_secs(20),
            mtime + Duration::from_secs(25),
        );
        fs::create_dir(env_path.join("lib")).await?;
        add_file(
            env_path,
            "lib/libfoo.so",
            "libfoo.so contents\n",
            window.0 + Duration::from_secs(5),
            false,
        )
        .await?;

        // Use provided prefix or default to "base"
        let prefix_path = PathBuf::from(prefix.unwrap_or("base"));

        // Create history.jsonl
        let history = History {
            entries: vec![
                HistoryRecord {
                    timestamp: mtime.duration_since(SystemTime::UNIX_EPOCH)?.as_secs(),
                    prefix: PathBuf::from("/conda/prefix"),
                    finished: true,
                },
                HistoryRecord {
                    timestamp: window.0.duration_since(SystemTime::UNIX_EPOCH)?.as_secs(),
                    prefix: prefix_path.clone(),
                    finished: false,
                },
                HistoryRecord {
                    timestamp: window.1.duration_since(SystemTime::UNIX_EPOCH)?.as_secs(),
                    prefix: prefix_path,
                    finished: true,
                },
            ],
        };
        add_file(
            env_path,
            "pack-meta/history.jsonl",
            &history.to_str()?,
            mtime,
            false,
        )
        .await?;

        Ok(dirpath)
    }

    /// Helper function to modify a file in the conda environment
    async fn modify_file(
        env_path: &Path,
        file_path: &str,
        content: &str,
        mtime: SystemTime,
    ) -> Result<()> {
        let full_path = env_path.join(file_path);
        fs::write(&full_path, content).await?;

        // Set the file time
        set_mtime(&full_path, mtime).await?;

        Ok(())
    }

    /// Helper function to add a new file to the conda environment
    async fn add_file(
        env_path: &Path,
        file_path: &str,
        content: &str,
        mtime: SystemTime,
        executable: bool,
    ) -> Result<()> {
        let full_path = env_path.join(file_path);
        fs::write(&full_path, content).await?;

        if executable {
            make_executable(&full_path).await?;
        }

        // Set the file time
        set_mtime(&full_path, mtime).await?;

        Ok(())
    }

    /// Helper function to verify file content
    async fn verify_file_content(path1: &Path, path2: &Path) -> Result<bool> {
        let content1 = fs::read_to_string(path1).await?;
        let content2 = fs::read_to_string(path2).await?;
        Ok(content1 == content2)
    }

    /// Helper function to verify file permissions
    async fn verify_file_permissions(path: &Path, expected_executable: bool) -> Result<bool> {
        let metadata = fs::metadata(path).await?;
        let mode = metadata.permissions().mode();
        let is_executable = mode & 0o111 != 0;
        Ok(is_executable == expected_executable)
    }

    /// Helper function to verify symlink target
    async fn verify_symlink_target(path: &Path, expected_target: &Path) -> Result<bool> {
        let target = fs::read_link(path).await?;
        Ok(target == expected_target)
    }

    #[tokio::test]
    async fn test_sync_modified_file() -> Result<()> {
        // Set base time for consistent file timestamps
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1672531200); // 2023-01-01 00:00:00 UTC

        // Setup identical conda environments
        let src_env = setup_conda_env(TempDir::new()?, base_time, None).await?;
        let dst_env = setup_conda_env(TempDir::new()?, base_time, None).await?;

        // Modify a file in the source environment
        let modified_content = "modified test data\n";
        let newer_time = base_time + Duration::from_hours(1); // 1 hour later
        modify_file(
            src_env.path(),
            "bin/test-file",
            modified_content,
            newer_time,
        )
        .await?;

        // Sync changes from source to destination
        let actions = sync(src_env.path(), dst_env.path()).await?;

        // Create expected actions map
        let expected_actions = HashMap::from([(
            PathBuf::from("bin/test-file"),
            Action::Receive(newer_time, Receive::File { executable: false }),
        )]);

        // Verify the entire actions map
        assert_eq!(actions, expected_actions);

        // Verify the file was updated in the destination
        assert!(
            verify_file_content(
                &src_env.path().join("bin/test-file"),
                &dst_env.path().join("bin/test-file")
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_new_file() -> Result<()> {
        // Set base time for consistent file timestamps
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1672531200); // 2023-01-01 00:00:00 UTC

        // Setup identical conda environments
        let src_env = setup_conda_env(TempDir::new()?, base_time, None).await?;
        let dst_env = setup_conda_env(TempDir::new()?, base_time, None).await?;

        // Add a new file to the source environment
        let new_file_content = "new file content\n";
        let newer_time = base_time + Duration::from_hours(1); // 1 hour later
        add_file(
            src_env.path(),
            "lib/new-file.txt",
            new_file_content,
            newer_time,
            false,
        )
        .await?;

        // Sync changes from source to destination
        let actions = sync(src_env.path(), dst_env.path()).await?;

        // Create expected actions map
        let expected_actions = HashMap::from([
            //(PathBuf::from("lib"), Action::Directory(newer_time)),
            (
                PathBuf::from("lib/new-file.txt"),
                Action::Receive(newer_time, Receive::File { executable: false }),
            ),
        ]);

        // Verify the entire actions map
        assert_eq!(actions, expected_actions);

        // Verify the new file was created in the destination
        assert!(
            verify_file_content(
                &src_env.path().join("lib/new-file.txt"),
                &dst_env.path().join("lib/new-file.txt")
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_directory_creation() -> Result<()> {
        // Set base time for consistent file timestamps
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1672531200); // 2023-01-01 00:00:00 UTC

        // Setup identical conda environments
        let src_env = setup_conda_env(TempDir::new()?, base_time, None).await?;
        let dst_env = setup_conda_env(TempDir::new()?, base_time, None).await?;

        // Create a new directory with a file in the source environment
        let newer_time = base_time + Duration::from_hours(1); // 1 hour later
        fs::create_dir(src_env.path().join("new_dir")).await?;
        add_file(
            src_env.path(),
            "new_dir/test.txt",
            "test content",
            newer_time,
            false,
        )
        .await?;
        set_mtime(&src_env.path().join("new_dir"), newer_time).await?;

        // Sync changes from source to destination
        let actions = sync(src_env.path(), dst_env.path()).await?;

        // Create expected actions map
        let expected_actions = HashMap::from([
            (PathBuf::from("new_dir"), Action::Directory),
            (
                PathBuf::from("new_dir/test.txt"),
                Action::Receive(newer_time, Receive::File { executable: false }),
            ),
        ]);

        // Verify the entire actions map
        assert_eq!(actions, expected_actions);

        // Verify the directory was created in the destination
        assert!(dst_env.path().join("new_dir").exists());

        // Verify the file was created in the destination
        assert!(
            verify_file_content(
                &src_env.path().join("new_dir/test.txt"),
                &dst_env.path().join("new_dir/test.txt")
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_symlink() -> Result<()> {
        // Set base time for consistent file timestamps
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1672531200); // 2023-01-01 00:00:00 UTC

        // Setup identical conda environments
        let src_env = setup_conda_env(TempDir::new()?, base_time, None).await?;
        let dst_env = setup_conda_env(TempDir::new()?, base_time, None).await?;

        // Create a symlink in the source environment
        fs::symlink("bin/test-file", src_env.path().join("link-to-test")).await?;

        // Set a newer time for the symlink to ensure it's synced
        let newer_time = base_time + Duration::from_hours(1); // 1 hour later
        set_mtime(&src_env.path().join("link-to-test"), newer_time).await?;

        // Sync changes from source to destination
        let actions = sync(src_env.path(), dst_env.path()).await?;

        // Create expected actions map
        let expected_actions = HashMap::from([(
            PathBuf::from("link-to-test"),
            Action::Receive(newer_time, Receive::Symlink),
        )]);

        // Verify the entire actions map
        assert_eq!(actions, expected_actions);

        // Verify the symlink was created in the destination
        assert!(dst_env.path().join("link-to-test").exists());

        // Verify the symlink target
        assert!(
            verify_symlink_target(
                &dst_env.path().join("link-to-test"),
                &PathBuf::from("bin/test-file")
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_file_deletion() -> Result<()> {
        // Set base time for consistent file timestamps
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1672531200); // 2023-01-01 00:00:00 UTC

        // Setup identical conda environments
        let src_env = setup_conda_env(TempDir::new()?, base_time, None).await?;
        let dst_env = setup_conda_env(TempDir::new()?, base_time, None).await?;

        // Add an extra file to the destination that doesn't exist in source
        let newer_time = base_time + Duration::from_hours(1); // 1 hour later
        add_file(
            dst_env.path(),
            "extra-file.txt",
            "should be deleted",
            newer_time,
            false,
        )
        .await?;

        // Sync changes from source to destination
        let actions = sync(src_env.path(), dst_env.path()).await?;

        // Create expected actions map
        let expected_actions = HashMap::from([(
            PathBuf::from("extra-file.txt"),
            Action::Delete { directory: false },
        )]);

        // Verify the entire actions map
        assert_eq!(actions, expected_actions);

        // Verify the extra file was deleted from the destination
        assert!(!dst_env.path().join("extra-file.txt").exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_ignores_pyc_files() -> Result<()> {
        // Set base time for consistent file timestamps
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1672531200); // 2023-01-01 00:00:00 UTC

        // Setup identical conda environments
        let src_env = setup_conda_env(TempDir::new()?, base_time, None).await?;
        let dst_env = setup_conda_env(TempDir::new()?, base_time, None).await?;

        // Add a .pyc file to the source.
        let newer_time = base_time + Duration::from_hours(1); // 1 hour later
        add_file(
            src_env.path(),
            "lib/test.pyc",
            "compiled python",
            newer_time,
            false,
        )
        .await?;

        // Add a file in __pycache__ directory to the destination
        fs::create_dir(dst_env.path().join("lib/__pycache__")).await?;
        add_file(
            dst_env.path(),
            "lib/__pycache__/cached.pyc",
            "cached python",
            newer_time,
            false,
        )
        .await?;

        // Sync changes from source to destination
        let actions = sync(src_env.path(), dst_env.path()).await?;

        // For this test, we expect an empty actions map since .pyc files are ignored
        let expected_actions = HashMap::from([
            (
                PathBuf::from("lib/__pycache__"),
                Action::Delete { directory: true },
            ),
            (
                PathBuf::from("lib/__pycache__/cached.pyc"),
                Action::Delete { directory: false },
            ),
        ]);

        // Verify the entire actions map
        assert_eq!(actions, expected_actions);

        // Verify the .pyc files were deleted (they should be ignored)
        assert!(!dst_env.path().join("lib/test.pyc").exists());
        assert!(!dst_env.path().join("lib/__pycache__").exists());
        assert!(!dst_env.path().join("lib/__pycache__/cached.pyc").exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_executable_permissions() -> Result<()> {
        // Set base time for consistent file timestamps
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1672531200); // 2023-01-01 00:00:00 UTC

        // Setup identical conda environments
        let src_env = setup_conda_env(TempDir::new()?, base_time, None).await?;
        let dst_env = setup_conda_env(TempDir::new()?, base_time, None).await?;

        // Add an executable file to the source
        let newer_time = base_time + Duration::from_hours(1); // 1 hour later
        add_file(
            src_env.path(),
            "bin/executable",
            "#!/bin/sh\necho hello",
            newer_time,
            true,
        )
        .await?;

        // Sync changes from source to destination
        let actions = sync(src_env.path(), dst_env.path()).await?;

        // Create expected actions map
        let expected_actions = HashMap::from([(
            PathBuf::from("bin/executable"),
            Action::Receive(newer_time, Receive::File { executable: true }),
        )]);

        // Verify the entire actions map
        assert_eq!(actions, expected_actions);

        // Verify the file was created in the destination
        assert!(dst_env.path().join("bin/executable").exists());

        // Verify the file content was synced correctly
        assert!(
            verify_file_content(
                &src_env.path().join("bin/executable"),
                &dst_env.path().join("bin/executable")
            )
            .await?
        );

        // Verify the executable permissions were preserved
        assert!(verify_file_permissions(&dst_env.path().join("bin/executable"), true).await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_text_file_prefix_replacement() -> Result<()> {
        // Set base time for consistent file timestamps
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1672531200); // 2023-01-01 00:00:00 UTC

        // Setup conda environments with different prefixes
        let src_prefix = "/opt/conda/src";
        let dst_prefix = "/opt/conda/dst";
        let src_env = setup_conda_env(TempDir::new()?, base_time, Some(src_prefix)).await?;
        let dst_env = setup_conda_env(TempDir::new()?, base_time, Some(dst_prefix)).await?;

        // Add a text file with prefix references to the source
        let newer_time = base_time + Duration::from_hours(1);
        let text_content = format!(
            "#!/bin/bash\nexport PATH={}/bin:$PATH\necho 'Using prefix: {}'\n",
            src_prefix, src_prefix
        );
        add_file(
            src_env.path(),
            "bin/script.sh",
            &text_content,
            newer_time,
            true,
        )
        .await?;

        // Add an empty file too.
        add_file(src_env.path(), "bin/script2.sh", "", newer_time, true).await?;

        // Sync changes from source to destination
        let actions = sync(src_env.path(), dst_env.path()).await?;

        // Verify the file was synced
        let expected_actions = HashMap::from([
            (
                PathBuf::from("bin/script.sh"),
                Action::Receive(newer_time, Receive::File { executable: true }),
            ),
            (
                PathBuf::from("bin/script2.sh"),
                Action::Receive(newer_time, Receive::File { executable: true }),
            ),
        ]);
        assert_eq!(actions, expected_actions);

        // Verify the prefix was replaced in the destination file
        let dst_content = fs::read_to_string(dst_env.path().join("bin/script.sh")).await?;
        let expected_content = format!(
            "#!/bin/bash\nexport PATH={}/bin:$PATH\necho 'Using prefix: {}'\n",
            dst_prefix, dst_prefix
        );
        assert_eq!(dst_content, expected_content);

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_binary_file_prefix_replacement() -> Result<()> {
        // Set base time for consistent file timestamps
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1672531200);

        // Setup conda environments with different prefixes
        let src_prefix = "/opt/conda/src";
        let dst_prefix = "/opt/conda/dst";
        let src_env = setup_conda_env(TempDir::new()?, base_time, Some(src_prefix)).await?;
        let dst_env = setup_conda_env(TempDir::new()?, base_time, Some(dst_prefix)).await?;

        // Create a binary file with embedded prefix and null bytes
        let newer_time = base_time + Duration::from_hours(1);
        let mut binary_content = Vec::new();
        binary_content.extend_from_slice(b"\x7fELF"); // ELF magic number
        binary_content.extend_from_slice(&[0u8; 10]); // null bytes to make it binary
        binary_content.extend_from_slice(src_prefix.as_bytes());
        binary_content.extend_from_slice(&[0u8; 20]); // more null bytes
        binary_content.extend_from_slice(b"end");

        fs::write(src_env.path().join("lib/binary"), &binary_content).await?;
        set_mtime(&src_env.path().join("lib/binary"), newer_time).await?;

        // Sync changes from source to destination
        let actions = sync(src_env.path(), dst_env.path()).await?;

        // Verify the file was synced
        let expected_actions = HashMap::from([(
            PathBuf::from("lib/binary"),
            Action::Receive(newer_time, Receive::File { executable: false }),
        )]);
        assert_eq!(actions, expected_actions);

        // Verify the prefix was replaced in the binary file with null padding
        let dst_content = fs::read(dst_env.path().join("lib/binary")).await?;

        // The original file size should be preserved
        assert_eq!(dst_content.len(), binary_content.len());

        // Check that the prefix was replaced
        let dst_content_str = String::from_utf8_lossy(&dst_content);
        assert!(dst_content_str.contains(dst_prefix));
        assert!(!dst_content_str.contains(src_prefix));

        // Verify the ELF header and end marker are still present
        assert!(dst_content.starts_with(b"\x7fELF"));
        assert!(dst_content.ends_with(b"end"));

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_symlink_prefix_replacement() -> Result<()> {
        // Set base time for consistent file timestamps
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1672531200);

        // Setup conda environments with different prefixes
        let src_prefix = "/opt/conda/src";
        let dst_prefix = "/opt/conda/dst";
        let src_env = setup_conda_env(TempDir::new()?, base_time, Some(src_prefix)).await?;
        let dst_env = setup_conda_env(TempDir::new()?, base_time, Some(dst_prefix)).await?;

        // Create a symlink that points to a path with the source prefix
        let newer_time = base_time + Duration::from_hours(1);
        let symlink_target = format!("{}/lib/target-file", src_prefix);
        fs::symlink(&symlink_target, src_env.path().join("bin/link-to-target")).await?;
        set_mtime(&src_env.path().join("bin/link-to-target"), newer_time).await?;

        // Sync changes from source to destination
        let actions = sync(src_env.path(), dst_env.path()).await?;

        // Verify the symlink was synced
        let expected_actions = HashMap::from([(
            PathBuf::from("bin/link-to-target"),
            Action::Receive(newer_time, Receive::Symlink),
        )]);
        assert_eq!(actions, expected_actions);

        // Verify the symlink target was updated with the destination prefix
        let dst_target = fs::read_link(dst_env.path().join("bin/link-to-target")).await?;
        let expected_target = PathBuf::from(format!("{}/lib/target-file", dst_prefix));
        assert_eq!(dst_target, expected_target);

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_symlink_no_prefix_replacement() -> Result<()> {
        // Set base time for consistent file timestamps
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1672531200);

        // Setup conda environments with different prefixes
        let src_prefix = "/opt/conda/src";
        let dst_prefix = "/opt/conda/dst";
        let src_env = setup_conda_env(TempDir::new()?, base_time, Some(src_prefix)).await?;
        let dst_env = setup_conda_env(TempDir::new()?, base_time, Some(dst_prefix)).await?;

        // Create a symlink that points to a relative path (should not be modified)
        let newer_time = base_time + Duration::from_hours(1);
        let symlink_target = "relative/path/target";
        fs::symlink(&symlink_target, src_env.path().join("bin/relative-link")).await?;
        set_mtime(&src_env.path().join("bin/relative-link"), newer_time).await?;

        // Sync changes from source to destination
        let actions = sync(src_env.path(), dst_env.path()).await?;

        // Verify the symlink was synced
        let expected_actions = HashMap::from([(
            PathBuf::from("bin/relative-link"),
            Action::Receive(newer_time, Receive::Symlink),
        )]);
        assert_eq!(actions, expected_actions);

        // Verify the symlink target was NOT modified (since it doesn't start with src_prefix)
        let dst_target = fs::read_link(dst_env.path().join("bin/relative-link")).await?;
        let expected_target = PathBuf::from(symlink_target);
        assert_eq!(dst_target, expected_target);

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_binary_file_prefix_replacement_fails_when_dst_longer() -> Result<()> {
        // Set base time for consistent file timestamps
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1672531200);

        // Setup conda environments where destination prefix is longer than source
        let src_prefix = "/opt/src"; // Short prefix
        let dst_prefix = "/opt/very/long/destination/prefix"; // Much longer prefix
        let src_env = setup_conda_env(TempDir::new()?, base_time, Some(src_prefix)).await?;
        let dst_env = setup_conda_env(TempDir::new()?, base_time, Some(dst_prefix)).await?;

        // Create a binary file with embedded prefix and null bytes
        let newer_time = base_time + Duration::from_hours(1);
        let mut binary_content = Vec::new();
        binary_content.extend_from_slice(b"\x7fELF"); // ELF magic number
        binary_content.extend_from_slice(&[0u8; 10]); // null bytes to make it binary
        binary_content.extend_from_slice(src_prefix.as_bytes());
        binary_content.extend_from_slice(&[0u8; 20]); // more null bytes
        binary_content.extend_from_slice(b"end");

        fs::write(src_env.path().join("lib/binary"), &binary_content).await?;
        set_mtime(&src_env.path().join("lib/binary"), newer_time).await?;

        // Sync changes from source to destination - this should fail
        let result = sync(src_env.path(), dst_env.path()).await;

        // Verify that the sync operation failed due to the destination prefix being longer
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Input is longer than target length"));

        Ok(())
    }
}
