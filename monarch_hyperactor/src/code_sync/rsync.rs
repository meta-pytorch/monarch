/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use anyhow::ensure;
use async_trait::async_trait;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use futures::try_join;
use hyperactor::Actor;
use hyperactor::Bind;
use hyperactor::Handler;
use hyperactor::PortRef;
use hyperactor::Unbind;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor_mesh::actor_mesh::ActorMesh;
use hyperactor_mesh::connect::Connect;
use hyperactor_mesh::connect::accept;
use hyperactor_mesh::sel;
#[cfg(feature = "packaged_rsync")]
use lazy_static::lazy_static;
use ndslice::Selection;
use nix::sys::signal;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use serde::Deserialize;
use serde::Serialize;
use tempfile::TempDir;
#[cfg(feature = "packaged_rsync")]
use tempfile::TempPath;
use tokio::fs;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::process::Child;
use tokio::process::Command;
#[cfg(feature = "packaged_rsync")]
use tokio::sync::OnceCell;
use tracing::warn;
use typeuri::Named;

use crate::code_sync::WorkspaceLocation;

#[cfg(feature = "packaged_rsync")]
lazy_static! {
    static ref RSYNC_BIN_PATH: OnceCell<TempPath> = OnceCell::new();
}

async fn get_rsync_bin_path() -> Result<&'static Path> {
    #[cfg(feature = "packaged_rsync")]
    {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        Ok(RSYNC_BIN_PATH
            .get_or_try_init(|| async {
                tokio::task::spawn_blocking(|| {
                    let mut tmp = tempfile::NamedTempFile::with_prefix("rsync.")?;
                    let rsync_bin = include_bytes!("rsync.bin");
                    tmp.write_all(rsync_bin)?;
                    let bin_path = tmp.into_temp_path();
                    std::fs::set_permissions(&bin_path, std::fs::Permissions::from_mode(0o755))?;
                    anyhow::Ok(bin_path)
                })
                .await?
            })
            .await?)
    }
    #[cfg(not(feature = "packaged_rsync"))]
    {
        Ok(Path::new("rsync"))
    }
}

/// Represents a single file change from rsync
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    /// The type of change that occurred
    pub change_type: ChangeType,
    /// The path of the file that changed
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeMessage {
    /// Path was deleted
    Deleting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeAction {
    /// File was received.
    Received,
    // Path was changed/created locally.
    LocalChange,
    NotTransferred,
}

/// The type of change that occurred to a file
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeType {
    Message(ChangeMessage),
    Action(ChangeAction, FileType),
}

/// The type of file that changed
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    /// Regular file
    File,
    /// Directory
    Directory,
    /// Symbolic link
    Symlink,
}

/// Represents the result of an rsync operation with details about what was transferred
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Named)]
pub struct RsyncResult {
    /// All changes that occurred during the rsync operation
    pub changes: Vec<Change>,
}
wirevalue::register_type!(RsyncResult);

impl RsyncResult {
    /// Create an empty rsync result
    pub fn empty() -> Self {
        Self {
            changes: Vec::new(),
        }
    }

    /// Parse rsync output to extract file transfer information
    /// Since create_dir_all ensures the workspace exists, we can assume all stdout lines are changes
    fn parse_from_output(stdout: &str) -> Result<Self> {
        let mut changes = Vec::new();

        // Parse stdout for file operations (when using --itemize-changes)
        // All lines in stdout represent changes since the workspace directory exists

        // rsync itemize format: YXcstpoguax path
        // Y = update type (>, c, h, ., etc.)
        // X = file type (f=file, d=directory, L=symlink, etc.)
        for line in stdout.lines() {
            let line = line.trim();
            let (raw_changes, path) = line.split_at(11);
            let raw_changes = raw_changes.trim();
            let path = &path[1..]; // remove leading space

            let mut iter = raw_changes.chars();
            let change_type = match iter.next().context("missing change type")? {
                '*' => ChangeType::Message(match iter.as_str() {
                    "deleting" => ChangeMessage::Deleting,
                    _ => bail!("unexpected change message: {}", raw_changes),
                }),
                c => {
                    let atype = match c {
                        '.' => ChangeAction::NotTransferred,
                        '>' => ChangeAction::Received,
                        'c' => ChangeAction::LocalChange,
                        _ => bail!("unexpected change type: {}", raw_changes),
                    };
                    let file_type = match iter.next().context("missing file type")? {
                        'f' => FileType::File,
                        'd' => FileType::Directory,
                        'L' => FileType::Symlink,
                        _ => bail!("unexpected file type: {}", raw_changes),
                    };
                    ChangeType::Action(atype, file_type)
                }
            };

            changes.push(Change {
                change_type,
                path: PathBuf::from(path),
            });
        }

        Ok(Self { changes })
    }
}

pub async fn do_rsync(addr: &SocketAddr, workspace: &Path) -> Result<RsyncResult> {
    // Make sure the target workspace exists, mainly to avoid the "created director ..."
    // line in rsync output.
    fs::create_dir_all(workspace).await?;

    let rsync_bin_path = get_rsync_bin_path().await?;
    let output = Command::new(rsync_bin_path)
        .arg("--archive")
        .arg("--delete")
        // Show detailed changes for each file
        .arg("--itemize-changes")
        // By setting these flags, we make `rsync` immune to multiple invocations
        // targeting the same dir, which can happen if we don't take care to only
        // allow one worker on a given host to do the `rsync`.
        .arg("--delete-after")
        .arg("--delay-updates")
        .arg("--exclude=.rsync-tmp.*")
        .arg(format!("--partial-dir=.rsync-tmp.{}", addr.port()))
        .arg(format!("rsync://{}/workspace", addr))
        .arg(format!("{}/", workspace.display()))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                anyhow::anyhow!(
                    "rsync binary: '{}' does not exist. Please ensure 'rsync' is installed and available in PATH.",
                    rsync_bin_path.display()
                )
            } else {
                anyhow::anyhow!("failed to execute rsync: {}", e)
            }
        })?;

    output
        .status
        .exit_ok()
        .with_context(|| format!("rsync failed: {}", String::from_utf8_lossy(&output.stderr)))?;

    RsyncResult::parse_from_output(&String::from_utf8(output.stdout)?)
}

#[derive(Debug)]
pub struct RsyncDaemon {
    child: Child,
    state: TempDir,
    addr: SocketAddr,
}

impl RsyncDaemon {
    pub async fn spawn(listener: TcpListener, workspace: &Path) -> Result<Self> {
        let state = TempDir::with_prefix("rsyncd.")?;

        // Write rsync config file
        // TODO(agallagher): We can setup a secrets file to provide some measure of
        // security and prevent stray rsync calls from hitting the server.
        let content = format!(
            r#"\
[workspace]
    path = {workspace}
    use chroot = no
    list = no
    read only = true
    write only = false
    uid = {uid}
    hosts allow = localhost
"#,
            workspace = workspace.display(),
            uid = nix::unistd::getuid().as_raw(),
        );
        let config = state.path().join("rsync.config");
        fs::write(&config, content).await?;

        // Find free port.  This is potentially racy, as some process could
        // potentially bind to this port in between now and when `rsync` starts up
        // below.  But I'm not sure a better way to do this, as rsync doesn't appear
        // to support `rsync --sockopts=SO_PORTREUSE` (to share this port we've
        // reserved) or `--port=0` (to pick a free port -- it'll just always use
        // 873).
        let addr = listener.local_addr()?;
        std::mem::drop(listener);

        // Spawn the rsync daemon.
        let mut child = Command::new(get_rsync_bin_path().await?)
            .arg("--daemon")
            .arg("--no-detach")
            .arg(format!("--address={}", addr.ip()))
            .arg(format!("--port={}", addr.port()))
            .arg(format!("--config={}", config.display()))
            .arg(format!("--log-file={}/log", state.path().display()))
            .kill_on_drop(true)
            .spawn()?;

        // Wait until the rsync daemon is ready to connect via polling it (I tried polling
        // the log file to wait for the "listening" log line, but that gets prevented *before*
        // it actually starts the listening loop).
        tokio::select! {
            res = child.wait() => bail!("unexpected early exit: {:?}", res),
            res = async {
                loop {
                    match TcpStream::connect(addr).await {
                        Err(err) if err.kind() == ErrorKind::ConnectionRefused => {
                            RealClock.sleep(Duration::from_millis(1)).await
                        }
                        Err(err) => return Err(err.into()),
                        Ok(_) => break,
                    }
                }
                anyhow::Ok(())
            } => res?,
        }

        Ok(Self { child, state, addr })
    }

    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub async fn shutdown(mut self) -> Result<String> {
        let logs = fs::read_to_string(self.state.path().join("log")).await;
        let id = self.child.id().context("missing pid")?;
        let pid = Pid::from_raw(id as i32);
        signal::kill(pid, Signal::SIGINT)?;
        let status = self.child.wait().await?;
        // rsync exists with 20 when sent SIGINT.
        ensure!(status.code() == Some(20));
        Ok(logs?)
    }
}

#[derive(Debug, Clone, Named, Serialize, Deserialize, Bind, Unbind)]
pub struct RsyncMessage {
    /// The connect message to create a duplex bytestream with the client.
    pub connect: PortRef<Connect>,
    /// A port to send back the rsync result or any errors.
    pub result: PortRef<Result<RsyncResult, String>>,
    /// The location of the workspace to sync.
    pub workspace: WorkspaceLocation,
}
wirevalue::register_type!(RsyncMessage);

#[derive(Debug, Default)]
#[hyperactor::export(spawn = true, handlers = [RsyncMessage { cast = true }])]
pub struct RsyncActor {
    //workspace: WorkspaceLocation,
}

impl Actor for RsyncActor {}

#[async_trait]
impl Handler<RsyncMessage> for RsyncActor {
    async fn handle(
        &mut self,
        cx: &hyperactor::Context<Self>,
        RsyncMessage {
            workspace,
            connect,
            result,
        }: RsyncMessage,
    ) -> Result<(), anyhow::Error> {
        let res = async {
            let workspace = workspace
                .resolve()
                .context("resolving workspace location")?;
            let (connect_msg, completer) = Connect::allocate(cx.self_id().clone(), cx);
            connect.send(cx, connect_msg)?;

            // some machines (e.g. github CI) do not have ipv6, so try ipv6 then fallback to ipv4
            let ipv6_lo: SocketAddr = "[::1]:0".parse()?;
            let ipv4_lo: SocketAddr = "127.0.0.1:0".parse()?;
            let addrs: [SocketAddr; 2] = [ipv6_lo, ipv4_lo];

            let (listener, mut stream) = try_join!(
                TcpListener::bind(&addrs[..]).err_into(),
                completer.complete(),
            )?;
            let addr = listener.local_addr()?;
            let (rsync_result, _) = try_join!(do_rsync(&addr, &workspace), async move {
                let (mut local, _) = listener.accept().await?;
                tokio::io::copy_bidirectional(&mut stream, &mut local).await?;
                anyhow::Ok(())
            },)?;
            anyhow::Ok(rsync_result)
        }
        .await;
        result.send(cx, res.map_err(|e| format!("{:#?}", e)))?;
        Ok(())
    }
}

pub async fn rsync_mesh<M>(
    actor_mesh: &M,
    local_workspace: PathBuf,
    remote_workspace: WorkspaceLocation,
) -> Result<Vec<RsyncResult>>
where
    M: ActorMesh<Actor = RsyncActor>,
{
    // Spawn a rsync daemon to accept incoming connections from actors.
    let daemon = RsyncDaemon::spawn(TcpListener::bind(("::1", 0)).await?, &local_workspace).await?;
    let daemon_addr = daemon.addr();

    let instance = actor_mesh.proc_mesh().client();
    let (rsync_conns_tx, rsync_conns_rx) = instance.open_port::<Connect>();

    let res = try_join!(
        rsync_conns_rx
            .take(actor_mesh.shape().slice().len())
            .err_into::<anyhow::Error>()
            .try_for_each_concurrent(None, |connect| async move {
                let (mut local, mut stream) = try_join!(
                    TcpStream::connect(daemon_addr.clone()).err_into(),
                    accept(instance, instance.self_id().clone(), connect),
                )?;
                tokio::io::copy_bidirectional(&mut local, &mut stream).await?;
                anyhow::Ok(())
            })
            .boxed(),
        async move {
            let (result_tx, result_rx) = instance.open_port::<Result<RsyncResult, String>>();
            actor_mesh.cast(
                instance,
                sel!(*),
                RsyncMessage {
                    connect: rsync_conns_tx.bind().into_port_ref(),
                    result: result_tx.bind().into_port_ref(),
                    workspace: remote_workspace,
                },
            )?;
            let res: Vec<RsyncResult> = result_rx
                .take(actor_mesh.shape().slice().len())
                .map(|res| res?.map_err(anyhow::Error::msg))
                .try_collect()
                .await?;
            anyhow::Ok(res)
        },
    );

    // Kill rsync server and attempt to grab the logs.
    let logs = daemon.shutdown().await;

    // Return results, attaching rsync daemon logs on error.
    match res {
        Ok(((), results)) => {
            let _ = logs?;
            Ok(results)
        }
        Err(err) => match logs {
            Ok(logs) => Err(err).with_context(|| format!("rsync server logs: {}", logs)),
            Err(shutdown_err) => {
                warn!("failed to read logs from rsync daemon: {:?}", shutdown_err);
                Err(err)
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use anyhow::anyhow;
    use hyperactor::channel::ChannelTransport;
    use hyperactor_mesh::actor_mesh::RootActorMesh;
    use hyperactor_mesh::alloc::AllocSpec;
    use hyperactor_mesh::alloc::Allocator;
    use hyperactor_mesh::alloc::local::LocalAllocator;
    use hyperactor_mesh::proc_mesh::ProcMesh;
    use hyperactor_mesh::proc_mesh::global_root_client;
    use ndslice::extent;
    use tempfile::TempDir;
    use tokio::fs;
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    // TODO: OSS: Cannot assign requested address (os error 99)
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_simple() -> Result<()> {
        let input = TempDir::new()?;
        fs::write(input.path().join("foo.txt"), "hello world").await?;

        let output = TempDir::new()?;

        let server = TcpListener::bind(("::", 0)).await?;
        let daemon = RsyncDaemon::spawn(server, output.path()).await?;
        do_rsync(daemon.addr(), input.path()).await?;
        daemon.shutdown().await?;

        assert!(!dir_diff::is_different(&input, &output).map_err(|e| anyhow!("{:?}", e))?);

        Ok(())
    }

    #[tokio::test]
    // TODO: OSS: Cannot assign requested address (os error 99)
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_rsync_actor_and_mesh() -> Result<()> {
        // Create source workspace with test files
        let source_workspace = TempDir::new()?;
        fs::write(source_workspace.path().join("test1.txt"), "content1").await?;
        fs::write(source_workspace.path().join("test2.txt"), "content2").await?;
        fs::create_dir(source_workspace.path().join("subdir")).await?;
        fs::write(source_workspace.path().join("subdir/test3.txt"), "content3").await?;

        // Create target workspace for the actors
        let target_workspace = TempDir::new()?;
        fs::create_dir(target_workspace.path().join("subdir5")).await?;
        fs::write(target_workspace.path().join("foo.txt"), "something").await?;

        // Set up actor mesh with 2 RsyncActors
        let alloc = LocalAllocator
            .allocate(AllocSpec {
                extent: extent! { replica = 1 },
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Local,
                proc_allocation_mode: Default::default(),
            })
            .await?;

        let proc_mesh = ProcMesh::allocate(alloc).await?;

        // TODO: thread through context, or access the actual python context;
        // for now this is basically equivalent (arguably better) to using the proc mesh client.
        let instance = global_root_client();

        // Spawn actor mesh with RsyncActors
        let actor_mesh: RootActorMesh<RsyncActor> =
            proc_mesh.spawn(&instance, "rsync_test", &()).await?;

        // Test rsync_mesh function - this coordinates rsync operations across the mesh
        let results = rsync_mesh(
            &actor_mesh,
            source_workspace.path().to_path_buf(),
            WorkspaceLocation::Constant(target_workspace.path().to_path_buf()),
        )
        .await?;

        // Verify we got results back
        assert_eq!(results.len(), 1); // We have 1 actor in the mesh

        let rsync_result = &results[0];

        // Verify that files were transferred (should be at least the files we created)
        // Note: The exact files detected may vary based on rsync's itemization,
        // but we should have some indication of transfer activity
        println!("Rsync result: {:#?}", rsync_result);

        // Verify we copied correctly.
        assert!(
            !dir_diff::is_different(&source_workspace, &target_workspace)
                .map_err(|e| anyhow!("{:?}", e))?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_rsync_result_parsing() -> Result<()> {
        // Test the parsing logic with mock rsync output
        let stdout = r#">f+++++++++ test1.txt
>f+++++++++ test2.txt
cd+++++++++ subdir/
>f+++++++++ subdir/test3.txt
*deleting   old_file.txt
"#;

        let result = RsyncResult::parse_from_output(stdout)?;

        // Define the expected changes
        let expected_changes = vec![
            Change {
                change_type: ChangeType::Action(ChangeAction::Received, FileType::File),
                path: PathBuf::from("test1.txt"),
            },
            Change {
                change_type: ChangeType::Action(ChangeAction::Received, FileType::File),
                path: PathBuf::from("test2.txt"),
            },
            Change {
                change_type: ChangeType::Action(ChangeAction::LocalChange, FileType::Directory),
                path: PathBuf::from("subdir/"),
            },
            Change {
                change_type: ChangeType::Action(ChangeAction::Received, FileType::File),
                path: PathBuf::from("subdir/test3.txt"),
            },
            Change {
                change_type: ChangeType::Message(ChangeMessage::Deleting),
                path: PathBuf::from("old_file.txt"),
            },
        ];

        // Verify we have all expected changes
        assert_eq!(result.changes.len(), expected_changes.len());

        // Compare each change
        for (actual, expected) in result.changes.iter().zip(expected_changes.iter()) {
            assert_eq!(
                actual, expected,
                "Change mismatch: actual={:?}, expected={:?}",
                actual, expected
            );
        }

        // Verify the entire result matches
        assert_eq!(result.changes, expected_changes);

        Ok(())
    }
}
