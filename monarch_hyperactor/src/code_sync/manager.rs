/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context as _;
use anyhow::Result;
use anyhow::ensure;
use async_once_cell::OnceCell;
use async_trait::async_trait;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use futures::try_join;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Named;
use hyperactor::PortRef;
use hyperactor::RemoteSpawn;
use hyperactor::Unbind;
use hyperactor::context;
use hyperactor::forward;
use hyperactor_mesh::RootActorMesh;
use hyperactor_mesh::SlicedActorMesh;
use hyperactor_mesh::actor_mesh::ActorMesh;
use hyperactor_mesh::connect::Connect;
use hyperactor_mesh::connect::accept;
use hyperactor_mesh::reference::ActorMeshId;
use hyperactor_mesh::reference::ActorMeshRef;
use hyperactor_mesh::reference::ProcMeshId;
use hyperactor_mesh::sel;
use hyperactor_mesh::v1;
use lazy_errors::ErrorStash;
use lazy_errors::TryCollectOrStash;
use monarch_conda::sync::sender;
use ndslice::Selection;
use ndslice::Shape;
use ndslice::ShapeError;
use ndslice::View;
use ndslice::view::RankedSliceable;
use ndslice::view::ViewExt;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use crate::code_sync::WorkspaceLocation;
use crate::code_sync::auto_reload::AutoReloadActor;
use crate::code_sync::auto_reload::AutoReloadMessage;
use crate::code_sync::conda_sync::CondaSyncActor;
use crate::code_sync::conda_sync::CondaSyncMessage;
use crate::code_sync::conda_sync::CondaSyncResult;
use crate::code_sync::rsync::RsyncActor;
use crate::code_sync::rsync::RsyncDaemon;
use crate::code_sync::rsync::RsyncMessage;
use crate::code_sync::rsync::RsyncResult;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Method {
    Rsync {
        connect: PortRef<Connect>,
    },
    CondaSync {
        connect: PortRef<Connect>,
        path_prefix_replacements: HashMap<PathBuf, WorkspaceLocation>,
    },
}

/// Describe the shape of the workspace.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkspaceShape {
    /// All actors accessing the workspace.
    pub shape: Shape,
    /// Starting dimension in the shape denoting all ranks that share the same workspace.
    pub dimension: Option<String>,
}

impl WorkspaceShape {
    /// Reduce the shape to contain only the "owners" of the remote workspace
    ///
    /// This is relevant when e.g. multiple worker on the same host share a workspace, in which case,
    /// we'll reduce the share to only contain one worker per workspace, so that we don't have multiple
    /// workers trying to sync to the same workspace at the same time.
    pub fn owners(&self) -> Result<Shape, ShapeError> {
        let mut new_shape = self.shape.clone();
        for label in self
            .shape
            .labels()
            .iter()
            .skip_while(|l| Some(*l) != self.dimension.as_ref())
        {
            new_shape = new_shape.select(label, 0)?;
            //new_shape = new_shape.slice(label, 0..1)?;
        }
        Ok(new_shape)
    }

    /// Return a new shape that contains all ranks that share the same workspace with the given "owning" rank.
    ///
    /// # Errors
    ///
    /// Returns an error if the given rank's coordinates aren't all zero starting at the specified dimension
    /// and continuing until the end. For example, if the dimension is Some("host"), then the coordinates
    /// for the rank must be 0 in the host dimension and all subsequent dimensions.
    pub fn downstream(&self, rank: usize) -> Result<Shape> {
        let coords = self.shape.coordinates(rank)?;

        for (label, value) in coords
            .iter()
            .skip_while(|(l, _)| Some(l) != self.dimension.as_ref())
        {
            ensure!(
                *value == 0,
                "Coordinate for dimension '{}' must be 0 for rank {}",
                label,
                rank
            );
        }

        Ok(self.shape.index(
            coords
                .into_iter()
                .take_while(|(l, _)| Some(l) != self.dimension.as_ref())
                .collect::<Vec<_>>(),
        )?)
    }

    fn downstream_mesh_v0(&self, actor_id: &ActorId) -> Result<ActorMeshRef<CodeSyncManager>> {
        let shape = self.downstream(actor_id.rank())?;
        Ok(ActorMeshRef::attest(
            ActorMeshId::V0(
                ProcMeshId(actor_id.world_name().to_owned()),
                actor_id.name().to_string(),
            ),
            shape,
            ActorRef::attest(actor_id.proc_id().actor_id("comm", 0)),
        ))
    }

    fn downstream_mesh(
        &self,
        mesh: &v1::actor_mesh::ActorMeshRef<CodeSyncManager>,
        rank: usize,
    ) -> Result<v1::actor_mesh::ActorMeshRef<CodeSyncManager>> {
        let shape = self.downstream(rank)?;
        Ok(mesh.sliced(shape.region()))
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkspaceConfig {
    pub location: WorkspaceLocation,
    pub shape: WorkspaceShape,
}

#[derive(Handler, Clone, Serialize, Deserialize, Debug, Named, Bind, Unbind)]
pub enum CodeSyncMessage {
    Sync {
        workspace: WorkspaceLocation,
        /// The method to use for syncing.
        method: Method,
        /// Whether to hot-reload code after syncing.
        reload: Option<WorkspaceShape>,
        /// A port to send back the result of the sync operation.
        result: PortRef<Result<(), String>>,
    },
    Reload {
        sender_rank: Option<usize>,
        result: PortRef<Result<(), String>>,
    },
}

#[derive(Clone, Serialize, Deserialize, Debug, Named, Bind, Unbind)]
pub struct SetActorMeshMessage {
    pub actor_mesh: v1::actor_mesh::ActorMeshRef<CodeSyncManager>,
}

#[derive(Debug, Named, Serialize, Deserialize)]
pub struct CodeSyncManagerParams {}

#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        CodeSyncMessage { cast = true },
        SetActorMeshMessage { cast = true }
    ],
)]
pub struct CodeSyncManager {
    rsync: OnceCell<ActorHandle<RsyncActor>>,
    auto_reload: OnceCell<ActorHandle<AutoReloadActor>>,
    conda_sync: OnceCell<ActorHandle<CondaSyncActor>>,
    self_mesh: once_cell::sync::OnceCell<v1::actor_mesh::ActorMeshRef<CodeSyncManager>>,
    rank: once_cell::sync::OnceCell<usize>,
}

impl Actor for CodeSyncManager {}

#[async_trait]
impl RemoteSpawn for CodeSyncManager {
    type Params = CodeSyncManagerParams;

    async fn new(CodeSyncManagerParams {}: Self::Params) -> Result<Self> {
        Ok(Self {
            rsync: OnceCell::new(),
            auto_reload: OnceCell::new(),
            conda_sync: OnceCell::new(),
            self_mesh: once_cell::sync::OnceCell::new(),
            rank: once_cell::sync::OnceCell::new(),
        })
    }
}

impl CodeSyncManager {
    async fn get_rsync_actor<'a>(
        &'a mut self,
        cx: &Context<'a, Self>,
    ) -> Result<&'a ActorHandle<RsyncActor>> {
        self.rsync
            .get_or_try_init(RsyncActor::default().spawn(cx))
            .await
    }

    async fn get_auto_reload_actor<'a>(
        &'a mut self,
        cx: &Context<'a, Self>,
    ) -> Result<&'a ActorHandle<AutoReloadActor>> {
        self.auto_reload
            .get_or_try_init(async move { AutoReloadActor::new().await?.spawn(cx).await })
            .await
    }

    async fn get_conda_sync_actor<'a>(
        &'a mut self,
        cx: &Context<'a, Self>,
    ) -> Result<&'a ActorHandle<CondaSyncActor>> {
        self.conda_sync
            .get_or_try_init(CondaSyncActor::default().spawn(cx))
            .await
    }
}

#[async_trait]
#[forward(CodeSyncMessage)]
impl CodeSyncMessageHandler for CodeSyncManager {
    async fn sync(
        &mut self,
        cx: &Context<Self>,
        workspace: WorkspaceLocation,
        method: Method,
        reload: Option<WorkspaceShape>,
        result: PortRef<Result<(), String>>,
    ) -> Result<()> {
        let res = async move {
            match method {
                Method::Rsync { connect } => {
                    // Forward rsync connection port to the RsyncActor, which will do the actual
                    // connection and run the client.
                    let (tx, mut rx) = cx.open_port::<Result<RsyncResult, String>>();
                    self.get_rsync_actor(cx).await?.send(RsyncMessage {
                        connect,
                        result: tx.bind(),
                        workspace,
                    })?;
                    // Observe any errors.
                    let _ = rx.recv().await?.map_err(anyhow::Error::msg)?;
                }
                Method::CondaSync {
                    connect,
                    path_prefix_replacements,
                } => {
                    // Forward rsync connection port to the RsyncActor, which will do the actual
                    // connection and run the client.
                    let (tx, mut rx) = cx.open_port::<Result<CondaSyncResult, String>>();
                    self.get_conda_sync_actor(cx)
                        .await?
                        .send(CondaSyncMessage {
                            connect,
                            result: tx.bind(),
                            workspace,
                            path_prefix_replacements,
                        })?;
                    // Observe any errors.
                    let _ = rx.recv().await?.map_err(anyhow::Error::msg)?;
                }
            }

            // Trigger hot reload on all ranks that use/share this workspace.
            if let Some(workspace_shape) = reload {
                let (tx, rx) = cx.open_port::<Result<(), String>>();
                let tx = tx.bind();
                let len;
                if let Some(rank) = self.rank.get() {
                    let mesh = self
                        .self_mesh
                        .get()
                        .ok_or_else(|| anyhow::anyhow!("missing self mesh"))?;
                    let mesh = workspace_shape.downstream_mesh(mesh, *rank)?;
                    mesh.cast(
                        cx,
                        CodeSyncMessage::Reload {
                            sender_rank: Some(*rank),
                            result: tx.clone(),
                        },
                    )?;
                    // Exclude self from the sync.
                    len = mesh.region().slice().len() - 1;
                } else {
                    let mesh = workspace_shape.downstream_mesh_v0(cx.self_id())?;
                    mesh.cast(
                        cx,
                        // We make sure to exclude the current rank from the sync, as this actor will
                        // be blocked here waiting for results.  We just manually call `reload` to run
                        // concurrently below.
                        sel!(*)
                            .without(mesh.shape().slice(), &HashSet::from([cx.self_id().rank()]))?,
                        CodeSyncMessage::Reload {
                            sender_rank: None,
                            result: tx.clone(),
                        },
                    )?;
                    len = mesh.shape().slice().len();
                }
                let _: ((), Vec<()>) = try_join!(
                    // Run reload for this rank.
                    self.reload(cx, self.rank.get().cloned(), tx),
                    rx.take(len)
                        .map(|res| res?.map_err(anyhow::Error::msg))
                        .try_collect(),
                )?;
            }

            anyhow::Ok(())
        }
        .await;
        result.send(
            cx,
            res.map_err(|e| {
                format!(
                    "{:#?}",
                    Err::<(), _>(e)
                        .with_context(|| format!("code sync from {}", cx.self_id()))
                        .unwrap_err()
                )
            }),
        )?;
        Ok(())
    }

    async fn reload(
        &mut self,
        cx: &Context<Self>,
        sender_rank: Option<usize>,
        result: PortRef<Result<(), String>>,
    ) -> Result<()> {
        if self
            .rank
            .get()
            .is_some_and(|rank| sender_rank.is_some_and(|sender_rank| *rank == sender_rank))
        {
            return Ok(());
        }
        let res = async move {
            let (tx, mut rx) = cx.open_port();
            self.get_auto_reload_actor(cx)
                .await?
                .send(AutoReloadMessage { result: tx.bind() })?;
            rx.recv().await?.map_err(anyhow::Error::msg)?;
            anyhow::Ok(())
        }
        .await;
        result.send(
            cx,
            res.map_err(|e| {
                format!(
                    "{:#?}",
                    Err::<(), _>(e)
                        .with_context(|| format!("module reload from {}", cx.self_id()))
                        .unwrap_err()
                )
            }),
        )?;
        Ok(())
    }
}

#[async_trait]
impl Handler<SetActorMeshMessage> for CodeSyncManager {
    async fn handle(&mut self, cx: &Context<Self>, msg: SetActorMeshMessage) -> Result<()> {
        let mesh = self.self_mesh.get_or_init(|| msg.actor_mesh);
        self.rank.get_or_init(|| {
            mesh.iter()
                .find(|(_, actor)| actor.actor_id() == cx.self_id())
                .unwrap()
                .0
                .rank()
        });
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CodeSyncMethod {
    Rsync,
    CondaSync {
        path_prefix_replacements: HashMap<PathBuf, WorkspaceLocation>,
    },
}

pub async fn code_sync_mesh(
    cx: &impl context::Actor,
    actor_mesh: &RootActorMesh<'_, CodeSyncManager>,
    local_workspace: PathBuf,
    remote_workspace: WorkspaceConfig,
    method: CodeSyncMethod,
    auto_reload: bool,
) -> Result<()> {
    let instance = cx.instance();

    // Create a slice of the actor mesh that only includes workspace "owners" (e.g. on multi-GPU hosts,
    // only one of the ranks on that host will participate in the code sync).
    let actor_mesh = SlicedActorMesh::new(actor_mesh, remote_workspace.shape.owners()?);
    let shape = actor_mesh.shape().clone();

    let (method, method_fut) = match method {
        CodeSyncMethod::Rsync => {
            // Spawn a rsync daemon to accept incoming connections from actors.
            // some machines (e.g. github CI) do not have ipv6, so try ipv6 then fallback to ipv4
            let ipv6_lo: SocketAddr = "[::1]:0".parse()?;
            let ipv4_lo: SocketAddr = "127.0.0.1:0".parse()?;
            let addrs: [SocketAddr; 2] = [ipv6_lo, ipv4_lo];
            let daemon =
                RsyncDaemon::spawn(TcpListener::bind(&addrs[..]).await?, &local_workspace).await?;

            let daemon_addr = daemon.addr().clone();
            let (rsync_conns_tx, rsync_conns_rx) = instance.open_port::<Connect>();
            (
                Method::Rsync {
                    connect: rsync_conns_tx.bind(),
                },
                // This async task will process rsync connection attempts concurrently, forwarding
                // them to the rsync daemon above.
                async move {
                    let res = rsync_conns_rx
                        .take(shape.slice().len())
                        .err_into::<anyhow::Error>()
                        .try_for_each_concurrent(None, |connect| async move {
                            let (mut local, mut stream) = try_join!(
                                TcpStream::connect(daemon_addr.clone()).err_into(),
                                accept(instance, instance.self_id().clone(), connect),
                            )?;
                            tokio::io::copy_bidirectional(&mut local, &mut stream).await?;
                            Ok(())
                        })
                        .await;
                    daemon.shutdown().await?;
                    res?;
                    anyhow::Ok(())
                }
                .boxed(),
            )
        }
        CodeSyncMethod::CondaSync {
            path_prefix_replacements,
        } => {
            let (conns_tx, conns_rx) = instance.open_port::<Connect>();
            (
                Method::CondaSync {
                    connect: conns_tx.bind(),
                    path_prefix_replacements,
                },
                async move {
                    conns_rx
                        .take(shape.slice().len())
                        .err_into::<anyhow::Error>()
                        .try_for_each_concurrent(None, |connect| async {
                            let (mut read, mut write) =
                                accept(instance, instance.self_id().clone(), connect)
                                    .await?
                                    .into_split();
                            let res = sender(&local_workspace, &mut read, &mut write).await;

                            // Shutdown our end, then read from the other end till exhaustion to avoid undeliverable
                            // message spam.
                            write.shutdown().await?;
                            let mut buf = vec![];
                            read.read_to_end(&mut buf).await?;

                            res
                        })
                        .await
                }
                .boxed(),
            )
        }
    };

    let ((), ()) = try_join!(
        method_fut,
        // This async task will cast the code sync message to workspace owners, and process any errors.
        async move {
            let (result_tx, result_rx) = instance.open_port::<Result<(), String>>();
            actor_mesh.cast(
                instance,
                sel!(*),
                CodeSyncMessage::Sync {
                    method,
                    workspace: remote_workspace.location.clone(),
                    reload: if auto_reload {
                        Some(remote_workspace.shape)
                    } else {
                        None
                    },
                    result: result_tx.bind(),
                },
            )?;

            // Wait for all actors to report result.
            let results = result_rx
                .take(actor_mesh.shape().slice().len())
                .try_collect::<Vec<_>>()
                .await?;

            // Combine all errors into one.
            let mut errs = ErrorStash::<_, _, anyhow::Error>::new(|| "remote failures");
            results
                .into_iter()
                .map(|res| res.map_err(anyhow::Error::msg))
                .try_collect_or_stash::<()>(&mut errs);
            Ok(errs.into_result()?)
        },
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use hyperactor::channel::ChannelTransport;
    use hyperactor_mesh::alloc::AllocSpec;
    use hyperactor_mesh::alloc::Allocator;
    use hyperactor_mesh::alloc::local::LocalAllocator;
    use hyperactor_mesh::proc_mesh::ProcMesh;
    use hyperactor_mesh::proc_mesh::global_root_client;
    use ndslice::extent;
    use ndslice::shape;
    use tempfile::TempDir;
    use tokio::fs;

    use super::*;

    #[test]
    fn test_workspace_shape_owners() {
        // Create a shape with multiple dimensions
        let shape = shape! { host = 2, replica = 3 };

        // Test case 1: dimension is None (should return the original shape)
        let ws_shape = WorkspaceShape {
            shape: shape.clone(),
            dimension: None,
        };
        let owners = ws_shape.owners().unwrap();
        assert_eq!(owners.slice().len(), 6); // 2 hosts * 3 replicas = 6 ranks

        // Test case 2: dimension is "host" (should return a shape with only one rank per host)
        let ws_shape = WorkspaceShape {
            shape: shape.clone(),
            dimension: Some("host".to_string()),
        };
        let owners = ws_shape.owners().unwrap();
        assert_eq!(owners.slice().len(), 1); // 2 hosts, 1 rank per host

        // Test case 3: dimension is "replica" (should return a shape with only one rank per replica)
        let ws_shape = WorkspaceShape {
            shape: shape.clone(),
            dimension: Some("replica".to_string()),
        };
        let owners = ws_shape.owners().unwrap();
        assert_eq!(owners.slice().len(), 2); // 3 replicas, 1 rank per replica
    }

    #[test]
    fn test_workspace_shape_downstream() -> Result<()> {
        // Create a shape with multiple dimensions
        let shape = shape! { host = 2, replica = 3 };

        // Test case 1: dimension is None (should return a shape with just the specified rank)
        let ws_shape = WorkspaceShape {
            shape: shape.clone(),
            dimension: None,
        };
        let downstream = ws_shape.downstream(0)?;
        assert_eq!(downstream.slice().len(), 1); // Just rank 0

        // Test case 2: dimension is "host" (should return a shape with all ranks on the same host)
        let ws_shape = WorkspaceShape {
            shape: shape.clone(),
            dimension: Some("host".to_string()),
        };
        let downstream = ws_shape.downstream(0)?;
        assert_eq!(downstream.slice().len(), 6); // All ranks in the shape
        assert!(ws_shape.downstream(3).is_err());

        // Test case 3: dimension is "e (should return a shape with all ranks on the same host)
        let ws_shape = WorkspaceShape {
            shape: shape.clone(),
            dimension: Some("replica".to_string()),
        };
        let downstream = ws_shape.downstream(0)?;
        assert_eq!(downstream.slice().len(), 3);
        let downstream = ws_shape.downstream(3)?;
        assert_eq!(downstream.slice().len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_code_sync_manager_and_mesh() -> Result<()> {
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

        // Set up actor mesh with CodeSyncManager actors
        let alloc = LocalAllocator
            .allocate(AllocSpec {
                extent: extent! { replica = 2 },
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Local,
                proc_allocation_mode: Default::default(),
            })
            .await?;

        let proc_mesh = ProcMesh::allocate(alloc).await?;

        // Create CodeSyncManagerParams
        let params = CodeSyncManagerParams {};

        // TODO: thread through context, or access the actual python context;
        // for now this is basically equivalent (arguably better) to using the proc mesh client.
        let instance = global_root_client();

        // Spawn actor mesh with CodeSyncManager actors
        let actor_mesh = proc_mesh
            .spawn::<CodeSyncManager>(&instance, "code_sync_test", &params)
            .await?;

        // Create workspace configuration
        let remote_workspace_config = WorkspaceConfig {
            location: WorkspaceLocation::Constant(target_workspace.path().to_path_buf()),
            shape: WorkspaceShape {
                shape: shape! { replica = 2 },
                dimension: Some("replica".to_string()),
            },
        };

        // Test code_sync_mesh function - this coordinates sync operations across the mesh
        // Test without auto-reload first
        code_sync_mesh(
            instance,
            &actor_mesh,
            source_workspace.path().to_path_buf(),
            remote_workspace_config.clone(),
            CodeSyncMethod::Rsync,
            false, // no auto-reload
        )
        .await?;

        // Verify that files were synchronized correctly
        assert!(
            !dir_diff::is_different(&source_workspace, &target_workspace)
                .map_err(|e| anyhow!("{:?}", e))?,
            "Source and target workspaces should be identical after sync"
        );

        Ok(())
    }
}
