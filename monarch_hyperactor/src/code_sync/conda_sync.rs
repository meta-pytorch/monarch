/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use hyperactor::Actor;
use hyperactor::Bind;
use hyperactor::Handler;
use hyperactor::Named;
use hyperactor::PortRef;
use hyperactor::Unbind;
use hyperactor_mesh::actor_mesh::ActorMesh;
use hyperactor_mesh::connect::Connect;
use hyperactor_mesh::connect::accept;
use hyperactor_mesh::sel;
use lazy_errors::ErrorStash;
use lazy_errors::OrStash;
use lazy_errors::StashedResult;
use lazy_errors::TryCollectOrStash;
use monarch_conda::sync::Action;
use monarch_conda::sync::receiver;
use monarch_conda::sync::sender;
use ndslice::Selection;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::code_sync::WorkspaceLocation;

/// Represents the result of an conda sync operation with details about what was transferred
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Named)]
pub struct CondaSyncResult {
    /// All changes that occurred during the sync operation
    pub changes: HashMap<PathBuf, Action>,
}

#[derive(Debug, Clone, Named, Serialize, Deserialize, Bind, Unbind)]
pub struct CondaSyncMessage {
    /// The connect message to create a duplex bytestream with the client.
    pub connect: PortRef<Connect>,
    /// A port to send back the result or any errors.
    pub result: PortRef<Result<CondaSyncResult, String>>,
    /// The location of the workspace to sync.
    pub workspace: WorkspaceLocation,
}

#[derive(Debug, Named, Serialize, Deserialize)]
pub struct CondaSyncParams {}

#[derive(Debug)]
#[hyperactor::export(spawn = true, handlers = [CondaSyncMessage { cast = true }])]
pub struct CondaSyncActor {}

#[async_trait]
impl Actor for CondaSyncActor {
    type Params = CondaSyncParams;

    async fn new(CondaSyncParams {}: Self::Params) -> Result<Self> {
        Ok(Self {})
    }
}

#[async_trait]
impl Handler<CondaSyncMessage> for CondaSyncActor {
    async fn handle(
        &mut self,
        cx: &hyperactor::Context<Self>,
        CondaSyncMessage {
            workspace,
            connect,
            result,
        }: CondaSyncMessage,
    ) -> Result<(), anyhow::Error> {
        let res = async {
            let workspace = workspace.resolve()?;
            let (connect_msg, completer) = Connect::allocate(cx.self_id().clone(), cx);
            connect.send(cx, connect_msg)?;
            let (mut read, mut write) = completer.complete().await?.into_split();
            let changes_result = receiver(&workspace, &mut read, &mut write).await;

            // Shutdown our end, then read from the other end till exhaustion to avoid undeliverable
            // message spam.
            write.shutdown().await?;
            let mut buf = vec![];
            read.read_to_end(&mut buf).await?;

            anyhow::Ok(CondaSyncResult {
                changes: changes_result?,
            })
        }
        .await;
        result.send(cx, res.map_err(|e| format!("{:#?}", e)))?;
        Ok(())
    }
}

pub async fn conda_sync_mesh<M>(
    actor_mesh: &M,
    local_workspace: PathBuf,
    remote_workspace: WorkspaceLocation,
) -> Result<Vec<CondaSyncResult>>
where
    M: ActorMesh<Actor = CondaSyncActor>,
{
    let mailbox = actor_mesh.proc_mesh().client();
    let (conns_tx, conns_rx) = mailbox.open_port::<Connect>();

    let (res1, res2) = futures::future::join(
        conns_rx
            .take(actor_mesh.shape().slice().len())
            .err_into::<anyhow::Error>()
            .try_for_each_concurrent(None, |connect| async {
                let (mut read, mut write) = accept(mailbox, mailbox.actor_id().clone(), connect)
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
            .boxed(),
        async move {
            let (result_tx, result_rx) = mailbox.open_port::<Result<CondaSyncResult, String>>();
            actor_mesh.cast(
                mailbox,
                sel!(*),
                CondaSyncMessage {
                    connect: conns_tx.bind(),
                    result: result_tx.bind(),
                    workspace: remote_workspace,
                },
            )?;

            // Wait for all actors to report result.
            let results = result_rx
                .take(actor_mesh.shape().slice().len())
                .try_collect::<Vec<_>>()
                .await?;

            // Combine all errors into one.
            let mut errs = ErrorStash::<_, _, anyhow::Error>::new(|| "remote failures");
            match results
                .into_iter()
                .map(|res| res.map_err(anyhow::Error::msg))
                .try_collect_or_stash::<Vec<_>>(&mut errs)
            {
                StashedResult::Ok(results) => anyhow::Ok(results),
                StashedResult::Err(_) => Err(errs.into_result().unwrap_err().into()),
            }
        },
    )
    .await;

    // Combine code sync handler and cast errors into one.
    let mut errs = ErrorStash::<_, _, anyhow::Error>::new(|| "code sync failed");
    res1.or_stash(&mut errs);
    if let StashedResult::Ok(results) = res2.or_stash(&mut errs) {
        errs.into_result()?;
        return Ok(results);
    }
    Err(errs.into_result().unwrap_err().into())
}
