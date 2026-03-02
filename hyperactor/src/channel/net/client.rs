/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Simplex client: dial + SimplexConnector implementation.

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::framed::FrameReader;
use super::session;
use super::session::Deliveries;
use super::session::MuxWriter;
use super::session::SendLoopResult;
use super::session::SessionConnector;
use super::session::SimplexFrameStream;
use crate::RemoteMessage;
use crate::channel::ChannelAddr;
use crate::channel::SendError;
use crate::channel::TxStatus;
use crate::channel::net::ClientError;
use crate::channel::net::Link;
use crate::channel::net::NetTx;
use crate::channel::net::Stream;
use crate::config;

pub(super) struct SimplexConnection<S: Stream> {
    reader: SimplexFrameStream<ReadHalf<S>>,
    writer: MuxWriter<WriteHalf<S>>,
}

pub(super) struct SimplexConnector<L: Link>(pub L);

#[async_trait]
impl<L: Link + 'static, M: RemoteMessage> SessionConnector<M> for SimplexConnector<L> {
    type Connected = SimplexConnection<L::Stream>;

    fn dest(&self) -> ChannelAddr {
        self.0.dest()
    }

    fn session_id(&self) -> u64 {
        self.0.link_id().0
    }

    async fn connect(&mut self) -> Result<Self::Connected, ClientError> {
        let stream = self.0.connect().await?;
        let (r, w) = tokio::io::split(stream);
        let max = hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH);
        Ok(SimplexConnection {
            reader: SimplexFrameStream::new(FrameReader::new(r, max)),
            writer: MuxWriter::new(w, max),
        })
    }

    async fn run_connected(
        &mut self,
        connected: &Self::Connected,
        deliveries: &mut Deliveries<M>,
        receiver: &mut mpsc::UnboundedReceiver<(M, oneshot::Sender<SendError<M>>, Instant)>,
        cancel: CancellationToken,
    ) -> SendLoopResult {
        session::send_loop(
            &connected.reader,
            &connected.writer,
            0,
            deliveries,
            receiver,
            cancel,
        )
        .await
    }

    async fn shutdown(connected: Self::Connected) {
        let inner = connected.writer.into_inner();
        let mut w = inner.lock().await;
        let _ = w.shutdown().await;
    }
}

/// Creates a new session, and assigns it a guid.
pub(super) fn dial<M: RemoteMessage>(link: impl Link + 'static) -> NetTx<M> {
    let (sender, receiver) = mpsc::unbounded_channel();
    let dest = link.dest();
    let (notify, status) = watch::channel(TxStatus::Active);

    let tx = NetTx {
        sender,
        dest,
        status,
    };
    crate::init::get_runtime().spawn(session::client_run(
        SimplexConnector(link),
        receiver,
        Some(notify),
    ));
    tx
}
