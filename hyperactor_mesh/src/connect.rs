/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Actor-based duplex bytestream connections.
//!
//! This module provides the equivalent of a `TcpStream` duplex bytestream connection between two actors,
//! implemented via actor message passing. It allows actors to communicate using familiar `AsyncRead` and
//! `AsyncWrite` interfaces while leveraging the hyperactor framework's message passing capabilities.
//!
//! # Overview
//!
//! The connection system consists of:
//! - [`ActorConnection`]: A duplex connection that implements both `AsyncRead` and `AsyncWrite`
//! - [`OwnedReadHalf`] and [`OwnedWriteHalf`]: Split halves for independent reading and writing
//! - [`Connect`] message for establishing connections
//! - Helper functions [`connect`] and [`accept`] for client and server usage
//!
//! # Usage Patterns
//!
//! ## Client Side (Initiating Connection)
//!
//! Clients use `Connect::allocate()` to create a connection request. This method returns:
//! 1. A `Connect` message to send to the server to initiate the connection
//! 2. A `ConnectionCompleter` object that can be awaited for the server to finish connecting,
//!    returning the `ActorConnection` used by the client.
//!
//! The typical pattern is: allocate components, send Connect message to server, await completion.
//!
//! ## Server Side (Accepting Connections)
//!
//! Servers forward `Connect` messages to the `accept()` helper function to finish setting up the
//! connection, which returns the `ActorConnection` they can use.

use std::io::Cursor;
use std::pin::Pin;
use std::time::Duration;

use anyhow::Result;
use future::Future;
use futures::Stream;
use futures::future;
use futures::stream::FusedStream;
use futures::task::Context;
use futures::task::Poll;
use hyperactor::ActorId;
use hyperactor::OncePortRef;
use hyperactor::PortRef;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::context;
use hyperactor::mailbox::OncePortReceiver;
use hyperactor::mailbox::PortReceiver;
use hyperactor::mailbox::open_once_port;
use hyperactor::mailbox::open_port;
use hyperactor::message::Bind;
use hyperactor::message::Bindings;
use hyperactor::message::Unbind;
use pin_project::pin_project;
use pin_project::pinned_drop;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio_util::io::StreamReader;
use typeuri::Named;

// Timeout for establishing a connection, used by both client and server.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Messages sent over the "connection" to facilitate communication.
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
enum Io {
    // A data packet.
    Data(#[serde(with = "serde_bytes")] Vec<u8>),
    // Signal the end of one side of the connection.
    Eof,
}
wirevalue::register_type!(Io);

struct OwnedReadHalfStream {
    port: PortReceiver<Io>,
    exhausted: bool,
}

/// Wrap a `PortReceiver<IoMsg>` as a `AsyncRead`.
pub struct OwnedReadHalf {
    peer: ActorId,
    inner: StreamReader<OwnedReadHalfStream, Cursor<Vec<u8>>>,
}

/// Wrap a `PortRef<IoMsg>` as a `AsyncWrite`.
#[pin_project(PinnedDrop)]
pub struct OwnedWriteHalf<C: context::Actor> {
    peer: ActorId,
    #[pin]
    caps: C,
    #[pin]
    port: PortRef<Io>,
    #[pin]
    shutdown: bool,
}

/// A duplex bytestream connection between two actors.  Can generally be used like a `TcpStream`.
#[pin_project]
pub struct ActorConnection<C: context::Actor> {
    #[pin]
    reader: OwnedReadHalf,
    #[pin]
    writer: OwnedWriteHalf<C>,
}

impl<C: context::Actor> ActorConnection<C> {
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf<C>) {
        (self.reader, self.writer)
    }

    pub fn peer(&self) -> &ActorId {
        self.reader.peer()
    }
}

impl OwnedReadHalf {
    fn new(peer: ActorId, port: PortReceiver<Io>) -> Self {
        Self {
            peer,
            inner: StreamReader::new(OwnedReadHalfStream {
                port,
                exhausted: false,
            }),
        }
    }

    pub fn peer(&self) -> &ActorId {
        &self.peer
    }

    pub fn reunited<C: context::Actor>(self, other: OwnedWriteHalf<C>) -> ActorConnection<C> {
        ActorConnection {
            reader: self,
            writer: other,
        }
    }
}

impl<C: context::Actor> OwnedWriteHalf<C> {
    fn new(peer: ActorId, caps: C, port: PortRef<Io>) -> Self {
        Self {
            peer,
            caps,
            port,
            shutdown: false,
        }
    }

    pub fn peer(&self) -> &ActorId {
        &self.peer
    }

    pub fn reunited(self, other: OwnedReadHalf) -> ActorConnection<C> {
        ActorConnection {
            reader: other,
            writer: self,
        }
    }
}

#[pinned_drop]
impl<C: context::Actor> PinnedDrop for OwnedWriteHalf<C> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        if !*this.shutdown {
            let _ = this.port.send(&*this.caps, Io::Eof);
        }
    }
}

impl<C: context::Actor> AsyncRead for ActorConnection<C> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Use project() to get pinned references to fields
        let this = self.project();
        this.reader.poll_read(cx, buf)
    }
}

impl<C: context::Actor> AsyncWrite for ActorConnection<C> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        // Use project() to get pinned references to fields
        let this = self.project();
        this.writer.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let this = self.project();
        this.writer.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let this = self.project();
        this.writer.poll_shutdown(cx)
    }
}

impl Stream for OwnedReadHalfStream {
    type Item = std::io::Result<Cursor<Vec<u8>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Once exhausted, always return None
        if self.exhausted {
            return Poll::Ready(None);
        }

        let result = futures::ready!(Box::pin(self.port.recv()).as_mut().poll(cx));
        match result {
            Err(err) => Poll::Ready(Some(Err(std::io::Error::other(err)))),
            Ok(Io::Data(buf)) => Poll::Ready(Some(Ok(Cursor::new(buf)))),
            // Break out of stream when we see EOF.
            Ok(Io::Eof) => {
                self.exhausted = true;
                Poll::Ready(None)
            }
        }
    }
}

impl FusedStream for OwnedReadHalfStream {
    fn is_terminated(&self) -> bool {
        self.exhausted
    }
}

impl AsyncRead for OwnedReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<C: context::Actor> AsyncWrite for OwnedWriteHalf<C> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = self.project();
        if *this.shutdown {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "write after shutdown",
            )));
        }
        match this.port.send(&*this.caps, Io::Data(buf.into())) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => Poll::Ready(Err(std::io::Error::other(e))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        // Send EOF on shutdown.
        match self.port.send(&self.caps, Io::Eof) {
            Ok(()) => {
                let mut this = self.project();
                *this.shutdown = true;
                Poll::Ready(Ok(()))
            }
            Err(e) => Poll::Ready(Err(std::io::Error::other(e))),
        }
    }
}

/// A helper struct that contains the state needed to complete a connection.
pub struct ConnectionCompleter<C> {
    caps: C,
    conn: PortReceiver<Io>,
    port: OncePortReceiver<Accept>,
}

impl<C: context::Actor> ConnectionCompleter<C> {
    /// Wait for the server to accept the connection and return the streams that can be used to communicate
    /// with the server.
    pub async fn complete(self) -> Result<ActorConnection<C>> {
        let accept = RealClock
            .timeout(CONNECT_TIMEOUT, self.port.recv())
            .await??;
        Ok(ActorConnection {
            reader: OwnedReadHalf::new(accept.id.clone(), self.conn),
            writer: OwnedWriteHalf::new(accept.id, self.caps, accept.conn),
        })
    }
}

/// A message sent from a client to initiate a connection.
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
pub struct Connect {
    /// The ID of the client initiating the connection.
    id: ActorId,
    conn: PortRef<Io>,
    /// The port the server can use to complete the connection.
    return_conn: OncePortRef<Accept>,
}
wirevalue::register_type!(Connect);

impl Connect {
    /// Allocate a new `Connect` message and return the associated `ConnectionCompleter` that can be used
    /// to finish setting up the connection.
    pub fn allocate<C: context::Actor>(id: ActorId, caps: C) -> (Self, ConnectionCompleter<C>) {
        let (conn_tx, conn_rx) = open_port::<Io>(&caps);
        let (return_tx, return_rx) = open_once_port::<Accept>(&caps);
        (
            Self {
                id,
                conn: conn_tx.bind().into_port_ref(),
                return_conn: return_tx.bind().into_port_ref(),
            },
            ConnectionCompleter {
                caps,
                conn: conn_rx,
                port: return_rx,
            },
        )
    }
}

/// A response message sent from the server back to the client to complete setting
/// up the connection.
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
struct Accept {
    /// The ID of the server that accepted the connection.
    id: ActorId,
    /// The port the client will use to send data over the connection to the server.
    conn: PortRef<Io>,
}
wirevalue::register_type!(Accept);

impl Bind for Connect {
    fn bind(&mut self, bindings: &mut Bindings) -> Result<()> {
        self.conn.bind(bindings)?;
        self.return_conn.bind(bindings)
    }
}

impl Unbind for Connect {
    fn unbind(&self, bindings: &mut Bindings) -> Result<()> {
        self.conn.unbind(bindings)?;
        self.return_conn.unbind(bindings)
    }
}

/// Helper used by `Handler<Connect>`s to accept a connection initiated by a `Connect` message and
/// return `AsyncRead` and `AsyncWrite` streams that can be used to communicate with the other side.
pub async fn accept<C: context::Actor>(
    caps: C,
    self_id: ActorId,
    message: Connect,
) -> Result<ActorConnection<C>> {
    let (tx, rx) = open_port::<Io>(&caps);
    message.return_conn.send(
        &caps,
        Accept {
            id: self_id,
            conn: tx.bind().into_port_ref(),
        },
    )?;
    Ok(ActorConnection {
        reader: OwnedReadHalf::new(message.id.clone(), rx),
        writer: OwnedWriteHalf::new(message.id, caps, message.conn),
    })
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use async_trait::async_trait;
    use futures::try_join;
    use hyperactor::Actor;
    use hyperactor::Context;
    use hyperactor::Handler;
    use hyperactor::proc::Proc;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[derive(Debug, Default)]
    struct EchoActor {}

    impl Actor for EchoActor {}

    #[async_trait]
    impl Handler<Connect> for EchoActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            message: Connect,
        ) -> Result<(), anyhow::Error> {
            let (mut rd, mut wr) = accept(cx, cx.self_id().clone(), message)
                .await?
                .into_split();
            tokio::io::copy(&mut rd, &mut wr).await?;
            wr.shutdown().await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_simple_connection() -> Result<()> {
        let proc = Proc::local();
        let (client, _) = proc.instance("client")?;
        let (connect, completer) = Connect::allocate(client.self_id().clone(), client);
        let actor = proc.spawn("actor", EchoActor {})?;
        actor.send(&completer.caps, connect)?;
        let (mut rd, mut wr) = completer.complete().await?.into_split();
        let send = [3u8, 4u8, 5u8, 6u8];
        try_join!(
            async move {
                wr.write_all(&send).await?;
                wr.shutdown().await?;
                anyhow::Ok(())
            },
            async {
                let mut recv = vec![];
                rd.read_to_end(&mut recv).await?;
                assert_eq!(&send, recv.as_slice());
                anyhow::Ok(())
            },
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn test_connection_close_on_drop() -> Result<()> {
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client")?;

        let (connect, completer) =
            Connect::allocate(client.self_id().clone(), client.clone_for_py());
        let (mut rd, _) = accept(client.clone_for_py(), client.self_id().clone(), connect)
            .await?
            .into_split();
        let (_, mut wr) = completer.complete().await?.into_split();

        // Write some data
        let send = [1u8, 2u8, 3u8];
        wr.write_all(&send).await?;

        // Drop the writer without explicit shutdown - this should send EOF
        drop(wr);

        // Reader should receive the data and then EOF (causing read_to_end to complete)
        let mut recv = vec![];
        rd.read_to_end(&mut recv).await?;
        assert_eq!(&send, recv.as_slice());

        Ok(())
    }

    #[tokio::test]
    async fn test_no_eof_on_drop_after_shutdown() -> Result<()> {
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client")?;

        let (connect, completer) =
            Connect::allocate(client.self_id().clone(), client.clone_for_py());
        let (mut rd, _) = accept(client.clone_for_py(), client.self_id().clone(), connect)
            .await?
            .into_split();
        let (_, mut wr) = completer.complete().await?.into_split();

        // Write some data
        let send = [1u8, 2u8, 3u8];
        wr.write_all(&send).await?;

        // Explicitly shutdown the writer - this sends EOF and sets shutdown=true
        wr.shutdown().await?;

        // Reader should receive the data and then EOF (from explicit shutdown, not from drop)
        let mut recv = vec![];
        rd.read_to_end(&mut recv).await?;
        assert_eq!(&send, recv.as_slice());

        // Drop the writer after explicit shutdown - this should NOT send another EOF
        drop(wr);

        // Verify we didn't see another EOF message.
        assert!(rd.inner.into_inner().port.try_recv().unwrap().is_none());

        Ok(())
    }
}
