/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io;
use std::net::Ipv6Addr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use quinn::Endpoint;
use quinn::RecvStream;
use quinn::SendStream;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

use super::*;
use crate::channel::TlsAddr;

#[derive(Debug, Clone, Copy)]
pub(crate) enum QuicAddrType {
    Quic,
    MetaQuic,
}

impl QuicAddrType {
    fn addr(self, addr: TlsAddr) -> ChannelAddr {
        match self {
            Self::Quic => ChannelAddr::Quic(addr),
            Self::MetaQuic => ChannelAddr::MetaQuic(addr),
        }
    }
}

pub(crate) struct QuicStream {
    send: SendStream,
    recv: RecvStream,
}

impl QuicStream {
    fn new(send: SendStream, recv: RecvStream) -> Self {
        Self { send, recv }
    }
}

impl std::fmt::Debug for QuicStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicStream").finish_non_exhaustive()
    }
}

impl AsyncRead for QuicStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuicStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        AsyncWrite::poll_write(Pin::new(&mut self.get_mut().send), cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_flush(Pin::new(&mut self.get_mut().send), cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_shutdown(Pin::new(&mut self.get_mut().send), cx)
    }
}

pub(crate) struct QuicLink {
    hostname: Hostname,
    port: Port,
    client_config: quinn::ClientConfig,
    addr_type: QuicAddrType,
    session_id: SessionId,
    stream_id: u8,
    kind: ProtocolKind,
}

impl std::fmt::Debug for QuicLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicLink")
            .field("hostname", &self.hostname)
            .field("port", &self.port)
            .field("addr_type", &self.addr_type)
            .finish()
    }
}

#[async_trait]
impl Link for QuicLink {
    type Stream = QuicStream;

    fn dest(&self) -> ChannelAddr {
        self.addr_type
            .addr(TlsAddr::new(self.hostname.clone(), self.port))
    }

    fn link_id(&self) -> SessionId {
        self.session_id
    }

    async fn next(&mut self) -> Result<Self::Stream, ClientError> {
        let reconnect_timeout = hyperactor_config::global::get(config::CHANNEL_RECONNECT_TIMEOUT);
        let mut backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(1))
            .with_multiplier(2.0)
            .with_randomization_factor(0.1)
            .with_max_interval(Duration::from_millis(1000))
            .with_max_elapsed_time(Some(reconnect_timeout))
            .build();

        loop {
            let mut addrs = (self.hostname.as_ref(), self.port)
                .to_socket_addrs()
                .map_err(|_| ClientError::Resolve(self.dest()))?;
            let addr = addrs.next().ok_or(ClientError::Resolve(self.dest()))?;
            let bind_addr = if addr.is_ipv6() {
                (Ipv6Addr::UNSPECIFIED, 0).into()
            } else {
                (std::net::Ipv4Addr::UNSPECIFIED, 0).into()
            };
            let endpoint = Endpoint::client(bind_addr).map_err(|err| {
                ClientError::Connect(
                    self.dest(),
                    err,
                    "failed to bind QUIC client endpoint".to_string(),
                )
            })?;
            match endpoint.connect_with(self.client_config.clone(), addr, &self.hostname) {
                Ok(connecting) => match connecting.await {
                    Ok(connection) => match connection.open_bi().await {
                        Ok((mut send, recv)) => {
                            write_link_init(&mut send, self.session_id, self.stream_id, self.kind)
                                .await
                                .map_err(|err| ClientError::Io(self.dest(), err))?;
                            return Ok(QuicStream::new(send, recv));
                        }
                        Err(err) => {
                            tracing::debug!(error = %err, "quic open_bi failed, backing off");
                        }
                    },
                    Err(err) => {
                        tracing::debug!(error = %err, "quic connect failed, backing off");
                    }
                },
                Err(err) => {
                    return Err(ClientError::Connect(
                        self.dest(),
                        io::Error::other(err.to_string()),
                        "failed to start QUIC connection".to_string(),
                    ));
                }
            }

            match backoff.next_backoff() {
                Some(delay) => tokio::time::sleep(delay).await,
                None => {
                    return Err(ClientError::ConnectTimeout(
                        self.dest(),
                        reconnect_timeout,
                        io::Error::other("failed to establish QUIC connection"),
                    ));
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct QuicSocketListener {
    endpoint: Endpoint,
    addr: TlsAddr,
    addr_type: QuicAddrType,
}

#[async_trait]
impl super::Listener for QuicSocketListener {
    type Stream = QuicStream;

    async fn accept(&mut self) -> Result<(Self::Stream, ChannelAddr), ServerError> {
        let Some(incoming) = self.endpoint.accept().await else {
            return Err(ServerError::Io(
                self.addr_type.addr(self.addr.clone()),
                io::Error::other("QUIC endpoint closed"),
            ));
        };
        let peer_addr = incoming.remote_address();
        let connection = incoming.await.map_err(|err| {
            ServerError::Io(
                self.addr_type.addr(self.addr.clone()),
                io::Error::other(err.to_string()),
            )
        })?;
        let (send, recv) = connection.accept_bi().await.map_err(|err| {
            ServerError::Io(
                self.addr_type.addr(self.addr.clone()),
                io::Error::other(err.to_string()),
            )
        })?;
        Ok((QuicStream::new(send, recv), ChannelAddr::Tcp(peer_addr)))
    }
}

fn client_config(addr_type: QuicAddrType) -> anyhow::Result<quinn::ClientConfig> {
    let rustls_config = match addr_type {
        QuicAddrType::Quic => tls::client_config_from_bundle(&tls::get_pem_bundle())?,
        QuicAddrType::MetaQuic => meta::client_config()?,
    };
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_config))?;
    Ok(quinn::ClientConfig::new(Arc::new(crypto)))
}

fn server_config(addr_type: QuicAddrType) -> anyhow::Result<quinn::ServerConfig> {
    let rustls_config = match addr_type {
        QuicAddrType::Quic => tls::server_config_from_bundle(&tls::get_pem_bundle(), true)?,
        QuicAddrType::MetaQuic => meta::server_config(true)?,
    };
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_config))?;
    Ok(quinn::ServerConfig::with_crypto(Arc::new(crypto)))
}

pub(crate) fn link(
    addr: TlsAddr,
    addr_type: QuicAddrType,
    session_id: SessionId,
    stream_id: u8,
    kind: ProtocolKind,
) -> Result<QuicLink, ClientError> {
    let client_config = client_config(addr_type).map_err(|e| {
        ClientError::Connect(
            addr_type.addr(addr.clone()),
            io::Error::other(e.to_string()),
            "failed to create QUIC client config".to_string(),
        )
    })?;
    let TlsAddr { hostname, port } = addr;
    Ok(QuicLink {
        hostname,
        port,
        client_config,
        addr_type,
        session_id,
        stream_id,
        kind,
    })
}

pub(crate) fn listen(
    addr: TlsAddr,
    addr_type: QuicAddrType,
) -> Result<(QuicSocketListener, ChannelAddr), ServerError> {
    let server_config = server_config(addr_type).map_err(|e| {
        ServerError::Listen(
            addr_type.addr(addr.clone()),
            io::Error::other(e.to_string()),
        )
    })?;
    let TlsAddr { hostname, port } = addr;
    let addrs: Vec<core::net::SocketAddr> = (hostname.as_ref(), port)
        .to_socket_addrs()
        .map_err(|err| ServerError::Resolve(addr_type.addr(TlsAddr::new(&hostname, port)), err))?
        .collect();

    if addrs.is_empty() {
        return Err(ServerError::Resolve(
            addr_type.addr(TlsAddr::new(&hostname, port)),
            io::Error::other("no available socket addr"),
        ));
    }

    let endpoint = Endpoint::server(server_config, addrs[0])
        .map_err(|err| ServerError::Listen(addr_type.addr(TlsAddr::new(&hostname, port)), err))?;
    let local_addr = endpoint
        .local_addr()
        .map_err(|err| ServerError::Resolve(addr_type.addr(TlsAddr::new(&hostname, port)), err))?;
    let bound_addr = addr_type.addr(TlsAddr::new(hostname, local_addr.port()));
    Ok((
        QuicSocketListener {
            endpoint,
            addr: match &bound_addr {
                ChannelAddr::Quic(addr) | ChannelAddr::MetaQuic(addr) => addr.clone(),
                _ => unreachable!(),
            },
            addr_type,
        },
        bound_addr,
    ))
}
