/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Local (in-process) channel implementation.
use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::Mutex;

use tokio::net::UnixStream;
use tokio::sync::mpsc;

use super::*;

// In-process channels, with a shared registry.

struct PortEntry {
    tx: Option<mpsc::UnboundedSender<UnixStream>>,
}

struct Ports {
    ports: HashMap<u64, PortEntry>,
    next_port: u64,
}

impl Ports {
    fn reserve(&mut self) -> u64 {
        let port = self.next_port;
        self.next_port += 1;
        if self.ports.insert(port, PortEntry { tx: None }).is_some() {
            panic!("port reused")
        }
        port
    }

    fn alloc_stream(&mut self) -> (u64, mpsc::UnboundedReceiver<UnixStream>) {
        let port = self.reserve();
        let rx = self
            .bind_stream(port)
            .expect("fresh local stream port must bind");
        (port, rx)
    }

    fn bind_stream(
        &mut self,
        port: u64,
    ) -> Result<mpsc::UnboundedReceiver<UnixStream>, ChannelError> {
        if self
            .ports
            .get(&port)
            .and_then(|entry| entry.tx.as_ref())
            .is_some()
        {
            return Err(ChannelError::InvalidAddress(format!(
                "local addr already bound: {}",
                port
            )));
        }
        self.next_port = self.next_port.max(port.saturating_add(1));
        let (tx, rx) = mpsc::unbounded_channel::<UnixStream>();
        self.ports.insert(port, PortEntry { tx: Some(tx) });
        Ok(rx)
    }

    fn free(&mut self, port: u64) {
        self.ports.remove(&port);
    }

    fn get_stream(&self, port: u64) -> Option<mpsc::UnboundedSender<UnixStream>> {
        self.ports.get(&port).and_then(|entry| entry.tx.clone())
    }

    fn has_stream(&self, port: u64) -> bool {
        self.ports
            .get(&port)
            .and_then(|entry| entry.tx.as_ref())
            .is_some()
    }
}

impl Default for Ports {
    fn default() -> Self {
        Self {
            ports: HashMap::new(),
            next_port: 1,
        }
    }
}

static PORTS: LazyLock<Mutex<Ports>> = LazyLock::new(|| Mutex::new(Ports::default()));

/// Reserve a local address that can be served later.
pub fn reserve() -> u64 {
    PORTS.lock().unwrap().reserve()
}

pub(crate) mod stream {
    use std::fmt;
    use std::os::unix::net::UnixStream as StdUnixStream;

    use async_trait::async_trait;
    use tokio::net::UnixStream;
    use tokio::sync::mpsc;

    use super::PORTS;
    use crate::channel::ChannelAddr;
    use crate::channel::net::ClientError;
    use crate::channel::net::Link;
    use crate::channel::net::Listener;
    use crate::channel::net::ProtocolKind;
    use crate::channel::net::ServerError;
    use crate::channel::net::SessionId;
    use crate::channel::net::write_link_init;

    #[derive(Debug)]
    pub(crate) struct LocalLink {
        port: u64,
        session_id: SessionId,
        stream_id: u8,
        kind: ProtocolKind,
    }

    impl LocalLink {
        pub(crate) fn new(
            port: u64,
            session_id: SessionId,
            stream_id: u8,
            kind: ProtocolKind,
        ) -> Self {
            Self {
                port,
                session_id,
                stream_id,
                kind,
            }
        }

        fn dest(&self) -> ChannelAddr {
            ChannelAddr::Local(self.port)
        }

        fn pair(&self) -> Result<(UnixStream, UnixStream), ClientError> {
            let dest = self.dest();
            let (client, server) = StdUnixStream::pair().map_err(|err| {
                ClientError::Connect(dest.clone(), err, "local socket pair failed".into())
            })?;
            client.set_nonblocking(true).map_err(|err| {
                ClientError::Connect(dest.clone(), err, "local client socket setup failed".into())
            })?;
            server.set_nonblocking(true).map_err(|err| {
                ClientError::Connect(dest.clone(), err, "local server socket setup failed".into())
            })?;
            let client =
                UnixStream::from_std(client).map_err(|err| ClientError::Io(dest.clone(), err))?;
            let server =
                UnixStream::from_std(server).map_err(|err| ClientError::Io(dest.clone(), err))?;
            Ok((client, server))
        }

        fn connect(&self, server: UnixStream) -> Result<(), ClientError> {
            let dest = self.dest();
            let accept_tx = PORTS.lock().unwrap().get_stream(self.port).ok_or_else(|| {
                ClientError::Connect(
                    dest.clone(),
                    std::io::Error::new(std::io::ErrorKind::NotConnected, "channel closed"),
                    "channel closed".into(),
                )
            })?;
            accept_tx.send(server).map_err(|_| {
                ClientError::Connect(
                    dest,
                    std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "channel closed"),
                    "channel closed".into(),
                )
            })
        }
    }

    pub(crate) fn check(port: u64) -> Result<(), ClientError> {
        if PORTS.lock().unwrap().has_stream(port) {
            Ok(())
        } else {
            Err(ClientError::Connect(
                ChannelAddr::Local(port),
                std::io::Error::new(std::io::ErrorKind::NotConnected, "channel closed"),
                "channel closed".into(),
            ))
        }
    }

    #[async_trait]
    impl Link for LocalLink {
        type Stream = UnixStream;

        fn dest(&self) -> ChannelAddr {
            ChannelAddr::Local(self.port)
        }

        fn link_id(&self) -> SessionId {
            self.session_id
        }

        async fn next(&mut self) -> Result<Self::Stream, ClientError> {
            let (mut client, server) = self.pair()?;
            self.connect(server)?;
            write_link_init(&mut client, self.session_id, self.stream_id, self.kind)
                .await
                .map_err(|err| ClientError::Io(self.dest(), err))?;
            Ok(client)
        }
    }

    pub(crate) struct LocalListener {
        accept_rx: mpsc::UnboundedReceiver<UnixStream>,
        port: u64,
    }

    impl fmt::Debug for LocalListener {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("LocalListener")
                .field("addr", &ChannelAddr::Local(self.port))
                .finish()
        }
    }

    impl Drop for LocalListener {
        fn drop(&mut self) {
            PORTS.lock().unwrap().free(self.port);
        }
    }

    #[async_trait]
    impl Listener for LocalListener {
        type Stream = UnixStream;

        async fn accept(&mut self) -> Result<(Self::Stream, ChannelAddr), ServerError> {
            let addr = ChannelAddr::Local(self.port);
            let stream = self.accept_rx.recv().await.ok_or_else(|| {
                ServerError::Io(addr.clone(), std::io::Error::other("local listener closed"))
            })?;
            Ok((stream, addr))
        }
    }

    pub(crate) fn listen(
        addr: ChannelAddr,
        prebound: Option<std::net::TcpListener>,
    ) -> Result<(LocalListener, ChannelAddr), ServerError> {
        if prebound.is_some() {
            return Err(ServerError::Listen(
                addr,
                std::io::Error::other("pre-opened listener not supported for Local transport"),
            ));
        }

        let ChannelAddr::Local(port) = addr else {
            return Err(ServerError::Listen(
                addr.clone(),
                std::io::Error::other(format!("unsupported transport: {}", addr)),
            ));
        };
        let (port, accept_rx) = if port == 0 {
            PORTS.lock().unwrap().alloc_stream()
        } else {
            let accept_rx = PORTS.lock().unwrap().bind_stream(port).map_err(|err| {
                ServerError::Listen(
                    ChannelAddr::Local(port),
                    std::io::Error::other(err.to_string()),
                )
            })?;
            (port, accept_rx)
        };
        let addr = ChannelAddr::Local(port);
        Ok((LocalListener { accept_rx, port }, addr))
    }

    pub(crate) fn link(
        port: u64,
        session_id: SessionId,
        stream_id: u8,
        kind: ProtocolKind,
    ) -> LocalLink {
        LocalLink::new(port, session_id, stream_id, kind)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;
    use crate::channel as public_channel;
    use crate::channel::duplex as public_duplex;

    #[tokio::test]
    async fn test_public_local_dial_serve() {
        let (addr, mut rx) =
            public_channel::serve::<u64>(ChannelAddr::any(ChannelTransport::Local)).unwrap();
        let ChannelAddr::Local(port) = addr else {
            panic!("local server must bind a local address");
        };
        assert!(port != 0);

        let tx = public_channel::dial::<u64>(ChannelAddr::Local(port)).unwrap();
        tx.post(123);
        assert_eq!(rx.recv().await.unwrap(), 123);

        drop(rx);

        assert_matches!(
            tx.try_post(123).await,
            Err(SendError {
                error: ChannelError::Closed,
                message: 123,
                ..
            })
        );
    }

    #[tokio::test]
    async fn test_public_local_drop() {
        let (addr, mut rx) =
            public_channel::serve::<u64>(ChannelAddr::any(ChannelTransport::Local)).unwrap();
        let ChannelAddr::Local(port) = addr else {
            panic!("local server must bind a local address");
        };
        let tx = public_channel::dial::<u64>(ChannelAddr::Local(port)).unwrap();

        tx.post(123);
        assert_eq!(rx.recv().await.unwrap(), 123);

        rx.join().await;

        assert!(public_channel::dial::<u64>(ChannelAddr::Local(port)).is_err());
    }

    #[tokio::test]
    async fn test_public_local_duplex_dial_serve() {
        assert!(ChannelTransport::Local.supports_duplex());

        let mut server =
            public_duplex::serve::<u64, String>(ChannelAddr::any(ChannelTransport::Local), None)
                .unwrap();
        let ChannelAddr::Local(port) = server.addr().clone() else {
            panic!("local duplex server must bind a local address");
        };
        assert!(port != 0);

        let mut client = public_duplex::dial::<u64, String>(server.addr().clone()).unwrap();
        let client_tx = client.tx();
        let mut client_rx = client.take_rx().unwrap();
        let (mut server_rx, server_tx) = server.accept().await.unwrap();

        client_tx.post(7);
        assert_eq!(server_rx.recv().await.unwrap(), 7);

        server_tx.post("seven".to_string());
        assert_eq!(client_rx.recv().await.unwrap(), "seven");
    }
}
