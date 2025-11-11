/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Local (in-process) channel implementation.
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::LazyLock;
use std::sync::Mutex;

use super::*;
use crate::Data;

/// Create a new local channel, returning its two ends.
pub fn new<M: RemoteMessage>() -> (impl Tx<M>, impl Rx<M>) {
    let (tx, rx) = mpsc::unbounded_channel::<M>();
    let (mpsc_tx, status_sender) = MpscTx::new(tx, ChannelAddr::Local(0));
    let mpsc_rx = MpscRx::new(rx, ChannelAddr::Local(0), status_sender);
    (mpsc_tx, mpsc_rx)
}

// In-process channels, with a shared registry.

struct Ports {
    ports: HashMap<u64, (mpsc::UnboundedSender<Data>, watch::Receiver<TxStatus>)>,
    next_port: u64,
}

impl Ports {
    fn alloc(&mut self) -> (u64, mpsc::UnboundedReceiver<Data>, watch::Sender<TxStatus>) {
        let port = self.next_port;
        self.next_port += 1;
        let (tx, rx) = mpsc::unbounded_channel::<Data>();
        let (status_tx, status_rx) = watch::channel(TxStatus::Active);
        if self.ports.insert(port, (tx.clone(), status_rx)).is_some() {
            panic!("port reused")
        }
        (port, rx, status_tx)
    }

    fn free(&mut self, port: u64) {
        self.ports.remove(&port);
    }

    fn get(&self, port: u64) -> Option<&(mpsc::UnboundedSender<Data>, watch::Receiver<TxStatus>)> {
        self.ports.get(&port)
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

#[derive(Debug)]
pub struct LocalTx<M: RemoteMessage> {
    tx: mpsc::UnboundedSender<Data>,
    port: u64,
    status: watch::Receiver<TxStatus>, // Default impl. Always reports `Active`.
    _phantom: PhantomData<M>,
}

#[async_trait]
impl<M: RemoteMessage> Tx<M> for LocalTx<M> {
    fn do_post(&self, message: M, return_channel: Option<oneshot::Sender<SendError<M>>>) {
        let data: Data = match bincode::serialize(&message) {
            Ok(data) => data,
            Err(err) => {
                if let Some(return_channel) = return_channel {
                    return_channel
                        .send(SendError(err.into(), message))
                        .unwrap_or_else(|m| tracing::warn!("failed to deliver SendError: {}", m));
                }
                return;
            }
        };
        if self.tx.send(data).is_err() {
            if let Some(return_channel) = return_channel {
                return_channel
                    .send(SendError(ChannelError::Closed, message))
                    .unwrap_or_else(|m| tracing::warn!("failed to deliver SendError: {}", m));
            }
        }
    }

    fn addr(&self) -> ChannelAddr {
        ChannelAddr::Local(self.port)
    }

    fn status(&self) -> &watch::Receiver<TxStatus> {
        &self.status
    }
}

#[derive(Debug)]
pub struct LocalRx<M: RemoteMessage> {
    data_rx: mpsc::UnboundedReceiver<Data>,
    status_tx: watch::Sender<TxStatus>,
    port: u64,
    _phantom: PhantomData<M>,
}

#[async_trait]
impl<M: RemoteMessage> Rx<M> for LocalRx<M> {
    async fn recv(&mut self) -> Result<M, ChannelError> {
        let data = self.data_rx.recv().await.ok_or(ChannelError::Closed)?;
        bincode::deserialize(&data).map_err(ChannelError::from)
    }

    fn addr(&self) -> ChannelAddr {
        ChannelAddr::Local(self.port)
    }
}

impl<M: RemoteMessage> Drop for LocalRx<M> {
    fn drop(&mut self) {
        let _ = self.status_tx.send(TxStatus::Closed);
        PORTS.lock().unwrap().free(self.port);
    }
}

/// Dial a local port, returning a Tx for it.
#[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `ChannelError`.
pub fn dial<M: RemoteMessage>(port: u64) -> Result<LocalTx<M>, ChannelError> {
    let ports = PORTS.lock().unwrap();
    let result = ports.get(port);
    if let Some((data_tx, status_rx)) = result {
        Ok(LocalTx {
            tx: data_tx.clone(),
            port,
            status: status_rx.clone(),
            _phantom: PhantomData,
        })
    } else {
        Err(ChannelError::Closed)
    }
}

/// Serve a local port. The server is shut down when the returned Rx is dropped.
pub fn serve<M: RemoteMessage>() -> (u64, LocalRx<M>) {
    let (port, data_rx, status_tx) = PORTS.lock().unwrap().alloc();
    (
        port,
        LocalRx {
            data_rx,
            status_tx,
            port,
            _phantom: PhantomData,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    #[tokio::test]
    async fn test_local_basic() {
        let (tx, mut rx) = local::new::<u64>();

        tx.post(123);
        assert_eq!(rx.recv().await.unwrap(), 123);
    }

    #[tokio::test]
    async fn test_local_dial_serve() {
        let (port, mut rx) = local::serve::<u64>();
        assert!(port != 0);

        let tx = local::dial::<u64>(port).unwrap();

        tx.post(123);
        assert_eq!(rx.recv().await.unwrap(), 123);

        drop(rx);

        let (return_tx, return_rx) = oneshot::channel();
        tx.try_post(123, return_tx);
        assert_matches!(return_rx.await, Ok(SendError(ChannelError::Closed, 123)));
    }

    #[tokio::test]
    async fn test_local_drop() {
        let (port, mut rx) = local::serve::<u64>();
        let tx = local::dial::<u64>(port).unwrap();

        tx.post(123);
        assert_eq!(rx.recv().await.unwrap(), 123);

        drop(rx);

        assert_matches!(
            local::dial::<u64>(port).err().unwrap(),
            ChannelError::Closed
        );
    }
}
