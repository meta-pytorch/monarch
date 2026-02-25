/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use log::*;

use super::*;

/// A [`DurableMailboxSender`] is a [`MailboxSender`] that writes messages to a write-ahead log
/// before the receiver consume any of them. It allows the receiver to recover from crashes by
/// replaying the log. It supports any implementation of [`MailboxSender`].
struct DurableMailboxSender(Buffer<MessageEnvelope>);

impl DurableMailboxSender {
    #[allow(dead_code)]
    fn new(
        write_ahead_log: impl MessageLog<MessageEnvelope> + 'static,
        inner: impl MailboxSender + 'static,
    ) -> Self {
        let write_ahead_log = Arc::new(tokio::sync::Mutex::new(write_ahead_log));
        let inner = Arc::new(inner);
        let sequencer =
            Buffer::new(
                move |envelope: MessageEnvelope,
                      return_handle: PortHandle<Undeliverable<MessageEnvelope>>| {
                    let write_ahead_log = write_ahead_log.clone();
                    let inner = inner.clone();
                    let return_handle = return_handle.clone();
                    async move {
                        let envelope_copy = envelope.clone(); // we maintain a copy in case we have to mark it failed
                        let port_id = envelope.dest().clone();
                        let mut log = write_ahead_log.lock().await;
                        // TODO: There are potentially two ways to avoid copy; both require interface change.
                        // (1) use Rc or Arc and (2) implement our own CopyOnDrop struct
                        let append_result = log.append(envelope).await.map_err(|err| {
                            MailboxSenderError::new_bound(port_id.clone(), err.into())
                        });

                        let flush_result = log.flush().await.map_err(|err| {
                            MailboxSenderError::new_bound(port_id.clone(), err.into())
                        });

                        drop(log);

                        if append_result.and(flush_result).is_ok() {
                            inner.post(envelope_copy, return_handle);
                        } else {
                            envelope_copy.undeliverable(
                                DeliveryError::BrokenLink(
                                    "failed to append or flush in durable sender".to_string(),
                                ),
                                return_handle,
                            );
                        }
                    }
                },
            );

        Self(sequencer)
    }

    #[allow(dead_code)]
    async fn flush(&mut self) -> Result<(), watch::error::RecvError> {
        self.0.flush().await
    }
}

#[async_trait]
impl MailboxSender for DurableMailboxSender {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        if let Err(mpsc::error::SendError((envelope, return_handle))) =
            self.0.send((envelope, return_handle))
        {
            envelope.undeliverable(
                DeliveryError::BrokenLink("failed to post in DurableMailboxSender".to_string()),
                return_handle,
            );
        }
    }
}

pub mod log {

    //! This module implements a write-ahead log for mailboxes. This can be used to provide
    //! durable messaging facilities for actors.

    use std::fmt::Debug;

    use async_trait::async_trait;
    use futures::stream::Stream;

    use crate::RemoteMessage;

    /// A sequence id is a unique identifier for a message.
    pub type SeqId = u64;

    /// Errors that occur during message log operations.
    /// This enum is marked non-exhaustive to allow for extensibility.
    #[derive(thiserror::Error, Debug)]
    #[non_exhaustive]
    pub enum MessageLogError {
        /// An error occured during flushing messages with a sequence id range.
        #[error("flush: [{0}, {1})")]
        Flush(SeqId, SeqId, #[source] anyhow::Error),

        /// An error occured during appending a message with an assigned sequence id.
        #[error("append: {0}")]
        Append(SeqId, #[source] anyhow::Error),

        /// An error occured during reading a message with the persistent sequence id.
        #[error("read: {0}")]
        Read(SeqId, #[source] anyhow::Error),

        /// An error occured during trimming a message with the persistent sequence id.
        #[error("trim: {0}")]
        Trim(SeqId, #[source] anyhow::Error),

        /// An other error.
        #[error(transparent)]
        Other(#[from] anyhow::Error),
    }

    /// This [`MessageLog`] is a log that serves as a building block to persist data before the
    /// consumer can process it. One typical example is to persist messages before an actor handles it.
    /// In such a case, it can be used as a white-ahead log. It allows the actor to recover from a
    /// crash without requesting resending the messages. The log is append-only and the messages are
    /// persisted in order with sequence ids.
    #[async_trait]
    pub trait MessageLog<M: RemoteMessage>: Sync + Send {
        /// The type of the stream returned from read operations on this log.
        type Stream<'a>: Stream<Item = Result<(SeqId, M), MessageLogError>> + Send
        where
            Self: 'a;

        /// Append a message to a buffer. The appended messages will only be persisted and available to
        /// read after calling [`flush`].
        async fn append(&mut self, message: M) -> Result<(), MessageLogError>;

        /// Flush the appended messages. Return the next sequence id of the last persistent message.
        async fn flush(&mut self) -> Result<SeqId, MessageLogError>;

        /// Directly flush the message. All previously buffered messages will be flushed as well.
        /// This convenience method can prevent an additional copy of the message by directly writing to the log.
        async fn append_and_flush(&mut self, message: &M) -> Result<SeqId, MessageLogError>;

        /// Trim the persistent logs before the given [`new_start`] non-inclusively.
        async fn trim(&mut self, new_start: SeqId) -> Result<(), MessageLogError>;

        /// Given a sequence id, return a stream of message and sequence id tuples that are persisted
        /// after the given sequence id inclusively. The stream will yield errors when streaming
        /// messages back if any. It will also yield errors if creating the stream itself fails.
        async fn read(&self, from: SeqId) -> Result<Self::Stream<'_>, MessageLogError>;

        /// Read exactly one message from the log. If the log is empty, return an error.
        // Ideally, this method can have a default implmentation. But the compiler complains
        // about `self` does not live long enough.
        async fn read_one(&self, seq_id: SeqId) -> Result<M, MessageLogError>;
    }
}

/// A test util mod so that it can be used beyond the crate
pub mod test_utils {

    use std::collections::VecDeque;

    use futures::pin_mut;
    use log::SeqId;
    use tokio_stream::StreamExt;

    use super::*;

    /// An in-memory log for testing.
    #[derive(Clone)]
    pub struct TestLog<M: RemoteMessage> {
        queue: Arc<Mutex<VecDeque<(SeqId, M)>>>,
        current_seq_id: Arc<Mutex<SeqId>>,
        // For outside to validate the values of saved messages.
        observer: Option<mpsc::UnboundedSender<(String, M)>>,
    }

    impl<M: RemoteMessage> Default for TestLog<M> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<M: RemoteMessage> TestLog<M> {
        /// Create a new, empty [`TestLog`].
        pub fn new() -> Self {
            Self {
                queue: Arc::new(Mutex::new(VecDeque::new())),
                current_seq_id: Arc::new(Mutex::new(0)),
                observer: None,
            }
        }

        /// Create a new test log that sends all log operations to the provided
        /// observer. The observer is sent tuples of `(op, message)`, where `op` is
        /// either "append" or "read".
        pub fn new_with_observer(observer: mpsc::UnboundedSender<(String, M)>) -> Self {
            Self {
                queue: Arc::new(Mutex::new(VecDeque::new())),
                current_seq_id: Arc::new(Mutex::new(0)),
                observer: Some(observer),
            }
        }
    }

    #[async_trait]
    impl<M: RemoteMessage + Clone> MessageLog<M> for TestLog<M> {
        type Stream<'a> =
            futures::stream::Iter<std::vec::IntoIter<Result<(SeqId, M), MessageLogError>>>;

        async fn append(&mut self, message: M) -> Result<(), MessageLogError> {
            let mut seq_id = self.current_seq_id.lock().unwrap();
            self.queue
                .lock()
                .unwrap()
                .push_back((*seq_id, message.clone()));
            *seq_id += 1;
            if let Some(observer) = &self.observer {
                observer.send(("append".to_string(), message)).unwrap();
            }
            Ok(())
        }

        async fn flush(&mut self) -> Result<SeqId, MessageLogError> {
            let seq_id = *self.current_seq_id.lock().unwrap();
            Ok(seq_id)
        }

        async fn append_and_flush(&mut self, message: &M) -> Result<SeqId, MessageLogError> {
            self.append(message.clone()).await?;
            self.flush().await
        }

        async fn trim(&mut self, new_start: SeqId) -> Result<(), MessageLogError> {
            let mut queue = self.queue.lock().unwrap();
            while let Some((id, _)) = queue.front() {
                if *id < new_start {
                    queue.pop_front();
                } else {
                    break;
                }
            }
            Ok(())
        }

        async fn read(&self, seq_id: SeqId) -> Result<Self::Stream<'_>, MessageLogError> {
            let queue = self.queue.lock().unwrap();
            let filtered_items: Vec<_> = queue
                .iter()
                .filter(move |(id, _)| *id >= seq_id)
                .map(|(seq_id, msg)| Ok((*seq_id, msg.clone())))
                .collect();
            for entry in filtered_items.iter() {
                if let Some(observer) = &self.observer
                    && let Ok((_, msg)) = entry.as_ref()
                {
                    observer.send(("read".to_string(), msg.clone())).unwrap();
                }
            }
            Ok(futures::stream::iter(filtered_items))
        }

        async fn read_one(&self, seq_id: SeqId) -> Result<M, MessageLogError> {
            let it = self.read(seq_id).await?;

            pin_mut!(it);
            match it.next().await {
                Some(Ok((result_seq_id, message))) => {
                    if result_seq_id != seq_id {
                        panic!("no seq id {}", seq_id);
                    }
                    return Ok(message);
                }
                Some(Err(err)) => {
                    return Err(err);
                }
                None => {
                    return Err(MessageLogError::Read(
                        seq_id,
                        anyhow::anyhow!("failed to find message with sequence {}", seq_id),
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::assert_matches::assert_matches;
    use std::mem::drop;

    use futures::StreamExt;

    use super::test_utils::TestLog;
    use super::*;
    use crate::id;
    use crate::mailbox::log::SeqId;

    #[tokio::test]
    async fn test_local_write_ahead_log_basic() {
        let mut wal = TestLog::new();
        wal.append(124u64).await.unwrap();
        wal.append(56u64).await.unwrap();
        let seq_id = wal.append_and_flush(&999u64).await.unwrap();
        assert_eq!(seq_id, 3);

        // Simple read given a sequence id
        let mut it = wal.read(1).await.unwrap();
        let (next_seq, message): (SeqId, u64) = it.next().await.unwrap().unwrap();
        assert_eq!(next_seq, 1);
        assert_eq!(message, 56u64);
        let (next_seq, message) = it.next().await.unwrap().unwrap();
        assert_eq!(next_seq, 2);
        assert_eq!(message, 999u64);
        assert_matches!(it.next().await, None);
        // Drop the iterator to release borrow from wal
        drop(it);

        // Trim then append
        wal.trim(2).await.unwrap();
        let seq_id = wal.append_and_flush(&777u64).await.unwrap();
        assert_eq!(seq_id, 4);
        let mut it = wal.read(2).await.unwrap();
        let (next_seq, message): (SeqId, u64) = it.next().await.unwrap().unwrap();
        assert_eq!(next_seq, 2);
        assert_eq!(message, 999u64);
        let (next_seq, message) = it.next().await.unwrap().unwrap();
        assert_eq!(next_seq, 3);
        assert_eq!(message, 777u64);
        assert_matches!(it.next().await, None);
    }

    #[tokio::test]
    async fn test_durable_mailbox_sender() {
        let inner = Mailbox::new_detached(id!(world0[0].actor0));
        let write_ahead_log = TestLog::new();
        let mut durable_mbox = DurableMailboxSender::new(write_ahead_log.clone(), inner.clone());

        let (port1, mut receiver1) = inner.open_port::<u64>();
        let (port2, mut _receiver2) = inner.open_port::<u64>();

        // Convert to references so that the ports are registered.
        let port1 = port1.bind();
        let port2 = port2.bind();

        durable_mbox.post(
            MessageEnvelope::new_unknown(
                port1.port_id().clone(),
                wirevalue::Any::serialize(&1u64).unwrap(),
            ),
            monitored_return_handle(),
        );
        durable_mbox.post(
            MessageEnvelope::new_unknown(
                port2.port_id().clone(),
                wirevalue::Any::serialize(&2u64).unwrap(),
            ),
            monitored_return_handle(),
        );
        durable_mbox.post(
            MessageEnvelope::new_unknown(
                port1.port_id().clone(),
                wirevalue::Any::serialize(&3u64).unwrap(),
            ),
            monitored_return_handle(),
        );
        assert_eq!(receiver1.recv().await.unwrap(), 1u64);

        durable_mbox.flush().await.unwrap();

        let mut it = write_ahead_log.read(1).await.unwrap();
        let (seq, message): (SeqId, MessageEnvelope) = it.next().await.unwrap().unwrap();
        assert_eq!(seq, 1);
        assert_eq!(port2.port_id(), message.dest());
        assert_eq!(2u64, message.deserialized::<u64>().unwrap());
        let (seq, message): (SeqId, MessageEnvelope) = it.next().await.unwrap().unwrap();
        assert_eq!(seq, 2);
        assert_eq!(3u64, message.deserialized::<u64>().unwrap());
    }
}
