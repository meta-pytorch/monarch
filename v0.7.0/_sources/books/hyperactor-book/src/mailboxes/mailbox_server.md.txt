# MailboxServer

A `MailboxServer` listens for incoming `MessageEnvelope`s from an external source and posts them into a mailbox using the `MailboxSender` trait.

This section describes:

- The `MailboxServer` trait and its `serve` method
- The `MailboxServerHandle` used for graceful shutdown
- The internal `tokio::select!` loop for serving messages

A `MailboxServer` is any `MailboxSender` that supports being connected to a channel from which it receives `MessageEnvelope`s. It defines a single function, `serve`, which spawns a background task that listens for messages on the channel and posts them into the system via its `post` method.

A `MailboxServerHandle` represents a running mailbox server. The handle composes a tokio `JoinHandle` and may be joined in the same manner (`MailboxServerHandle` implements `Future`):
```rust
#[derive(Debug)]
pub struct MailboxServerHandle {
    join_handle: JoinHandle<Result<(), MailboxServerError>>,
    stopped_tx: watch::Sender<bool>,
}
```

A mailbox server can be turned down using the `watch::Sender`:
```rust
impl MailboxServerHandle {
    pub fn stop(&self) {
        self.stopped_tx.send(true).expect("stop called twice");
    }
}
```

There is an error type associated with "mailbox serving":
```rust
#[derive(thiserror::Error, Debug)]
pub enum MailboxServerError {
    /// An underlying channel error.
    #[error(transparent)]
    Channel(#[from] ChannelError),

    /// An underlying mailbox sender error.
    #[error(transparent)]
    MailboxSender(#[from] MailboxSenderError),
}
```

A `MailboxServer` is any `MailboxSender` that supports being connected to a channel from which it receieves `MessageEnvelope`s. It runs a background task that listens for messages on the channel and posts them into the system via its `post` method:
```rust
pub trait MailboxServer: MailboxSender + Sized + 'static {
    fn serve(
        self,
        mut rx: impl channel::Rx<MessageEnvelope> + Send + 'static,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) -> MailboxServerHandle {
        let (stopped_tx, mut stopped_rx) = watch::channel(false);
        let join_handle = tokio::spawn(async move {
            let mut detached = false;

            loop {
                if *stopped_rx.borrow_and_update() {
                    break Ok(());
                }

                tokio::select! {
                    message = rx.recv() => {
                        match message {
                            // Relay the message to the port directly.
                            Ok(envelope) => self.post(envelope, return_handle.clone()),

                            // Closed is a "graceful" error in this case.
                            // We simply stop serving.
                            Err(ChannelError::Closed) => break Ok(()),
                            Err(channel_err) => break Err(MailboxServerError::from(channel_err)),
                        }
                    }
                    result = stopped_rx.changed(), if !detached  => {
                        tracing::debug!(
                            "the mailbox server is stopped"
                        );
                        detached = result.is_err();
                    }
                }
            }
        });

        MailboxServerHandle {
            join_handle,
            stopped_tx,
        }
    }
}
```
The use of `detached` above is clever - there is no point on waiting for `stopped_rx.changed()` any more if the sender has been dropped.

This provides a general mechanism for bridging external message sources such as remote transport with local mailbox delivery.

The `serve` function spawns this background task and returns a handle that can be used to signal shutdown or await termination.

This blanket impl declares that **any type `T`** which:
- implements the `MailboxSender` trait
- is `Sized`, `Sync`, `Send` and `'static`

will **automatically implement `MailboxServer`** by inheriting the default `serve` function provided in the trait definition:
```rust
impl<T: MailboxSender + Sized + Sync + Send + 'static> MailboxServer for T {}
```
