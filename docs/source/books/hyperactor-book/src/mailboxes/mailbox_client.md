# MailboxClient

A `MailboxClient` is the sending counterpart to a `MailboxServer`. It transmits messages directly over a `channel::Tx` interface to a remote server.

The client handles undeliverable returns, maintains a background task for monitoring channel health, and implements `MailboxSender` for compatibility.

Topics in this section:

- The `MailboxClient` struct and its `new` constructor
- Direct transmission via `channel::Tx`
- Delivery error handling and monitoring

## The Buffer Abstraction

While `MailboxClient` itself transmits messages directly without internal buffering, the `Buffer<T>` abstraction is a foundational type used in several other components in the system (such as `DurableMailboxSender`). It's a concurrency-safe buffered message processor, parameterized on the message type `T`.

The buffer:
- accepts messages of type `T`
- spawns an internal background task to process messages asynchronously
- tracks how many messages have been processed via a `watch` channel + `AtomicUsize`:
```rust
struct Buffer<T: Message> {
    queue: mpsc::UnboundedSender<(T, PortHandle<Undeliverable<T>>)>,
    processed: watch::Receiver<usize>,
    seq: AtomicUsize,
}
```
For functions of type `Fn(T) -> impl Future<Output = ()>`, a new `Buffer<T>` can be constructed:
```rust
impl<T: Message> Buffer<T> {
    fn new<Fut>(
        process: impl Fn(T, PortHandle<Undeliverable<T>>) -> Fut + Send + Sync + 'static,
    ) -> Self
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (queue, mut next) = mpsc::unbounded_channel();
        let (last_processed, processed) = watch::channel(0);
        crate::init::RUNTIME.spawn(async move {
            let mut seq = 0;
            while let Some((msg, return_handle)) = next.recv().await {
                process(msg, return_handle).await;
                seq += 1;
                let _ = last_processed.send(seq);
            }
        });
        Self {
            queue,
            processed,
            seq: AtomicUsize::new(0),
        }
    }
}
```
The `Buffer<T>` type is constructed by providing a user-supplied asynchronous processing function. This function accepts incoming messages of type `T` together with a return handle for undeliverable messages. Each time a message is enqueued into the buffer, it is delivered to the processing function in the order received.

Internally, the buffer maintains an unbounded channel for queued messages and spawns a background task responsible for processing messages sequentially. As each message is handled, the buffer advances an internal sequence counter and updates a watch channel, allowing external components to monitor processing progress if needed. The processing function is fully asynchronous: the buffer awaits its completion before proceeding to the next message, ensuring that processing remains ordered and that no work is dropped or skipped.

This design decouples message submission from processing, allowing producers to enqueue messages immediately while processing occurs concurrently in the background. This pattern is used in components like `DurableMailboxSender` where buffering is needed.

We can write a `send` function for `Buffer<T>`. It is not `async` since it just enqueues the incoming `T` for processing:
```rust
impl<T: Message> Buffer<T> {
    fn send(
        &self,
        item: (T, PortHandle<Undeliverable<T>>),
    ) -> Result<(), mpsc::error::SendError<(T, PortHandle<Undeliverable<T>>)>> {
        self.seq.fetch_add(1, Ordering::SeqCst);
        self.queue.send(item)?;
        Ok(())
    }
}
```
The buffer maintains two separate counters: one tracking the number of messages submitted for processing, and another tracking the number of messages fully processed. The submission counter (`seq`) is updated atomically each time a message is enqueued. This allows external components to observe the current backlog of unprocessed messages by comparing the two counters.

The `flush` operation however is `async`:
```rust
impl<T: Message> Buffer<T> {
    async fn flush(&mut self) -> Result<(), watch::error::RecvError> {
        let seq = self.seq.load(Ordering::SeqCst);
        while *self.processed.borrow_and_update() < seq {
            self.processed.changed().await?;
        }
        Ok(())
    }
}
```
This function allows callers to await the completion of all previously submitted messages. When invoked, the current submission sequence number is read to capture the total number of messages that have been enqueued at that point. The function then asynchronously waits until the processing counter reaches or exceeds this value, indicating that all submitted messages have been fully processed.

Internally, `flush(`) uses the buffer’s watch channel to observe updates as message processing advances. Each time a message completes processing, the background task updates the watch channel, allowing` flush()` to efficiently wait without busy-waiting or polling.

## Role and Behavior of `MailboxClient`

The `MailboxServer` listens for incoming messages on a channel and delivers them to the system. The `MailboxClient` acts as the sender, transmitting messages directly to the server.

A `MailboxClient` is the **dual** of a `MailboxServer`. It:
- holds a reference to a `channel::Tx<MessageEnvelope>` for direct transmission;
- transmits messages synchronously via `try_post`;
- reports undeliverable messages via a `PortHandle<Undeliverable<MessageEnvelope>>`;
- monitors the transmission channel for health and shuts down appropriately.

`MailboxServer` is a trait defining the receiving side of a message channel; `MailboxClient` is a concrete sender that transmits messages directly to it:
```rust
pub struct MailboxClient {
    tx: Arc<dyn channel::Tx<MessageEnvelope> + Send + Sync>,
    _tx_monitoring: CancellationToken,
}
```

The `MailboxClient::new` constructor creates a client capable of sending `MessageEnvelope`s over a `channel::Tx`. This channel represents the transmission path to a remote `MailboxServer`.
```rust
impl MailboxClient {
    pub fn new(tx: impl channel::Tx<MessageEnvelope> + Send + Sync + 'static) -> Self {
        let addr = tx.addr();
        let tx: Arc<dyn channel::Tx<MessageEnvelope> + Send + Sync> = Arc::new(tx);
        let tx_status = tx.status().clone();
        let tx_monitoring = CancellationToken::new();
        let this = Self {
            tx,
            _tx_monitoring: tx_monitoring.clone(),
        };
        Self::monitor_tx_health(tx_status, tx_monitoring, addr);
        this
    }
}
```

Constructing a `MailboxClient` is straightforward:
- The provided `tx` (a `channel::Tx<MessageEnvelope>`) is wrapped in an `Arc` so it can be shared safely.
- A `CancellationToken` is created to coordinate shutdown or monitoring cancellation.
- The constructor installs a monitoring task using `monitor_tx_health`, allowing the client to detect when the transmission channel becomes unhealthy.

The resulting `MailboxClient` consists of the `Arc`-wrapped transmission channel and the cancellation token used to coordinate monitoring.

### `MailboxClient` implements `MailboxSender`

`MailboxClient` itself implements the `MailboxSender` trait. This is made possible by delegating its `post_unchecked` method directly to the underlying `channel::Tx` (by calling `try_post` on it). As a result, any component expecting a `MailboxSender` can use a `MailboxClient` transparently:
```rust
impl MailboxSender for MailboxClient {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        tracing::trace!(name = "post_unchecked", "posting message to {}", envelope.dest);
        let (return_channel, return_receiver) = oneshot::channel();
        // Set up for delivery failure.
        let return_handle_clone = return_handle.clone();
        tokio::spawn(async move {
            let result = return_receiver.await;
            if let Ok(SendError { error: e, message, .. }) = result {
                message.undeliverable(
                    DeliveryError::BrokenLink(format!(
                        "failed to send in MailboxClient: {e}"
                    )),
                    return_handle_clone,
                );
            }
        });
        // Send the message for transmission directly.
        self.tx.try_post(envelope, return_channel);
    }
}
```

The `post_unchecked` method transmits messages directly via the `channel::Tx`:
1. A fresh one-shot channel is created for each message to support delivery-failure return paths.
2. A background task is spawned that awaits the outcome of the one-shot channel.
   - If the `channel::Tx` reports delivery failure by sending the message back on the one-shot channel, the task uses the return handle to report it as undeliverable.
3. The message is transmitted immediately using `tx.try_post()`.

Although `MailboxClient` and `MailboxServer` play dual roles (one sends, the other receives) both implement the `MailboxSender` trait.

In the client's case, implementing `MailboxSender` allows it to participate in code paths that post messages by transmitting them directly over the channel. For the server, `MailboxSender` reflects its ability to post directly into the system after receiving a message from a channel.
