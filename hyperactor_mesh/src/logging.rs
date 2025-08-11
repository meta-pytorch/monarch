/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context as TaskContext;
use std::task::Poll;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Result;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Local;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::RefClient;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelRx;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::ChannelTx;
use hyperactor::channel::Rx;
use hyperactor::channel::Tx;
use hyperactor::channel::TxStatus;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::data::Serialized;
use hyperactor::message::Bind;
use hyperactor::message::Bindings;
use hyperactor::message::Unbind;
use hyperactor_telemetry::env;
use hyperactor_telemetry::log_file_path;
use serde::Deserialize;
use serde::Serialize;
use tokio::io;
use tokio::sync::watch::Receiver;

use crate::bootstrap::BOOTSTRAP_LOG_CHANNEL;

mod line_prefixing_writer;
use line_prefixing_writer::LinePrefixingWriter;

const DEFAULT_AGGREGATE_WINDOW_SEC: u64 = 5;

/// Calculate the Levenshtein distance between two strings
fn levenshtein_distance(left: &str, right: &str) -> usize {
    let left_chars: Vec<char> = left.chars().collect();
    let right_chars: Vec<char> = right.chars().collect();

    let left_len = left_chars.len();
    let right_len = right_chars.len();

    // Handle edge cases
    if left_len == 0 {
        return right_len;
    }
    if right_len == 0 {
        return left_len;
    }

    // Create a matrix of size (len_s1+1) x (len_s2+1)
    let mut matrix = vec![vec![0; right_len + 1]; left_len + 1];

    // Initialize the first row and column
    for (i, row) in matrix.iter_mut().enumerate().take(left_len + 1) {
        row[0] = i;
    }
    for (j, cell) in matrix[0].iter_mut().enumerate().take(right_len + 1) {
        *cell = j;
    }

    // Fill the matrix
    for i in 1..=left_len {
        for j in 1..=right_len {
            let cost = if left_chars[i - 1] == right_chars[j - 1] {
                0
            } else {
                1
            };

            matrix[i][j] = std::cmp::min(
                std::cmp::min(
                    matrix[i - 1][j] + 1, // deletion
                    matrix[i][j - 1] + 1, // insertion
                ),
                matrix[i - 1][j - 1] + cost, // substitution
            );
        }
    }

    // Return the bottom-right cell
    matrix[left_len][right_len]
}

/// Calculate the normalized edit distance between two strings (0.0 to 1.0)
fn normalized_edit_distance(left: &str, right: &str) -> f64 {
    let distance = levenshtein_distance(left, right) as f64;
    let max_len = std::cmp::max(left.len(), right.len()) as f64;

    if max_len == 0.0 {
        0.0 // Both strings are empty, so they're identical
    } else {
        distance / max_len
    }
}

#[derive(Debug, Clone)]
/// LogLine represents a single log line with its content and count
struct LogLine {
    content: String,
    pub count: u64,
}

impl LogLine {
    fn new(content: String) -> Self {
        Self { content, count: 1 }
    }
}

impl fmt::Display for LogLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "\x1b[33m[{} similar log lines]\x1b[0m {}",
            self.count, self.content
        )
    }
}

#[derive(Debug, Clone)]
/// Aggregator is a struct that holds a list of LogLines and a start time.
/// It can aggregate new log lines to existing ones if they are "similar" based on edit distance.
struct Aggregator {
    lines: Vec<LogLine>,
    start_time: SystemTime,
    similarity_threshold: f64, // Threshold for considering two strings similar (0.0 to 1.0)
}

impl Aggregator {
    fn new() -> Self {
        // Default threshold: strings with normalized edit distance < 0.15 are considered similar
        Self::new_with_threshold(0.15)
    }

    fn new_with_threshold(threshold: f64) -> Self {
        Aggregator {
            lines: vec![],
            start_time: RealClock.system_time_now(),
            similarity_threshold: threshold,
        }
    }

    fn reset(&mut self) {
        self.lines.clear();
        self.start_time = RealClock.system_time_now();
    }

    fn add_line(&mut self, line: &str) -> anyhow::Result<()> {
        // Find the most similar existing line
        let mut best_match_idx = None;
        let mut best_similarity = f64::MAX;

        for (idx, existing_line) in self.lines.iter().enumerate() {
            let distance = normalized_edit_distance(&existing_line.content, line);

            // If this line is more similar than our current best match
            if distance < best_similarity && distance < self.similarity_threshold {
                best_match_idx = Some(idx);
                best_similarity = distance;
            }
        }

        // If we found a similar enough line, increment its count
        if let Some(idx) = best_match_idx {
            self.lines[idx].count += 1;
        } else {
            // Otherwise, add a new line
            self.lines.push(LogLine::new(line.to_string()));
        }

        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }
}

// Helper function to format SystemTime
fn format_system_time(time: SystemTime) -> String {
    let datetime: DateTime<Local> = time.into();
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}

impl fmt::Display for Aggregator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Format the start time
        let start_time_str = format_system_time(self.start_time);

        // Get and format the current time
        let current_time = RealClock.system_time_now();
        let end_time_str = format_system_time(current_time);

        // Write the header with formatted time window
        writeln!(
            f,
            "\x1b[36m>>> Aggregated Logs ({}) >>>\x1b[0m",
            start_time_str
        )?;

        // Write each log line
        for line in self.lines.iter() {
            writeln!(f, "{}", line)?;
        }
        writeln!(
            f,
            "\x1b[36m<<< Aggregated Logs ({}) <<<\x1b[0m",
            end_time_str
        )?;
        Ok(())
    }
}

/// Messages that can be sent to the LogClientActor remotely.
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Named,
    Handler,
    HandleClient,
    RefClient
)]
pub enum LogMessage {
    /// Log details
    Log {
        /// The hostname of the process that generated the log
        hostname: String,
        /// The pid of the process that generated the log
        pid: u32,
        /// The target output stream (stdout or stderr)
        output_target: OutputTarget,
        /// The log payload as bytes
        payload: Serialized,
    },

    /// Flush the log
    Flush {},
}

/// Messages that can be sent to the LogClient locally.
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Named,
    Handler,
    HandleClient,
    RefClient
)]
pub enum LogClientMessage {
    SetAggregate {
        /// The time window in seconds to aggregate logs. If None, aggregation is disabled.
        aggregate_window_sec: Option<u64>,
    },
}

/// Trait for sending logs
#[async_trait]
pub trait LogSender: Send + Sync {
    /// Send a log payload in bytes
    fn send(&mut self, target: OutputTarget, payload: Vec<u8>) -> anyhow::Result<()>;

    /// Flush the log channel, ensuring all messages are delivered
    /// Returns when the flush message has been acknowledged
    async fn flush(&mut self) -> anyhow::Result<()>;
}

/// Represents the target output stream (stdout or stderr)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum OutputTarget {
    /// Standard output stream
    Stdout,
    /// Standard error stream
    Stderr,
}

/// Write the log to a local unix channel so some actors can listen to it and stream the log back.
pub struct LocalLogSender {
    hostname: String,
    pid: u32,
    tx: ChannelTx<LogMessage>,
    status: Receiver<TxStatus>,
}

impl LocalLogSender {
    fn new(log_channel: ChannelAddr, pid: u32) -> Result<Self, anyhow::Error> {
        let tx = channel::dial::<LogMessage>(log_channel)?;
        let status = tx.status().clone();

        let hostname = hostname::get()
            .unwrap_or_else(|_| "unknown_host".into())
            .into_string()
            .unwrap_or("unknown_host".to_string());
        Ok(Self {
            hostname,
            pid,
            tx,
            status,
        })
    }
}

#[async_trait]
impl LogSender for LocalLogSender {
    fn send(&mut self, target: OutputTarget, payload: Vec<u8>) -> anyhow::Result<()> {
        if TxStatus::Active == *self.status.borrow() {
            // post does not guarantee the message to be delivered
            self.tx.post(LogMessage::Log {
                hostname: self.hostname.clone(),
                pid: self.pid,
                output_target: target,
                payload: Serialized::serialize_anon(&payload)?,
            });
        } else {
            tracing::debug!(
                "log sender {} is not active, skip sending log",
                self.tx.addr()
            )
        }

        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        // send will make sure message is delivered
        if TxStatus::Active == *self.status.borrow() {
            match self.tx.send(LogMessage::Flush {}).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    tracing::error!("log sender {} error sending flush message: {}", self.pid, e);
                    Err(anyhow::anyhow!("error sending flush message: {}", e))
                }
            }
        } else {
            tracing::debug!(
                "log sender {} is not active, skip sending flush message",
                self.tx.addr()
            );
            Ok(())
        }
    }
}

/// A custom writer that tees to both stdout/stderr.
/// It captures output lines and sends them to the child process.
pub struct LogWriter<T: LogSender + Unpin + 'static, S: io::AsyncWrite + Send + Unpin + 'static> {
    output_target: OutputTarget,
    std_writer: S,
    log_sender: T,
}

fn create_file_writer(
    local_rank: usize,
    output_target: OutputTarget,
    env: env::Env,
) -> Result<Box<dyn io::AsyncWrite + Send + Unpin + 'static>> {
    let suffix = match output_target {
        OutputTarget::Stderr => "stderr",
        OutputTarget::Stdout => "stdout",
    };
    let (path, filename) = log_file_path(env)?;
    let path = Path::new(&path);
    let mut full_path = PathBuf::from(path);
    full_path.push(format!("{}_{}.{}", filename, local_rank, suffix));
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(full_path)?;
    let tokio_file = tokio::fs::File::from_std(file);
    // TODO: should we buffer this?
    Ok(Box::new(tokio_file))
}

fn get_local_log_destination(
    local_rank: usize,
    output_target: OutputTarget,
) -> Result<Box<dyn io::AsyncWrite + Send + Unpin>> {
    let env: env::Env = env::Env::current();
    Ok(match env {
        env::Env::Test => match output_target {
            OutputTarget::Stdout => Box::new(LinePrefixingWriter::new(local_rank, io::stdout())),
            OutputTarget::Stderr => Box::new(LinePrefixingWriter::new(local_rank, io::stderr())),
        },
        env::Env::Local | env::Env::MastEmulator | env::Env::Mast => {
            create_file_writer(local_rank, output_target, env)?
        }
    })
}

/// Helper function to create stdout and stderr LogWriter instances
///
/// # Arguments
///
/// * `log_channel` - The unix channel for the writer to stream logs to
/// * `pid` - The process ID of the process
///
/// # Returns
///
/// A tuple of boxed writers for stdout and stderr
pub fn create_log_writers(
    local_rank: usize,
    log_channel: ChannelAddr,
    pid: u32,
) -> Result<
    (
        Box<dyn io::AsyncWrite + Send + Unpin + 'static>,
        Box<dyn io::AsyncWrite + Send + Unpin + 'static>,
    ),
    anyhow::Error,
> {
    // Create LogWriter instances for stdout and stderr using the shared log sender
    let stdout_writer = LogWriter::with_default_writer(
        local_rank,
        OutputTarget::Stdout,
        LocalLogSender::new(log_channel.clone(), pid)?,
    )?;
    let stderr_writer = LogWriter::with_default_writer(
        local_rank,
        OutputTarget::Stderr,
        LocalLogSender::new(log_channel, pid)?,
    )?;

    Ok((Box::new(stdout_writer), Box::new(stderr_writer)))
}

impl<T: LogSender + Unpin + 'static, S: io::AsyncWrite + Send + Unpin + 'static> LogWriter<T, S> {
    /// Creates a new LogWriter.
    ///
    /// # Arguments
    ///
    /// * `output_target` - The target output stream (stdout or stderr)
    /// * `std_writer` - The writer to use for stdout/stderr
    /// * `log_sender` - The log sender to use for sending logs
    pub fn new(output_target: OutputTarget, std_writer: S, log_sender: T) -> Self {
        Self {
            output_target,
            std_writer,
            log_sender,
        }
    }
}

impl<T: LogSender + Unpin + 'static> LogWriter<T, Box<dyn io::AsyncWrite + Send + Unpin>> {
    /// Creates a new LogWriter with the default stdout/stderr writer.
    ///
    /// # Arguments
    ///
    /// * `output_target` - The target output stream (stdout or stderr)
    /// * `log_sender` - The log sender to use for sending logs
    pub fn with_default_writer(
        local_rank: usize,
        output_target: OutputTarget,
        log_sender: T,
    ) -> Result<Self> {
        // Use a default writer based on the output target
        let std_writer = get_local_log_destination(local_rank, output_target)?;

        Ok(Self {
            output_target,
            std_writer,
            log_sender,
        })
    }
}

impl<T: LogSender + Unpin + 'static, S: io::AsyncWrite + Send + Unpin + 'static> io::AsyncWrite
    for LogWriter<T, S>
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        // Get a mutable reference to the std_writer field
        let this = self.get_mut();

        // First, write to stdout/stderr
        match Pin::new(&mut this.std_writer).poll_write(cx, buf) {
            Poll::Ready(Ok(_)) => {
                // Forward the buffer directly to the log sender without parsing
                let output_target = this.output_target.clone();
                let data_to_send = buf.to_vec();

                // Use the log sender directly without cloning
                // Since LogSender::send takes &self, we don't need to clone it
                if let Err(e) = this.log_sender.send(output_target, data_to_send) {
                    tracing::error!("error sending log: {}", e);
                }
                // Return success with the full buffer size
                Poll::Ready(Ok(buf.len()))
            }
            other => other, // Propagate any errors or Pending state
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Result<(), io::Error>> {
        let this = self.get_mut();

        // First, flush the standard writer
        match Pin::new(&mut this.std_writer).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                // Now send a Flush message to the other side of the channel.
                let mut flush_future = this.log_sender.flush();
                match flush_future.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
                        // Successfully sent the flush message
                        Poll::Ready(Ok(()))
                    }
                    Poll::Ready(Err(e)) => {
                        // Error sending the flush message
                        tracing::error!("error sending flush message: {}", e);
                        Poll::Ready(Err(io::Error::other(format!(
                            "error sending flush message: {}",
                            e
                        ))))
                    }
                    Poll::Pending => {
                        // The future is not ready yet, so we return Pending
                        // The waker is already registered by polling the future
                        Poll::Pending
                    }
                }
            }
            other => other, // Propagate any errors or Pending state from the std_writer flush
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let this = self.get_mut();
        Pin::new(&mut this.std_writer).poll_shutdown(cx)
    }
}

/// Messages that can be sent to the LogWriterActor
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Named,
    Handler,
    HandleClient,
    RefClient
)]
pub enum LogForwardMessage {
    /// Receive the log from the parent process and forward ti to the client.
    Forward {},

    /// If to stream the log back to the client.
    SetMode { stream_to_client: bool },
}

impl Bind for LogForwardMessage {
    fn bind(&mut self, _bindings: &mut Bindings) -> anyhow::Result<()> {
        Ok(())
    }
}

impl Unbind for LogForwardMessage {
    fn unbind(&self, _bindings: &mut Bindings) -> anyhow::Result<()> {
        Ok(())
    }
}

/// A log forwarder that receives the log from its parent process and forward it back to the client
#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [LogForwardMessage {cast = true}],
)]
pub struct LogForwardActor {
    rx: ChannelRx<LogMessage>,
    logging_client_ref: ActorRef<LogClientActor>,
    stream_to_client: bool,
}

#[async_trait]
impl Actor for LogForwardActor {
    type Params = ActorRef<LogClientActor>;

    async fn new(logging_client_ref: Self::Params) -> Result<Self> {
        let log_channel: ChannelAddr = match std::env::var(BOOTSTRAP_LOG_CHANNEL) {
            Ok(channel) => channel.parse()?,
            Err(err) => {
                tracing::debug!(
                    "log forwarder actor failed to read env var {}: {}",
                    BOOTSTRAP_LOG_CHANNEL,
                    err
                );
                // TODO: an empty channel to serve
                ChannelAddr::any(ChannelTransport::Unix)
            }
        };
        tracing::info!(
            "log forwarder {} serve at {}",
            std::process::id(),
            log_channel
        );

        let rx = match channel::serve(log_channel.clone()).await {
            Ok((_, rx)) => rx,
            Err(err) => {
                // This can happen if we are not spanwed on a separate process like local.
                // For local mesh, log streaming anyway is not needed.
                tracing::error!(
                    "log forwarder actor failed to bootstrap on given channel {}: {}",
                    log_channel,
                    err
                );
                channel::serve(ChannelAddr::any(ChannelTransport::Unix))
                    .await?
                    .1
            }
        };
        Ok(Self {
            rx,
            logging_client_ref,
            stream_to_client: true,
        })
    }

    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.self_message_with_delay(LogForwardMessage::Forward {}, Duration::from_secs(0))?;
        Ok(())
    }
}

#[async_trait]
#[hyperactor::forward(LogForwardMessage)]
impl LogForwardMessageHandler for LogForwardActor {
    async fn forward(&mut self, ctx: &Context<Self>) -> Result<(), anyhow::Error> {
        if let Ok(LogMessage::Log {
            hostname,
            pid,
            output_target,
            payload,
        }) = self.rx.recv().await
        {
            if self.stream_to_client {
                self.logging_client_ref
                    .log(ctx, hostname, pid, output_target, payload)
                    .await?;
            }
        }

        // This is not ideal as we are using raw tx/rx.
        ctx.self_message_with_delay(LogForwardMessage::Forward {}, Duration::from_secs(0))?;

        Ok(())
    }

    async fn set_mode(
        &mut self,
        _ctx: &Context<Self>,
        stream_to_client: bool,
    ) -> Result<(), anyhow::Error> {
        self.stream_to_client = stream_to_client;
        Ok(())
    }
}

/// Deserialize a serialized message and split it into UTF-8 lines
fn deserialize_message_lines(
    serialized_message: &hyperactor::data::Serialized,
) -> Result<Vec<String>> {
    // Try to deserialize as String first
    if let Ok(message_str) = serialized_message.deserialized::<String>() {
        return Ok(message_str.lines().map(|s| s.to_string()).collect());
    }

    // If that fails, try to deserialize as Vec<u8> and convert to UTF-8
    if let Ok(message_bytes) = serialized_message.deserialized::<Vec<u8>>() {
        let message_str = String::from_utf8(message_bytes)?;
        return Ok(message_str.lines().map(|s| s.to_string()).collect());
    }

    // If both fail, return an error
    anyhow::bail!("Failed to deserialize message as either String or Vec<u8>")
}

/// A client to receive logs from remote processes
#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [LogMessage, LogClientMessage],
)]
pub struct LogClientActor {
    aggregate_window_sec: Option<u64>,
    aggregators: HashMap<OutputTarget, Aggregator>,
    last_flush_time: SystemTime,
    next_flush_deadline: Option<SystemTime>,
}

impl LogClientActor {
    fn print_aggregators(&mut self) {
        for (output_target, aggregator) in self.aggregators.iter_mut() {
            if aggregator.is_empty() {
                continue;
            }
            match output_target {
                OutputTarget::Stdout => {
                    println!("{}", aggregator);
                }
                OutputTarget::Stderr => {
                    eprintln!("{}", aggregator);
                }
            }

            // Reset the aggregator
            aggregator.reset();
        }
    }

    fn print_log_line(hostname: &str, pid: u32, output_target: OutputTarget, line: String) {
        let message = format!("[{} {}] {}", hostname, pid, line);
        match output_target {
            OutputTarget::Stdout => println!("{}", message),
            OutputTarget::Stderr => eprintln!("{}", message),
        }
    }
}

#[async_trait]
impl Actor for LogClientActor {
    /// The aggregation window in seconds.
    type Params = ();

    async fn new(_: ()) -> Result<Self, anyhow::Error> {
        // Initialize aggregators
        let mut aggregators = HashMap::new();
        aggregators.insert(OutputTarget::Stderr, Aggregator::new());
        aggregators.insert(OutputTarget::Stdout, Aggregator::new());

        Ok(Self {
            aggregate_window_sec: Some(DEFAULT_AGGREGATE_WINDOW_SEC),
            aggregators,
            last_flush_time: RealClock.system_time_now(),
            next_flush_deadline: None,
        })
    }
}

impl Drop for LogClientActor {
    fn drop(&mut self) {
        // Flush the remaining logs before shutting down
        self.print_aggregators();
    }
}

#[async_trait]
#[hyperactor::forward(LogMessage)]
impl LogMessageHandler for LogClientActor {
    async fn log(
        &mut self,
        cx: &Context<Self>,
        hostname: String,
        pid: u32,
        output_target: OutputTarget,
        payload: Serialized,
    ) -> Result<(), anyhow::Error> {
        // Deserialize the message and process line by line with UTF-8
        let message_lines = deserialize_message_lines(&payload)?;
        let hostname = hostname.as_str();

        match self.aggregate_window_sec {
            None => {
                for line in message_lines {
                    Self::print_log_line(hostname, pid, output_target, line);
                }
                self.last_flush_time = RealClock.system_time_now();
            }
            Some(window) => {
                for line in message_lines {
                    if let Some(aggregator) = self.aggregators.get_mut(&output_target) {
                        if let Err(e) = aggregator.add_line(&line) {
                            tracing::error!("error adding log line: {}", e);
                            // For the sake of completeness, flush the log lines.
                            Self::print_log_line(hostname, pid, output_target, line);
                        }
                    } else {
                        tracing::error!("unknown output target: {:?}", output_target);
                        // For the sake of completeness, flush the log lines.
                        Self::print_log_line(hostname, pid, output_target, line);
                    }
                }

                let new_deadline = self.last_flush_time + Duration::from_secs(window);
                let now = RealClock.system_time_now();
                if new_deadline <= now {
                    self.flush(cx).await?;
                } else {
                    let delay = new_deadline.duration_since(now)?;
                    match self.next_flush_deadline {
                        None => {
                            self.next_flush_deadline = Some(new_deadline);
                            cx.self_message_with_delay(LogMessage::Flush {}, delay)?;
                        }
                        Some(deadline) => {
                            // Some early log lines have alrady triggered the flush.
                            if new_deadline < deadline {
                                // This can happen if the user has adjusted the aggregation window.
                                self.next_flush_deadline = Some(new_deadline);
                                cx.self_message_with_delay(LogMessage::Flush {}, delay)?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn flush(&mut self, _cx: &Context<Self>) -> Result<(), anyhow::Error> {
        self.print_aggregators();
        self.last_flush_time = RealClock.system_time_now();
        self.next_flush_deadline = None;

        Ok(())
    }
}

#[async_trait]
#[hyperactor::forward(LogClientMessage)]
impl LogClientMessageHandler for LogClientActor {
    async fn set_aggregate(
        &mut self,
        _cx: &Context<Self>,
        aggregate_window_sec: Option<u64>,
    ) -> Result<(), anyhow::Error> {
        if self.aggregate_window_sec.is_some() && aggregate_window_sec.is_none() {
            // Make sure we flush whatever in the aggregators before disabling aggregation.
            self.print_aggregators();
        }
        self.aggregate_window_sec = aggregate_window_sec;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use hyperactor::channel;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::channel::ChannelTx;
    use hyperactor::channel::Tx;
    use hyperactor::id;
    use hyperactor::mailbox::BoxedMailboxSender;
    use hyperactor::mailbox::DialMailboxRouter;
    use hyperactor::mailbox::MailboxServer;
    use hyperactor::proc::Proc;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn test_forwarding_log_to_client() {
        // Setup the basics
        let router = DialMailboxRouter::new();
        let (proc_addr, client_rx) = channel::serve(ChannelAddr::any(ChannelTransport::Unix))
            .await
            .unwrap();
        let proc = Proc::new(id!(client[0]), BoxedMailboxSender::new(router.clone()));
        proc.clone().serve(client_rx);
        router.bind(id!(client[0]).into(), proc_addr.clone());
        let client = proc.attach("client").unwrap();

        // Spin up both the forwarder and the client
        let log_channel = ChannelAddr::any(ChannelTransport::Unix);
        // SAFETY: Unit test
        unsafe {
            std::env::set_var(BOOTSTRAP_LOG_CHANNEL, log_channel.to_string());
        }
        let log_client: ActorRef<LogClientActor> =
            proc.spawn("log_client", ()).await.unwrap().bind();
        let log_forwarder: ActorRef<LogForwardActor> = proc
            .spawn("log_forwarder", log_client)
            .await
            .unwrap()
            .bind();

        // Write some logs that will not be streamed
        let tx: ChannelTx<LogMessage> = channel::dial(log_channel).unwrap();
        tx.post(LogMessage::Log {
            hostname: "my_host".into(),
            pid: 1,
            output_target: OutputTarget::Stderr,
            payload: Serialized::serialize_anon(&"will not stream".to_string()).unwrap(),
        });

        // Turn on streaming
        log_forwarder.set_mode(&client, true).await.unwrap();
        tx.post(LogMessage::Log {
            hostname: "my_host".into(),
            pid: 1,
            output_target: OutputTarget::Stderr,
            payload: Serialized::serialize_anon(&"will stream".to_string()).unwrap(),
        });

        // TODO: it is hard to test out anything meaningful here as the client flushes to stdout.
    }

    #[test]
    fn test_deserialize_message_lines_string() {
        // Test deserializing a String message with multiple lines
        let message = "Line 1\nLine 2\nLine 3".to_string();
        let serialized = Serialized::serialize_anon(&message).unwrap();

        let result = deserialize_message_lines(&serialized).unwrap();

        assert_eq!(result, vec!["Line 1", "Line 2", "Line 3"]);

        // Test deserializing a Vec<u8> message with UTF-8 content
        let message_bytes = "Hello\nWorld\nUTF-8 \u{1F980}".as_bytes().to_vec();
        let serialized = Serialized::serialize_anon(&message_bytes).unwrap();

        let result = deserialize_message_lines(&serialized).unwrap();

        assert_eq!(result, vec!["Hello", "World", "UTF-8 \u{1F980}"]);

        // Test deserializing a single line message
        let message = "Single line message".to_string();
        let serialized = Serialized::serialize_anon(&message).unwrap();

        let result = deserialize_message_lines(&serialized).unwrap();

        assert_eq!(result, vec!["Single line message"]);

        // Test deserializing an empty lines
        let message = "\n\n".to_string();
        let serialized = Serialized::serialize_anon(&message).unwrap();

        let result = deserialize_message_lines(&serialized).unwrap();

        assert_eq!(result, vec!["", ""]);

        // Test error handling for invalid UTF-8 bytes
        let invalid_utf8_bytes = vec![0xFF, 0xFE, 0xFD]; // Invalid UTF-8 sequence
        let serialized = Serialized::serialize_anon(&invalid_utf8_bytes).unwrap();

        let result = deserialize_message_lines(&serialized);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid utf-8"));
    }

    // Mock implementation of AsyncWrite that captures written data
    struct MockWriter {
        data: Arc<Mutex<Vec<u8>>>,
    }

    impl MockWriter {
        fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
            let data = Arc::new(Mutex::new(Vec::new()));
            (Self { data: data.clone() }, data)
        }
    }

    impl io::AsyncWrite for MockWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            let mut data = self.data.lock().unwrap();
            data.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    // Mock implementation of LogSender for testing
    struct MockLogSender {
        log_sender: mpsc::UnboundedSender<(OutputTarget, String)>, // (output_target, content)
        flush_called: Arc<Mutex<bool>>,                            // Track if flush was called
    }

    impl MockLogSender {
        fn new(log_sender: mpsc::UnboundedSender<(OutputTarget, String)>) -> Self {
            Self {
                log_sender,
                flush_called: Arc::new(Mutex::new(false)),
            }
        }
    }

    #[async_trait]
    impl LogSender for MockLogSender {
        fn send(&mut self, output_target: OutputTarget, payload: Vec<u8>) -> anyhow::Result<()> {
            // For testing purposes, convert to string if it's valid UTF-8
            let line = match std::str::from_utf8(&payload) {
                Ok(s) => s.to_string(),
                Err(_) => String::from_utf8_lossy(&payload).to_string(),
            };

            self.log_sender
                .send((output_target, line))
                .map_err(|e| anyhow::anyhow!("Failed to send log in test: {}", e))
        }

        async fn flush(&mut self) -> anyhow::Result<()> {
            // Mark that flush was called
            let mut flush_called = self.flush_called.lock().unwrap();
            *flush_called = true;

            // For testing purposes, just return Ok
            // In a real implementation, this would wait for all messages to be delivered
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_log_writer_direct_forwarding() {
        // Create a channel to receive logs
        let (log_sender, mut log_receiver) = mpsc::unbounded_channel();

        // Create a mock log sender
        let mock_log_sender = MockLogSender::new(log_sender);

        // Create a mock writer for stdout
        let (mock_writer, _) = MockWriter::new();
        let std_writer: Box<dyn io::AsyncWrite + Send + Unpin> = Box::new(mock_writer);

        // Create a log writer with the mock sender
        let mut writer = LogWriter::new(OutputTarget::Stdout, std_writer, mock_log_sender);

        // Write some data
        writer.write_all(b"Hello, world!").await.unwrap();
        writer.flush().await.unwrap();

        // Check that the log was sent as is
        let (output_target, content) = log_receiver.recv().await.unwrap();
        assert_eq!(output_target, OutputTarget::Stdout);
        assert_eq!(content, "Hello, world!");

        // Write more data
        writer.write_all(b"\nNext line").await.unwrap();
        writer.flush().await.unwrap();

        // Check that the second chunk was sent as is
        let (output_target, content) = log_receiver.recv().await.unwrap();
        assert_eq!(output_target, OutputTarget::Stdout);
        assert_eq!(content, "\nNext line");
    }

    #[tokio::test]
    async fn test_log_writer_stdout_stderr() {
        // Create a channel to receive logs
        let (log_sender, mut log_receiver) = mpsc::unbounded_channel();

        // Create mock log senders for stdout and stderr
        let stdout_sender = MockLogSender::new(log_sender.clone());
        let stderr_sender = MockLogSender::new(log_sender);

        // Create mock writers for stdout and stderr
        let (stdout_mock_writer, _) = MockWriter::new();
        let stdout_writer: Box<dyn io::AsyncWrite + Send + Unpin> = Box::new(stdout_mock_writer);

        let (stderr_mock_writer, _) = MockWriter::new();
        let stderr_writer: Box<dyn io::AsyncWrite + Send + Unpin> = Box::new(stderr_mock_writer);

        // Create log writers with the mock senders
        let mut stdout_writer = LogWriter::new(OutputTarget::Stdout, stdout_writer, stdout_sender);
        let mut stderr_writer = LogWriter::new(OutputTarget::Stderr, stderr_writer, stderr_sender);

        // Write to stdout and stderr
        stdout_writer.write_all(b"Stdout data").await.unwrap();
        stdout_writer.flush().await.unwrap();

        stderr_writer.write_all(b"Stderr data").await.unwrap();
        stderr_writer.flush().await.unwrap();

        // Check that logs were sent with correct output targets
        // Note: We can't guarantee the order of reception since they're sent from different tasks
        let mut received_stdout = false;
        let mut received_stderr = false;

        for _ in 0..2 {
            let (output_target, content) = log_receiver.recv().await.unwrap();
            match output_target {
                OutputTarget::Stdout => {
                    assert_eq!(content, "Stdout data");
                    received_stdout = true;
                }
                OutputTarget::Stderr => {
                    assert_eq!(content, "Stderr data");
                    received_stderr = true;
                }
            }
        }

        assert!(received_stdout);
        assert!(received_stderr);
    }

    #[tokio::test]
    async fn test_log_writer_binary_data() {
        // Create a channel to receive logs
        let (log_sender, mut log_receiver) = mpsc::unbounded_channel();

        // Create a mock log sender
        let mock_log_sender = MockLogSender::new(log_sender);

        // Create a mock writer for stdout
        let (mock_writer, _) = MockWriter::new();
        let std_writer: Box<dyn io::AsyncWrite + Send + Unpin> = Box::new(mock_writer);

        // Create a log writer with the mock sender
        let mut writer = LogWriter::new(OutputTarget::Stdout, std_writer, mock_log_sender);

        // Write binary data (including non-UTF8 bytes)
        let binary_data = vec![0x48, 0x65, 0x6C, 0x6C, 0x6F, 0xFF, 0xFE, 0x00];
        writer.write_all(&binary_data).await.unwrap();
        writer.flush().await.unwrap();

        // Check that the log was sent and converted to string (with lossy UTF-8 conversion in MockLogSender)
        let (output_target, content) = log_receiver.recv().await.unwrap();
        assert_eq!(output_target, OutputTarget::Stdout);
        // The content should be "Hello" followed by replacement characters for invalid bytes
        assert!(content.starts_with("Hello"));
        // The rest of the content will be replacement characters, but we don't care about the exact representation
    }

    #[tokio::test]
    async fn test_log_writer_poll_flush() {
        // Create a channel to receive logs
        let (log_sender, _log_receiver) = mpsc::unbounded_channel();

        // Create a mock log sender that tracks flush calls
        let mock_log_sender = MockLogSender::new(log_sender);
        let log_sender_flush_tracker = mock_log_sender.flush_called.clone();

        // Create mock writers for stdout and stderr
        let (stdout_mock_writer, _) = MockWriter::new();
        let stdout_writer: Box<dyn io::AsyncWrite + Send + Unpin> = Box::new(stdout_mock_writer);

        // Create a log writer with the mocks
        let mut writer = LogWriter::new(OutputTarget::Stdout, stdout_writer, mock_log_sender);

        // Call flush on the writer
        writer.flush().await.unwrap();

        // Verify that log sender's flush were called
        assert!(
            *log_sender_flush_tracker.lock().unwrap(),
            "LogSender's flush was not called"
        );
    }

    #[test]
    fn test_string_similarity() {
        // Test exact match
        assert_eq!(normalized_edit_distance("hello", "hello"), 0.0);

        // Test completely different strings
        assert_eq!(normalized_edit_distance("hello", "i'mdiff"), 1.0);

        // Test similar strings
        assert!(normalized_edit_distance("hello", "helo") < 0.5);
        assert!(normalized_edit_distance("hello", "hello!") < 0.5);

        // Test empty strings
        assert_eq!(normalized_edit_distance("", ""), 0.0);
        assert_eq!(normalized_edit_distance("hello", ""), 1.0);
    }

    #[test]
    fn test_add_line_to_empty_aggregator() {
        let mut aggregator = Aggregator::new();
        let result = aggregator.add_line("ERROR 404 not found");

        assert!(result.is_ok());
        assert_eq!(aggregator.lines.len(), 1);
        assert_eq!(aggregator.lines[0].content, "ERROR 404 not found");
        assert_eq!(aggregator.lines[0].count, 1);
    }

    #[test]
    fn test_add_line_merges_with_similar_line() {
        let mut aggregator = Aggregator::new_with_threshold(0.2);

        // Add first line
        aggregator.add_line("ERROR 404 timeout").unwrap();
        assert_eq!(aggregator.lines.len(), 1);

        // Add second line that should merge (similar enough)
        aggregator.add_line("ERROR 500 timeout").unwrap();
        assert_eq!(aggregator.lines.len(), 1); // Should still be 1 line after merge
        assert_eq!(aggregator.lines[0].count, 2);

        // Add third line that's too different
        aggregator
            .add_line("WARNING database connection failed")
            .unwrap();
        assert_eq!(aggregator.lines.len(), 2); // Should be 2 lines now

        // Add fourth line similar to third
        aggregator
            .add_line("WARNING database connection timed out")
            .unwrap();
        assert_eq!(aggregator.lines.len(), 2); // Should still be 2 lines
        assert_eq!(aggregator.lines[1].count, 2); // Second group has 2 lines
    }

    #[test]
    fn test_aggregation_of_similar_log_lines() {
        let mut aggregator = Aggregator::new_with_threshold(0.2);

        // Add the provided log lines with small differences
        aggregator.add_line("[1 similar log lines] WARNING <<2025, 2025>> -07-30 <<0, 0>> :41:45,366 conda-unpack-fb:292] Found invalid offsets for share/terminfo/i/ims-ansi, falling back to search/replace to update prefixes for this file.").unwrap();
        aggregator.add_line("[1 similar log lines] WARNING <<2025, 2025>> -07-30 <<0, 0>> :41:45,351 conda-unpack-fb:292] Found invalid offsets for lib/pkgconfig/ncursesw.pc, falling back to search/replace to update prefixes for this file.").unwrap();
        aggregator.add_line("[1 similar log lines] WARNING <<2025, 2025>> -07-30 <<0, 0>> :41:45,366 conda-unpack-fb:292] Found invalid offsets for share/terminfo/k/kt7, falling back to search/replace to update prefixes for this file.").unwrap();

        // Check that we have only one aggregated line due to similarity
        assert_eq!(aggregator.lines.len(), 1);

        // Check that the count is 3
        assert_eq!(aggregator.lines[0].count, 3);
    }
}
