/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context as TaskContext;
use std::task::Poll;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Result;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Local;
use hostname;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::OncePortRef;
use hyperactor::RefClient;
use hyperactor::Unbind;
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
use hyperactor_telemetry::env;
use hyperactor_telemetry::log_file_path;
use notify::Event;
use notify::EventKind;
use notify::RecommendedWatcher;
use notify::Watcher;
use serde::Deserialize;
use serde::Serialize;
use tokio::fs;
use tokio::fs::File;
use tokio::io;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::BufReader;
use tokio::io::SeekFrom;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;

use crate::bootstrap::BOOTSTRAP_LOG_CHANNEL;
use crate::shortuuid::ShortUuid;

mod line_prefixing_writer;
use line_prefixing_writer::LinePrefixingWriter;

pub(crate) const DEFAULT_AGGREGATE_WINDOW_SEC: u64 = 5;
const MAX_LINE_SIZE: usize = 256 * 1024;

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
    Flush {
        /// Indicate if the current flush is synced or non-synced.
        /// If synced, a version number is available. Otherwise, none.
        sync_version: Option<u64>,
    },
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

    /// Synchronously flush all the logs from all the procs. This is for client to call.
    StartSyncFlush {
        /// Expect these many procs to ack the flush message.
        expected_procs: usize,
        /// Return once we have received the acks from all the procs
        reply: OncePortRef<()>,
        /// Return to the caller the current flush version
        version: OncePortRef<u64>,
    },
}

/// Trait for sending logs
#[async_trait]
pub trait LogSender: Send + Sync {
    /// Send a log payload in bytes
    fn send(&mut self, target: OutputTarget, payload: Vec<u8>) -> anyhow::Result<()>;

    /// Flush the log channel, ensuring all messages are delivered
    /// Returns when the flush message has been acknowledged
    fn flush(&mut self) -> anyhow::Result<()>;
}

/// Represents the target output stream (stdout or stderr)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum OutputTarget {
    /// Standard output stream
    Stdout,
    /// Standard error stream
    Stderr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Stream {
    /// Standard output stream
    ChildStdout,
    /// Standard error stream
    ChildStderr,
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
            // Do not use tx.send, it will block the allocator as the child process state is unknown.
            self.tx.post(LogMessage::Log {
                hostname: self.hostname.clone(),
                pid: self.pid,
                output_target: target,
                payload: Serialized::serialize(&payload)?,
            });
        } else {
            tracing::debug!(
                "log sender {} is not active, skip sending log",
                self.tx.addr()
            )
        }

        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        // send will make sure message is delivered
        if TxStatus::Active == *self.status.borrow() {
            // Do not use tx.send, it will block the allocator as the child process state is unknown.
            self.tx.post(LogMessage::Flush { sync_version: None });
        } else {
            tracing::debug!(
                "log sender {} is not active, skip sending flush message",
                self.tx.addr()
            );
        }
        Ok(())
    }
}

/// A custom writer that tees to both stdout/stderr.
/// It captures output lines and sends them to the child process.
// TODO delete once FileLogMonitor is validated
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
    let (path, filename) = log_file_path(env, None)?;
    let path = Path::new(&path);
    let mut full_path = PathBuf::from(path);

    // This is the PID of the "owner" of the proc mesh, the proc mesh
    // this proc "belongs" to. In other words,the PID of the process
    // that invokes `cmd.spawn()` (where `cmd: &mut
    // tokio::process::Command`) to start the process that will host
    // the proc that this file writer relates to.
    let file_created_by_pid = std::process::id();

    full_path.push(format!(
        "{}_{}_{}.{}",
        filename, file_created_by_pid, local_rank, suffix
    ));
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
        env::Env::Test | env::Env::Local => match output_target {
            OutputTarget::Stdout => Box::new(LinePrefixingWriter::new(local_rank, io::stdout())),
            OutputTarget::Stderr => Box::new(LinePrefixingWriter::new(local_rank, io::stderr())),
        },
        env::Env::MastEmulator | env::Env::Mast => {
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
                let output_target = this.output_target;
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

        match Pin::new(&mut this.std_writer).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                if let Err(e) = this.log_sender.flush() {
                    tracing::error!("error sending flush: {}", e);
                }
                Poll::Ready(Ok(()))
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

struct FileWatcher {
    rx: UnboundedReceiver<Event>,
    watcher: RecommendedWatcher,
    path: PathBuf,
}
impl FileWatcher {
    fn new(path: PathBuf) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut watcher = notify::recommended_watcher({
            let tx = tx.clone();
            move |res| match res {
                Ok(event) => {
                    if let Err(e) = tx.send(event) {
                        tracing::warn!("stream watcher dropped: {:?}", e);
                    }
                }
                Err(e) => tracing::warn!("stream watcher error: {:?}", e),
            }
        })?;
        watcher.watch(&path.clone(), notify::RecursiveMode::NonRecursive)?;

        Ok(Self { rx, watcher, path })
    }
}

/// Given a stream forwards data to the provided channel.
pub struct StreamFwder {
    fwder: JoinHandle<()>,
    // Shared buffer for peek functionality
    recent_lines: Arc<RwLock<VecDeque<String>>>,
    max_buffer_size: usize,
    // Shutdown signal to stop the monitoring loop
    stop: Arc<Notify>,
}

impl StreamFwder {
    /// Create a new StreamFwder instance, and start monitoring the provided path.
    /// Once started Monitor will
    /// - foward logs to the provided address
    /// - pipe reader to target
    /// - And capture last `max_buffer_size` which can be used to inspect file contents via `peek`.
    pub async fn start(
        reader: impl AsyncRead + Unpin + Send + 'static,
        target: OutputTarget,
        max_buffer_size: usize,
        log_channel: ChannelAddr,
        pid: u32,
    ) -> Result<Self> {
        let stop = Arc::new(Notify::new());

        let path = create_temp_log().await?;
        let path_clone = path.clone();
        let file_watcher = FileWatcher::new(path.clone())?;

        // Keep recent lines to allow peeks
        let recent_lines = Arc::new(RwLock::new(VecDeque::with_capacity(max_buffer_size)));
        let recent_lines_clone = recent_lines.clone();

        let log_sender = Box::new(LocalLogSender::new(log_channel, pid)?);
        let fwd_stop = stop.clone();
        let fwder = tokio::spawn(async move {
            if let Err(e) = fwd_on_notify(
                file_watcher,
                &fwd_stop,
                log_sender,
                target,
                recent_lines_clone,
                max_buffer_size,
            )
            .await
            {
                tracing::error!(
                    "file {} fwder failed: {}",
                    path.to_string_lossy().into_owned(),
                    e
                );
            }
        });

        Ok(StreamFwder {
            fwder,
            recent_lines,
            max_buffer_size,
            stop,
        })
    }

    pub async fn abort(self) -> (Vec<String>, Result<(), anyhow::Error>) {
        // Send shutdown signal to stop the monitoring loop
        self.stop.notify_waiters();

        let lines = self.peek().await;
        let result = match self.fwder.await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        };

        (lines, result)
    }

    /// Inspect the latest `max_buffer` lines read from the file being monitored
    /// Returns lines in chronological order (oldest first)
    pub async fn peek(&self) -> Vec<String> {
        let lines = self.recent_lines.read().await;
        let start_idx = if lines.len() > self.max_buffer_size {
            lines.len() - self.max_buffer_size
        } else {
            0
        };

        lines.range(start_idx..).cloned().collect()
    }
}

/// Start monitoring the log file and forwarding content to the logging client
async fn fwd_on_notify(
    mut watcher: FileWatcher,
    stop: &Arc<Notify>,
    mut log_sender: Box<dyn LogSender + Send>,
    target: OutputTarget,
    recent_lines: Arc<RwLock<VecDeque<String>>>,
    max_buffer_size: usize,
) -> Result<()> {
    let _watcher_guard = watcher.watcher;
    let path = watcher.path;
    let file = fs::OpenOptions::new().read(true).open(&path).await?;
    let mut reader = BufReader::new(file);
    let mut position = reader.seek(SeekFrom::End(0)).await?;

    tracing::debug!("Monitoring {:?} for new lines...", path);

    loop {
        tokio::select! {
            event = watcher.rx.recv() => {
                match event {
                    Some(event) => {
                        if let EventKind::Modify(_) = &event.kind {
                            // Get the current file size to detect truncation
                            let file_metadata = fs::metadata(&path).await?;
                            let file_size = file_metadata.len();

                            // Handle potential file truncation/rotation
                            if position > file_size {
                                tracing::warn!("Log file {:?} appears to have been truncated (position {} > file size {}), resetting to start", path, position, file_size);
                                reader.seek(SeekFrom::Start(0)).await?;
                                position = 0;
                            } else if position < file_size {
                                reader.seek(SeekFrom::Start(position)).await?;
                            }

                            let mut buf = vec![0u8; 128 * 1024];
                            let mut line_buffer = Vec::with_capacity(2048);
                            let mut lines = Vec::with_capacity(100);

                            loop {
                                let bytes_read = reader.read(&mut buf).await?;
                                if bytes_read == 0 {
                                    break;
                                }

                                let chunk = &buf[..bytes_read];

                                let mut start = 0;
                                while let Some(newline_pos) = chunk[start..].iter().position(|&b| b == b'\n') {
                                    let absolute_pos = start + newline_pos;

                                    line_buffer.extend_from_slice(&chunk[start..absolute_pos]);

                                    if !line_buffer.is_empty() {
                                        if line_buffer.len() > MAX_LINE_SIZE {
                                            line_buffer.truncate(MAX_LINE_SIZE);
                                            line_buffer.extend_from_slice(b"... [TRUNCATED]");
                                        }

                                        let line_data = std::mem::replace(&mut line_buffer, Vec::with_capacity(2048));
                                        lines.push(line_data);
                                    }

                                    start = absolute_pos + 1;
                                }

                                if start < chunk.len() {
                                    line_buffer.extend_from_slice(&chunk[start..]);
                                }
                            }

                            if !line_buffer.is_empty() {
                                if line_buffer.len() > MAX_LINE_SIZE {
                                    line_buffer.truncate(MAX_LINE_SIZE);
                                    line_buffer.extend_from_slice(b"... [TRUNCATED]");
                                }
                                let line_data = line_buffer.clone();
                                lines.push(line_data);
                            }

                            // Add all complete lines to the recent_lines buffer
                            if !lines.is_empty() {
                                let mut recent_lines_guard = recent_lines.write().await;
                                for line_data in &lines {
                                    let line_str = String::from_utf8_lossy(line_data);
                                    recent_lines_guard.push_back(line_str.trim_end().to_string());
                                    if recent_lines_guard.len() > max_buffer_size {
                                        recent_lines_guard.pop_front();
                                    }
                                }
                                drop(recent_lines_guard);
                            }

                            // TODO(pablorfb) change to batch update
                            for line in lines {
                                if let Err(e) = log_sender.send(target, line) {
                                tracing::error!("Failed to send log lines: {}", e);
                                }
                            }

                            position = reader.stream_position().await?;
                        }
                    }
                    None => {
                        tracing::debug!("File event channel closed, stopping monitoring");
                        break;
                    }
                }
            }
            _ = stop.notified() => {
                tracing::debug!("Shutdown signal received, stopping monitoring");
                 if let Err(e) = log_sender.flush() {
                    tracing::error!("Failed to send log lines: {}", e);
                }
                break;
            }
        }
    }
    Ok(())
}

pub async fn create_temp_log() -> Result<PathBuf> {
    let key = &ShortUuid::generate().to_string();
    let (parent, filename) = log_file_path(env::Env::current(), Some(key))?;
    let path = PathBuf::from(parent).join(filename);

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let _ = File::create(&path).await?;

    Ok(path)
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
    RefClient,
    Bind,
    Unbind
)]
pub enum LogForwardMessage {
    /// Receive the log from the parent process and forward ti to the client.
    Forward {},

    /// If to stream the log back to the client.
    SetMode { stream_to_client: bool },

    /// Flush the log with a version number.
    ForceSyncFlush { version: u64 },
}

/// A log forwarder that receives the log from its parent process and forward it back to the client
#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [LogForwardMessage {cast = true}],
)]
pub struct LogForwardActor {
    rx: ChannelRx<LogMessage>,
    flush_tx: Arc<Mutex<ChannelTx<LogMessage>>>,
    next_flush_deadline: SystemTime,
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

        let rx = match channel::serve(log_channel.clone()) {
            Ok((_, rx)) => rx,
            Err(err) => {
                // This can happen if we are not spanwed on a separate process like local.
                // For local mesh, log streaming anyway is not needed.
                tracing::error!(
                    "log forwarder actor failed to bootstrap on given channel {}: {}",
                    log_channel,
                    err
                );
                channel::serve(ChannelAddr::any(ChannelTransport::Unix))?.1
            }
        };

        // Dial the same channel to send flush message to drain the log queue.
        let flush_tx = Arc::new(Mutex::new(channel::dial::<LogMessage>(log_channel)?));
        let now = RealClock.system_time_now();

        Ok(Self {
            rx,
            flush_tx,
            next_flush_deadline: now,
            logging_client_ref,
            stream_to_client: true,
        })
    }

    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.self_message_with_delay(LogForwardMessage::Forward {}, Duration::from_secs(0))?;

        // Make sure we start the flush loop periodically so the log channel will not deadlock.
        self.flush_tx
            .lock()
            .await
            .send(LogMessage::Flush { sync_version: None })
            .await?;
        Ok(())
    }
}

#[async_trait]
#[hyperactor::forward(LogForwardMessage)]
impl LogForwardMessageHandler for LogForwardActor {
    async fn forward(&mut self, ctx: &Context<Self>) -> Result<(), anyhow::Error> {
        match self.rx.recv().await {
            Ok(LogMessage::Flush { sync_version }) => {
                let now = RealClock.system_time_now();
                match sync_version {
                    None => {
                        // Schedule another flush to keep the log channel from deadlocking.
                        let delay = Duration::from_secs(1);
                        if now >= self.next_flush_deadline {
                            self.next_flush_deadline = now + delay;
                            let flush_tx = self.flush_tx.clone();
                            tokio::spawn(async move {
                                RealClock.sleep(delay).await;
                                if let Err(e) = flush_tx
                                    .lock()
                                    .await
                                    .send(LogMessage::Flush { sync_version: None })
                                    .await
                                {
                                    tracing::error!("failed to send flush message: {}", e);
                                }
                            });
                        }
                    }
                    version => {
                        self.logging_client_ref.flush(ctx, version).await?;
                    }
                }
            }
            Ok(LogMessage::Log {
                hostname,
                pid,
                output_target,
                payload,
            }) => {
                if self.stream_to_client {
                    self.logging_client_ref
                        .log(ctx, hostname, pid, output_target, payload)
                        .await?;
                }
            }
            Err(e) => {
                return Err(e.into());
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

    async fn force_sync_flush(
        &mut self,
        _cx: &Context<Self>,
        version: u64,
    ) -> Result<(), anyhow::Error> {
        self.flush_tx
            .lock()
            .await
            .send(LogMessage::Flush {
                sync_version: Some(version),
            })
            .await
            .map_err(anyhow::Error::from)
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
    anyhow::bail!("failed to deserialize message as either String or Vec<u8>")
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

    // For flush sync barrier
    current_flush_version: u64,
    current_flush_port: Option<OncePortRef<()>>,
    current_unflushed_procs: usize,
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

        #[cfg(test)]
        crate::logging::test_tap::push(&message);

        match output_target {
            OutputTarget::Stdout => println!("{}", message),
            OutputTarget::Stderr => eprintln!("{}", message),
        }
    }

    fn flush_internal(&mut self) {
        self.print_aggregators();
        self.last_flush_time = RealClock.system_time_now();
        self.next_flush_deadline = None;
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
            current_flush_version: 0,
            current_flush_port: None,
            current_unflushed_procs: 0,
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
                    self.flush_internal();
                } else {
                    let delay = new_deadline.duration_since(now)?;
                    match self.next_flush_deadline {
                        None => {
                            self.next_flush_deadline = Some(new_deadline);
                            cx.self_message_with_delay(
                                LogMessage::Flush { sync_version: None },
                                delay,
                            )?;
                        }
                        Some(deadline) => {
                            // Some early log lines have alrady triggered the flush.
                            if new_deadline < deadline {
                                // This can happen if the user has adjusted the aggregation window.
                                self.next_flush_deadline = Some(new_deadline);
                                cx.self_message_with_delay(
                                    LogMessage::Flush { sync_version: None },
                                    delay,
                                )?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn flush(
        &mut self,
        cx: &Context<Self>,
        sync_version: Option<u64>,
    ) -> Result<(), anyhow::Error> {
        match sync_version {
            None => {
                self.flush_internal();
            }
            Some(version) => {
                if version != self.current_flush_version {
                    tracing::error!(
                        "found mismatched flush versions: got {}, expect {}; this can happen if some previous flush didn't finish fully",
                        version,
                        self.current_flush_version
                    );
                    return Ok(());
                }

                if self.current_unflushed_procs == 0 || self.current_flush_port.is_none() {
                    // This is a serious issue; it's better to error out.
                    anyhow::bail!("found no ongoing flush request");
                }
                self.current_unflushed_procs -= 1;

                tracing::debug!(
                    "ack sync flush: version {}; remaining procs: {}",
                    self.current_flush_version,
                    self.current_unflushed_procs
                );

                if self.current_unflushed_procs == 0 {
                    self.flush_internal();
                    let reply = self.current_flush_port.take().unwrap();
                    self.current_flush_port = None;
                    reply.send(cx, ()).map_err(anyhow::Error::from)?;
                }
            }
        }

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

    async fn start_sync_flush(
        &mut self,
        cx: &Context<Self>,
        expected_procs_flushed: usize,
        reply: OncePortRef<()>,
        version: OncePortRef<u64>,
    ) -> Result<(), anyhow::Error> {
        if self.current_unflushed_procs > 0 || self.current_flush_port.is_some() {
            tracing::warn!(
                "found unfinished ongoing flush: version {}; {} unflushed procs",
                self.current_flush_version,
                self.current_unflushed_procs,
            );
        }

        self.current_flush_version += 1;
        tracing::debug!(
            "start sync flush with version {}",
            self.current_flush_version
        );
        self.current_flush_port = Some(reply.clone());
        self.current_unflushed_procs = expected_procs_flushed;
        version
            .send(cx, self.current_flush_version)
            .map_err(anyhow::Error::from)?;
        Ok(())
    }
}

#[cfg(test)]
pub mod test_tap {
    use std::sync::Mutex;
    use std::sync::OnceLock;

    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::sync::mpsc::UnboundedSender;

    static TAP: OnceLock<UnboundedSender<String>> = OnceLock::new();
    static RX: OnceLock<Mutex<UnboundedReceiver<String>>> = OnceLock::new();

    // Called by tests to install the sender.
    pub fn install(tx: UnboundedSender<String>) {
        let _ = TAP.set(tx);
    }

    // Called by tests to register the receiver so we can drain later.
    pub fn set_receiver(rx: UnboundedReceiver<String>) {
        let _ = RX.set(Mutex::new(rx));
    }

    // Used by LogClientActor (under #[cfg(test)]) to push a line.
    pub fn push(s: &str) {
        if let Some(tx) = TAP.get() {
            let _ = tx.send(s.to_string());
        }
    }

    // Tests call this to collect everything observed so far.
    pub fn drain() -> Vec<String> {
        let mut out = Vec::new();
        if let Some(rx) = RX.get() {
            let mut rx = rx.lock().unwrap();
            while let Ok(line) = rx.try_recv() {
                out.push(line);
            }
        }
        out
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
        let (proc_addr, client_rx) =
            channel::serve(ChannelAddr::any(ChannelTransport::Unix)).unwrap();
        let proc = Proc::new(id!(client[0]), BoxedMailboxSender::new(router.clone()));
        proc.clone().serve(client_rx);
        router.bind(id!(client[0]).into(), proc_addr.clone());
        let (client, _handle) = proc.instance("client").unwrap();

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
            payload: Serialized::serialize(&"will not stream".to_string()).unwrap(),
        });

        // Turn on streaming
        log_forwarder.set_mode(&client, true).await.unwrap();
        tx.post(LogMessage::Log {
            hostname: "my_host".into(),
            pid: 1,
            output_target: OutputTarget::Stderr,
            payload: Serialized::serialize(&"will stream".to_string()).unwrap(),
        });

        // TODO: it is hard to test out anything meaningful here as the client flushes to stdout.
    }

    #[test]
    fn test_deserialize_message_lines_string() {
        // Test deserializing a String message with multiple lines
        let message = "Line 1\nLine 2\nLine 3".to_string();
        let serialized = Serialized::serialize(&message).unwrap();

        let result = deserialize_message_lines(&serialized).unwrap();

        assert_eq!(result, vec!["Line 1", "Line 2", "Line 3"]);

        // Test deserializing a Vec<u8> message with UTF-8 content
        let message_bytes = "Hello\nWorld\nUTF-8 \u{1F980}".as_bytes().to_vec();
        let serialized = Serialized::serialize(&message_bytes).unwrap();

        let result = deserialize_message_lines(&serialized).unwrap();

        assert_eq!(result, vec!["Hello", "World", "UTF-8 \u{1F980}"]);

        // Test deserializing a single line message
        let message = "Single line message".to_string();
        let serialized = Serialized::serialize(&message).unwrap();

        let result = deserialize_message_lines(&serialized).unwrap();

        assert_eq!(result, vec!["Single line message"]);

        // Test deserializing an empty lines
        let message = "\n\n".to_string();
        let serialized = Serialized::serialize(&message).unwrap();

        let result = deserialize_message_lines(&serialized).unwrap();

        assert_eq!(result, vec!["", ""]);

        // Test error handling for invalid UTF-8 bytes
        let invalid_utf8_bytes = vec![0xFF, 0xFE, 0xFD]; // Invalid UTF-8 sequence
        let serialized = Serialized::serialize_as::<Vec<u8>, _>(&invalid_utf8_bytes).unwrap();

        let result = deserialize_message_lines(&serialized);

        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(message.contains("invalid utf-8"), "{}", message);
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
                .map_err(|e| anyhow::anyhow!("Failed to send log in test: {}", e))?;
            Ok(())
        }

        fn flush(&mut self) -> anyhow::Result<()> {
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

    #[test]
    fn test_aggregator_custom_threshold() {
        // Test with very strict threshold (0.05)
        let mut strict_aggregator = Aggregator::new_with_threshold(0.05);
        strict_aggregator.add_line("ERROR 404").unwrap();
        strict_aggregator.add_line("ERROR 500").unwrap(); // Should not merge due to strict threshold
        assert_eq!(strict_aggregator.lines.len(), 2);

        // Test with very lenient threshold (0.8)
        let mut lenient_aggregator = Aggregator::new_with_threshold(0.8);
        lenient_aggregator.add_line("ERROR 404").unwrap();
        lenient_aggregator.add_line("WARNING 200").unwrap(); // Should merge due to lenient threshold
        assert_eq!(lenient_aggregator.lines.len(), 1);
        assert_eq!(lenient_aggregator.lines[0].count, 2);
    }

    #[test]
    fn test_format_system_time() {
        let test_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1609459200); // 2021-01-01 00:00:00 UTC
        let formatted = format_system_time(test_time);

        // Just verify it's a reasonable format (contains date and time components)
        assert!(formatted.contains("-"));
        assert!(formatted.contains(":"));
        assert!(formatted.len() > 10); // Should be reasonable length
    }

    #[test]
    fn test_aggregator_display_formatting() {
        let mut aggregator = Aggregator::new();
        aggregator.add_line("Test error message").unwrap();
        aggregator.add_line("Test error message").unwrap(); // Should merge

        let display_string = format!("{}", aggregator);

        // Verify the output contains expected elements
        assert!(display_string.contains("Aggregated Logs"));
        assert!(display_string.contains("[2 similar log lines]"));
        assert!(display_string.contains("Test error message"));
        assert!(display_string.contains(">>>") && display_string.contains("<<<"));
    }

    #[tokio::test]
    async fn test_local_log_sender_inactive_status() {
        let (log_channel, _) =
            channel::serve::<LogMessage>(ChannelAddr::any(ChannelTransport::Unix)).unwrap();
        let mut sender = LocalLogSender::new(log_channel, 12345).unwrap();

        // This test verifies that the sender handles inactive status gracefully
        // In a real scenario, the channel would be closed, but for testing we just
        // verify the send/flush methods don't panic
        let result = sender.send(OutputTarget::Stdout, b"test".to_vec());
        assert!(result.is_ok());

        let result = sender.flush();
        assert!(result.is_ok());
    }

    #[test]
    fn test_levenshtein_distance_edge_cases() {
        // Test with empty strings
        assert_eq!(levenshtein_distance("", ""), 0);
        assert_eq!(levenshtein_distance("", "hello"), 5);
        assert_eq!(levenshtein_distance("hello", ""), 5);

        // Test with identical strings
        assert_eq!(levenshtein_distance("hello", "hello"), 0);

        // Test with single character differences
        assert_eq!(levenshtein_distance("hello", "helo"), 1); // deletion
        assert_eq!(levenshtein_distance("helo", "hello"), 1); // insertion
        assert_eq!(levenshtein_distance("hello", "hallo"), 1); // substitution

        // Test with unicode characters
        assert_eq!(levenshtein_distance("café", "cafe"), 1);
    }

    #[test]
    fn test_normalized_edit_distance_edge_cases() {
        // Test with empty strings
        assert_eq!(normalized_edit_distance("", ""), 0.0);

        // Test normalization
        assert_eq!(normalized_edit_distance("hello", ""), 1.0);
        assert_eq!(normalized_edit_distance("", "hello"), 1.0);

        // Test that result is always between 0.0 and 1.0
        let distance = normalized_edit_distance("completely", "different");
        assert!(distance >= 0.0 && distance <= 1.0);
    }

    #[tokio::test]
    async fn test_deserialize_message_lines_edge_cases() {
        // Test with empty string
        let empty_message = "".to_string();
        let serialized = Serialized::serialize(&empty_message).unwrap();
        let result = deserialize_message_lines(&serialized).unwrap();
        assert_eq!(result, Vec::<String>::new());

        // Test with trailing newline
        let trailing_newline = "line1\nline2\n".to_string();
        let serialized = Serialized::serialize(&trailing_newline).unwrap();
        let result = deserialize_message_lines(&serialized).unwrap();
        assert_eq!(result, vec!["line1", "line2"]);
    }

    #[test]
    fn test_output_target_serialization() {
        // Test that OutputTarget can be serialized and deserialized
        let stdout_serialized = serde_json::to_string(&OutputTarget::Stdout).unwrap();
        let stderr_serialized = serde_json::to_string(&OutputTarget::Stderr).unwrap();

        let stdout_deserialized: OutputTarget = serde_json::from_str(&stdout_serialized).unwrap();
        let stderr_deserialized: OutputTarget = serde_json::from_str(&stderr_serialized).unwrap();

        assert_eq!(stdout_deserialized, OutputTarget::Stdout);
        assert_eq!(stderr_deserialized, OutputTarget::Stderr);
    }

    #[test]
    fn test_log_line_display_formatting() {
        let log_line = LogLine::new("Test message".to_string());
        let display_string = format!("{}", log_line);

        assert!(display_string.contains("[1 similar log lines]"));
        assert!(display_string.contains("Test message"));

        // Test with higher count
        let mut log_line_multi = LogLine::new("Test message".to_string());
        log_line_multi.count = 5;
        let display_string_multi = format!("{}", log_line_multi);

        assert!(display_string_multi.contains("[5 similar log lines]"));
        assert!(display_string_multi.contains("Test message"));
    }
}
