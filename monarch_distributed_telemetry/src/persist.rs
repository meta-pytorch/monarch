/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Persist log exporter: writes every appended table batch to disk.
//!
//! The store hands each appended batch to [`Exporter::send`], which never
//! blocks: a full queue drops the batch and counts it. One writer thread per
//! collector merges each table's batches between flushes and appends them to
//! a [`SegmentWriter`]. A write failure disables the exporter without
//! affecting live telemetry.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use monarch_telemetry_schema::frame_log::SegmentWriter;

/// Tuning for a persist exporter.
#[derive(Debug, Clone)]
pub(crate) struct PersistConfig {
    /// How long batches wait to be merged before they are written.
    pub flush_interval: Duration,
    /// How often written data is made durable.
    pub sync_interval: Duration,
    /// Segment size that triggers rotation.
    pub segment_bytes: u64,
    /// Queued batches before sends start dropping.
    pub capacity: usize,
    /// Pending rows that trigger an immediate write for one table.
    pub coalesce_rows: usize,
}

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_millis(200),
            sync_interval: Duration::from_secs(5),
            segment_bytes: 256 * 1024 * 1024,
            capacity: 10_000,
            coalesce_rows: 8192,
        }
    }
}

enum Message {
    Batch(String, RecordBatch),
    Close(mpsc::Sender<()>),
}

/// Handle to one collector's persist writer thread.
pub(crate) struct Exporter {
    sender: mpsc::SyncSender<Message>,
    dropped: Arc<AtomicU64>,
    closed: AtomicBool,
}

impl Exporter {
    /// Start segment 0 in `dir` and spawn the writer thread.
    ///
    /// Fails without spawning anything if the directory or first segment
    /// cannot be created.
    pub(crate) fn start(dir: &Path, config: PersistConfig) -> anyhow::Result<Arc<Self>> {
        let segments = SegmentWriter::create(dir, config.segment_bytes)?;
        let (sender, receiver) = mpsc::sync_channel(config.capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let writer = Writer {
            segments: Some(segments),
            config,
            schemas: BTreeMap::new(),
            pending: BTreeMap::new(),
            dropped: dropped.clone(),
        };
        std::thread::Builder::new()
            .name("telemetry-persist".to_string())
            .spawn(move || writer.run(receiver))?;
        Ok(Arc::new(Self {
            sender,
            dropped,
            closed: AtomicBool::new(false),
        }))
    }

    /// An exporter whose writer never runs, for exercising the queue.
    #[cfg(test)]
    fn unstarted(capacity: usize) -> (Self, mpsc::Receiver<Message>) {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        let exporter = Self {
            sender,
            dropped: Arc::new(AtomicU64::new(0)),
            closed: AtomicBool::new(false),
        };
        (exporter, receiver)
    }

    /// Queue one appended batch; a zero-row batch registers its table.
    pub(crate) fn send(&self, table: &str, batch: RecordBatch) {
        if self
            .sender
            .try_send(Message::Batch(table.to_string(), batch))
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Write everything queued, then the end frame, waiting up to `timeout`.
    ///
    /// Later calls return immediately.
    pub(crate) fn close(&self, timeout: Duration) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let deadline = Instant::now() + timeout;
        let (ack, acked) = mpsc::channel();
        let mut message = Message::Close(ack);
        loop {
            match self.sender.try_send(message) {
                Ok(()) => break,
                Err(mpsc::TrySendError::Full(returned)) if Instant::now() < deadline => {
                    message = returned;
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => {
                    tracing::warn!("telemetry persist: close did not reach the writer");
                    return;
                }
            }
        }
        if acked
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .is_err()
        {
            tracing::warn!("telemetry persist: writer did not finish within {timeout:?}");
        }
    }

    /// Batches dropped so far.
    #[cfg(test)]
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
struct Pending {
    batches: Vec<RecordBatch>,
    rows: usize,
}

struct Writer {
    /// `None` once a write error has disabled the exporter.
    segments: Option<SegmentWriter>,
    config: PersistConfig,
    /// Every registered table, re-emitted at the start of each segment.
    schemas: BTreeMap<String, SchemaRef>,
    pending: BTreeMap<String, Pending>,
    dropped: Arc<AtomicU64>,
}

impl Writer {
    fn run(mut self, receiver: mpsc::Receiver<Message>) {
        let mut last_flush = Instant::now();
        let mut last_sync = last_flush;
        loop {
            let wait = self
                .config
                .flush_interval
                .saturating_sub(last_flush.elapsed());
            match receiver.recv_timeout(wait) {
                Ok(Message::Batch(table, batch)) => self.accept(table, batch),
                Ok(Message::Close(ack)) => {
                    self.finish();
                    let _ = ack.send(());
                    return;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.finish();
                    return;
                }
            }

            if last_flush.elapsed() >= self.config.flush_interval {
                self.write_pending();
                self.on_segments(SegmentWriter::flush);
                last_flush = Instant::now();
            }
            if last_sync.elapsed() >= self.config.sync_interval {
                self.on_segments(SegmentWriter::sync);
                last_sync = Instant::now();
            }
        }
    }

    fn accept(&mut self, table: String, batch: RecordBatch) {
        if self.segments.is_none() {
            self.drop_batches(1);
            return;
        }
        if batch.num_rows() == 0 {
            self.schemas.insert(table.clone(), batch.schema());
            self.write(&table, &batch);
            return;
        }
        let pending = self.pending.entry(table.clone()).or_default();
        pending.rows += batch.num_rows();
        pending.batches.push(batch);
        if pending.rows >= self.config.coalesce_rows {
            self.write_table(&table);
        }
    }

    fn write_pending(&mut self) {
        let tables: Vec<String> = self.pending.keys().cloned().collect();
        for table in tables {
            self.write_table(&table);
        }
    }

    /// Write one table's pending batches, merged into one batch when their
    /// schemas allow it.
    fn write_table(&mut self, table: &str) {
        let Some(Pending { batches, .. }) = self.pending.remove(table) else {
            return;
        };
        let merged = match batches.as_slice() {
            [] => return,
            [only] => Ok(only.clone()),
            [first, ..] => concat_batches(&first.schema(), &batches),
        };
        match merged {
            Ok(batch) => self.write(table, &batch),
            Err(_) => {
                for batch in &batches {
                    self.write(table, batch);
                }
            }
        }
    }

    fn write(&mut self, table: &str, batch: &RecordBatch) {
        let Some(segments) = self.segments.as_mut() else {
            self.drop_batches(1);
            return;
        };
        match segments.write_batch(table, batch) {
            Ok(_) => {}
            Err(error) if is_io(&error) => {
                self.drop_batches(1);
                self.disable(error);
                return;
            }
            Err(error) => {
                tracing::warn!(
                    "telemetry persist: dropped unwritable batch for {table}: {error:#}"
                );
                self.drop_batches(1);
                return;
            }
        }
        self.rotate_if_needed();
    }

    /// Start a new segment once the current one is full, and register every
    /// known table in it.
    fn rotate_if_needed(&mut self) {
        let Self {
            segments: Some(segments),
            schemas,
            ..
        } = self
        else {
            return;
        };
        if !segments.needs_rotation() {
            return;
        }
        let rotated = segments.rotate().and_then(|()| {
            for (table, schema) in schemas.iter() {
                segments.write_batch(table, &RecordBatch::new_empty(schema.clone()))?;
            }
            Ok(())
        });
        if let Err(error) = rotated {
            self.disable(error);
        }
    }

    fn on_segments(&mut self, f: impl FnOnce(&mut SegmentWriter) -> anyhow::Result<()>) {
        let Some(segments) = self.segments.as_mut() else {
            return;
        };
        if let Err(error) = f(segments) {
            self.disable(error);
        }
    }

    fn disable(&mut self, error: anyhow::Error) {
        tracing::warn!("telemetry persist: disabled after write error: {error:#}");
        self.segments = None;
        let pending: usize = self.pending.values().map(|p| p.batches.len()).sum();
        self.pending.clear();
        self.drop_batches(pending as u64);
    }

    fn finish(&mut self) {
        self.write_pending();
        if let Some(segments) = self.segments.take()
            && let Err(error) = segments.finish()
        {
            tracing::warn!("telemetry persist: failed to close log: {error:#}");
        }
    }

    fn drop_batches(&self, count: u64) {
        self.dropped.fetch_add(count, Ordering::Relaxed);
    }
}

fn is_io(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<std::io::Error>())
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::datatypes::Field;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use monarch_telemetry_schema::MAX_FRAME_LEN;
    use monarch_telemetry_schema::frame::FrameReader;
    use monarch_telemetry_schema::frame::ReadOutcome;
    use monarch_telemetry_schema::frame_log::InstanceSummary;
    use monarch_telemetry_schema::frame_log::read_instance;
    use monarch_telemetry_schema::frame_log::segment_path;
    use monarch_telemetry_schema::serialize_batch;

    use super::*;

    const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

    fn ints(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    fn read_all(dir: &Path) -> (Vec<(String, RecordBatch)>, InstanceSummary) {
        let mut batches = Vec::new();
        let summary = read_instance(dir, |table, batch| {
            batches.push((table.to_string(), batch));
            Ok(())
        })
        .unwrap();
        (batches, summary)
    }

    /// Names of the zero-row frames in one segment file.
    fn schema_frames(path: &Path) -> Vec<String> {
        let mut reader = FrameReader::new(File::open(path).unwrap());
        let mut names = Vec::new();
        while let ReadOutcome::Frame(frame) = reader.read_frame().unwrap() {
            if frame.name.starts_with("__") {
                continue;
            }
            let batch = monarch_telemetry_schema::deserialize_one_batch(&frame.payload).unwrap();
            if batch.num_rows() == 0 {
                names.push(frame.name);
            }
        }
        names
    }

    #[test]
    fn exporter_persists_sent_batches() {
        let dir = tempfile::tempdir().unwrap();
        let exporter = Exporter::start(dir.path(), PersistConfig::default()).unwrap();

        exporter.send("t", ints(&[]));
        for _ in 0..3 {
            exporter.send("t", ints(&[1, 2]));
        }
        exporter.close(CLOSE_TIMEOUT);

        let (batches, summary) = read_all(dir.path());
        let rows: usize = batches
            .iter()
            .filter(|(table, _)| table == "t")
            .map(|(_, batch)| batch.num_rows())
            .sum();
        assert_eq!(rows, 6);
        assert!(summary.clean, "close writes the end frame");
        assert_eq!(exporter.dropped(), 0);
    }

    #[test]
    fn exporter_coalesces_within_interval() {
        let dir = tempfile::tempdir().unwrap();
        let config = PersistConfig {
            flush_interval: Duration::from_secs(3600),
            coalesce_rows: 4,
            ..PersistConfig::default()
        };
        let exporter = Exporter::start(dir.path(), config).unwrap();

        for value in 0..4 {
            exporter.send("t", ints(&[value]));
        }
        exporter.close(CLOSE_TIMEOUT);

        let (batches, _) = read_all(dir.path());
        let data: Vec<usize> = batches
            .iter()
            .map(|(_, batch)| batch.num_rows())
            .filter(|&rows| rows > 0)
            .collect();
        assert_eq!(data, vec![4], "four one-row batches merge into one frame");
    }

    #[test]
    fn rotation_reemits_schemas() {
        let dir = tempfile::tempdir().unwrap();
        let config = PersistConfig {
            segment_bytes: 1,
            ..PersistConfig::default()
        };
        let exporter = Exporter::start(dir.path(), config).unwrap();

        exporter.send("a", ints(&[]));
        exporter.send("b", ints(&[]));
        exporter.send("a", ints(&[1]));
        exporter.send("a", ints(&[2]));
        exporter.close(CLOSE_TIMEOUT);

        let mut seq = 1;
        while segment_path(dir.path(), seq).exists() {
            let names = schema_frames(&segment_path(dir.path(), seq));
            assert!(
                names.contains(&"a".to_string()) && names.contains(&"b".to_string()),
                "segment {seq} re-registers every known table: {names:?}"
            );
            seq += 1;
        }
        assert!(seq > 2, "a one-byte limit rotates more than once");
        let (batches, summary) = read_all(dir.path());
        let rows: usize = batches.iter().map(|(_, batch)| batch.num_rows()).sum();
        assert_eq!(rows, 2);
        assert!(summary.clean);
    }

    #[test]
    fn oversized_batch_is_split_across_frames() {
        let dir = tempfile::tempdir().unwrap();
        let exporter = Exporter::start(dir.path(), PersistConfig::default()).unwrap();
        let value = "x".repeat(512 * 1024);
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let column = StringArray::from(vec![value.as_str(); 40]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(column)]).unwrap();
        assert!(serialize_batch(&batch).unwrap().len() > MAX_FRAME_LEN);

        exporter.send("big", batch);
        exporter.close(CLOSE_TIMEOUT);

        let (batches, summary) = read_all(dir.path());
        assert!(batches.len() > 1, "a batch over the frame limit is split");
        let rows: usize = batches.iter().map(|(_, batch)| batch.num_rows()).sum();
        assert_eq!(rows, 40);
        assert!(summary.clean);
    }

    #[test]
    fn write_error_disables_exporter_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance");
        let config = PersistConfig {
            segment_bytes: 1,
            ..PersistConfig::default()
        };
        let exporter = Exporter::start(&path, config).unwrap();
        // The next rotation cannot create a segment.
        std::fs::remove_dir_all(&path).unwrap();

        exporter.send("a", ints(&[]));
        exporter.send("a", ints(&[1]));
        exporter.send("a", ints(&[2]));
        exporter.close(CLOSE_TIMEOUT);

        assert!(
            exporter.dropped() >= 1,
            "batches after the failed rotation are dropped"
        );
    }

    #[test]
    fn full_channel_counts_drops() {
        let (exporter, _receiver) = Exporter::unstarted(1);

        for value in 0..5 {
            exporter.send("t", ints(&[value]));
        }

        assert_eq!(exporter.dropped(), 4, "only one batch fits the queue");
    }

    #[test]
    fn close_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let exporter = Exporter::start(dir.path(), PersistConfig::default()).unwrap();

        exporter.close(CLOSE_TIMEOUT);
        exporter.close(CLOSE_TIMEOUT);

        let (_, summary) = read_all(dir.path());
        assert!(summary.clean);
    }
}
