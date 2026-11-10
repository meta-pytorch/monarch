/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Size-rotated segment files of telemetry frames, and reading them back.
//!
//! A collector's persist log is a directory of segments named
//! `{seq:06}.frames` containing Arrow data frames. Only a cleanly closed log
//! ends with an empty [`END_FRAME`]. A crash can leave the last segment with a torn
//! final frame, which the reader drops and reports.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use monarch_record_batch::arrow::array::AsArray;
use monarch_record_batch::arrow::compute;
use monarch_record_batch::arrow::datatypes::DataType;
use monarch_record_batch::arrow::datatypes::Int64Type;
use monarch_record_batch::arrow::record_batch::RecordBatch;

use crate::MAX_FRAME_LEN;
use crate::deserialize_one_batch;
use crate::frame::END_FRAME;
use crate::frame::FrameReader;
use crate::frame::RawFrame;
use crate::frame::ReadOutcome;
use crate::frame::encode_frame;
use crate::serialize_batch;

const SEGMENT_SUFFIX: &str = ".frames";

/// Path of segment `seq` in a persist log directory.
pub fn segment_path(dir: &Path, seq: u32) -> PathBuf {
    dir.join(format!("{seq:06}{SEGMENT_SUFFIX}"))
}

/// Appends frames to a persist log directory, rotating segments by size.
pub struct SegmentWriter {
    dir: PathBuf,
    seq: u32,
    file: BufWriter<File>,
    segment_written: u64,
    segment_bytes: u64,
}

impl SegmentWriter {
    /// Create `dir` if needed and start segment 0.
    ///
    /// Fails if segment 0 already exists, so two writers never share a
    /// directory.
    pub fn create(dir: &Path, segment_bytes: u64) -> anyhow::Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            seq: 0,
            file: open_segment(dir, 0)?,
            segment_written: 0,
            segment_bytes,
        })
    }

    /// Append one table batch, returning the bytes written.
    ///
    /// A batch too large for one frame is split by rows across several. A
    /// single row too large for a frame is an error, and nothing is written.
    pub fn write_batch(&mut self, table: &str, batch: &RecordBatch) -> anyhow::Result<u64> {
        let payload = serialize_batch(batch)?;
        let rows = batch.num_rows();
        if payload.len() <= MAX_FRAME_LEN || rows <= 1 {
            return self.write_frame(table, &payload);
        }
        let half = rows / 2;
        let head = self.write_batch(table, &batch.slice(0, half))?;
        let tail = self.write_batch(table, &batch.slice(half, rows - half))?;
        Ok(head + tail)
    }

    /// Whether the current segment has reached its size limit.
    pub fn needs_rotation(&self) -> bool {
        self.segment_written >= self.segment_bytes
    }

    /// Seal the current segment and start the next one.
    pub fn rotate(&mut self) -> anyhow::Result<()> {
        self.sync()?;
        self.seq += 1;
        self.file = open_segment(&self.dir, self.seq)?;
        self.segment_written = 0;
        Ok(())
    }

    /// Hand buffered bytes to the kernel.
    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.file.flush()?;
        Ok(())
    }

    /// Flush and make the current segment durable.
    pub fn sync(&mut self) -> anyhow::Result<()> {
        self.flush()?;
        self.file.get_ref().sync_data()?;
        Ok(())
    }

    /// Write the end frame and make the log durable.
    pub fn finish(mut self) -> anyhow::Result<()> {
        self.write_frame(END_FRAME, &[])?;
        self.sync()
    }

    fn write_frame(&mut self, name: &str, payload: &[u8]) -> anyhow::Result<u64> {
        let mut buf = Vec::with_capacity(payload.len() + name.len() + 6);
        encode_frame(&mut buf, name, payload)?;
        self.file.write_all(&buf)?;
        let written = buf.len() as u64;
        self.segment_written += written;
        Ok(written)
    }
}

fn open_segment(dir: &Path, seq: u32) -> anyhow::Result<BufWriter<File>> {
    let path = segment_path(dir, seq);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| format!("create {}", path.display()))?;
    Ok(BufWriter::new(file))
}

/// What reading one collector's persist log found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InstanceSummary {
    /// Segment files read.
    pub segments: u32,
    /// Whether the log ends with an end frame.
    pub clean: bool,
    /// Bytes dropped from a torn final frame.
    pub torn_bytes: u64,
    /// Rows in data frames.
    pub rows: u64,
    /// Smallest `timestamp_us` value in any data frame.
    pub first_timestamp_us: Option<i64>,
    /// Largest `timestamp_us` value in any data frame.
    pub last_timestamp_us: Option<i64>,
}

/// Read every segment in `dir` in order, passing each table batch to
/// `on_batch`, including the zero-row batches that register tables.
///
/// Reading stops at the first torn frame.
/// Table schemas come from the stored Arrow IPC streams. Reading is supported
/// with the Monarch build that wrote the log.
pub fn read_instance(
    dir: &Path,
    mut on_batch: impl FnMut(&str, RecordBatch) -> anyhow::Result<()>,
) -> anyhow::Result<InstanceSummary> {
    let mut summary = InstanceSummary::default();
    for path in segment_paths(dir)? {
        summary.segments += 1;
        let file_len = fs::metadata(&path)?.len();
        let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let mut reader = FrameReader::new(BufReader::new(file));
        loop {
            match reader.read_frame()? {
                ReadOutcome::End => break,
                ReadOutcome::Torn { offset } => {
                    summary.torn_bytes = file_len - offset;
                    return Ok(summary);
                }
                ReadOutcome::Frame(frame) => {
                    apply_frame(&mut summary, frame, &mut on_batch)
                        .with_context(|| format!("read {}", path.display()))?;
                }
            }
        }
    }
    Ok(summary)
}

fn apply_frame(
    summary: &mut InstanceSummary,
    frame: RawFrame,
    on_batch: &mut impl FnMut(&str, RecordBatch) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    summary.clean = false;
    if frame.name == END_FRAME {
        anyhow::ensure!(frame.payload.is_empty(), "end frame has a payload");
        summary.clean = true;
    } else {
        let batch = deserialize_one_batch(&frame.payload)
            .with_context(|| format!("decode frame for table {}", frame.name))?;
        summary.rows += batch.num_rows() as u64;
        update_timestamps(summary, &batch);
        on_batch(&frame.name, batch)?;
    }
    Ok(())
}

fn update_timestamps(summary: &mut InstanceSummary, batch: &RecordBatch) {
    let Some(column) = batch.column_by_name("timestamp_us") else {
        return;
    };
    if column.data_type() != &DataType::Int64 {
        return;
    }
    let values = column.as_primitive::<Int64Type>();
    if let Some(min) = compute::min(values) {
        summary.first_timestamp_us = Some(summary.first_timestamp_us.map_or(min, |t| t.min(min)));
    }
    if let Some(max) = compute::max(values) {
        summary.last_timestamp_us = Some(summary.last_timestamp_us.map_or(max, |t| t.max(max)));
    }
}

/// Segment files in `dir`, ordered by sequence number.
fn segment_paths(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut segments: Vec<(u32, PathBuf)> = fs::read_dir(dir)
        .with_context(|| format!("read {}", dir.display()))?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let seq = path
                .file_name()?
                .to_str()?
                .strip_suffix(SEGMENT_SUFFIX)?
                .parse()
                .ok()?;
            Some((seq, path))
        })
        .collect();
    segments.sort();
    Ok(segments.into_iter().map(|(_, path)| path).collect())
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::sync::Arc;

    use monarch_record_batch::arrow::array::Array;
    use monarch_record_batch::arrow::array::AsArray;
    use monarch_record_batch::arrow::array::Int64Array;
    use monarch_record_batch::arrow::array::StringDictionaryBuilder;
    use monarch_record_batch::arrow::datatypes::DataType;
    use monarch_record_batch::arrow::datatypes::Field;
    use monarch_record_batch::arrow::datatypes::Int32Type;
    use monarch_record_batch::arrow::datatypes::Schema;

    use super::*;
    use crate::frame::FrameReader;
    use crate::frame::ReadOutcome;

    fn timestamps(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp_us",
            DataType::Int64,
            false,
        )]));
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

    fn values(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_primitive::<monarch_record_batch::arrow::datatypes::Int64Type>()
            .values()
            .to_vec()
    }

    /// Byte offsets where each complete frame in `bytes` ends.
    fn frame_ends(bytes: &[u8]) -> Vec<usize> {
        let mut ends = Vec::new();
        let mut pos = 0;
        while pos < bytes.len() {
            let name_len = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]) as usize;
            let len_at = pos + 2 + name_len;
            let payload_len = u32::from_be_bytes(bytes[len_at..len_at + 4].try_into().unwrap());
            pos = len_at + 4 + payload_len as usize;
            ends.push(pos);
        }
        ends
    }

    #[test]
    fn write_then_read_round_trips_tables() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), u64::MAX).unwrap();
        writer.write_batch("a", &timestamps(&[])).unwrap();
        writer.write_batch("a", &timestamps(&[1, 2])).unwrap();
        writer.write_batch("b", &timestamps(&[3])).unwrap();
        writer.finish().unwrap();

        let (batches, summary) = read_all(dir.path());

        let seen: Vec<(String, usize)> = batches
            .iter()
            .map(|(table, batch)| (table.clone(), batch.num_rows()))
            .collect();
        assert_eq!(
            seen,
            vec![
                ("a".to_string(), 0),
                ("a".to_string(), 2),
                ("b".to_string(), 1)
            ]
        );
        assert!(summary.clean, "finish writes the end frame");
        assert_eq!(summary.rows, 3);
        assert_eq!(summary.first_timestamp_us, Some(1));
        assert_eq!(summary.last_timestamp_us, Some(3));
        assert_eq!(summary.segments, 1);
        assert_eq!(summary.torn_bytes, 0);
    }

    #[test]
    fn rotation_produces_ordered_segments() {
        let dir = tempfile::tempdir().unwrap();
        // A one-byte limit rotates before every write after the first.
        let mut writer = SegmentWriter::create(dir.path(), 1).unwrap();
        for (i, value) in [1, 2, 3].into_iter().enumerate() {
            if i > 0 && writer.needs_rotation() {
                writer.rotate().unwrap();
            }
            writer.write_batch("t", &timestamps(&[value])).unwrap();
        }
        writer.finish().unwrap();

        for seq in 0..3 {
            let mut reader = FrameReader::new(File::open(segment_path(dir.path(), seq)).unwrap());
            let ReadOutcome::Frame(frame) = reader.read_frame().unwrap() else {
                panic!("segment {seq} should start with a frame");
            };
            assert_eq!(frame.name, "t");
            assert_eq!(
                values(&deserialize_one_batch(&frame.payload).unwrap()),
                vec![i64::from(seq) + 1]
            );
        }
        assert!(!segment_path(dir.path(), 3).exists());

        let (batches, summary) = read_all(dir.path());
        let all: Vec<i64> = batches.iter().flat_map(|(_, b)| values(b)).collect();
        assert_eq!(all, vec![1, 2, 3]);
        assert_eq!(summary.segments, 3);
        assert!(summary.clean);
    }

    #[test]
    fn missing_end_is_unclean() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), u64::MAX).unwrap();
        writer.write_batch("t", &timestamps(&[7, 8])).unwrap();
        writer.flush().unwrap();
        drop(writer);

        let (_, summary) = read_all(dir.path());

        assert!(!summary.clean, "no end frame means an unclean shutdown");
        assert_eq!(summary.rows, 2);
    }

    #[test]
    fn truncated_segment_keeps_complete_frames_at_every_offset() {
        let source = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(source.path(), u64::MAX).unwrap();
        writer.write_batch("t", &timestamps(&[1, 2])).unwrap();
        writer.write_batch("t", &timestamps(&[3])).unwrap();
        writer.finish().unwrap();
        let bytes = std::fs::read(segment_path(source.path(), 0)).unwrap();
        let ends = frame_ends(&bytes);
        assert_eq!(ends.len(), 3);

        for n in 0..=bytes.len() {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(segment_path(dir.path(), 0), &bytes[..n]).unwrap();

            let (_, summary) = read_all(dir.path());

            let last_complete = ends
                .iter()
                .copied()
                .filter(|&end| end <= n)
                .max()
                .unwrap_or(0);
            let expected_rows = if n < ends[0] {
                0
            } else if n < ends[1] {
                2
            } else {
                3
            };
            assert_eq!(
                summary.rows, expected_rows,
                "complete data frames survive at {n}"
            );
            assert_eq!(summary.torn_bytes, (n - last_complete) as u64, "at {n}");
            assert_eq!(summary.clean, n == bytes.len(), "at {n}");
        }
    }

    #[test]
    fn empty_segment_reads_as_unclean_with_no_rows() {
        let dir = tempfile::tempdir().unwrap();
        File::create(segment_path(dir.path(), 0)).unwrap();

        let (batches, summary) = read_all(dir.path());

        assert!(batches.is_empty());
        assert_eq!(summary.rows, 0);
        assert!(!summary.clean);
        assert_eq!(summary.segments, 1);
    }

    #[test]
    fn dictionary_column_round_trips() {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append_value("x");
        builder.append_value("y");
        builder.append_value("x");
        let column = builder.finish();
        let data_type = column.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new("d", data_type.clone(), false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(column)]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), u64::MAX).unwrap();
        writer.write_batch("d", &batch).unwrap();
        writer.finish().unwrap();

        let (batches, _) = read_all(dir.path());

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1.schema().field(0).data_type(), &data_type);
        assert_eq!(batches[0].1, batch);
    }
}
