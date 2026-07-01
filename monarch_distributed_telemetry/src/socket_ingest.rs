/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Unix-socket ingestion for telemetry Arrow IPC frames.
//!
//! This module is the collector-side transport for telemetry producers that
//! cannot write directly into `DatabaseScanner`. It owns the Unix socket bind,
//! accepts producer connections, validates the frame envelope, decodes Arrow
//! IPC payloads, and appends the decoded `RecordBatch` into a pre-registered
//! `TableStore`.
//!
//! The wire format is intentionally narrow: each frame names one destination
//! table and carries one Arrow IPC stream payload for that table. Table
//! registration and schema ownership stay in `DatabaseScanner`/`TableStore`,
//! so this layer only does transport validation and hands decoded batches to
//! the storage contract.
//!
//! Frame layout:
//!
//! - `u16`: big-endian table-name byte length
//! - `[u8]`: UTF-8 table name
//! - `u32`: big-endian Arrow IPC payload byte length
//! - `[u8]`: Arrow IPC stream containing exactly one non-empty `RecordBatch`

use std::io::Cursor;
use std::io::ErrorKind;
use std::os::unix::net::UnixListener as StdUnixListener;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use monarch_hyperactor::runtime::get_tokio_runtime;
use monarch_telemetry_schema::MAX_FRAME_LEN;
use monarch_telemetry_schema::MAX_TABLE_NAME_LEN;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio::net::UnixListener;
use tokio::task::JoinHandle;

use crate::database_scanner::TableStore;

/// Buffered read capacity for producer socket connections.
const READER_BUFFER_CAPACITY: usize = 64 * 1024;

/// Handle for the background socket ingest task.
pub struct IngestServerHandle {
    task: JoinHandle<()>,
}

impl Drop for IngestServerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Bind the telemetry ingest socket for the active collector.
///
/// A stale socket file from a crashed collector is removed and replaced. A live
/// collector is an activation error: callers should avoid starting duplicate
/// collectors for the same host-local socket namespace.
pub(crate) fn bind_ingest_socket(path: &Path) -> anyhow::Result<StdUnixListener> {
    match bind_listener(path) {
        Ok(listener) => Ok(listener),
        Err(error) if error.kind() == ErrorKind::AddrInUse => {
            // `AddrInUse` can mean either a live collector or a stale socket
            // file. Probe by connecting first so we only remove stale files.
            if StdUnixStream::connect(path).is_ok() {
                return Err(anyhow::anyhow!(
                    "telemetry socket already has a live collector: {}",
                    path.display()
                ));
            }

            std::fs::remove_file(path)
                .with_context(|| format!("remove stale socket {}", path.display()))?;
            bind_listener(path).with_context(|| format!("bind socket {}", path.display()))
        }
        Err(error) => Err(error).with_context(|| format!("bind socket {}", path.display())),
    }
}

/// Run the ingest accept loop on the active Tokio runtime.
pub fn run_ingest_server(
    listener: StdUnixListener,
    store: TableStore,
) -> anyhow::Result<IngestServerHandle> {
    // `bind_ingest_socket` returns a std listener because binding is a
    // synchronous ownership decision, not part of the async accept loop. Once
    // ownership is established, convert the fd into Tokio's listener for serving
    // connections. Tokio requires the fd to be nonblocking before `from_std`.
    listener.set_nonblocking(true)?;
    // Called from a PyO3 thread with no ambient Tokio runtime. Use monarch's
    // shared runtime.
    let runtime = get_tokio_runtime();
    let _guard = runtime.enter();
    let listener = UnixListener::from_std(listener)?;
    let task = runtime.spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let store = store.clone();
                    tokio::spawn(async move {
                        // Treat malformed frames as connection-level errors.
                        // Once the envelope is invalid, stream alignment is no
                        // longer trustworthy; producers can reconnect, and the
                        // listener remains available for other connections.
                        if let Err(error) = read_connection(stream, store).await {
                            tracing::warn!("telemetry socket connection closed: {error}");
                        }
                    });
                }
                Err(error) => {
                    // Most accept errors here are transient (FD exhaustion,
                    // per-connection aborts). The listener fd stays valid, so
                    // back off briefly and keep accepting rather than tearing
                    // down ingest for the scanner lifetime.
                    tracing::warn!("telemetry socket accept failed: {error}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    });

    Ok(IngestServerHandle { task })
}

fn bind_listener(path: &Path) -> std::io::Result<StdUnixListener> {
    let listener = StdUnixListener::bind(path)?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

async fn read_connection(stream: tokio::net::UnixStream, store: TableStore) -> anyhow::Result<()> {
    let mut reader = BufReader::with_capacity(READER_BUFFER_CAPACITY, stream);

    // A producer may keep one socket connection open and stream many frames.
    // Each frame declares the destination table, then carries one Arrow IPC
    // stream payload for that table.
    loop {
        let Some(table_name) = read_table_name(&mut reader).await? else {
            return Ok(());
        };
        let payload = read_payload(&mut reader).await?;
        let batch = decode_one_batch(&table_name, &payload)?;
        store.push_to_registered(&table_name, batch).await?;
    }
}

async fn read_table_name<R>(reader: &mut R) -> anyhow::Result<Option<String>>
where
    R: AsyncRead + Unpin,
{
    let mut name_len_bytes = [0; 2];
    // EOF before the next table-name length is a clean connection close.
    // Once any header bytes are present, a truncated frame is an error from
    // the stricter reads below.
    if !read_exact_or_eof(reader, &mut name_len_bytes).await? {
        return Ok(None);
    }

    let name_len = u16::from_be_bytes(name_len_bytes) as usize;
    if name_len == 0 || name_len > MAX_TABLE_NAME_LEN {
        anyhow::bail!("invalid table name length {name_len}");
    }

    let mut name_bytes = vec![0; name_len];
    reader.read_exact(&mut name_bytes).await?;
    Ok(Some(String::from_utf8(name_bytes)?))
}

async fn read_payload<R>(reader: &mut R) -> anyhow::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut frame_len_bytes = [0; 4];
    reader.read_exact(&mut frame_len_bytes).await?;

    let frame_len = u32::from_be_bytes(frame_len_bytes) as usize;
    if frame_len == 0 || frame_len > MAX_FRAME_LEN {
        anyhow::bail!("invalid frame length {frame_len}");
    }

    let mut payload = vec![0; frame_len];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

async fn read_exact_or_eof<R>(reader: &mut R, buf: &mut [u8]) -> anyhow::Result<bool>
where
    R: AsyncRead + Unpin,
{
    match reader.read_exact(buf).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn decode_one_batch(table_name: &str, payload: &[u8]) -> anyhow::Result<RecordBatch> {
    let mut reader = StreamReader::try_new(Cursor::new(payload), None)?;
    // Keep the wire contract one table batch per frame. This lets append-time
    // schema validation report one table context and avoids partial success
    // when a later batch in the same frame is invalid.
    let Some(batch) = reader.next().transpose()? else {
        anyhow::bail!("frame for table {table_name} contained no record batch");
    };

    if batch.num_rows() == 0 {
        anyhow::bail!("frame for table {table_name} contained an empty record batch");
    }

    if reader.next().transpose()?.is_some() {
        anyhow::bail!("frame for table {table_name} contained multiple record batches");
    }

    Ok(batch)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::datatypes::Field;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use hyperactor_telemetry::initialize_logging_for_test;
    use hyperactor_telemetry::set_unix_socket_sink_path;
    use monarch_record_batch::RecordBatchBuffer;
    use monarch_telemetry_schema::serialize_batch;
    use monarch_telemetry_schema::trace_tables::EVENTS;
    use monarch_telemetry_schema::trace_tables::EventBuffer;
    use tokio::io::AsyncWriteExt;

    use super::*;

    static TEST_SEQ: AtomicU64 = AtomicU64::new(0);

    fn socket_path(name: &str) -> std::path::PathBuf {
        let seq = TEST_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "monarch_socket_ingest_{}_{}",
            std::process::id(),
            seq
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    fn make_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let col = Int64Array::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap()
    }

    fn make_other_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("y", DataType::Int64, false)]));
        let col = Int64Array::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap()
    }

    async fn write_frame(path: &Path, table_name: &str, batch: &RecordBatch) {
        let payload = serialize_batch(batch).unwrap();
        let mut frame = Vec::new();
        append_frame_header(&mut frame, table_name, payload.len());
        frame.extend_from_slice(&payload);

        let mut stream = tokio::net::UnixStream::connect(path).await.unwrap();
        stream.write_all(&frame).await.unwrap();
        stream.shutdown().await.unwrap();
    }

    fn append_frame_header(buf: &mut Vec<u8>, table_name: &str, payload_len: usize) {
        let name_len = u16::try_from(table_name.len()).unwrap();
        let frame_len = u32::try_from(payload_len).unwrap();

        buf.extend_from_slice(&name_len.to_be_bytes());
        buf.extend_from_slice(table_name.as_bytes());
        buf.extend_from_slice(&frame_len.to_be_bytes());
    }

    async fn count_rows(store: &TableStore, table_name: &str) -> usize {
        let provider = store.table_provider(table_name).unwrap().unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(table_name, provider).unwrap();
        let batches = ctx
            .sql(&format!("SELECT COUNT(*) AS cnt FROM {table_name}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let counts = batches[0]
            .column_by_name("cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        counts.value(0) as usize
    }

    async fn wait_for_rows(store: &TableStore, table_name: &str, expected: usize) {
        for _ in 0..50 {
            if count_rows(store, table_name).await == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(count_rows(store, table_name).await, expected);
    }

    async fn count_event_rows_for_target(store: &TableStore, target: &str) -> usize {
        let provider = store.table_provider(EVENTS).unwrap().unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(EVENTS, provider).unwrap();
        let batches = ctx
            .sql(&format!(
                "SELECT COUNT(*) AS cnt FROM {EVENTS} WHERE target = '{target}'"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let counts = batches[0]
            .column_by_name("cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        counts.value(0) as usize
    }

    async fn wait_for_event_target(store: &TableStore, target: &str, expected: usize) {
        for _ in 0..50 {
            if count_event_rows_for_target(store, target).await == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(count_event_rows_for_target(store, target).await, expected);
    }

    #[test]
    fn bind_ingest_socket_rejects_live_collector() {
        let path = socket_path("telemetry.sock");
        let _listener = StdUnixListener::bind(&path).unwrap();

        let error = bind_ingest_socket(&path).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("telemetry socket already has a live collector")
        );
    }

    #[test]
    fn bind_ingest_socket_replaces_stale_socket_file() {
        let path = socket_path("stale.sock");
        std::fs::write(&path, b"stale").unwrap();

        let _listener = bind_ingest_socket(&path).unwrap();
    }

    #[tokio::test]
    async fn ingest_server_pushes_registered_batch() {
        let path = socket_path("ingest.sock");
        let listener = bind_ingest_socket(&path).unwrap();
        let store = TableStore::new_empty();
        store.register_table("t", make_batch(&[]).schema()).unwrap();
        let _handle = run_ingest_server(listener, store.clone()).unwrap();

        write_frame(&path, "t", &make_batch(&[1, 2, 3])).await;

        wait_for_rows(&store, "t", 3).await;
    }

    #[tokio::test]
    async fn ingest_server_receives_unix_socket_sink_frame() {
        let path = socket_path("integration.sock");
        let listener = bind_ingest_socket(&path).unwrap();
        let store = TableStore::new_empty();
        let mut buffer = EventBuffer::default();
        // Register the generated schema without rows; the socket producer must
        // create the first stored event.
        store
            .register_table(EVENTS, buffer.drain_to_record_batch().unwrap().schema())
            .unwrap();
        let _handle = run_ingest_server(listener, store.clone()).unwrap();

        // Use the public producer API, not the local `write_frame` helper, so
        // this catches drift between the Unix sink and ingest server.
        initialize_logging_for_test();
        set_unix_socket_sink_path(path).unwrap();
        tracing::info!(
            target: "telemetry_socket_integration_test",
            count = 3u64,
            "producer event"
        );

        wait_for_event_target(&store, "telemetry_socket_integration_test", 1).await;
    }

    #[tokio::test]
    async fn ingest_server_rejects_schema_mismatch() {
        let path = socket_path("schema.sock");
        let listener = bind_ingest_socket(&path).unwrap();
        let store = TableStore::new_empty();
        store.register_table("t", make_batch(&[]).schema()).unwrap();
        let _handle = run_ingest_server(listener, store.clone()).unwrap();

        write_frame(&path, "t", &make_other_batch(&[1, 2, 3])).await;

        wait_for_rows(&store, "t", 0).await;
    }

    #[tokio::test]
    async fn ingest_server_keeps_accepting_after_malformed_frame() {
        let path = socket_path("survive.sock");
        let listener = bind_ingest_socket(&path).unwrap();
        let store = TableStore::new_empty();
        store.register_table("t", make_batch(&[]).schema()).unwrap();
        let _handle = run_ingest_server(listener, store.clone()).unwrap();

        // First connection sends a header that declares a name length of zero,
        // which `read_table_name` rejects. The per-connection task should die
        // while the accept loop stays alive.
        let mut bad = tokio::net::UnixStream::connect(&path).await.unwrap();
        bad.write_all(&0u16.to_be_bytes()).await.unwrap();
        bad.shutdown().await.unwrap();
        drop(bad);

        // A subsequent valid connection on the same server should still be
        // accepted and its batch pushed.
        write_frame(&path, "t", &make_batch(&[1, 2, 3])).await;

        wait_for_rows(&store, "t", 3).await;
    }
}
