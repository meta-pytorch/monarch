/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Glog-formatted text sink for trace events.
//! Replicates the behavior of the fmt::Layer with glog formatting.

use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::io::Write;
use std::str::FromStr;

use anyhow::Result;
use indexmap::IndexMap;
use tracing_core::LevelFilter;
use tracing_subscriber::filter::Targets;

use crate::config::MONARCH_FILE_LOG_LEVEL;
use crate::trace_dispatcher::FieldValue;
use crate::trace_dispatcher::TraceEvent;
use crate::trace_dispatcher::TraceEventSink;

const MAX_LINE_SIZE: usize = 4096;
const TRUNCATION_SUFFIX_RESERVE: usize = 32;

/// A string buffer that limits writes to a maximum size.
/// Once the limit is reached, further writes are silently ignored and
/// truncated chars are tracked for reporting.
struct LimitedBuffer {
    buffer: String,
    /// Max bytes for content (excluding truncation suffix and newline).
    limit: usize,
    truncated_chars: usize,
}

impl LimitedBuffer {
    fn new(limit: usize) -> Self {
        Self {
            buffer: String::with_capacity(limit + TRUNCATION_SUFFIX_RESERVE),
            limit,
            truncated_chars: 0,
        }
    }

    fn clear(&mut self) {
        self.buffer.clear();
        self.truncated_chars = 0;
    }

    /// Write truncation suffix + add a newline
    fn finish_line(&mut self) {
        if self.truncated_chars > 0 {
            use std::fmt::Write;
            let _ = write!(
                &mut self.buffer,
                "...[truncated {} chars]",
                self.truncated_chars
            );
        }
        self.buffer.push('\n');
    }

    fn as_bytes(&self) -> &[u8] {
        self.buffer.as_bytes()
    }
}

impl FmtWrite for LimitedBuffer {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let remaining = self.limit.saturating_sub(self.buffer.len());
        if remaining == 0 {
            self.truncated_chars += s.chars().count();
            return Ok(());
        }
        if s.len() <= remaining {
            self.buffer.push_str(s);
        } else {
            let mut truncate_at = remaining;
            while truncate_at > 0 && !s.is_char_boundary(truncate_at) {
                truncate_at -= 1;
            }
            self.buffer.push_str(&s[..truncate_at]);
            self.truncated_chars += s[truncate_at..].chars().count();
        }
        Ok(())
    }
}

/// Glog sink that writes events in glog format to a file.
/// This replaces the fmt::Layer that was previously used for text logging.
///
/// This only logs Events, not Spans (matching old fmt::Layer behavior).
pub struct GlogSink {
    writer: Box<dyn Write + Send>,
    prefix: Option<String>,
    /// Track active spans by ID with (name, fields, parent_id) to show span context in event logs
    active_spans: HashMap<u64, (String, IndexMap<String, FieldValue>, Option<u64>)>,
    targets: Targets,
    /// Reusable buffer for formatting log lines to ensure atomic writes.
    /// We build the entire line in this buffer, then write it atomically to avoid
    /// interleaving with other threads writing to the same fd (e.g., stderr).
    line_buffer: LimitedBuffer,
}

impl GlogSink {
    /// Create a new glog sink with the given writer.
    ///
    /// # Arguments
    /// * `writer` - Writer to write log events to (used directly without buffering)
    /// * `prefix_env_var` - Optional environment variable name to read prefix from (matching old impl)
    /// * `file_log_level` - Minimum log level to capture (e.g., "info", "debug")
    pub fn new(
        writer: Box<dyn Write + Send>,
        prefix_env_var: Option<String>,
        file_log_level: &str,
    ) -> Self {
        let prefix = if let Some(ref env_var_name) = prefix_env_var {
            std::env::var(env_var_name).ok()
        } else {
            None
        };

        Self {
            writer,
            prefix,
            active_spans: HashMap::new(),
            targets: Targets::new()
                .with_default(LevelFilter::from_level({
                    let log_level_str =
                        hyperactor_config::global::try_get_cloned(MONARCH_FILE_LOG_LEVEL)
                            .unwrap_or_else(|| file_log_level.to_string());
                    tracing::Level::from_str(&log_level_str).unwrap_or_else(|_| {
                        tracing::Level::from_str(file_log_level).expect("Invalid default log level")
                    })
                }))
                .with_target("opentelemetry", LevelFilter::OFF), // otel has some log span under debug that we don't care about
            line_buffer: LimitedBuffer::new(MAX_LINE_SIZE - TRUNCATION_SUFFIX_RESERVE),
        }
    }

    fn write_event(&mut self, event: &TraceEvent) -> Result<()> {
        self.line_buffer.clear();

        let timestamp_str = match event {
            TraceEvent::Event { timestamp, .. } => {
                let datetime: chrono::DateTime<chrono::Local> = (*timestamp).into();
                datetime.format("%m%d %H:%M:%S%.6f").to_string()
            }
            // write_event is only called for Events, but keep this for safety
            _ => chrono::Local::now().format("%m%d %H:%M:%S%.6f").to_string(),
        };

        let prefix_str = if let Some(ref p) = self.prefix {
            format!("[{}]", p)
        } else {
            "[-]".to_string()
        };

        match event {
            TraceEvent::Event {
                level,
                fields,
                parent_span,
                thread_id,
                file,
                line,
                ..
            } => {
                let level_char = match *level {
                    tracing::Level::ERROR => 'E',
                    tracing::Level::WARN => 'W',
                    tracing::Level::INFO => 'I',
                    tracing::Level::DEBUG => 'D',
                    tracing::Level::TRACE => 'T',
                };

                // [prefix]LMMDD HH:MM:SS.ffffff thread_id file:line] message, key:value, key:value
                write!(
                    &mut self.line_buffer,
                    "{}{}{} {} ",
                    prefix_str, level_char, timestamp_str, thread_id
                )?;

                if let (Some(f), Some(l)) = (file, line) {
                    write!(&mut self.line_buffer, "{}:{}] ", f, l)?;
                } else {
                    write!(&mut self.line_buffer, "unknown:0] ")?;
                }

                if let Some(parent_id) = parent_span {
                    self.write_span_context(*parent_id)?;
                }

                if let Some(v) = fields.get("message") {
                    match v {
                        FieldValue::Str(s) => write!(&mut self.line_buffer, "{}", s)?,
                        FieldValue::Debug(s) => write!(&mut self.line_buffer, "{}", s)?,
                        _ => write!(&mut self.line_buffer, "event")?,
                    }
                } else {
                    write!(&mut self.line_buffer, "event")?;
                }

                for (k, v) in fields.iter() {
                    if k != "message" {
                        write!(&mut self.line_buffer, ", {}:", k)?;
                        match v {
                            FieldValue::Bool(b) => write!(&mut self.line_buffer, "{}", b)?,
                            FieldValue::I64(i) => write!(&mut self.line_buffer, "{}", i)?,
                            FieldValue::U64(u) => write!(&mut self.line_buffer, "{}", u)?,
                            FieldValue::F64(f) => write!(&mut self.line_buffer, "{}", f)?,
                            FieldValue::Str(s) => write!(&mut self.line_buffer, "{}", s)?,
                            FieldValue::Debug(s) => write!(&mut self.line_buffer, "{}", s)?,
                        }
                    }
                }

                self.line_buffer.finish_line();

                self.writer.write_all(self.line_buffer.as_bytes())?;
            }

            // write_event should only be called for Events, but handle gracefully
            _ => {
                self.line_buffer.clear();
                write!(
                    &mut self.line_buffer,
                    "{}I{} - unknown:0] unexpected event type",
                    prefix_str, timestamp_str
                )?;
                self.line_buffer.finish_line();
                self.writer.write_all(self.line_buffer.as_bytes())?;
            }
        }

        Ok(())
    }

    /// Writes span context into line_buffer: "[outer{field:value}, inner{field:value}] "
    fn write_span_context(&mut self, span_id: u64) -> Result<()> {
        let mut span_ids = Vec::new();
        let mut current_id = Some(span_id);

        while let Some(id) = current_id {
            if let Some((_, _, parent_id)) = self.active_spans.get(&id) {
                span_ids.push(id);
                current_id = *parent_id;
            } else {
                break;
            }
        }
        if span_ids.is_empty() {
            return Ok(());
        }

        write!(&mut self.line_buffer, "[")?;

        for (i, id) in span_ids.iter().rev().enumerate() {
            if i > 0 {
                write!(&mut self.line_buffer, ", ")?;
            }

            if let Some((name, fields, _)) = self.active_spans.get(id) {
                write!(&mut self.line_buffer, "{}", name)?;
                if !fields.is_empty() {
                    write!(&mut self.line_buffer, "{{")?;

                    let mut first = true;
                    for (k, v) in fields.iter() {
                        if !first {
                            write!(&mut self.line_buffer, ", ")?;
                        }
                        first = false;
                        write!(&mut self.line_buffer, "{}:", k)?;

                        match v {
                            FieldValue::Bool(b) => write!(&mut self.line_buffer, "{}", b)?,
                            FieldValue::I64(i) => write!(&mut self.line_buffer, "{}", i)?,
                            FieldValue::U64(u) => write!(&mut self.line_buffer, "{}", u)?,
                            FieldValue::F64(f) => write!(&mut self.line_buffer, "{}", f)?,
                            FieldValue::Str(s) => write!(&mut self.line_buffer, "{}", s)?,
                            FieldValue::Debug(s) => write!(&mut self.line_buffer, "{}", s)?,
                        }
                    }

                    write!(&mut self.line_buffer, "}}")?;
                }
            }
        }

        write!(&mut self.line_buffer, "] ")?;
        Ok(())
    }
}

impl TraceEventSink for GlogSink {
    fn consume(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error> {
        match event {
            // Track span lifecycle for context display (must happen even if we don't export spans)
            TraceEvent::NewSpan {
                id,
                name,
                fields,
                parent_id,
                ..
            } => {
                self.active_spans
                    .insert(*id, (name.to_string(), fields.clone(), *parent_id));
            }
            TraceEvent::SpanClose { id, .. } => {
                self.active_spans.remove(id);
            }
            TraceEvent::Event { .. } => {
                self.write_event(event)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), anyhow::Error> {
        self.writer.flush()?;
        Ok(())
    }

    fn name(&self) -> &str {
        "GlogSink"
    }

    fn target_filter(&self) -> Option<&Targets> {
        Some(&self.targets)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_limited_buffer_truncation() {
        let mut buf = LimitedBuffer::new(20);

        write!(
            &mut buf,
            "Hello, this is a very long message that exceeds the limit"
        )
        .unwrap();
        buf.finish_line();

        let output = std::str::from_utf8(buf.as_bytes()).unwrap();

        assert_eq!(output, "Hello, this is a ver...[truncated 37 chars]\n");
    }

    #[test]
    fn test_limited_buffer_no_truncation() {
        let mut buf = LimitedBuffer::new(50);

        write!(&mut buf, "Short message").unwrap();
        buf.finish_line();

        let output = std::str::from_utf8(buf.as_bytes()).unwrap();

        assert_eq!(output, "Short message\n");
    }
}
