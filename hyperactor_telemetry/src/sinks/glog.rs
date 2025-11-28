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
use std::io::BufWriter;
use std::io::Write;
use std::str::FromStr;

use anyhow::Result;
use indexmap::IndexMap;
use tracing_core::LevelFilter;
use tracing_subscriber::filter::Targets;

use crate::MONARCH_FILE_LOG_ENV;
use crate::trace_dispatcher::FieldValue;
use crate::trace_dispatcher::TraceEvent;
use crate::trace_dispatcher::TraceEventSink;

/// Glog sink that writes events in glog format to a file.
/// This replaces the fmt::Layer that was previously used for text logging.
///
/// This only logs Events, not Spans (matching old fmt::Layer behavior).
pub struct GlogSink {
    writer: BufWriter<Box<dyn Write + Send>>,
    prefix: Option<String>,
    /// Track active spans by ID with (name, fields, parent_id) to show span context in event logs
    active_spans: HashMap<u64, (String, IndexMap<String, FieldValue>, Option<u64>)>,
    targets: Targets,
}

impl GlogSink {
    /// Create a new glog sink with the given writer.
    ///
    /// # Arguments
    /// * `writer` - Writer to write log events to (will be buffered)
    /// * `prefix_env_var` - Optional environment variable name to read prefix from (matching old impl)
    /// * `min_level` - Minimum log level to capture (e.g., INFO, DEBUG)
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
            writer: BufWriter::new(writer),
            prefix,
            active_spans: HashMap::new(),
            targets: Targets::new()
                .with_default(LevelFilter::from_level(
                    tracing::Level::from_str(
                        &std::env::var(MONARCH_FILE_LOG_ENV).unwrap_or(file_log_level.to_string()),
                    )
                    .expect("Invalid log level"),
                ))
                .with_target("opentelemetry", LevelFilter::OFF), // otel has some log span under debug that we don't care about
        }
    }

    fn write_event(&mut self, event: &TraceEvent) -> Result<()> {
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
                    self.writer,
                    "{}{}{} {} ",
                    prefix_str, level_char, timestamp_str, thread_id
                )?;

                if let (Some(f), Some(l)) = (file, line) {
                    write!(self.writer, "{}:{}] ", f, l)?;
                } else {
                    write!(self.writer, "unknown:0] ")?;
                }

                if let Some(parent_id) = parent_span {
                    self.write_span_context(*parent_id)?;
                }

                if let Some(v) = fields.get("message") {
                    match v {
                        FieldValue::Str(s) => write!(self.writer, "{}", s)?,
                        FieldValue::Debug(s) => write!(self.writer, "{}", s)?,
                        _ => write!(self.writer, "event")?,
                    }
                } else {
                    write!(self.writer, "event")?;
                }

                for (k, v) in fields.iter() {
                    if k != "message" {
                        write!(self.writer, ", {}:", k)?;
                        match v {
                            FieldValue::Bool(b) => write!(self.writer, "{}", b)?,
                            FieldValue::I64(i) => write!(self.writer, "{}", i)?,
                            FieldValue::U64(u) => write!(self.writer, "{}", u)?,
                            FieldValue::F64(f) => write!(self.writer, "{}", f)?,
                            FieldValue::Str(s) => write!(self.writer, "{}", s)?,
                            FieldValue::Debug(s) => write!(self.writer, "{}", s)?,
                        }
                    }
                }

                writeln!(self.writer)?;
            }

            // write_event should only be called for Events, but handle gracefully
            _ => {
                writeln!(
                    self.writer,
                    "{}I{} - unknown:0] unexpected event type",
                    prefix_str, timestamp_str
                )?;
            }
        }

        Ok(())
    }

    /// Writes span context: "[outer{field:value}, inner{field:value}] "
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

        write!(self.writer, "[")?;

        for (i, id) in span_ids.iter().rev().enumerate() {
            if i > 0 {
                write!(self.writer, ", ")?;
            }

            if let Some((name, fields, _)) = self.active_spans.get(id) {
                write!(self.writer, "{}", name)?;
                if !fields.is_empty() {
                    write!(self.writer, "{{")?;

                    let mut first = true;
                    for (k, v) in fields.iter() {
                        if !first {
                            write!(self.writer, ", ")?;
                        }
                        first = false;
                        write!(self.writer, "{}:", k)?;

                        match v {
                            FieldValue::Bool(b) => write!(self.writer, "{}", b)?,
                            FieldValue::I64(i) => write!(self.writer, "{}", i)?,
                            FieldValue::U64(u) => write!(self.writer, "{}", u)?,
                            FieldValue::F64(f) => write!(self.writer, "{}", f)?,
                            FieldValue::Str(s) => write!(self.writer, "{}", s)?,
                            FieldValue::Debug(s) => write!(self.writer, "{}", s)?,
                        }
                    }

                    write!(self.writer, "}}")?;
                }
            }
        }

        write!(self.writer, "] ")?;
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
