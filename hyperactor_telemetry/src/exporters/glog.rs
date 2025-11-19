/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Glog-formatted text exporter for trace events.
//! Replicates the behavior of the fmt::Layer with glog formatting.

use std::io::Write;

use anyhow::Result;
use indexmap::IndexMap;
use tracing_appender::non_blocking::NonBlocking;

use crate::unified::FieldValue;
use crate::unified::TraceEvent;
use crate::unified::TraceExporter;

/// Glog exporter that writes events in glog format to a file.
/// This replaces the fmt::Layer that was previously used for text logging.
///
/// This only logs Events, not Spans (matching old fmt::Layer behavior).
pub struct GlogExporter {
    writer: NonBlocking,
    prefix: Option<String>,
    min_level: tracing::Level,
    /// Track active spans by ID with (name, fields, parent_id) to show span context in event logs
    active_spans: IndexMap<u64, (String, IndexMap<String, FieldValue>, Option<u64>)>,
}

impl GlogExporter {
    /// Create a new glog exporter with the given writer.
    ///
    /// # Arguments
    /// * `writer` - NonBlocking writer (from tracing_appender)
    /// * `prefix_env_var` - Optional environment variable name to read prefix from (matching old impl)
    /// * `min_level` - Minimum log level to capture (e.g., INFO, DEBUG)
    pub fn new(
        writer: NonBlocking,
        prefix_env_var: Option<String>,
        min_level: tracing::Level,
    ) -> Self {
        let prefix = if let Some(ref env_var_name) = prefix_env_var {
            std::env::var(env_var_name).ok()
        } else {
            None
        };

        Self {
            writer,
            prefix,
            min_level,
            active_spans: IndexMap::new(),
        }
    }

    fn format_event(&self, event: &TraceEvent) -> String {
        let timestamp_str = match event {
            TraceEvent::Event { timestamp, .. } => {
                let datetime: chrono::DateTime<chrono::Local> = (*timestamp).into();
                datetime.format("%m%d %H:%M:%S%.6f").to_string()
            }
            // format_event is only called for Events, but keep this for safety
            _ => chrono::Local::now().format("%m%d %H:%M:%S%.6f").to_string(),
        };

        let prefix_str = if let Some(ref p) = self.prefix {
            format!("[{}]", p)
        } else {
            "[-]".to_string()
        };

        match event {
            TraceEvent::Event {
                name,
                target,
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
                let file_line = if let (Some(f), Some(l)) = (file, line) {
                    format!("{}:{}", f, l)
                } else {
                    "unknown:0".to_string()
                };

                let (message_text, other_fields_str) = Self::format_event_with_message(fields);

                let span_context = if let Some(parent_id) = parent_span {
                    self.format_span_context(*parent_id)
                } else {
                    String::new()
                };

                let message_and_fields = if other_fields_str.is_empty() {
                    message_text
                } else {
                    format!("{}, {}", message_text, other_fields_str)
                };

                format!(
                    "{}{}{} {} {}] {}{}",
                    prefix_str,
                    level_char,
                    timestamp_str,
                    thread_id,
                    file_line,
                    span_context,
                    message_and_fields
                )
            }

            // format_event should only be called for Events, but handle gracefully
            _ => {
                format!(
                    "{}I{} - unknown:0] unexpected event type",
                    prefix_str, timestamp_str
                )
            }
        }
    }

    fn format_fields_compact(fields: &IndexMap<String, FieldValue>) -> String {
        fields
            .iter()
            .map(|(k, v)| {
                let value_str = match v {
                    FieldValue::Bool(b) => b.to_string(),
                    FieldValue::I64(i) => i.to_string(),
                    FieldValue::U64(u) => u.to_string(),
                    FieldValue::F64(f) => f.to_string(),
                    FieldValue::Str(s) => s.clone(),
                    FieldValue::Debug(s) => s.clone(),
                };
                format!("{}:{}", k, value_str)
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn format_event_with_message(fields: &IndexMap<String, FieldValue>) -> (String, String) {
        let message_text = fields
            .get("message")
            .map(|v| match v {
                FieldValue::Str(s) => s.clone(),
                FieldValue::Debug(s) => s.clone(),
                _ => String::new(),
            })
            .unwrap_or_else(|| "event".to_string());

        let other_fields: IndexMap<String, FieldValue> = fields
            .iter()
            .filter(|(k, _)| *k != "message")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let other_fields_str = Self::format_fields_compact(&other_fields);

        (message_text, other_fields_str)
    }

    /// Formats to: "[outer{field:value}, inner{field:value}]
    fn format_span_context(&self, span_id: u64) -> String {
        let mut span_chain = Vec::new();
        let mut current_id = Some(span_id);

        while let Some(id) = current_id {
            if let Some((span_name, span_fields, parent_id)) = self.active_spans.get(&id) {
                span_chain.push((span_name.clone(), span_fields.clone()));
                current_id = *parent_id;
            } else {
                break;
            }
        }

        span_chain.reverse();

        if span_chain.is_empty() {
            String::new()
        } else {
            let formatted_spans: Vec<String> = span_chain
                .iter()
                .map(|(name, fields)| {
                    let fields_str = Self::format_fields_compact_curly(fields);
                    if fields_str.is_empty() {
                        name.clone()
                    } else {
                        format!("{}{{{}}}", name, fields_str)
                    }
                })
                .collect();
            format!("[{}] ", formatted_spans.join(", "))
        }
    }

    fn format_fields_compact_curly(fields: &IndexMap<String, FieldValue>) -> String {
        fields
            .iter()
            .map(|(k, v)| {
                let value_str = match v {
                    FieldValue::Bool(b) => b.to_string(),
                    FieldValue::I64(i) => i.to_string(),
                    FieldValue::U64(u) => u.to_string(),
                    FieldValue::F64(f) => f.to_string(),
                    FieldValue::Str(s) => s.clone(),
                    FieldValue::Debug(s) => s.clone(),
                };
                format!("{}:{}", k, value_str)
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl TraceExporter for GlogExporter {
    fn export(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error> {
        // Track span lifecycle for context display (must happen even if we don't export spans)
        match event {
            TraceEvent::SpanEnter {
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
                self.active_spans.shift_remove(id);
            }
            _ => {}
        }

        // Only export Events, not Spans (matching old fmt::Layer behavior)
        if let TraceEvent::Event { .. } = event {
            let line = self.format_event(event);
            writeln!(self.writer, "{}", line)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), anyhow::Error> {
        Ok(())
    }

    fn name(&self) -> &str {
        "GlogExporter"
    }

    fn should_export(&self, event: &TraceEvent) -> bool {
        match event {
            TraceEvent::SpanEnter { .. } | TraceEvent::SpanClose { .. } => true,

            TraceEvent::Event { level, target, .. } => {
                *level <= self.min_level && *target != "opentelemetry"
            }

            _ => false,
        }
    }
}
