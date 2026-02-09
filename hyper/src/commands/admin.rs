/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Admin commands for interacting with the hyperactor admin HTTP API.

use std::io::Write;

use anyhow::Result;
use chrono::DateTime;
use chrono::Duration;
use chrono::Local;
use hyperactor::admin::ActorDetails;
use hyperactor::admin::ProcDetails;
use hyperactor::admin::ProcSummary;
use hyperactor::admin::ReferenceInfo;
use tabwriter::TabWriter;

/// Admin command for interacting with the admin HTTP API.
#[derive(clap::Args, Debug)]
pub struct AdminCommand {
    /// Admin server address (e.g., localhost:1234)
    server: String,

    #[command(subcommand)]
    action: AdminAction,
}

#[derive(clap::Subcommand, Debug)]
enum AdminAction {
    /// List actors
    Ps(PsCommand),
    /// Show actor info
    Info(InfoCommand),
    /// Show flight recorder events
    Events(EventsCommand),
}

#[derive(clap::Args, Debug)]
struct PsCommand {
    /// Optional proc name filter
    proc_name: Option<String>,
}

#[derive(clap::Args, Debug)]
struct InfoCommand {
    /// Actor reference
    actor_ref: String,
}

#[derive(clap::Args, Debug)]
struct EventsCommand {
    /// Actor reference
    actor_ref: String,
}

impl AdminCommand {
    pub async fn run(self) -> Result<()> {
        let client = reqwest::Client::new();
        let base_url = format!("http://{}", self.server);

        match self.action {
            AdminAction::Ps(cmd) => cmd.run(&client, &base_url).await,
            AdminAction::Info(cmd) => cmd.run(&client, &base_url).await,
            AdminAction::Events(cmd) => cmd.run(&client, &base_url).await,
        }
    }
}

/// Fetch actor details by reference, with proper URL encoding.
async fn fetch_actor_details(
    client: &reqwest::Client,
    base_url: &str,
    actor_ref: &str,
) -> Result<ActorDetails> {
    let encoded_ref = urlencoding::encode(actor_ref);
    let url = format!("{}/{}", base_url, encoded_ref);
    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("actor not found: {}", actor_ref);
    }
    let ref_info: ReferenceInfo = resp.json().await?;
    match ref_info {
        ReferenceInfo::Actor(details) => Ok(details),
        ReferenceInfo::Proc(_) => anyhow::bail!("expected actor, got proc: {}", actor_ref),
    }
}

/// Recursively collect all actors starting from a given actor.
async fn collect_actors_recursive(
    client: &reqwest::Client,
    base_url: &str,
    actor_ref: &str,
    actors: &mut Vec<(String, ActorDetails)>,
) {
    if let Ok(details) = fetch_actor_details(client, base_url, actor_ref).await {
        let children: Vec<String> = details.children.clone();
        actors.push((actor_ref.to_string(), details));

        // Recursively fetch children
        for child_ref in children {
            Box::pin(collect_actors_recursive(
                client, base_url, &child_ref, actors,
            ))
            .await;
        }
    }
}

impl PsCommand {
    async fn run(self, client: &reqwest::Client, base_url: &str) -> Result<()> {
        // List all procs first
        let url = format!("{}/", base_url);
        let resp = client.get(&url).send().await?;
        let procs: Vec<ProcSummary> = resp.json().await?;

        // Collect all actors from all procs (optionally filtered)
        let mut all_actors = Vec::new();

        for proc in &procs {
            // Apply proc filter if specified
            if let Some(ref filter) = self.proc_name {
                if !proc.name.contains(filter) {
                    continue;
                }
            }

            // Get proc details to find root actors
            let encoded_proc = urlencoding::encode(&proc.name);
            let url = format!("{}/procs/{}", base_url, encoded_proc);
            if let Ok(resp) = client.get(&url).send().await {
                if resp.status().is_success() {
                    if let Ok(proc_details) = resp.json::<ProcDetails>().await {
                        // Recursively collect all actors starting from roots
                        for root_actor in &proc_details.root_actors {
                            collect_actors_recursive(client, base_url, root_actor, &mut all_actors)
                                .await;
                        }
                    }
                }
            }
        }

        print_actor_table(&all_actors)?;
        Ok(())
    }
}

impl InfoCommand {
    async fn run(self, client: &reqwest::Client, base_url: &str) -> Result<()> {
        let details = fetch_actor_details(client, base_url, &self.actor_ref).await?;

        println!("id: {}", self.actor_ref);
        println!("type: {}", details.actor_type);
        println!("status: {}", details.actor_status);
        println!("parent: {}", details.parent.as_deref().unwrap_or("-"));
        println!("created: {}", details.created_at);
        println!("messages_processed: {}", details.messages_processed);
        println!(
            "processing_time: {}",
            format_duration_us(details.total_processing_time_us)
        );
        println!(
            "last_handler: {}",
            details.last_message_handler.as_deref().unwrap_or("-")
        );
        if details.children.is_empty() {
            println!("children: []");
        } else {
            println!("children:");
            for child in &details.children {
                println!("  - {}", child);
            }
        }
        Ok(())
    }
}

impl EventsCommand {
    async fn run(self, client: &reqwest::Client, base_url: &str) -> Result<()> {
        let details = fetch_actor_details(client, base_url, &self.actor_ref).await?;

        for event in &details.flight_recorder {
            // Format in glog style: L0202 15:30:45.123456 message
            let level_char = match event.level.as_str() {
                "INFO" => 'I',
                "DEBUG" => 'D',
                "WARN" => 'W',
                "ERROR" => 'E',
                "TRACE" => 'T',
                _ => '?',
            };

            // Parse timestamp and format for glog
            let formatted_time = if let Ok(dt) = DateTime::parse_from_rfc3339(&event.timestamp) {
                dt.format("%m%d %H:%M:%S%.6f").to_string()
            } else {
                event.timestamp.clone()
            };

            // Format fields as key:value pairs
            let fields_str = if let Some(obj) = event.fields.as_object() {
                obj.iter()
                    .map(|(k, v)| {
                        let v_str = match v {
                            serde_json::Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        };
                        format!("{}:{}", k, v_str)
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            } else {
                event.fields.to_string()
            };

            println!(
                "{}{} {}, {}",
                level_char, formatted_time, event.name, fields_str
            );
        }
        Ok(())
    }
}

/// Format a relative time string from an ISO timestamp.
fn format_relative_time(iso_timestamp: &str) -> String {
    let dt = DateTime::parse_from_rfc3339(iso_timestamp)
        .map(|d| d.with_timezone(&Local))
        .unwrap_or_else(|_| Local::now());
    let dur = Local::now().signed_duration_since(dt);

    if dur > Duration::days(6) {
        dt.format("%Y-%m-%dT%H:%M:%S").to_string()
    } else if dur > Duration::days(1) {
        dt.format("%a%-l:%M%P").to_string()
    } else {
        dt.format("%-l:%M%P").to_string()
    }
}

/// Format a duration in microseconds as a human-readable string.
fn format_duration_us(us: u64) -> String {
    if us < 1_000 {
        format!("{}µs", us)
    } else if us < 1_000_000 {
        format!("{:.2}ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

/// Print a table of actors with their details.
fn print_actor_table(actors: &[(String, ActorDetails)]) -> Result<()> {
    let mut tw = TabWriter::new(vec![]);
    writeln!(tw, "ACTOR_ID\tTYPE\tSTATUS\tPROC_TIME\tCREATED")?;
    for (actor_id, details) in actors {
        let created = format_relative_time(&details.created_at);
        let proc_time = format_duration_us(details.total_processing_time_us);
        writeln!(
            tw,
            "{}\t{}\t{}\t{}\t{}",
            actor_id, details.actor_type, details.actor_status, proc_time, created
        )?;
    }
    tw.flush()?;
    print!("{}", String::from_utf8(tw.into_inner()?)?);
    Ok(())
}
