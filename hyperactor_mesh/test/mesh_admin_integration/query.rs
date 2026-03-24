/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `/v1/query` endpoint integration tests.
//!
//! Requires the `query_workload` binary, which starts telemetry and
//! passes a `QueryEngine` to the admin server.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use serde_json::Value;

use crate::harness;
use crate::harness::WorkloadFixture;

fn query_workload_binary() -> PathBuf {
    buck_resources::get("monarch/hyperactor_mesh/query_workload")
        .expect("query_workload resource not found")
        .to_path_buf()
}

/// Start the query workload and wait for the admin URL sentinel.
/// The sentinel is printed only after telemetry data is populated.
async fn start_query_workload() -> Result<WorkloadFixture> {
    let bin = query_workload_binary();
    harness::start_workload(&bin, &[], Duration::from_secs(120)).await
}

/// POST a SQL query to `/v1/query` and return the raw response.
async fn post_query_raw(
    fixture: &WorkloadFixture,
    sql: &str,
) -> Result<reqwest::Response, reqwest::Error> {
    let url = format!("{}/v1/query", fixture.admin_url);
    let body = serde_json::json!({ "sql": sql });
    fixture.client.post(&url).json(&body).send().await
}

/// POST a SQL query to `/v1/query` and return the parsed JSON rows.
async fn post_query(fixture: &WorkloadFixture, sql: &str) -> Result<Vec<Value>> {
    let resp = post_query_raw(fixture, sql)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("POST /v1/query: HTTP {status}: {text}");
    }
    let rows: Vec<Value> = serde_json::from_str(&text)?;
    Ok(rows)
}

/// E2E: `/v1/query` returns actor rows as a JSON array.
pub async fn check(fixture: &WorkloadFixture) {
    // Basic SELECT * — should return at least the query_worker actors.
    let rows = post_query(fixture, "SELECT * FROM actors")
        .await
        .expect("SELECT * FROM actors failed");
    assert!(!rows.is_empty(), "expected at least one actor row");

    // Each row should have the actors table columns.
    let first = &rows[0];
    for col in &["id", "full_name", "mesh_id", "rank", "timestamp_us"] {
        assert!(
            first.get(*col).is_some(),
            "missing column '{col}' in response: {first}"
        );
    }

    // Verify the test worker appears.
    let has_worker = rows.iter().any(|r| {
        r["full_name"]
            .as_str()
            .is_some_and(|n| n.contains("query_worker"))
    });
    assert!(has_worker, "expected 'query_worker' in actor names");

    // Filtered query with WHERE clause.
    let filtered = post_query(
        fixture,
        "SELECT full_name FROM actors WHERE full_name LIKE '%query_worker%'",
    )
    .await
    .expect("filtered query failed");
    assert!(!filtered.is_empty(), "filtered query returned no rows");
    for row in &filtered {
        let name = row["full_name"].as_str().unwrap_or("");
        assert!(
            name.contains("query_worker"),
            "unexpected actor in filtered results: {name}"
        );
    }

    // Empty result query.
    let empty = post_query(
        fixture,
        "SELECT * FROM actors WHERE full_name = 'nonexistent_xyz'",
    )
    .await
    .expect("empty query failed");
    assert!(empty.is_empty(), "expected zero rows for nonexistent actor");

    // Invalid SQL returns an error status.
    let resp = post_query_raw(fixture, "NOT VALID SQL !!!")
        .await
        .expect("invalid SQL request failed to send");
    assert!(
        !resp.status().is_success(),
        "expected error status for invalid SQL, got {}",
        resp.status()
    );
}

/// Run the full query endpoint test suite.
pub async fn run_query_endpoint() {
    let fixture = start_query_workload()
        .await
        .expect("failed to start query workload");
    check(&fixture).await;
    fixture.shutdown().await;
}
