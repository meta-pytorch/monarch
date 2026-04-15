/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Integration tests for `POST /v1/query` and
//! `POST /v1/pyspy_dump/{*proc_reference}`.
//!
//! These routes proxy to the Monarch dashboard and require
//! `telemetry_url` to be configured. The Python dining_philosophers
//! binary is launched with `--dashboard` so that `start_telemetry`
//! starts the dashboard and passes `telemetry_url` to `_spawn_admin`.

use std::time::Duration;

use hyperactor_mesh::mesh_admin::ApiErrorEnvelope;
use hyperactor_mesh::mesh_admin::PyspyDumpAndStoreResponse;
use hyperactor_mesh::mesh_admin::QueryRequest;
use hyperactor_mesh::mesh_admin::QueryResponse;

use crate::harness;
use crate::harness::WorkloadFixture;

/// Pick an ephemeral port for the dashboard by binding to `:0` and
/// reading back the OS-assigned port.
fn pick_dashboard_port() -> u16 {
    let listener = std::net::TcpListener::bind("0.0.0.0:0").expect("bind :0");
    listener.local_addr().expect("local_addr").port()
}

/// Start the Python dining_philosophers binary with dashboard enabled.
async fn start_with_dashboard() -> WorkloadFixture {
    let bin = harness::dining_philosophers_python_binary();
    let port = pick_dashboard_port().to_string();
    harness::start_workload(
        &bin,
        &["--dashboard", "--dashboard-port", &port],
        Duration::from_secs(90),
    )
    .await
    .expect("failed to start dining_philosophers with --dashboard")
}

/// MIT-63: `/v1/query` returns rows for a valid SQL query against DataFusion.
pub async fn run_query_success() {
    let fixture = start_with_dashboard().await;

    let req = QueryRequest {
        sql: "SELECT 1 AS n".to_string(),
    };
    let resp: QueryResponse = fixture
        .post_json_with_retry("/v1/query", &req)
        .await
        .expect("query proxy should return rows");
    let rows = resp.rows.as_array().expect("rows should be an array");
    assert!(!rows.is_empty(), "expected at least one row");

    fixture.shutdown().await;
}

/// MIT-64: `/v1/query` returns 400 with `ApiErrorEnvelope` for invalid SQL.
pub async fn run_query_invalid_sql() {
    let fixture = start_with_dashboard().await;

    let req = QueryRequest {
        sql: "NOT VALID SQL".to_string(),
    };
    let resp = fixture
        .post("/v1/query", &req)
        .await
        .expect("transport should succeed");
    assert_eq!(
        resp.status().as_u16(),
        400,
        "invalid SQL should return 400, got {}",
        resp.status()
    );
    let body = resp.text().await.unwrap();
    let envelope: ApiErrorEnvelope =
        serde_json::from_str(&body).expect("response should be ApiErrorEnvelope");
    assert_eq!(envelope.error.code, "bad_request");
    assert!(
        !envelope.error.message.is_empty(),
        "error message should be non-empty"
    );

    fixture.shutdown().await;
}

/// MIT-65: `/v1/query` can query telemetry tables populated by the workload.
pub async fn run_query_telemetry_tables() {
    let fixture = start_with_dashboard().await;

    // Wait for topology to settle.
    fixture
        .classify_procs()
        .await
        .expect("procs should be classifiable");

    let req = QueryRequest {
        sql: "SELECT COUNT(*) AS cnt FROM meshes".to_string(),
    };
    let resp: QueryResponse = fixture
        .post_json_with_retry("/v1/query", &req)
        .await
        .expect("meshes query should succeed");
    let rows = resp.rows.as_array().expect("rows should be an array");
    assert!(!rows.is_empty(), "expected mesh count row");

    fixture.shutdown().await;
}

/// MIT-67: End-to-end: discover a proc via SQL, dump its py-spy stacks via
/// `/v1/pyspy_dump`, then verify the dump exists via SQL query.
pub async fn run_pyspy_dump_and_query() {
    let fixture = start_with_dashboard().await;

    // Wait for topology to settle so actors table is populated.
    fixture
        .classify_procs()
        .await
        .expect("procs should be classifiable");

    // 1. Use SQL to discover a worker proc_ref from ProcAgent actors.
    //    ProcAgent full_name = "{proc_id},proc_agent[0]"
    let resp: QueryResponse = fixture
        .post_json_with_retry(
            "/v1/query",
            &QueryRequest {
                sql: "SELECT full_name FROM actors WHERE full_name LIKE '%,proc_agent[0]'"
                    .to_string(),
            },
        )
        .await
        .expect("proc_agent query should succeed");
    let rows = resp.rows.as_array().expect("rows should be an array");
    assert!(!rows.is_empty(), "expected at least one proc_agent actor");
    let full_name = rows[0]["full_name"]
        .as_str()
        .expect("full_name should be a string");
    let proc_ref = full_name
        .strip_suffix(",proc_agent[0]")
        .expect("full_name should end with ,proc_agent[0]");

    // 2. Trigger py-spy dump via /v1/pyspy_dump/{proc_ref}.
    let encoded = urlencoding::encode(proc_ref);
    let pyspy_path = format!("/v1/pyspy_dump/{encoded}");

    let mut dump_id = String::new();
    let resp = fixture
        .post(&pyspy_path, &serde_json::json!(null))
        .await
        .expect("transport should succeed");
    if resp.status().is_success() {
        let body = resp.text().await.unwrap();
        let result: PyspyDumpAndStoreResponse =
            serde_json::from_str(&body).expect("should deserialize as PyspyDumpAndStoreResponse");
        dump_id = result.dump_id;
    }
    assert!(!dump_id.is_empty(), "dump_id should be set");

    // 3. Verify the dump exists in the pyspy_dumps table via SQL.
    let resp: QueryResponse = fixture
        .post_json_with_retry(
            "/v1/query",
            &QueryRequest {
                sql: format!(
                    "SELECT dump_id, proc_ref FROM pyspy_dumps WHERE dump_id = '{dump_id}'"
                ),
            },
        )
        .await
        .expect("pyspy_dumps query should succeed");
    let rows = resp.rows.as_array().expect("rows should be an array");
    assert!(
        !rows.is_empty(),
        "expected dump_id '{dump_id}' in pyspy_dumps table"
    );
    assert_eq!(
        rows[0]["proc_ref"].as_str().unwrap(),
        proc_ref,
        "proc_ref should match the queried proc"
    );

    fixture.shutdown().await;
}

/// MIT-66: `/v1/pyspy_dump/{*proc_reference}` with a bogus proc reference
/// returns a non-success status with a structured error envelope.
pub async fn run_pyspy_dump_bogus_ref() {
    let fixture = start_with_dashboard().await;

    let bogus = "unix:@nonexistent_bogus_socket_xyz,bogus-ffffffffffffffff";
    let encoded = urlencoding::encode(bogus);
    let resp = fixture
        .post(
            &format!("/v1/pyspy_dump/{encoded}"),
            &serde_json::json!(null),
        )
        .await
        .expect("transport should succeed");
    let status = resp.status();
    assert!(
        !status.is_success(),
        "expected error for bogus proc ref, got {}",
        status
    );
    let body = resp.text().await.unwrap();
    let envelope: ApiErrorEnvelope =
        serde_json::from_str(&body).expect("response should be ApiErrorEnvelope");
    assert!(
        !envelope.error.code.is_empty(),
        "error code should be non-empty"
    );
    assert!(
        !envelope.error.message.is_empty(),
        "error message should be non-empty"
    );

    fixture.shutdown().await;
}

/// MIT-68, MIT-69: `/v1/query` and `/v1/pyspy_dump` return 404 when no
/// dashboard is configured.
pub async fn run_no_dashboard_returns_404() {
    let bin = harness::dining_philosophers_python_binary();
    let fixture = harness::start_workload(&bin, &[], Duration::from_secs(60))
        .await
        .expect("failed to start dining_philosophers without --dashboard");

    // MIT-68: POST /v1/query without dashboard → 404.
    let req = QueryRequest {
        sql: "SELECT 1".to_string(),
    };
    let resp = fixture
        .post("/v1/query", &req)
        .await
        .expect("transport should succeed");
    assert_eq!(
        resp.status().as_u16(),
        404,
        "/v1/query without dashboard should return 404, got {}",
        resp.status()
    );
    let body = resp.text().await.unwrap();
    let envelope: ApiErrorEnvelope =
        serde_json::from_str(&body).expect("response should be ApiErrorEnvelope");
    assert_eq!(envelope.error.code, "not_found");

    // MIT-69: POST /v1/pyspy_dump/{ref} without dashboard → 404.
    let encoded = urlencoding::encode("unix:@fake,fake-0000000000000000");
    let resp = fixture
        .post(
            &format!("/v1/pyspy_dump/{encoded}"),
            &serde_json::json!(null),
        )
        .await
        .expect("transport should succeed");
    assert_eq!(
        resp.status().as_u16(),
        404,
        "/v1/pyspy_dump without dashboard should return 404, got {}",
        resp.status()
    );
    let body = resp.text().await.unwrap();
    let envelope: ApiErrorEnvelope =
        serde_json::from_str(&body).expect("response should be ApiErrorEnvelope");
    assert_eq!(envelope.error.code, "not_found");

    fixture.shutdown().await;
}

/// MIT-70: `/v1/query` with malformed JSON body (missing `sql` field)
/// returns a non-success status with an error body.
pub async fn run_query_malformed_body() {
    let fixture = start_with_dashboard().await;

    // Send `{}` — missing the required `sql` field.
    let resp = fixture
        .post("/v1/query", &serde_json::json!({}))
        .await
        .expect("transport should succeed");

    // Axum's Json extractor returns 422 for deserialization errors.
    assert!(
        !resp.status().is_success(),
        "malformed body should return error, got {}",
        resp.status()
    );

    fixture.shutdown().await;
}
