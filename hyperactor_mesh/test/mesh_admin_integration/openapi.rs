/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! OpenAPI conformance assertion helpers.
//!
//! Validates live HTTP responses against the published OpenAPI 3.1
//! spec served at `/v1/openapi.json`.
//!
//! See MIT-37 through MIT-52 in `main` module doc.

use serde_json::Value;

use crate::dining::DiningScenario;

/// Validates HTTP responses against the published OpenAPI 3.1 spec.
struct OpenApiValidator {
    doc: Value,
    /// Pre-extracted `components/schemas` map for compilation.
    schemas: serde_json::Map<String, Value>,
}

impl OpenApiValidator {
    fn new(doc: Value) -> Self {
        let schemas = doc
            .pointer("/components/schemas")
            .and_then(|v| v.as_object())
            .expect("components/schemas must be an object")
            .clone();
        Self { doc, schemas }
    }

    /// MIT-37: parseable JSON with required top-level structure.
    fn check_document_structure(&self) {
        assert_eq!(
            self.doc.get("openapi").and_then(|v| v.as_str()),
            Some("3.1.0"),
            "MIT-37: openapi version must be 3.1.0"
        );
        assert!(self.doc.get("info").is_some(), "MIT-37: missing info");
        assert!(self.doc.get("paths").is_some(), "MIT-37: missing paths");
        assert!(
            !self.schemas.is_empty(),
            "MIT-37: components/schemas is empty"
        );
    }

    /// MIT-38: all `$ref` targets resolve within the document.
    fn check_all_refs_resolve(&self) {
        let mut refs = Vec::new();
        collect_refs(&self.doc, &mut refs);
        for r in &refs {
            assert!(
                resolve_pointer(&self.doc, r).is_some(),
                "MIT-38: unresolvable $ref: {r}"
            );
        }
    }

    /// MIT-39: expected client-facing routes are present in `paths`.
    fn check_routes_covered(&self) {
        let paths = self.doc.get("paths").and_then(|v| v.as_object()).unwrap();
        for route in [
            "/v1/root",
            "/v1/{reference}",
            "/v1/config/{proc_reference}",
            "/v1/pyspy/{proc_reference}",
            "/v1/tree",
            "/v1/schema",
            "/v1/schema/error",
        ] {
            assert!(paths.contains_key(route), "MIT-39: missing path: {route}");
        }
    }

    /// MIT-40: every `components/schemas` entry compiles when
    /// embedded in a synthetic document with sibling schemas as
    /// `$defs`.
    fn check_schemas_compile(&self) {
        let defs = Value::Object(self.schemas.clone());
        for (name, schema) in &self.schemas {
            let mut standalone = schema.clone();
            if let Some(obj) = standalone.as_object_mut() {
                obj.insert("$defs".into(), defs.clone());
            }
            rewrite_component_refs_to_defs(&mut standalone);
            let result = jsonschema::JSONSchema::compile(&standalone);
            assert!(
                result.is_ok(),
                "MIT-40: schema {name} failed to compile: {:?}",
                result.err()
            );
        }
    }

    /// MIT-48: path parameters match declared contract (type: string,
    /// required: true).
    fn check_path_params(&self) {
        for path in [
            "/v1/{reference}",
            "/v1/config/{proc_reference}",
            "/v1/pyspy/{proc_reference}",
        ] {
            let params = self
                .doc
                .pointer(&format!(
                    "/paths/{}/get/parameters",
                    escape_pointer_segment(path)
                ))
                .and_then(|v| v.as_array())
                .unwrap_or_else(|| panic!("MIT-48: no parameters for {path}"));
            for p in params {
                assert_eq!(
                    p.get("in").and_then(|v| v.as_str()),
                    Some("path"),
                    "MIT-48: {path}: param 'in' must be 'path'"
                );
                assert_eq!(
                    p.get("required").and_then(|v| v.as_bool()),
                    Some(true),
                    "MIT-48: {path}: param must be required"
                );
                assert_eq!(
                    p.pointer("/schema/type").and_then(|v| v.as_str()),
                    Some("string"),
                    "MIT-48: {path}: param schema type must be string"
                );
            }
        }
    }

    /// Compile the response schema for an operation + status code.
    ///
    /// Resolves `$ref` if the schema is a reference, injects all
    /// component schemas as `$defs`, rewrites refs, and compiles.
    fn compile_response_schema(&self, path: &str, status: u16) -> jsonschema::JSONSchema {
        let pointer = format!(
            "/paths/{}/get/responses/{}/content/application~1json/schema",
            escape_pointer_segment(path),
            status
        );
        let schema_ref = self
            .doc
            .pointer(&pointer)
            .unwrap_or_else(|| panic!("no schema at {pointer}"));

        // Resolve top-level $ref if present.
        let base = if let Some(r) = schema_ref.get("$ref").and_then(|v| v.as_str()) {
            resolve_pointer(&self.doc, r)
                .unwrap_or_else(|| panic!("unresolvable $ref: {r}"))
                .clone()
        } else {
            schema_ref.clone()
        };

        // Inject all component schemas as $defs and rewrite refs.
        let mut standalone = base;
        if let Some(obj) = standalone.as_object_mut() {
            obj.insert("$defs".into(), Value::Object(self.schemas.clone()));
        }
        rewrite_component_refs_to_defs(&mut standalone);

        jsonschema::JSONSchema::compile(&standalone)
            .unwrap_or_else(|e| panic!("compile_response_schema({path}, {status}): {e}"))
    }

    /// MIT-41/MIT-43: check status code is documented for the
    /// operation.
    fn check_status_documented(&self, path: &str, status: u16, mit: &str) {
        let pointer = format!("/paths/{}/get/responses", escape_pointer_segment(path));
        let responses = self
            .doc
            .pointer(&pointer)
            .and_then(|v| v.as_object())
            .unwrap_or_else(|| panic!("{mit}: no responses for {path}"));
        assert!(
            responses.contains_key(&status.to_string()),
            "{mit}: status {status} not documented for {path}"
        );
    }

    /// MIT-42: validate a success response body against the
    /// operation's response schema.
    fn validate_success(&self, path: &str, status: u16, body: &Value) {
        let schema = self.compile_response_schema(path, status);
        assert!(
            schema.is_valid(body),
            "MIT-42: {path} {status}: response body does not match schema"
        );
    }

    /// MIT-44: validate an error response body against the
    /// operation's error response schema.
    fn validate_error(&self, path: &str, status: u16, body: &Value) {
        let schema = self.compile_response_schema(path, status);
        assert!(
            schema.is_valid(body),
            "MIT-44: {path} {status}: error body does not match schema"
        );
    }
}

// --- helpers ---

/// Recursively collect all `$ref` strings from a JSON value.
fn collect_refs(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(r)) = map.get("$ref") {
                out.push(r.clone());
            }
            for v in map.values() {
                collect_refs(v, out);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                collect_refs(v, out);
            }
        }
        _ => {}
    }
}

/// Resolve a JSON Pointer like `#/components/schemas/Foo`.
fn resolve_pointer<'a>(doc: &'a Value, pointer: &str) -> Option<&'a Value> {
    let path = pointer.strip_prefix('#')?;
    doc.pointer(path)
}

/// Escape a string for use as a single JSON Pointer segment
/// (RFC 6901): `~` → `~0`, `/` → `~1`.
fn escape_pointer_segment(s: &str) -> String {
    s.replace('~', "~0").replace('/', "~1")
}

/// Rewrite `#/components/schemas/X` → `#/$defs/X` in-place.
fn rewrite_component_refs_to_defs(value: &mut Value) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(r)) = map.get_mut("$ref") {
                if let Some(suffix) = r.strip_prefix("#/components/schemas/") {
                    *r = format!("#/$defs/{suffix}");
                }
            }
            for v in map.values_mut() {
                rewrite_component_refs_to_defs(v);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                rewrite_component_refs_to_defs(v);
            }
        }
        _ => {}
    }
}

/// Extract the content-type header as a string.
fn content_type(resp: &reqwest::Response) -> String {
    resp.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

/// Assert that a content-type header's media type matches the
/// expected value. Extracts the token before `;` (if any) and
/// compares case-insensitively, so `application/json; charset=utf-8`
/// is conformant for `application/json` but `x-application/json-bad`
/// is not.
fn assert_content_type(ct: &str, expected: &str, mit: &str, endpoint: &str) {
    let media_type = ct.split(';').next().unwrap_or("").trim();
    assert!(
        media_type.eq_ignore_ascii_case(expected),
        "{mit}: {endpoint}: expected media type {expected}, got {ct}"
    );
}

/// MIT-37 through MIT-52: OpenAPI conformance checks.
pub(crate) async fn check(s: &DiningScenario) {
    // --- Fetch live spec (MIT-37, MIT-39 liveness of /v1/openapi.json) ---
    let resp = s.fixture.get("/v1/openapi.json").await.unwrap();
    assert!(
        resp.status().is_success(),
        "MIT-37: /v1/openapi.json returned {}",
        resp.status()
    );
    assert_content_type(
        &content_type(&resp),
        "application/json",
        "MIT-51",
        "/v1/openapi.json",
    );
    let doc: Value = resp.json().await.unwrap();
    let v = OpenApiValidator::new(doc);

    // --- Static checks ---
    v.check_document_structure(); // MIT-37
    v.check_all_refs_resolve(); // MIT-38
    v.check_routes_covered(); // MIT-39
    v.check_schemas_compile(); // MIT-40
    v.check_path_params(); // MIT-48

    // --- Success responses ---

    // /v1/root — MIT-41, MIT-42, MIT-51
    let resp = s.fixture.get("/v1/root").await.unwrap();
    let status = resp.status().as_u16();
    assert_content_type(
        &content_type(&resp),
        "application/json",
        "MIT-51",
        "/v1/root",
    );
    v.check_status_documented("/v1/root", status, "MIT-41");
    let body: Value = resp.json().await.unwrap();
    v.validate_success("/v1/root", status, &body);

    // /v1/{reference} — host — MIT-41, MIT-42, MIT-51
    let host_ref = body["children"][0]
        .as_str()
        .expect("root must have at least one child");
    let encoded = urlencoding::encode(host_ref);
    let resp = s.fixture.get(&format!("/v1/{encoded}")).await.unwrap();
    let status = resp.status().as_u16();
    assert_content_type(
        &content_type(&resp),
        "application/json",
        "MIT-51",
        "/v1/{ref} host",
    );
    v.check_status_documented("/v1/{reference}", status, "MIT-41");
    let body: Value = resp.json().await.unwrap();
    v.validate_success("/v1/{reference}", status, &body);

    // /v1/{reference} — proc — MIT-41, MIT-42, MIT-51
    let encoded = urlencoding::encode(&s.worker);
    let resp = s.fixture.get(&format!("/v1/{encoded}")).await.unwrap();
    let status = resp.status().as_u16();
    assert_content_type(
        &content_type(&resp),
        "application/json",
        "MIT-51",
        "/v1/{ref} proc",
    );
    v.check_status_documented("/v1/{reference}", status, "MIT-41");
    let body: Value = resp.json().await.unwrap();
    v.validate_success("/v1/{reference}", status, &body);

    // /v1/{reference} — actor (first child of worker proc) — MIT-41,
    // MIT-42, MIT-51
    let actor_ref = body["children"][0]
        .as_str()
        .expect("proc must have at least one child");
    let encoded = urlencoding::encode(actor_ref);
    let resp = s.fixture.get(&format!("/v1/{encoded}")).await.unwrap();
    let status = resp.status().as_u16();
    assert_content_type(
        &content_type(&resp),
        "application/json",
        "MIT-51",
        "/v1/{ref} actor",
    );
    v.check_status_documented("/v1/{reference}", status, "MIT-41");
    let body: Value = resp.json().await.unwrap();
    v.validate_success("/v1/{reference}", status, &body);

    // /v1/config/{proc_reference} — MIT-41, MIT-42, MIT-51
    let encoded = urlencoding::encode(&s.worker);
    let resp = s
        .fixture
        .get(&format!("/v1/config/{encoded}"))
        .await
        .unwrap();
    let status = resp.status().as_u16();
    assert_content_type(
        &content_type(&resp),
        "application/json",
        "MIT-51",
        "/v1/config",
    );
    v.check_status_documented("/v1/config/{proc_reference}", status, "MIT-41");
    let body: Value = resp.json().await.unwrap();
    v.validate_success("/v1/config/{proc_reference}", status, &body);

    // /v1/tree — MIT-41, MIT-51 (text/plain, no JSON body to
    // validate)
    let resp = s.fixture.get("/v1/tree").await.unwrap();
    let status = resp.status().as_u16();
    assert_content_type(&content_type(&resp), "text/plain", "MIT-51", "/v1/tree");
    v.check_status_documented("/v1/tree", status, "MIT-41");

    // --- Error responses ---

    // /v1/{reference} — garbage ref (404) — MIT-43, MIT-44, MIT-46,
    // MIT-52
    let resp = s.fixture.get("/v1/xyzzy").await.unwrap();
    let status = resp.status().as_u16();
    assert!(!resp.status().is_success());
    assert_content_type(
        &content_type(&resp),
        "application/json",
        "MIT-52",
        "/v1/{ref} error",
    );
    v.check_status_documented("/v1/{reference}", status, "MIT-43");
    let body_text = resp.text().await.unwrap();
    let body: Value = serde_json::from_str(&body_text)
        .unwrap_or_else(|e| panic!("MIT-44: not JSON: {e}: {body_text}"));
    v.validate_error("/v1/{reference}", status, &body);
    // MIT-46: error envelope shape
    assert!(
        body["error"]["code"].is_string(),
        "MIT-46: error.code must be string"
    );
    assert!(
        body["error"]["message"].is_string(),
        "MIT-46: error.message must be string"
    );

    // /v1/config/{proc_reference} — bogus proc — MIT-43, MIT-44,
    // MIT-46, MIT-52
    let bogus = "unix:@nonexistent_bogus_socket_xyz,bogus-ffffffffffffffff";
    let encoded = urlencoding::encode(bogus);
    let resp = s
        .fixture
        .get(&format!("/v1/config/{encoded}"))
        .await
        .unwrap();
    let status = resp.status().as_u16();
    assert!(!resp.status().is_success());
    assert_content_type(
        &content_type(&resp),
        "application/json",
        "MIT-52",
        "/v1/config error",
    );
    v.check_status_documented("/v1/config/{proc_reference}", status, "MIT-43");
    let body_text = resp.text().await.unwrap();
    let body: Value = serde_json::from_str(&body_text)
        .unwrap_or_else(|e| panic!("MIT-44: not JSON: {e}: {body_text}"));
    v.validate_error("/v1/config/{proc_reference}", status, &body);
    assert!(
        body["error"]["code"].is_string(),
        "MIT-46: error.code must be string"
    );
    assert!(
        body["error"]["message"].is_string(),
        "MIT-46: error.message must be string"
    );

    // --- Parameters and encoding ---

    // MIT-49: double-encoded ref resolves correctly.
    let single = urlencoding::encode(&s.worker);
    let double = urlencoding::encode(&single);
    let resp = s.fixture.get(&format!("/v1/{double}")).await.unwrap();
    let status = resp.status().as_u16();
    v.check_status_documented("/v1/{reference}", status, "MIT-41");
    let body: Value = resp.json().await.unwrap();
    v.validate_success("/v1/{reference}", status, &body);

    // MIT-50: truncated ref returns documented error.
    let truncated = &s.worker[..s.worker.len() / 2];
    let encoded = urlencoding::encode(truncated);
    let resp = s.fixture.get(&format!("/v1/{encoded}")).await.unwrap();
    let status = resp.status().as_u16();
    assert!(!resp.status().is_success());
    assert_content_type(
        &content_type(&resp),
        "application/json",
        "MIT-52",
        "/v1/{ref} truncated error",
    );
    v.check_status_documented("/v1/{reference}", status, "MIT-43");
    let body_text = resp.text().await.unwrap();
    let body: Value = serde_json::from_str(&body_text)
        .unwrap_or_else(|e| panic!("MIT-44: not JSON: {e}: {body_text}"));
    v.validate_error("/v1/{reference}", status, &body);

    // --- py-spy endpoint conformance (MIT-55 through MIT-62) ---

    // MIT-55, MIT-56, MIT-62: live-response conformance on a valid
    // proc reference. The dining_philosophers worker is a Rust
    // process, so py-spy will return BinaryNotFound or Failed, both
    // valid PySpyResult variants at HTTP 200. If the endpoint returns
    // a documented non-success instead (e.g., 504 timeout), we
    // validate it as an error response.
    let encoded = urlencoding::encode(&s.worker);
    let resp = s
        .fixture
        .get(&format!("/v1/pyspy/{encoded}"))
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let ct = content_type(&resp);
    if resp.status().is_success() {
        assert_content_type(&ct, "application/json", "MIT-62", "/v1/pyspy success");
        v.check_status_documented("/v1/pyspy/{proc_reference}", status, "MIT-55");
        let body: Value = resp.json().await.unwrap();
        v.validate_success("/v1/pyspy/{proc_reference}", status, &body);
    } else {
        assert_content_type(
            &ct,
            "application/json",
            "MIT-62",
            "/v1/pyspy error-fallback",
        );
        v.check_status_documented("/v1/pyspy/{proc_reference}", status, "MIT-57");
        let body_text = resp.text().await.unwrap();
        let body: Value = serde_json::from_str(&body_text)
            .unwrap_or_else(|e| panic!("MIT-58: not JSON: {e}: {body_text}"));
        v.validate_error("/v1/pyspy/{proc_reference}", status, &body);
    }

    // MIT-57, MIT-58, MIT-59, MIT-61, MIT-62: error path — bogus proc
    // ref.
    let bogus = "unix:@nonexistent_bogus_socket_xyz,bogus-ffffffffffffffff";
    let encoded = urlencoding::encode(bogus);
    let resp = s
        .fixture
        .get(&format!("/v1/pyspy/{encoded}"))
        .await
        .unwrap();
    let status = resp.status().as_u16();
    assert!(!resp.status().is_success());
    assert_content_type(
        &content_type(&resp),
        "application/json",
        "MIT-62",
        "/v1/pyspy error",
    );
    v.check_status_documented("/v1/pyspy/{proc_reference}", status, "MIT-57");
    let body_text = resp.text().await.unwrap();
    let body: Value = serde_json::from_str(&body_text)
        .unwrap_or_else(|e| panic!("MIT-58: not JSON: {e}: {body_text}"));
    v.validate_error("/v1/pyspy/{proc_reference}", status, &body);
    assert!(
        body["error"]["code"].is_string(),
        "MIT-59: error.code must be string"
    );
    assert!(
        body["error"]["message"].is_string(),
        "MIT-59: error.message must be string"
    );
}
