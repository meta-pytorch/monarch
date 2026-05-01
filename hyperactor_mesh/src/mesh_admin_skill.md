# Mesh Admin API (Introspection)

Base URL: `{base}`

This server exposes a reference-walking introspection API for a mesh.
Start at `root`, resolve it, then follow `children` to traverse topology.

## TLS

In Meta environments, all endpoints require mutual TLS. Every
request needs:
```
--cacert /var/facebook/rootcanal/ca.pem --cert /var/facebook/x509_identities/server.pem --key /var/facebook/x509_identities/server.pem
```

The base URL may show `http://` but the server listens on
`https://`. Always use `https://` with the TLS flags above.

## Contract

The authoritative API contract is machine-readable:

- `GET {base}/v1/openapi.json` — OpenAPI 3.1 spec
- `GET {base}/v1/schema` — JSON Schema for `NodePayload` responses
- `GET {base}/v1/schema/admin` — JSON Schema for `AdminInfo` responses
- `GET {base}/v1/schema/error` — JSON Schema for error responses

Schema is authoritative over prose in this document. Fetch schema
first when building against this API.

## Error handling

Errors return an `ApiErrorEnvelope` JSON body (see error schema).
The `error.code` field is authoritative for programmatic decisions,
not the HTTP status code. Stable codes: `not_found`, `bad_request`,
`gateway_timeout`, `service_unavailable`, `internal_error`.

`service_unavailable` is transient (server at capacity) — retry
with backoff. `gateway_timeout` means a downstream host did not
respond in time; the node may still exist.

## Schema-first workflow

1. Fetch schema: `GET {base}/v1/schema`
2. Fetch root: `GET {base}/v1/root`
3. Follow `children` references via `GET {base}/v1/{url_encode(reference)}`
   (references must be percent-encoded in the URL path)
4. On error: match on `error.code`, not HTTP status
5. References are opaque — round-trip values exactly as received,
   but percent-encode when placing in URL paths

All requests require the TLS flags above.

## One-shot diagnostic (recommended starting point)

Run this first to get a structured JSON health report of the full mesh
(root → hosts → service proc actors → every user proc → user actors).
Exit code 0 = healthy, 1 = any failure.

```
cargo run -p hyperactor_mesh --bin hyperactor_mesh_admin_tui -- \
  --addr {base} --diagnose
```

Each entry in `checks[]` includes `reference` (the exact ref that
failed), `note` (role), `phase` (AdminInfra or Mesh), and `outcome`
(Pass/Slow/Fail with `elapsed_ms` and `error`). Use failing
`reference` values to probe further with the endpoints below.

## Endpoints

Most endpoints are read-only (`GET`). Four endpoints accept `POST`:
`/v1/query` (SQL queries), `/v1/pyspy_dump/{proc_reference}`
(dump-and-store), `/v1/pyspy_profile_svg/{proc_reference}`
(profile → SVG), and `/v1/tools_provision/{proc_reference}`
(install a diagnostic tool on a host's service proc). The companion
`GET /v1/tools/{proc_reference}` returns that host's
diagnostic-tool inventory. All endpoints return `application/json`
except `/SKILL.md` (`text/markdown`) and
`/v1/pyspy_profile_svg/{proc_reference}` (`image/svg+xml`).

Tool provisioning is not a general remote-artifact or remote-filesystem
access path. Use remote mount to expose arbitrary or mutable remote file
trees through the host filesystem view. Use the tool endpoints here to
install small, versioned, digest-pinned diagnostic binaries into a
host-local cache.

- `GET {base}/v1/admin`
  Admin self-identification: returns `AdminInfo` with `actor_id`,
  `proc_id`, `host`, and `url`. Use to verify placement and discover
  the admin's identity.

- `GET {base}/v1/schema`
  JSON Schema for `NodePayload` (authoritative contract).

- `GET {base}/v1/schema/admin`
  JSON Schema for `AdminInfo`.

- `GET {base}/v1/schema/error`
  JSON Schema for error envelope.

- `GET {base}/v1/openapi.json`
  OpenAPI 3.1 spec (embeds JSON Schemas, full response mapping).

- `GET {base}/v1/root`
  Returns the synthetic root `NodePayload`.

- `GET {base}/v1/{reference}`
  Resolves `{reference}` to a JSON `NodePayload`.

- `GET {base}/v1/tree`
  Human-readable ASCII topology dump (convenience endpoint).

- `GET {base}/v1/pyspy/{proc_reference}`
  Requests a py-spy stack dump from the process hosting
  `{proc_reference}`. The reference must be a valid ProcAddr
  (percent-encoded in the URL path). Requires ptrace permissions
  on the target.

  Success returns a `PySpyResult` JSON variant:
  - `{"Ok": {"pid": N, "binary": "...", "stack_traces": [...], "warnings": [...]}}` — structured stack dump
  - `{"BinaryNotFound": {"searched": [...]}}` — no py-spy candidate resolved on the target proc
  - `{"Failed": {"pid": N, "binary": "...", "exit_code": N, "stderr": "..."}}` — py-spy error

  Binary resolution depends on which proc handles the request:
  - **Service proc (HostAgent):** managed candidate from
    `tool_provision`, then `PYSPY_BIN` config attr (if non-empty),
    then `"py-spy"` on `PATH`.
  - **Worker proc (ProcAgent):** `PYSPY_BIN` config attr, then
    `"py-spy"` on `PATH`. Worker procs do not yet consult
    `tool_provision`; only the service proc benefits from managed
    candidates today.

  Recovery when a service-proc request returns `BinaryNotFound`:
  1. `GET /v1/tools/{service_proc_reference}` to inspect the
     host's tool inventory.
  2. `POST /v1/tools_provision/{service_proc_reference}` with a
     `ToolSpec` body. An example py-spy 0.4.1 `ToolSpec` is shown
     under `POST /v1/tools_provision/...` below; callers can
     copy-paste it or adapt the same shape for any other tool.
  3. Retry the original `GET /v1/pyspy/{proc_reference}`.

  Worker-proc `BinaryNotFound` is not addressable through these
  endpoints today: the worker proc has no managed-candidate path.

  The endpoint supports worker procs and the service proc. A
  proc supports py-spy iff its stable handler actor is
  reachable: the service proc requires `host_agent`; non-service
  procs require `proc_agent[0]`. On worker procs, the request is
  handled by ProcAgent. On the service proc (which hosts
  HostAgent instead of ProcAgent), the bridge automatically
  routes to HostAgent. If the target agent is not reachable, an
  immediate `not_found` error is returned instead of waiting for
  the full bridge timeout. If the probe send itself fails (a
  bridge-side infrastructure problem), `internal_error` is
  returned.

  Timeout returns the standard `gateway_timeout` error envelope.

- `POST {base}/v1/pyspy_profile_svg/{proc_reference}`
  Profiles the process for a requested duration and returns an SVG
  flamegraph. POST body is JSON `PySpyProfileOpts`:
  `{"duration_s": 5, "rate_hz": 100, "native": true, "threads": false, "nonblocking": false}`

  Returns `image/svg+xml` on success. Long-running — timeout scales
  with `duration_s`. Max duration is configurable (default 300s).

  Error responses:
  - 400 — invalid `duration_s` or `rate_hz`
  - 404 — proc not found or handler not reachable
  - 503 — py-spy not available on target host
  - 504 — py-spy record subprocess timed out

  Agent note: `{encoded_proc_ref}` is the percent-encoded ProcAddr
  string for the target process. If you save the
  returned SVG on a remote host for browser viewing, tell the user
  the remote file path, the serving port, the exact `ssh -L`
  tunnel command, and the browser URL.

  Example (adapt ports if already in use):
  `curl {TLS} -X POST -H 'Content-Type: application/json' -d '{"duration_s":5,"rate_hz":100,"native":false,"threads":false,"nonblocking":false}' '{base}/v1/pyspy_profile_svg/{encoded_proc_ref}' -o /tmp/profile.svg`
  `cd /tmp && python3 -m http.server 8888 --bind 127.0.0.1`
  User tunnel: `ssh -L <local_port>:127.0.0.1:8888 {host}`
  Browser: `http://localhost:<local_port>/profile.svg`

- `GET {base}/v1/config/{proc_reference}`
  Returns the effective CONFIG-marked configuration entries from the
  process hosting `{proc_reference}`. The reference must be a valid
  ProcAddr (percent-encoded in the URL path).

  Success returns a `ConfigDumpResult` JSON object:
  ```json
  {
    "entries": [
      {
        "name": "hyperactor::config::codec_max_frame_length",
        "value": "1048576",
        "default_value": "1048576",
        "source": "Default",
        "changed_from_default": false,
        "env_var": "HYPERACTOR_CODEC_MAX_FRAME_LENGTH"
      }
    ]
  }
  ```

  Each entry contains:
  - `name` — fully-qualified config key (module_path::key_name)
  - `value` — current resolved value (display string)
  - `default_value` — declared default (null if none)
  - `source` — which layer provided the value: Default,
    ClientOverride, File, Env, Runtime, or TestOverride
  - `changed_from_default` — true when value differs from default
  - `env_var` — environment variable name (null if not env-backed)

  Entries are sorted by `name`. Only CONFIG-marked keys are
  included (not INTROSPECT keys).

  The endpoint supports worker procs and the service proc. Same
  routing as py-spy: ProcAgent for worker procs, HostAgent for the
  service proc. If the target agent is not reachable, an immediate
  `not_found` error is returned. Timeout returns `gateway_timeout`.

  Automated integration test:
  ```
  buck2 test fbcode//monarch/hyperactor_mesh:config_integration_test
  ```

- `POST {base}/v1/query`
  Execute a SQL query to distributed telemetry DataFusion engine.
  Requires `telemetry_url` to be configured.

  Request body (`QueryRequest`):
  ```json
  {"sql": "SELECT * FROM actors LIMIT 10"}
  ```

  Success returns a `QueryResponse`:
  ```json
  {"rows": [ ... ]}
  ```

  `rows` contains the DataFusion result set as a JSON array. On
  invalid SQL or query failure, a non-200 status is returned with
  the dashboard's error message.

  Discover tables with: `SELECT table_name FROM information_schema.tables`.

- `POST {base}/v1/pyspy_dump/{proc_reference}`
  Captures a py-spy stack dump from the process hosting
  `{proc_reference}` and persists the result in the telemetry
  store. The reference must be a valid ProcAddr (percent-encoded
  in the URL path). Requires `telemetry_url` to be configured.

  The endpoint performs two steps:
  1. Sends a `PySpyDump` message to the target proc's agent
     (same routing as `GET /v1/pyspy/{proc_reference}`).
  2. Stores the result in DataFusion via the dashboard, keyed
     by a generated UUID.

  Success returns a `PyspyDumpAndStoreResponse`:
  ```json
  {"dump_id": "550e8400-e29b-41d4-a716-446655440000"}
  ```

  Use `dump_id` to retrieve the stored dump via `/v1/query`:
  ```json
  {"sql": "SELECT * FROM pyspy_dumps WHERE dump_id = '550e8400-...'"}
  ```

  Error handling follows the same conventions as
  `GET /v1/pyspy/{proc_reference}`: `not_found` if the target
  agent is unreachable, `gateway_timeout` on timeout.

- `GET {base}/v1/tools/{proc_reference}`
  Returns the diagnostic-tool inventory for the host that owns
  `{proc_reference}`. The `tool_provision` actor only runs on a
  host's service proc, so non-service references return
  `bad_request`. The reference must be percent-encoded in the URL
  path.

  Success returns a `ToolInventory` JSON object:
  ```json
  {
    "tools": [
      {
        "name": "py-spy",
        "version": "0.4.1",
        "state": {
          "Available": {
            "executable": "<cache_dir>/extracted/<digest_prefix>/<digest>/<exec_path>",
            "artifact_digest": "6a80ec05eb8a6883863a367c6a4d4f2d57de68466f7956b6367d4edd5c61bb29",
            "provisioned_at": "2026-04-29T15:48:16Z"
          }
        }
      }
    ]
  }
  ```

  Each entry's `state` is one of:
  - `Available { executable, artifact_digest, provisioned_at }` — installed and runnable.
  - `Fetching { started_at }` — provision attempt in flight.
  - `Failed { error, last_attempt }` — last attempt failed; resubmit to retry.
  - `CachedButNotRegistered { executable, artifact_digest, provisioned_at }` —
    artifact found by cache scan but no spec has been registered for
    it on this host yet.

  Timestamps are RFC 3339 UTC. If the target agent is not reachable,
  an immediate `not_found` error is returned. Timeout returns
  `gateway_timeout`.

- `POST {base}/v1/tools_provision/{proc_reference}`
  Install one diagnostic tool on the host that owns
  `{proc_reference}`. Service-proc-only; non-service references
  return `bad_request`.

  This endpoint is intentionally narrower than remote mount. Remote
  mount is the right layer for arbitrary or mutable remote trees and
  large artifacts you do not want to materialize eagerly. Tool
  provisioning is for small diagnostic executables whose exact version
  and sha256 digest must be pinned and verified before use.

  Request body is a `ToolSpec` JSON object — tool name, version,
  and a per-platform map of artifact entries (provider URL,
  format, hash algorithm, digest, byte size, and for archive
  formats the executable path inside the archive).

  Example `ToolSpec` for py-spy 0.4.1, shown to give the JSON shape
  concrete form. This is illustrative, not authoritative — agents
  should treat it as a starting template and confirm digests, sizes,
  and provider URLs against their own source before using it for a
  production install. The version, URLs, and hashes here will drift
  over time. (Rust consumers can obtain a live spec via
  `tool_fetch::bundled_pyspy_spec()`.)
  ```json
  {
    "name": "py-spy",
    "version": "0.4.1",
    "platforms": {
      "macos-aarch64": {
        "size": 1796395,
        "hash_algorithm": "sha256",
        "digest": "1fb8bf71ab8df95a95cc387deed6552934c50feef2cf6456bc06692a5508fd0c",
        "format": "zip",
        "executable_path": "py_spy-0.4.1.data/scripts/py-spy",
        "providers": [
          {
            "Http": {
              "url": "https://files.pythonhosted.org/packages/4f/bf/e4d280e9e0bec71d39fc646654097027d4bbe8e04af18fb68e49afcff404/py_spy-0.4.1-py2.py3-none-macosx_11_0_arm64.whl"
            }
          }
        ]
      },
      "linux-x86_64": {
        "size": 2763338,
        "hash_algorithm": "sha256",
        "digest": "6a80ec05eb8a6883863a367c6a4d4f2d57de68466f7956b6367d4edd5c61bb29",
        "format": "zip",
        "executable_path": "py_spy-0.4.1.data/scripts/py-spy",
        "providers": [
          {
            "Http": {
              "url": "https://files.pythonhosted.org/packages/68/fb/bc7f639aed026bca6e7beb1e33f6951e16b7d315594e7635a4f7d21d63f4/py_spy-0.4.1-py2.py3-none-manylinux_2_5_x86_64.manylinux1_x86_64.whl"
            }
          }
        ]
      }
    }
  }
  ```

  Success returns a `ProvisionResult` JSON variant:
  - `{"Available": {"name": "py-spy", "version": "0.4.1", "executable": "...", "artifact_digest": "..."}}`
  - `{"Failed": {"name": "...", "version": "...", "error": "..."}}`

  Provisioning is idempotent and cache-aware: re-issuing the same
  spec after success is a fast no-op (size + hash reverification,
  no re-download). Failures register a `Failed` state visible
  through `/v1/tools/{...}`. Long-running — extraction of large
  archives can take seconds. Timeout returns `gateway_timeout`.

  Example (provision py-spy 0.4.1 by piping the spec above through
  curl; the heredoc keeps the example self-contained):
  ```
  curl {TLS} -X POST -H 'Content-Type: application/json' \
    --data @- \
    '{base}/v1/tools_provision/{encoded_service_proc_reference}' <<'EOF'
  {
    "name": "py-spy",
    "version": "0.4.1",
    "platforms": { ... }
  }
  EOF
  ```
  Replace the truncated `platforms` value with the block from the
  example spec above before invoking.

- `GET {base}/SKILL.md`
  This document.

## Response: NodePayload

Successful resolves return a JSON object:

- `identity` — the resolved reference string (opaque; round-trip it exactly)
- `properties` — externally-tagged variant, one of:
  `{"Root": {...}}`, `{"Host": {...}}`, `{"Proc": {...}}`,
  `{"Actor": {...}}`, `{"Error": {...}}`
- `children` — list of reference strings to resolve next
- `parent` — optional parent reference (navigation context)
- `as_of` — ISO 8601 timestamp of when this data was captured

Each child reference can be resolved via `/v1/{reference}` (URL-encode first).
Clients should treat reference strings as opaque tokens.

## Key fields

**`actor_status`** (Actor variant): lifecycle state of the actor.
Values: `running` (processing messages), `idle` (waiting for
messages), `stopped` / `stopped: <reason>`, `failed` /
`failed: <reason>`.

**`system_children`** (Root, Host, Proc variants): infrastructure
actors that are part of the mesh framework (proc_agent, comm,
logger, etc.), not user workloads. When debugging user actors,
filter `children` to exclude entries that also appear in
`system_children`.

**`flight_recorder`** (Actor variant): JSON-encoded string
containing recent trace spans. Can be large (tens of KB).
Exclude it when summarizing topology. Parse as JSON if
trace-level debugging is needed. Filter with:
`jq '{identity, properties: {Actor: (.properties.Actor | del(.flight_recorder))}, children}'`

**`failure_info`** (Actor variant): present only when
`actor_status` starts with `failed`. Contains `error_message`,
`root_cause_actor`, `occurred_at`, and `is_propagated`.

## Navigation algorithm

1. Fetch root:
   `curl --cacert /var/facebook/rootcanal/ca.pem --cert /var/facebook/x509_identities/server.pem --key /var/facebook/x509_identities/server.pem '{base}/v1/root'`

2. Select a child reference:
   `curl --cacert /var/facebook/rootcanal/ca.pem --cert /var/facebook/x509_identities/server.pem --key /var/facebook/x509_identities/server.pem '{base}/v1/{child_reference}'`

3. Repeat. Each node describes its next traversal step.

Notes:

- References must be URL-encoded when used in paths.
- The topology is eventually consistent; nodes may disappear between requests.
  Treat HTTP 404 as "node not found" and continue traversal.

## Reference strings

References are opaque identifiers. Clients must not parse them.
Always round-trip references exactly as returned in `children`.

Common examples include:

- `root` — synthetic entrypoint
- Actor references (`ActorAddr`)
- Proc references (`ProcAddr`)

## Examples

List root children:

`curl --cacert /var/facebook/rootcanal/ca.pem --cert /var/facebook/x509_identities/server.pem --key /var/facebook/x509_identities/server.pem '{base}/v1/root' | jq -r '.children[]'`

Resolve a child (URL-encoded):

`curl --cacert /var/facebook/rootcanal/ca.pem --cert /var/facebook/x509_identities/server.pem --key /var/facebook/x509_identities/server.pem '{base}/v1/'$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" '{example_ref}')`

Actor details without flight recorder noise:

`curl --cacert /var/facebook/rootcanal/ca.pem --cert /var/facebook/x509_identities/server.pem --key /var/facebook/x509_identities/server.pem '{base}/v1/{reference}' | jq '{identity, properties: {Actor: (.properties.Actor | del(.flight_recorder))}, children}'`

## Agent self-test

Run these checks against a live server to verify correct API
usage. Each check is binary (1 = pass, 0 = fail). Report total
score and per-check results. Track scores across sessions to
measure improvement or regression.

**Contract discovery (2 pts)**

1. `GET {base}/v1/schema` returns 200 with valid JSON containing
   a `$id` field with "v1" in the value.
2. `GET {base}/v1/schema/error` returns 200 with valid JSON
   containing a `$id` field.

**Navigation (3 pts)**

3. `GET {base}/v1/root` returns 200 with `properties` key ==
   "Root" and non-empty `children`.
4. Pick first child from root. URL-encode it. `GET
   {base}/v1/{encoded_child}` returns 200 with `properties`
   key in {Root, Host, Proc, Actor, Error}.
5. The child response has `parent` set (non-null).

**Schema conformance (2 pts)**

6. The root response from check 3 validates against the schema
   from check 1 (parse schema as JSON Schema Draft 2020-12,
   validate response structure).
7. The child response from check 4 also validates against the
   schema from check 1.

**Error handling (2 pts)**

8. `GET {base}/v1/nonexistent_ref_xxxx` returns a JSON body
   with top-level `error` object containing `code` field.
9. The `error.code` value is exactly `"not_found"`.

**Security boundary (1 pt)**

10. Across all responses collected in checks 3, 4, 8: no
    response body contains substrings `PortId`, `PortRef`,
    `MailboxSender`, or `ChannelTransport`.

### Scoring

- **10/10:** Full competence. Agent discovers, navigates,
  validates, and handles errors correctly.
- **8-9/10:** Functional with gaps. Review failing checks.
- **<8/10:** Re-read this document from the top.

### Tracking

Record: `{timestamp, score, failing_checks[], server_version}`.
Compare across sessions. A score regression after a SKILL.md
change means the edit made the document harder to follow. A
score regression after a server change means the API or schema
drifted. Use the schema `$id` to correlate.

## py-spy validation

Automated integration test (runs all three modes — cpu, block,
mixed — sequentially):

```
buck2 test fbcode//monarch/hyperactor_mesh:pyspy_integration_test
```

Manual verification against a live mesh:

1. Start the py-spy workload:

```
buck2 run fbcode//monarch/python/examples:pyspy_workload -- \
  --mode cpu --work-ms 500 --concurrency 3
```

2. Run the verification script (exit codes: 0 PASS, 1 FAIL,
   2 SKIP when py-spy is unavailable):

```
buck2 run fbcode//monarch/python/examples:verify_pyspy -- \
  --admin-url <url> --mode cpu --samples 10 \
  --cacert /var/facebook/rootcanal/ca.pem \
  --cert /var/facebook/x509_identities/server.pem \
  --key /var/facebook/x509_identities/server.pem
```

Modes: `cpu` (iterative CPU burn), `block` (blocking sleep),
`mixed` (alternating CPU + async). The verifier checks for
mode-specific evidence frames in py-spy stacks.
