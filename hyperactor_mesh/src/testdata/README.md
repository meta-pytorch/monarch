# testdata — generated API artifacts

These JSON files are **generated** — do not edit by hand.

Snapshot tests in `introspect::tests` compare dynamically derived
schemas against these golden files to detect unintentional API drift.

## Files

- `node_payload_schema.json` — JSON Schema for `NodePayload`
- `admin_info_schema.json` — JSON Schema for `AdminInfo`
- `error_schema.json` — JSON Schema for `ApiErrorEnvelope`
- `openapi.json` — OpenAPI 3.1 spec (embeds all schemas)

## Regenerating

After intentional changes to `NodePayload`, `NodeProperties`,
`FailureInfo`, `ApiError`, `ApiErrorEnvelope`, or `AdminInfo`:

```sh
# Buck
buck run fbcode//monarch/hyperactor_mesh:generate_api_artifacts \
  @fbcode//mode/dev-nosan -- \
  fbcode/monarch/hyperactor_mesh/src/testdata

# Cargo
cargo run -p hyperactor_mesh --bin generate_api_artifacts -- \
  hyperactor_mesh/src/testdata
```

Then re-run tests to confirm the new snapshots pass:

```sh
buck test fbcode//monarch/hyperactor_mesh:hyperactor_mesh-unittest \
  @fbcode//mode/dev-nosan -- introspect::tests
```
