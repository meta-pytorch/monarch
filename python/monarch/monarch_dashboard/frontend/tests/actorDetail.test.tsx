/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { render, screen } from "@testing-library/react";
import { ActorDetail } from "../src/components/hierarchy/ActorDetail";
import "./dom";

test("actor detail shows the actor's metrics and logs", async () => {
  const responses: Record<string, unknown> = {
    "/api/actors/1": {
      id: "1", timestamp_us: 0, mesh_id: "2", rank: 0, full_name: "w.worker[0]@p",
    },
    "/api/actors/1/logs": [
      { timestamp_us: 1, level: "WARN", target: "log_events", message: "handled step", message_id: "5" },
    ],
    "/api/actors/1/metrics": [
      { name: "actor.message_handler_duration.ns", unit: "ns", kind: "histogram", count: 307, total: 13910876 },
    ],
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input) =>
    new Response(JSON.stringify(responses[String(input)] ?? []));
  try {
    render(<ActorDetail actorId="1" />);
    assert.ok(await screen.findByText("actor.message_handler_duration.ns"));
    assert.ok(await screen.findByText("Handler time"));
    assert.ok(await screen.findByText("307 samples · avg 45.3 µs · total 13.9 ms"));
    assert.ok(await screen.findByText("handled step"));
  } finally {
    globalThis.fetch = originalFetch;
  }
});
