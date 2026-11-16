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
import userEvent from "@testing-library/user-event";
import { ActorDetail } from "../src/components/hierarchy/ActorDetail";
import "./dom";

test("actor detail shows the actor's metrics, logs and message traces", async () => {
  const responses: Record<string, unknown> = {
    "/api/actors/1": {
      id: "1", timestamp_us: 0, mesh_id: "2", rank: 0, full_name: "w.worker[0]@p",
    },
    "/api/actors/1/logs": [
      { timestamp_us: 1, level: "WARN", target: "log_events", message: "handled step", message_id: "5" },
    ],
    "/api/actors/1/messages": [
      { id: "10", timestamp_us: 0, from_actor_id: "3", to_actor_id: "1", endpoint: "step", latest_status: "complete" },
    ],
    "/api/messages/10/trace": {
      spans: [
        { timestamp_us: 1, name: "call step", actor: "c.caller[0]@p", message_id: null, correlation_id: "77" },
        { timestamp_us: 2, name: "step", actor: "w.worker[0]@p", message_id: "10", correlation_id: null },
      ],
      logs: [{ timestamp_us: 3, level: "WARN", actor: "w.worker[0]@p", message: "inside step" }],
    },
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

    await userEvent.setup().click(await screen.findByText("step"));
    assert.ok(await screen.findByText("call step"));
    assert.ok(await screen.findByText("caller span"));
    assert.ok(await screen.findByText("inside step"));
  } finally {
    globalThis.fetch = originalFetch;
  }
});
