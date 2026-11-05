/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ExecutionsPanel } from "../src/components/overview/ExecutionsPanel";
import "./dom";

test("execution history and details tolerate an unfamiliar server status", async () => {
  const execution = {
    id: "0123456789abcdef0123456789abcdef",
    command: "python workload.py",
    status: "queued",
    started_us: 1_000_000,
    finished_us: null,
    returncode: null,
    rank_count: 0,
  };
  const page = {
    available: true,
    counts: { queued: 1 },
    active: [],
    history: [execution],
    next_before: null,
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input) => new Response(JSON.stringify(
    String(input).startsWith("/api/executions?")
      ? page
      : { ...execution, ranks: [] },
  ));
  const user = userEvent.setup();
  const view = render(<ExecutionsPanel compact />);
  try {
    const row = await screen.findByRole("button", { name: /Unknown.*python workload.py/ });
    const badge = within(row).getByText("Unknown");
    assert.equal(badge.style.getPropertyValue("--execution-color"), "var(--st-unknown)");

    await user.click(row);
    const drawer = screen.getByRole("dialog", { name: "Execution details" });
    const detailBadge = await within(drawer).findByText("Unknown");
    assert.equal(detailBadge.style.getPropertyValue("--execution-color"), "var(--st-unknown)");
  } finally {
    view.unmount();
    globalThis.fetch = originalFetch;
  }
});

test("command rows expose result context and keyboard opens static fault details", async () => {
  const started = Date.UTC(2026, 10, 2, 12) * 1000;
  const execution = {
    id: "abcdef0123456789abcdef0123456789",
    command: "python crash.py",
    status: "failed",
    started_us: started,
    finished_us: started + 1_000_000,
    returncode: 7,
    rank_count: 0,
    runtime_faults: 1,
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input) => new Response(JSON.stringify(
    String(input).startsWith("/api/executions?")
      ? { available: true, counts: { failed: 1 }, active: [], history: [execution], next_before: null }
      : { ...execution, ranks: [], runtime_actor_failures: [] },
  ));
  const user = userEvent.setup();
  const view = render(<ExecutionsPanel compact />);
  try {
    const row = await screen.findByRole("button", {
      name: /Failed.*python crash.py.*Runtime fault.*2026.*1\.0s.*exit 7/,
    });
    row.focus();
    await user.keyboard("{Enter}");
    const drawer = screen.getByRole("dialog", { name: "Execution details" });
    await within(drawer).findByText("1 runtime actor failure");
    assert.equal(within(drawer).queryByRole("alert"), null);
    await user.keyboard("{Escape}");
    assert.equal(screen.queryByRole("dialog"), null);
    assert.equal(document.activeElement, row);
  } finally {
    view.unmount();
    globalThis.fetch = originalFetch;
  }
});
