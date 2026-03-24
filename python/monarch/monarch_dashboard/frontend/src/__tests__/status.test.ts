/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import {
  statusColor,
  formatTimestamp,
  formatShape,
  messageStatusColor,
  leafName,
  splitMessages,
} from "../utils/status";

describe("statusColor", () => {
  test("returns distinct color for each ActorStatus variant", () => {
    expect(statusColor("idle")).toBe("var(--status-idle)");
    expect(statusColor("client")).toBe("var(--status-client)");
    expect(statusColor("processing")).toBe("var(--status-processing)");
    expect(statusColor("saving")).toBe("var(--status-saving)");
    expect(statusColor("loading")).toBe("var(--status-loading)");
    expect(statusColor("created")).toBe("var(--status-created)");
    expect(statusColor("initializing")).toBe("var(--status-initializing)");
    expect(statusColor("stopping")).toBe("var(--status-stopping)");
    expect(statusColor("failed")).toBe("var(--status-failed)");
    expect(statusColor("stopped")).toBe("var(--status-stopped)");
    expect(statusColor("unknown")).toBe("var(--status-unknown)");
  });

  test("returns muted for null/undefined", () => {
    expect(statusColor(null)).toBe("var(--text-muted)");
    expect(statusColor(undefined)).toBe("var(--text-muted)");
  });
});

describe("formatTimestamp", () => {
  test("converts microseconds to ISO string", () => {
    // 1700000000000000 us = 1700000000000 ms = 2023-11-14T22:13:20.000Z
    const result = formatTimestamp(1700000000000000);
    expect(result).toContain("2023-11-14");
    expect(result).toContain("22:13:20");
  });

  test("preserves milliseconds", () => {
    const result = formatTimestamp(1700000000500000);
    expect(result).toContain("500");
  });
});

describe("formatShape", () => {
  test("formats ndslice Extent format", () => {
    expect(formatShape('{"inner": {"labels": ["workers", "gpu"], "sizes": [2, 4]}}')).toBe("[2, 4]");
  });

  test("formats single dim ndslice Extent", () => {
    expect(formatShape('{"inner": {"labels": ["replica"], "sizes": [1]}}')).toBe("[1]");
  });

  test("formats legacy dims array", () => {
    expect(formatShape('{"dims": [2, 4]}')).toBe("[2, 4]");
  });

  test("returns raw on invalid JSON", () => {
    expect(formatShape("not json")).toBe("not json");
  });

  test("returns raw when no recognized key", () => {
    expect(formatShape('{"foo": 1}')).toBe('{"foo": 1}');
  });
});

describe("messageStatusColor", () => {
  test("queued is amber", () => {
    expect(messageStatusColor("queued")).toBe("var(--msg-status-queued)");
  });

  test("active is blue", () => {
    expect(messageStatusColor("active")).toBe("var(--msg-status-active)");
  });

  test("complete is green", () => {
    expect(messageStatusColor("complete")).toBe("var(--msg-status-complete)");
  });

  test("unknown status falls back to muted", () => {
    expect(messageStatusColor("failed")).toBe("var(--text-muted)");
    expect(messageStatusColor("delivered")).toBe("var(--text-muted)");
    expect(messageStatusColor("unknown_status")).toBe("var(--text-muted)");
  });
});

describe("leafName", () => {
  test("extracts last segment from slash-separated name", () => {
    expect(leafName("host_mesh_0/proc_mesh_0/Trainer")).toBe("Trainer");
  });

  test("extracts last segment from comma-separated name", () => {
    expect(leafName("host,proc,Trainer")).toBe("Trainer");
  });

  test("handles mixed separators", () => {
    expect(leafName("host/proc,unit")).toBe("unit");
  });

  test("returns dash for null/undefined", () => {
    expect(leafName(null)).toBe("—");
    expect(leafName(undefined)).toBe("—");
  });

  test("returns name as-is when no separators", () => {
    expect(leafName("Trainer")).toBe("Trainer");
  });
});

describe("splitMessages", () => {
  const msgs = [
    { id: 1, from_actor_id: 10, to_actor_id: 20 },
    { id: 2, from_actor_id: 20, to_actor_id: 10 },
    { id: 3, from_actor_id: 10, to_actor_id: 30 },
  ];

  test("splits by actor id", () => {
    const { incoming, outgoing } = splitMessages(msgs, 10);
    expect(incoming).toHaveLength(1);
    expect(incoming[0].id).toBe(2);
    expect(outgoing).toHaveLength(2);
  });

  test("handles string actor ids", () => {
    const { incoming, outgoing } = splitMessages(msgs, "20");
    expect(incoming).toHaveLength(1);
    expect(incoming[0].id).toBe(1);
    expect(outgoing).toHaveLength(1);
    expect(outgoing[0].id).toBe(2);
  });
});
