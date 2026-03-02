/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import {
  statusColor,
  worstStatus,
  formatTimestamp,
  formatShape,
  messageStatusColor,
} from "../utils/status";

describe("statusColor", () => {
  test("returns green for idle", () => {
    expect(statusColor("idle")).toBe("var(--status-healthy)");
  });

  test("returns blue for processing", () => {
    expect(statusColor("processing")).toBe("var(--status-processing)");
  });

  test("returns amber for transitional statuses", () => {
    for (const s of ["created", "initializing", "saving", "loading", "stopping"]) {
      expect(statusColor(s)).toBe("var(--status-transitional)");
    }
  });

  test("returns red for failed", () => {
    expect(statusColor("failed")).toBe("var(--status-failed)");
  });

  test("returns gray for stopped", () => {
    expect(statusColor("stopped")).toBe("var(--status-stopped)");
  });

  test("returns muted for null/undefined", () => {
    expect(statusColor(null)).toBe("var(--text-muted)");
    expect(statusColor(undefined)).toBe("var(--text-muted)");
  });
});

describe("worstStatus", () => {
  test("returns failed when present", () => {
    expect(worstStatus(["idle", "processing", "failed"])).toBe("failed");
  });

  test("returns stopped over idle", () => {
    expect(worstStatus(["idle", "stopped"])).toBe("stopped");
  });

  test("returns idle for single idle", () => {
    expect(worstStatus(["idle"])).toBe("idle");
  });

  test("handles nulls gracefully", () => {
    expect(worstStatus([null, "idle", undefined])).toBe("idle");
  });

  test("returns idle for empty array", () => {
    expect(worstStatus([])).toBe("idle");
  });

  test("stopping is worse than processing", () => {
    expect(worstStatus(["processing", "stopping"])).toBe("stopping");
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
  test("formats dims array", () => {
    expect(formatShape('{"dims": [2, 4]}')).toBe("[2, 4]");
  });

  test("formats single dim", () => {
    expect(formatShape('{"dims": [1]}')).toBe("[1]");
  });

  test("returns raw on invalid JSON", () => {
    expect(formatShape("not json")).toBe("not json");
  });

  test("returns raw when no dims key", () => {
    expect(formatShape('{"foo": 1}')).toBe('{"foo": 1}');
  });
});

describe("messageStatusColor", () => {
  test("delivered is green", () => {
    expect(messageStatusColor("delivered")).toBe("var(--status-healthy)");
  });

  test("failed is red", () => {
    expect(messageStatusColor("failed")).toBe("var(--status-failed)");
  });

  test("queued is amber", () => {
    expect(messageStatusColor("queued")).toBe("var(--status-transitional)");
  });

  test("sent is blue", () => {
    expect(messageStatusColor("sent")).toBe("var(--status-processing)");
  });
});
