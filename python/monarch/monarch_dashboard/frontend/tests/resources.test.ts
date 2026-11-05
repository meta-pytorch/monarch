/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { points, pressure, sampleAge } from "../src/lib/resources";

test("resource charts leave missing samples and long gaps open", () => {
  const samples = [{ timestamp_us: 1_000_000, value: 10 }, { timestamp_us: 2_000_000, value: null }, { timestamp_us: 30_000_000, value: 20 }];
  assert.deepEqual(points(samples, s => s.value, 10), [[1000, 10], [2000, null], [16000, null], [30000, 20]]);
});

test("RAM pressure accounts for available memory including reclaimable cache", () => {
  assert.equal(pressure({ memory_total_bytes: 100, memory_available_bytes: 75 } as Parameters<typeof pressure>[0]), 25);
  assert.equal(pressure({ memory_total_bytes: 0, memory_available_bytes: 0 } as Parameters<typeof pressure>[0]), null);
});

test("sample age is separate from successful poll age", () => {
  assert.ok((sampleAge(1_000_000, 31_000_000, Date.now()) ?? 0) >= 30);
  assert.equal(sampleAge(null, 31_000_000, Date.now()), null);
});
