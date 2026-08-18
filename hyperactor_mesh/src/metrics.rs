/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor_telemetry::*;

// Per-proc memory samples emitted on the periodic tick of
// `Handler<RepublishIntrospect>` for `ProcAgent`, governed by
// `PROCESS_MEMORY_METRIC_INTERVAL`. Values are bytes; the underlying
// source is `/proc/self/statm`. Linux only — non-Linux procs skip
// the emit (PD-2: never fabricated).
declare_static_gauge!(PROCESS_RSS_BYTES, "process.memory.rss_bytes");
declare_static_gauge!(PROCESS_VM_SIZE_BYTES, "process.memory.vm_bytes");
