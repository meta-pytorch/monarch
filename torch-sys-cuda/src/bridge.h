/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <nccl.h> // @manual

namespace monarch {

/// This function exists because ncclConfig initialization requires the use of
/// a macro. We cannot reference the macro directly from Rust code, so we wrap
/// the macro use in a function and bind that to Rust instead.
inline ncclConfig_t make_nccl_config() {
  ncclConfig_t ret = NCCL_CONFIG_INITIALIZER;
  return ret;
}

} // namespace monarch
