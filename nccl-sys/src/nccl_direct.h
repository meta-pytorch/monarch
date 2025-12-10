/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

// For Buck builds, we directly include the real NCCL header
// and link against the NCCL library (no dynamic loading needed)
#include <nccl.h>
