/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stdio.h>
#include "rdmaxcel.h"

int main() {
  // Reference the host launcher rather than the `cu_db_ring` kernel itself:
  // CUDA 13 emits `__global__` host stubs with hidden visibility, so they are
  // not linkable from another translation unit.
  void* func_ptr = (void*)&launch_db_ring;
  printf("launch_db_ring function address: %p\n", func_ptr);
  return 0;
}
