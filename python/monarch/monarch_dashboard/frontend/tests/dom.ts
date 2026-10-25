/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { after, afterEach } from "node:test";
import { cleanup } from "@testing-library/react";

afterEach(cleanup);
after(() => window.close());
