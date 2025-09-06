/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use bytes::BytesMut;

const DEFAULT_SIZE: usize = 10_000_000;

lazy_static::lazy_static! {
    static ref GLOBAL_POOL: std::sync::Mutex<Pool> = std::sync::Mutex::new(Pool::new());
}

/// Pool of resources that is manually managed. Useful for encoding and network interactions where you don't want to keep allocating
pub struct Pool {
    freelist: Vec<BytesMut>,
}

impl Pool {
    pub fn new() -> Self {
        Self { freelist: vec![] }
    }

    pub fn aquire() -> BytesMut {
        GLOBAL_POOL.lock().unwrap()._aquire()
    }

    pub fn release(val: BytesMut) {
        GLOBAL_POOL.lock().unwrap()._release(val);
    }

    fn _aquire(&mut self) -> BytesMut {
        self.freelist
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(DEFAULT_SIZE))
    }

    fn _release(&mut self, val: BytesMut) {
        self.freelist.push(val);
    }
}
