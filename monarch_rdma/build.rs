/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Build script for monarch_rdma.

fn main() {
    // rdmaxcel-sys links libstdc++ statically. In an executable (this crate's
    // tests and examples), the linker exports any of those libstdc++ symbols
    // that a linked shared library also references, so the GPU runtime's own
    // shared libstdc++ binds to some of the executable's copy (e.g. the
    // basic_stringstream constructor) and some of its own (the destructor).
    // On ROCm, hipInit then aborts with `free(): invalid pointer` destroying
    // a std::locale. Keep static-archive symbols out of the executable's
    // dynamic symbol table so each copy stays self-contained. This only
    // affects executables built from this package; the Python extension
    // exports no C++ symbols already.
    println!("cargo::rustc-link-arg=-Wl,--exclude-libs,ALL");
}
