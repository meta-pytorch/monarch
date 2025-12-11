/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use cxx::ExternType;
use cxx::type_id;

/// SAFETY: bindings
#[cfg(not(rocm))]
unsafe impl ExternType for CUstream_st {
    type Id = type_id!("CUstream_st");
    type Kind = cxx::kind::Opaque;
}

/// SAFETY: bindings
#[cfg(rocm)]
unsafe impl ExternType for ihipStream_t {
    type Id = type_id!("ihipStream_t");
    type Kind = cxx::kind::Opaque;
}

/// SAFETY: bindings
/// Trivial because this is POD struct
unsafe impl ExternType for ncclConfig_t {
    type Id = type_id!("ncclConfig_t");
    type Kind = cxx::kind::Trivial;
}

/// SAFETY: bindings
unsafe impl ExternType for ncclComm {
    type Id = type_id!("ncclComm");
    type Kind = cxx::kind::Opaque;
}

// When building with cargo, this is actually the lib.rs file for a crate.
// Include the generated bindings.rs and suppress lints.
#[allow(non_camel_case_types)]
#[allow(non_upper_case_globals)]
#[allow(non_snake_case)]
mod inner {
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;
    use serde::ser::SerializeSeq;

    #[cfg(cargo)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

    // This type is manually defined instead of generated because we want to dervice
    // Serialize/Deserialize on it.
    #[repr(C)]
    #[derive(Debug, Copy, Clone, Serialize, Deserialize)]
    pub struct ncclUniqueId {
        // Custom serializer required, as serde does not provide a built-in
        // implementation of serialization for large arrays.
        #[serde(
            serialize_with = "serialize_array",
            deserialize_with = "deserialize_array"
        )]
        pub internal: [::std::os::raw::c_char; 128usize],
    }

    fn deserialize_array<'de, D>(deserializer: D) -> Result<[::std::os::raw::c_char; 128], D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<::std::os::raw::c_char> = Deserialize::deserialize(deserializer)?;
        vec.try_into().map_err(|v: Vec<::std::os::raw::c_char>| {
            serde::de::Error::invalid_length(v.len(), &"expected an array of length 128")
        })
    }

    fn serialize_array<S>(
        array: &[::std::os::raw::c_char; 128],
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(128))?;
        for element in array {
            seq.serialize_element(element)?;
        }
        seq.end()
    }
}

pub use inner::*;

// =============================================================================
// ROCm/HIP Compatibility Aliases
// =============================================================================
// These allow consumers (like torch-sys-cuda) to use CUDA names transparently on ROCm.

#[cfg(rocm)]
pub use inner::hipError_t as cudaError_t;

#[cfg(rocm)]
pub use inner::hipStream_t as cudaStream_t;

#[cfg(rocm)]
pub use inner::hipSetDevice as cudaSetDevice;

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;

    use super::*;

    #[test]
    fn sanity() {
        // SAFETY: testing bindings
        unsafe {
            let mut version = MaybeUninit::<i32>::uninit();
            let result = ncclGetVersion(version.as_mut_ptr());
            assert_eq!(result.0, 0);
        }
    }
}
