use cxx::ExternType;
use cxx::type_id;

/// SAFETY: bindings
unsafe impl ExternType for CUstream_st {
    type Id = type_id!("CUstream_st");
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

    fn deserialize_array<'de, D>(deserializer: D) -> Result<[i8; 128], D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<i8> = Deserialize::deserialize(deserializer)?;
        vec.try_into().map_err(|v: Vec<i8>| {
            serde::de::Error::invalid_length(v.len(), &"expected an array of length 128")
        })
    }

    fn serialize_array<S>(array: &[i8; 128], serializer: S) -> Result<S::Ok, S::Error>
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
