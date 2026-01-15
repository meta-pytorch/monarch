/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Wirevalue provides an erased serialization format. [`Any`] is a type-erased
//! envelope containing a serialized value identified by a [`typeuri::Named`].
//!
//! Wirevalues also provide encoding polymorphism, allowing the same representation
//! to carry multiple serialization formats, and to transcode between them for
//! types that are registered through [`register_type!`].

use std::any::TypeId;
use std::collections::HashMap;
use std::fmt;
use std::io::Cursor;
use std::sync::LazyLock;

use enum_as_inner::EnumAsInner;
use hyperactor_config::AttrValue;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
pub use typeuri::Named;
pub use typeuri::intern_typename;

pub mod config;

/// Typehash value indicating a broken (unknown type, no value) Any.
pub const BROKEN_TYPEHASH: u64 = 0;

#[doc(hidden)]
/// Dump trait for Named types that are also serializable/deserializable.
/// This is a utility used by [`Any::dump`], and is not intended
/// for direct use.
pub trait NamedDumpable: Named + Serialize + for<'de> Deserialize<'de> {
    /// Dump the data in Any to a JSON value.
    fn dump(data: Any) -> Result<serde_json::Value, anyhow::Error>;
}

impl<T: Named + Serialize + for<'de> Deserialize<'de>> NamedDumpable for T {
    fn dump(data: Any) -> Result<serde_json::Value, anyhow::Error> {
        let value = data.deserialized::<Self>()?;
        Ok(serde_json::to_value(value)?)
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct TypeInfo {
    /// Named::typename()
    pub typename: fn() -> &'static str,
    /// Named::typehash()
    pub typehash: fn() -> u64,
    /// Named::typeid()
    pub typeid: fn() -> TypeId,
    /// Named::typehash()
    pub port: fn() -> u64,
    /// A function that can transcode a serialized value to JSON.
    pub dump: Option<fn(Any) -> Result<serde_json::Value, anyhow::Error>>,
    /// Return the arm for this type, if available.
    pub arm_unchecked: unsafe fn(*const ()) -> Option<&'static str>,
}

#[allow(dead_code)]
impl TypeInfo {
    /// Get the typeinfo for the provided type hash.
    pub fn get(typehash: u64) -> Option<&'static TypeInfo> {
        TYPE_INFO.get(&typehash).map(|v| &**v)
    }

    /// Get the typeinfo for the provided type id.
    pub fn get_by_typeid(typeid: TypeId) -> Option<&'static TypeInfo> {
        TYPE_INFO_BY_TYPE_ID.get(&typeid).map(|v| &**v)
    }

    /// Get the typeinfo for the provided type.
    pub fn of<T: ?Sized + 'static>() -> Option<&'static TypeInfo> {
        Self::get_by_typeid(TypeId::of::<T>())
    }

    /// Get the typename for this type.
    pub fn typename(&self) -> &'static str {
        (self.typename)()
    }

    /// Get the typehash for this type.
    pub fn typehash(&self) -> u64 {
        (self.typehash)()
    }

    /// Get the typeid for this type.
    pub fn typeid(&self) -> TypeId {
        (self.typeid)()
    }

    /// Get the port for this type.
    pub fn port(&self) -> u64 {
        (self.port)()
    }

    /// Dump the serialized data to a JSON value.
    pub fn dump(&self, data: Any) -> Result<serde_json::Value, anyhow::Error> {
        if let Some(dump) = self.dump {
            (dump)(data)
        } else {
            anyhow::bail!("binary does not have dumper for {}", self.typehash())
        }
    }

    /// Get the arm name for an enum value.
    ///
    /// # Safety
    /// The caller must ensure the value pointer is valid for this type.
    pub unsafe fn arm_unchecked(&self, value: *const ()) -> Option<&'static str> {
        // SAFETY: This isn't safe, we're passing it on.
        unsafe { (self.arm_unchecked)(value) }
    }
}

inventory::collect!(TypeInfo);

/// Type infos for all types that have been linked into the binary, keyed by typehash.
static TYPE_INFO: LazyLock<HashMap<u64, &'static TypeInfo>> = LazyLock::new(|| {
    inventory::iter::<TypeInfo>()
        .map(|entry| (entry.typehash(), entry))
        .collect()
});

/// Type infos for all types that have been linked into the binary, keyed by typeid.
static TYPE_INFO_BY_TYPE_ID: LazyLock<HashMap<std::any::TypeId, &'static TypeInfo>> =
    LazyLock::new(|| {
        TYPE_INFO
            .values()
            .map(|info| (info.typeid(), &**info))
            .collect()
    });

/// Register a (concrete) type so that it may be looked up by name or hash. Type registration
/// is required only to improve diagnostics, as it allows a binary to introspect serialized
/// payloads under type erasure.
///
/// The provided type must implement [`typeuri::Named`], and must be concrete.
#[macro_export]
macro_rules! register_type {
    ($type:ty) => {
        $crate::submit! {
            $crate::TypeInfo {
                typename: <$type as $crate::Named>::typename,
                typehash: <$type as $crate::Named>::typehash,
                typeid: <$type as $crate::Named>::typeid,
                port: <$type as $crate::Named>::port,
                dump: Some(<$type as $crate::NamedDumpable>::dump),
                arm_unchecked: <$type as $crate::Named>::arm_unchecked,
            }
        }
    };
}

// Re-export inventory::submit for the register_type! macro
#[doc(hidden)]
pub use inventory::submit;

/// An enumeration containing the supported encodings of serialized values.
#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    AttrValue,
    typeuri::Named,
    strum::EnumIter,
    strum::Display,
    strum::EnumString
)]
pub enum Encoding {
    /// Serde bincode encoding.
    #[strum(to_string = "bincode")]
    Bincode,
    /// Serde JSON encoding.
    #[strum(to_string = "serde_json")]
    Json,
    /// Serde multipart encoding.
    #[strum(to_string = "serde_multipart")]
    Multipart,
}

/// The encoding used for a serialized value.
#[derive(Clone, Serialize, Deserialize, PartialEq, EnumAsInner)]
enum Encoded {
    Bincode(bytes::Bytes),
    Json(bytes::Bytes),
    Multipart(serde_multipart::Message),
}

impl Encoded {
    /// The length of the underlying serialized message
    pub fn len(&self) -> usize {
        match &self {
            Encoded::Bincode(data) => data.len(),
            Encoded::Json(data) => data.len(),
            Encoded::Multipart(message) => message.len(),
        }
    }

    /// Is the message empty. This should always return false.
    pub fn is_empty(&self) -> bool {
        match &self {
            Encoded::Bincode(data) => data.is_empty(),
            Encoded::Json(data) => data.is_empty(),
            Encoded::Multipart(message) => message.is_empty(),
        }
    }

    /// Returns the encoding of this serialized value.
    pub fn encoding(&self) -> Encoding {
        match &self {
            Encoded::Bincode(_) => Encoding::Bincode,
            Encoded::Json(_) => Encoding::Json,
            Encoded::Multipart(_) => Encoding::Multipart,
        }
    }

    /// Computes the 32bit crc of the encoded data
    pub fn crc(&self) -> u32 {
        match &self {
            Encoded::Bincode(data) => crc32fast::hash(data),
            Encoded::Json(data) => crc32fast::hash(data),
            Encoded::Multipart(message) => {
                let mut hasher = crc32fast::Hasher::new();
                for fragment in message.body().iter() {
                    hasher.update(fragment);
                }
                for part in message.parts() {
                    for fragment in part.iter() {
                        hasher.update(fragment);
                    }
                }
                hasher.finalize()
            }
        }
    }
}

impl std::fmt::Debug for Encoded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Encoded::Bincode(data) => write!(f, "Encoded::Bincode({})", HexFmt(data)),
            Encoded::Json(data) => write!(f, "Encoded::Json({})", HexFmt(data)),
            Encoded::Multipart(message) => {
                write!(
                    f,
                    "Encoded::Multipart(body={}",
                    HexFmt(&message.body().to_bytes())
                )?;
                for (index, part) in message.parts().iter().enumerate() {
                    write!(f, ", part[{}]={}", index, HexFmt(&part.to_bytes()))?;
                }
                write!(f, ")")
            }
        }
    }
}

/// The type of error returned by operations on [`Any`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Errors returned from serde bincode.
    #[error(transparent)]
    Bincode(#[from] bincode::Error),

    /// Errors returned from serde JSON.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// The encoding was not recognized.
    #[error("unknown encoding: {0}")]
    InvalidEncoding(String),
}

/// Represents a serialized value, wrapping the underlying serialization
/// and deserialization details, while ensuring that we pass correctly-serialized
/// message throughout the system.
///
/// Currently, Any passes through to bincode, but in the future we may include
/// content-encoding information to allow for other codecs as well.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Any {
    /// The encoded data
    encoded: Encoded,
    /// The typehash of the serialized value. This is used to provide
    /// typed introspection of the value.
    typehash: u64,
}

impl std::fmt::Display for Any {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.dump() {
            Ok(value) => {
                // unwrap okay, self.dump() would return Err otherwise.
                let typename = self.typename().unwrap();
                // take the basename of the type (e.g. "foo::bar::baz" -> "baz")
                let basename = typename.split("::").last().unwrap_or(typename);
                write!(f, "{}{}", basename, JsonFmt(&value))
            }
            Err(_) => write!(f, "{:?}", self.encoded),
        }
    }
}

impl Any {
    /// Construct a new serialized value by serializing the provided T-typed value.
    /// Serialize uses the default encoding defined by the configuration key
    /// [`config::DEFAULT_ENCODING`] in the global configuration; use [`serialize_with_encoding`]
    /// to serialize values with a specific encoding.
    pub fn serialize<T: Serialize + Named>(value: &T) -> Result<Self, Error> {
        Self::serialize_with_encoding(
            hyperactor_config::global::get(config::DEFAULT_ENCODING),
            value,
        )
    }

    /// Serialize U-typed value as a T-typed value. This should be used with care
    /// (typically only in testing), as the value's representation may be illegally
    /// coerced.
    pub fn serialize_as<T: Named, U: Serialize>(value: &U) -> Result<Self, Error> {
        Self::serialize_with_encoding_as::<T, U>(
            hyperactor_config::global::get(config::DEFAULT_ENCODING),
            value,
        )
    }

    /// Serialize the value with the using the provided encoding.
    pub fn serialize_with_encoding<T: Serialize + Named>(
        encoding: Encoding,
        value: &T,
    ) -> Result<Self, Error> {
        Self::serialize_with_encoding_as::<T, T>(encoding, value)
    }

    /// Serialize U-typed value as a T-typed value. This should be used with care
    /// (typically only in testing), as the value's representation may be illegally
    /// coerced.
    pub fn serialize_with_encoding_as<T: Named, U: Serialize>(
        encoding: Encoding,
        value: &U,
    ) -> Result<Self, Error> {
        Ok(Self {
            encoded: match encoding {
                Encoding::Bincode => Encoded::Bincode(bincode::serialize(value)?.into()),
                Encoding::Json => Encoded::Json(serde_json::to_vec(value)?.into()),
                Encoding::Multipart => {
                    Encoded::Multipart(serde_multipart::serialize_bincode(value)?)
                }
            },
            typehash: T::typehash(),
        })
    }

    /// Create a new broken Any value. A broken value has unknown type and
    /// no valid data. Attempting to deserialize a broken value will fail.
    pub fn new_broken() -> Self {
        Self {
            encoded: Encoded::Bincode(bytes::Bytes::new()),
            typehash: BROKEN_TYPEHASH,
        }
    }

    /// Returns true if this Any is broken (unknown type, no value).
    pub fn is_broken(&self) -> bool {
        self.typehash == BROKEN_TYPEHASH
    }

    /// Deserialize a value to the provided type T.
    pub fn deserialized<T: DeserializeOwned + Named>(&self) -> Result<T, anyhow::Error> {
        anyhow::ensure!(
            !self.is_broken(),
            "attempted to deserialize a broken Any value"
        );
        anyhow::ensure!(
            self.is::<T>(),
            "attempted to serialize {}-typed serialized into type {}",
            self.typename().unwrap_or("unknown"),
            T::typename()
        );
        self.deserialized_unchecked()
    }

    /// Deserialize a value to the provided type T, without checking for type conformance.
    /// This should be used carefully, only when you know that the dynamic type check is
    /// not needed.
    pub fn deserialized_unchecked<T: DeserializeOwned>(&self) -> Result<T, anyhow::Error> {
        match &self.encoded {
            Encoded::Bincode(data) => bincode::deserialize(data).map_err(anyhow::Error::from),
            Encoded::Json(data) => serde_json::from_slice(data).map_err(anyhow::Error::from),
            Encoded::Multipart(message) => {
                serde_multipart::deserialize_bincode(message.clone()).map_err(anyhow::Error::from)
            }
        }
    }

    /// Transcode the serialized value to JSON. This operation will succeed if the type hash
    /// is embedded in the value, and the corresponding type is available in this binary.
    pub fn transcode_to_json(self) -> Result<Self, Self> {
        match self.encoded {
            Encoded::Bincode(_) | Encoded::Multipart(_) => {
                let json_value = match self.dump() {
                    Ok(json_value) => json_value,
                    Err(_) => return Err(self),
                };
                let json_data = match serde_json::to_vec(&json_value) {
                    Ok(json_data) => json_data,
                    Err(_) => return Err(self),
                };
                Ok(Self {
                    encoded: Encoded::Json(json_data.into()),
                    typehash: self.typehash,
                })
            }
            Encoded::Json(_) => Ok(self),
        }
    }

    /// Dump the Any message into a JSON value. This will succeed if: 1) the typehash is embedded
    /// in the serialized value; 2) the named type is linked into the binary.
    pub fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        match &self.encoded {
            Encoded::Bincode(_) | Encoded::Multipart(_) => {
                let Some(typeinfo) = TYPE_INFO.get(&self.typehash) else {
                    anyhow::bail!("binary does not have typeinfo for {}", self.typehash);
                };
                typeinfo.dump(self.clone())
            }
            Encoded::Json(data) => serde_json::from_slice(data).map_err(anyhow::Error::from),
        }
    }

    /// The encoding used by this serialized value.
    pub fn encoding(&self) -> Encoding {
        self.encoded.encoding()
    }

    /// The typehash of the serialized value.
    pub fn typehash(&self) -> u64 {
        self.typehash
    }

    /// The typename of the serialized value, if available.
    pub fn typename(&self) -> Option<&'static str> {
        TYPE_INFO
            .get(&self.typehash)
            .map(|typeinfo| typeinfo.typename())
    }

    /// Deserialize a prefix of the value. This is currently only supported
    /// for bincode-serialized values.
    // TODO: we should support this by formalizing the notion of a 'prefix'
    // serialization, and generalize it to other codecs as well.
    pub fn prefix<T: DeserializeOwned>(&self) -> Result<T, anyhow::Error> {
        match &self.encoded {
            Encoded::Bincode(data) => bincode::deserialize(data).map_err(anyhow::Error::from),
            _ => anyhow::bail!("only bincode supports prefix emplacement"),
        }
    }

    /// Emplace a new prefix to this value. This is currently only supported
    /// for bincode-serialized values.
    pub fn emplace_prefix<T: Serialize + DeserializeOwned>(
        &mut self,
        prefix: T,
    ) -> Result<(), anyhow::Error> {
        let data = match &self.encoded {
            Encoded::Bincode(data) => data,
            _ => anyhow::bail!("only bincode supports prefix emplacement"),
        };

        // This is a bit ugly, but: we first deserialize out the old prefix,
        // then serialize the new prefix, then splice the two together.
        // This is safe because we know that the prefix is the first thing
        // in the serialized value, and that the serialization format is stable.
        let mut cursor = Cursor::new(data.clone());
        let _prefix: T = bincode::deserialize_from(&mut cursor).unwrap();
        let position = cursor.position() as usize;
        let suffix = &cursor.into_inner()[position..];
        let mut data = bincode::serialize(&prefix)?;
        data.extend_from_slice(suffix);
        self.encoded = Encoded::Bincode(data.into());

        Ok(())
    }

    /// The length of the underlying serialized message
    pub fn len(&self) -> usize {
        self.encoded.len()
    }

    /// Is the message empty. This should always return false.
    pub fn is_empty(&self) -> bool {
        self.encoded.is_empty()
    }

    /// Returns the 32bit crc of the serialized data
    pub fn crc(&self) -> u32 {
        self.encoded.crc()
    }

    /// Returns whether this value contains a serialized M-typed value. Returns None
    /// when type information is unavailable.
    pub fn is<M: Named>(&self) -> bool {
        self.typehash == M::typehash()
    }
}

const MAX_BYTE_PREVIEW_LENGTH: usize = 8;

fn display_bytes_as_hash(f: &mut impl std::fmt::Write, bytes: &[u8]) -> std::fmt::Result {
    let hash = crc32fast::hash(bytes);
    write!(f, "CRC:{:x}", hash)?;
    // Implementing in this way lets us print without allocating a new intermediate string.
    for &byte in bytes.iter().take(MAX_BYTE_PREVIEW_LENGTH) {
        write!(f, " {:x}", byte)?;
    }
    if bytes.len() > MAX_BYTE_PREVIEW_LENGTH {
        write!(f, " [...{} bytes]", bytes.len() - MAX_BYTE_PREVIEW_LENGTH)?;
    }
    Ok(())
}

/// Formats a binary slice as hex when its display function is called.
pub struct HexFmt<'a>(pub &'a [u8]);

impl std::fmt::Display for HexFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // calculate a 2 byte checksum to prepend to the message
        display_bytes_as_hash(f, self.0)
    }
}

/// Formats a JSON value for display, printing all keys but
/// truncating and displaying a hash if the content is too long.
pub struct JsonFmt<'a>(pub &'a serde_json::Value);

const MAX_JSON_VALUE_DISPLAY_LENGTH: usize = 8;

impl std::fmt::Display for JsonFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        /// Truncate the input string to MAX_JSON_VALUE_DISPLAY_LENGTH and append
        /// the truncated hash of the full value for easy comparison.
        fn truncate_and_hash(value_str: &str) -> String {
            let truncate_at = MAX_JSON_VALUE_DISPLAY_LENGTH.min(value_str.len());

            // Respect UTF-8 boundaries (multi-byte chars like emojis can be up to 4 bytes)
            let mut safe_truncate_at = truncate_at;
            while safe_truncate_at > 0 && !value_str.is_char_boundary(safe_truncate_at) {
                safe_truncate_at -= 1;
            }

            let truncated_str = &value_str[..safe_truncate_at];
            let mut result = truncated_str.to_string();
            result.push_str(&format!("[...{} chars] ", value_str.len()));
            display_bytes_as_hash(&mut result, value_str.as_bytes()).unwrap();
            result
        }

        /// Recursively truncate a serde_json::Value object.
        fn truncate_json_values(value: &serde_json::Value) -> serde_json::Value {
            match value {
                serde_json::Value::String(s) => {
                    if s.len() > MAX_JSON_VALUE_DISPLAY_LENGTH {
                        serde_json::Value::String(truncate_and_hash(s))
                    } else {
                        value.clone()
                    }
                }
                serde_json::Value::Array(arr) => {
                    let array_str = serde_json::to_string(arr).unwrap();
                    if array_str.len() > MAX_JSON_VALUE_DISPLAY_LENGTH {
                        serde_json::Value::String(truncate_and_hash(&array_str))
                    } else {
                        value.clone()
                    }
                }
                serde_json::Value::Object(obj) => {
                    let truncated_obj: serde_json::Map<_, _> = obj
                        .iter()
                        .map(|(k, v)| (k.clone(), truncate_json_values(v)))
                        .collect();
                    serde_json::Value::Object(truncated_obj)
                }
                _ => value.clone(),
            }
        }

        let truncated = truncate_json_values(self.0);
        write!(f, "{}", truncated)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde::Serialize;
    use serde_multipart::Part;
    use strum::IntoEnumIterator;
    use typeuri::Named;

    use super::*;

    #[derive(typeuri::Named, Serialize, Deserialize)]
    struct TestStruct;

    #[test]
    fn test_names() {
        assert_eq!(String::typename(), "String");
        assert_eq!(Option::<String>::typename(), "Option<String>");
        assert_eq!(Vec::<String>::typename(), "Vec<String>");
        assert_eq!(Vec::<Vec::<String>>::typename(), "Vec<Vec<String>>");
        assert_eq!(
            Vec::<Vec::<Vec::<String>>>::typename(),
            "Vec<Vec<Vec<String>>>"
        );
        assert_eq!(
            <(u64, String, Option::<isize>)>::typename(),
            "(u64, String, Option<isize>)"
        );
        assert_eq!(TestStruct::typename(), "wirevalue::tests::TestStruct");
        assert_eq!(
            Vec::<TestStruct>::typename(),
            "Vec<wirevalue::tests::TestStruct>"
        );
    }

    #[test]
    fn test_ports() {
        assert_eq!(String::typehash(), 3947244799002047352u64);
        assert_eq!(String::port(), 13170616835856823160u64);
        assert_ne!(
            Vec::<Vec::<Vec::<String>>>::typehash(),
            Vec::<Vec::<Vec::<Vec::<String>>>>::typehash(),
        );
    }

    #[derive(typeuri::Named, Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct TestDumpStruct {
        a: String,
        b: u64,
        c: Option<i32>,
        d: Option<Part>,
    }
    crate::register_type!(TestDumpStruct);

    #[test]
    fn test_dump_struct() {
        let data = TestDumpStruct {
            a: "hello".to_string(),
            b: 1234,
            c: Some(5678),
            d: None,
        };
        let serialized = Any::serialize(&data).unwrap();
        let serialized_json = serialized.clone().transcode_to_json().unwrap();

        assert!(serialized.encoded.is_multipart());
        assert!(serialized_json.encoded.is_json());

        let json_string =
            String::from_utf8(serialized_json.encoded.as_json().unwrap().to_vec().clone()).unwrap();
        // The serialized data for JSON is just the (compact) JSON string.
        assert_eq!(
            json_string,
            "{\"a\":\"hello\",\"b\":1234,\"c\":5678,\"d\":null}"
        );

        for serialized in [serialized, serialized_json] {
            // Note, at this point, serialized has no knowledge other than its embedded typehash.

            assert_eq!(
                serialized.typename(),
                Some("wirevalue::tests::TestDumpStruct")
            );

            let json = serialized.dump().unwrap();
            assert_eq!(
                json,
                serde_json::json!({
                    "a": "hello",
                    "b": 1234,
                    "c": 5678,
                    "d": null,
                })
            );

            assert_eq!(
                format!("{}", serialized),
                "TestDumpStruct{\"a\":\"hello\",\"b\":1234,\"c\":5678,\"d\":null}",
            );
        }
    }

    #[test]
    fn test_emplace_prefix() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(config::DEFAULT_ENCODING, Encoding::Bincode);
        let data = TestDumpStruct {
            a: "hello".to_string(),
            b: 1234,
            c: Some(5678),
            d: None,
        };

        let mut ser = Any::serialize(&data).unwrap();
        assert_eq!(ser.prefix::<String>().unwrap(), "hello".to_string());

        ser.emplace_prefix("hello, world, 123!".to_string())
            .unwrap();

        assert_eq!(
            ser.deserialized::<TestDumpStruct>().unwrap(),
            TestDumpStruct {
                a: "hello, world, 123!".to_string(),
                b: 1234,
                c: Some(5678),
                d: None,
            }
        );
    }

    #[test]
    fn test_arms() {
        #[derive(typeuri::Named, Serialize, Deserialize)]
        enum TestArm {
            #[allow(dead_code)]
            A(u32),
            B,
            C(),
            D {
                #[allow(dead_code)]
                a: u32,
                #[allow(dead_code)]
                b: String,
            },
        }

        assert_eq!(TestArm::A(1234).arm(), Some("A"));
        assert_eq!(TestArm::B.arm(), Some("B"));
        assert_eq!(TestArm::C().arm(), Some("C"));
        assert_eq!(
            TestArm::D {
                a: 1234,
                b: "hello".to_string()
            }
            .arm(),
            Some("D")
        );
    }

    #[test]
    fn display_hex() {
        assert_eq!(
            format!("{}", HexFmt("hello world".as_bytes())),
            "CRC:d4a1185 68 65 6c 6c 6f 20 77 6f [...3 bytes]"
        );
        assert_eq!(format!("{}", HexFmt("".as_bytes())), "CRC:0");
        assert_eq!(
            format!("{}", HexFmt("a very long string that is long".as_bytes())),
            "CRC:c7e24f62 61 20 76 65 72 79 20 6c [...23 bytes]"
        );
    }

    #[test]
    fn test_json_fmt() {
        let json_value = serde_json::json!({
            "name": "test",
            "number": 42,
            "nested": {
                "key": "value"
            }
        });
        // JSON values with short values should print normally
        assert_eq!(
            format!("{}", JsonFmt(&json_value)),
            "{\"name\":\"test\",\"nested\":{\"key\":\"value\"},\"number\":42}",
        );

        let empty_json = serde_json::json!({});
        assert_eq!(format!("{}", JsonFmt(&empty_json)), "{}");

        let simple_array = serde_json::json!([1, 2, 3]);
        assert_eq!(format!("{}", JsonFmt(&simple_array)), "[1,2,3]");

        // JSON values with very long strings should be truncated
        let long_string_json = serde_json::json!({
            "long_string": "a".repeat(MAX_JSON_VALUE_DISPLAY_LENGTH * 5)
        });
        assert_eq!(
            format!("{}", JsonFmt(&long_string_json)),
            "{\"long_string\":\"aaaaaaaa[...40 chars] CRC:c95b8a25 61 61 61 61 61 61 61 61 [...32 bytes]\"}"
        );

        // JSON values with very long arrays should be truncated
        let long_array_json =
            serde_json::json!((1..=(MAX_JSON_VALUE_DISPLAY_LENGTH + 4)).collect::<Vec<_>>());
        assert_eq!(
            format!("{}", JsonFmt(&long_array_json)),
            "\"[1,2,3,4[...28 chars] CRC:e5c881af 5b 31 2c 32 2c 33 2c 34 [...20 bytes]\""
        );

        // Test for truncation within nested blocks
        let nested_json = serde_json::json!({
            "simple_number": 42,
            "simple_bool": true,
            "outer": {
                "long_string": "a".repeat(MAX_JSON_VALUE_DISPLAY_LENGTH + 10),
                "long_array": (1..=(MAX_JSON_VALUE_DISPLAY_LENGTH + 4)).collect::<Vec<_>>(),
                "inner": {
                    "simple_value": "short",
                }
            }
        });
        println!("{}", JsonFmt(&nested_json));
        assert_eq!(
            format!("{}", JsonFmt(&nested_json)),
            "{\"outer\":{\"inner\":{\"simple_value\":\"short\"},\"long_array\":\"[1,2,3,4[...28 chars] CRC:e5c881af 5b 31 2c 32 2c 33 2c 34 [...20 bytes]\",\"long_string\":\"aaaaaaaa[...18 chars] CRC:b8ac0e31 61 61 61 61 61 61 61 61 [...10 bytes]\"},\"simple_bool\":true,\"simple_number\":42}",
        );
    }

    #[test]
    fn test_json_fmt_utf8_truncation() {
        // Test that UTF-8 character boundaries are respected during truncation
        // Create a string with multi-byte characters that would be truncated

        // String with 7 ASCII chars + 4-byte emoji (total 11 bytes, truncates at 8)
        let utf8_json = serde_json::json!({
            "emoji": "1234567🦀"  // 7 + 4 = 11 bytes, MAX is 8
        });

        // Should truncate at byte 7 (before the emoji) to respect UTF-8 boundary
        let result = format!("{}", JsonFmt(&utf8_json));

        // Verify it doesn't panic and produces valid output
        assert!(result.contains("1234567"));
        assert!(!result.contains("🦀")); // Emoji should be truncated away

        // Test with all multi-byte characters
        let all_multibyte = serde_json::json!({
            "chinese": "你好世界"  // Each char is 3 bytes = 12 bytes total
        });
        let result3 = format!("{}", JsonFmt(&all_multibyte));
        assert!(!result3.is_empty());
    }

    #[test]
    fn test_encodings() {
        let value = TestDumpStruct {
            a: "hello, world".to_string(),
            b: 123,
            c: Some(321),
            d: Some(Part::from("hello, world, again")),
        };
        for enc in Encoding::iter() {
            let ser = Any::serialize_with_encoding(enc, &value).unwrap();
            assert_eq!(ser.encoding(), enc);
            assert_eq!(ser.deserialized::<TestDumpStruct>().unwrap(), value);
        }
    }

    #[test]
    fn test_broken_any() {
        let broken = Any::new_broken();
        assert!(broken.is_broken());
        assert_eq!(broken.typehash(), BROKEN_TYPEHASH);

        // Normal values are not broken
        let normal = Any::serialize(&"hello".to_string()).unwrap();
        assert!(!normal.is_broken());

        // deserialized() should fail for broken values
        let err = broken.deserialized::<String>().unwrap_err();
        assert!(err.to_string().contains("broken"));
    }
}
