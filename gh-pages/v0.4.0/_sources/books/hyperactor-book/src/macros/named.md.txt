# `#[derive(Named)]`

The `#[derive(Named)]` macro implements the `typeuri::Named` trait for a type, making it identifiable at runtime through a globally unique string and stable hash.

## The `Named` trait

The `typeuri::Named` trait is the foundation of type identification in hyperactor. It gives each type a globally unique identity based on its name used in routing.
```rust
pub trait Named: Sized + 'static {
    fn typename() -> &'static str;
    fn typehash() -> u64 { ... }
    fn typeid() -> TypeId { ... }
    fn port() -> u64 { ... }
    fn arm(&self) -> Option<&'static str> { ... }
    unsafe fn arm_unchecked(self_: *const ()) -> Option<&'static str> { ... }
}
```

### Trait Methods

#### `typename() -> &'static str`

Returns the globally unique, fully-qualified type name for the type. This should typically look like:
```rust
"foo::bar::Corge<quux::garpy::Waldo>"
```

#### `typehash() -> u64`

Returns a stable hash derived from `typename()`. This value is used for message port derivation.
```rust
cityhasher::hash(Self::typename())
```

#### `typeid() -> TypeId`

Returns the Rust `TypeId` for the type (, which is only unique within a single binary).

#### `port() -> u64`

Returns a globally unique port number for the type:
```rust
Self::typehash() | (1 << 63)
```
Typed ports are reserved in the range 2^63 .. 2^64 - 1.

### `arm(&self) -> Option<&'static str>`

For enum types, this returns the name of the current variant, e.g., "Add" or "Remove".

### `unsafe fn arm_unchecked(ptr: *const ()) -> Option<&'static str>`

The type-erased version of `arm()`. Casts ptr back to `&Self` and calls `arm()`.

Useful for dynamic reflection when the concrete type isn’t statically known

### Runtime Registration

In addition to implementing the `Named` trait, the macro registers the type’s metadata at startup using the `inventory` crate:
```rust
const _: () = {
    static __INVENTORY: ::inventory::Node = ::inventory::Node {
        value: &TypeInfo { ... },
        ...
    };
    // Registers the type info before main() runs
    #[link_section = ".init_array"]
    static __CTOR: unsafe extern "C" fn() = __ctor;
};
```
This allows the type to be discovered at runtime, enabling:
- Message dispatch from erased or serialized inputs
- Introspection and diagnostics
- Dynamic spawning or reflection
- Tooling support

Types registered this way appear in the global `inventory::iter<TypeInfo>` set, which is how the hyperactor runtime locates known message types.
