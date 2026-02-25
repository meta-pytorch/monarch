/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Procedural macros for hyperactor_config.

use proc_macro::TokenStream;
use quote::quote;
use syn::DeriveInput;
use syn::parse_macro_input;

/// Derive the [`hyperactor_config::attrs::AttrValue`] trait for a struct or enum.
///
/// This macro generates an implementation that uses the type's `ToString` and `FromStr`
/// implementations for the `display` and `parse` methods respectively.
///
/// The type must already implement the required super-traits:
/// `Named + Sized + Serialize + DeserializeOwned + Send + Sync + Clone + 'static`
/// as well as `ToString` and `FromStr`.
///
/// # Example
///
/// ```ignore
/// #[derive(AttrValue, Named, Serialize, Deserialize, Clone)]
/// struct MyCustomType {
///     value: String,
/// }
///
/// impl std::fmt::Display for MyCustomType {
///     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
///         write!(f, "{}", self.value)
///     }
/// }
///
/// impl std::str::FromStr for MyCustomType {
///     type Err = std::io::Error;
///
///     fn from_str(s: &str) -> Result<Self, Self::Err> {
///         Ok(MyCustomType {
///             value: s.to_string(),
///         })
///     }
/// }
/// ```
#[proc_macro_derive(AttrValue)]
pub fn derive_attr_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    TokenStream::from(quote! {
        impl #impl_generics hyperactor_config::attrs::AttrValue for #name #ty_generics #where_clause {
            fn display(&self) -> String {
                self.to_string()
            }

            fn parse(value: &str) -> std::result::Result<Self, anyhow::Error> {
                value.parse().map_err(|e| anyhow::anyhow!("failed to parse {}: {}", stringify!(#name), e))
            }
        }
    })
}
