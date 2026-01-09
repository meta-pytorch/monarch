/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Derive macros for the typeuri crate.

use proc_macro::TokenStream;
use quote::quote;
use syn::Data;
use syn::DataEnum;
use syn::DeriveInput;
use syn::Expr;
use syn::Fields;
use syn::Lit;
use syn::Meta;
use syn::MetaNameValue;
use syn::parse_macro_input;

/// Derive the [`typeuri::Named`] trait for a struct or enum.
///
/// The name of the type is its fully-qualified Rust path. The name may be
/// overridden by providing a string value for the `name` attribute.
///
/// Example:
/// ```ignore
/// use typeuri_macros::Named;
///
/// #[derive(Named)]
/// struct MyType;
///
/// #[derive(Named)]
/// #[named(name = "custom::path::MyEnum")]
/// enum MyEnum { A, B }
/// ```
#[proc_macro_derive(Named, attributes(named))]
pub fn derive_named(input: TokenStream) -> TokenStream {
    // Parse the input struct or enum
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    let mut typename = quote! {
        concat!(std::module_path!(), "::", stringify!(#struct_name))
    };

    let type_params: Vec<_> = input.generics.type_params().collect();
    let has_generics = !type_params.is_empty();

    for attr in &input.attrs {
        if attr.path().is_ident("named") {
            if let Ok(meta) = attr.parse_args_with(
                syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
            ) {
                for item in meta {
                    if let Meta::NameValue(MetaNameValue {
                        path,
                        value: Expr::Lit(expr_lit),
                        ..
                    }) = item
                    {
                        if path.is_ident("name") {
                            if let Lit::Str(name) = expr_lit.lit {
                                typename = quote! { #name };
                            } else {
                                return TokenStream::from(
                                    syn::Error::new_spanned(path, "invalid name")
                                        .to_compile_error(),
                                );
                            }
                        } else {
                            return TokenStream::from(
                                syn::Error::new_spanned(
                                    path,
                                    "unsupported attribute (only `name` is supported)",
                                )
                                .to_compile_error(),
                            );
                        }
                    }
                }
            }
        }
    }

    // Create a version of generics with Named bounds for the impl block
    let mut generics_with_bounds = input.generics.clone();
    if has_generics {
        for param in generics_with_bounds.type_params_mut() {
            param.bounds.push(syn::parse_quote!(typeuri::Named));
        }
    }
    let (impl_generics_with_bounds, _, _) = generics_with_bounds.split_for_impl();

    // Generate typename implementation based on whether we have generics
    let (typename_impl, typehash_impl) = if has_generics {
        // Create format string with placeholders for each generic parameter
        let placeholders = vec!["{}"; type_params.len()].join(", ");
        let placeholders_format_string = format!("<{}>", placeholders);
        let format_string = quote! { concat!(std::module_path!(), "::", stringify!(#struct_name), #placeholders_format_string) };

        let type_param_idents: Vec<_> = type_params.iter().map(|p| &p.ident).collect();
        (
            quote! {
                typeuri::intern_typename!(Self, #format_string, #(#type_param_idents),*)
            },
            quote! {
                typeuri::cityhasher::hash(Self::typename())
            },
        )
    } else {
        (
            typename,
            quote! {
                static TYPEHASH: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
                    typeuri::cityhasher::hash(<#struct_name as typeuri::Named>::typename())
                });
                *TYPEHASH
            },
        )
    };

    // Generate 'arm' for enums only.
    let arm_impl = match &input.data {
        Data::Enum(DataEnum { variants, .. }) => {
            let match_arms = variants.iter().map(|v| {
                let variant_name = &v.ident;
                let variant_str = variant_name.to_string();
                match &v.fields {
                    Fields::Unit => quote! { Self::#variant_name => Some(#variant_str) },
                    Fields::Unnamed(_) => quote! { Self::#variant_name(..) => Some(#variant_str) },
                    Fields::Named(_) => quote! { Self::#variant_name { .. } => Some(#variant_str) },
                }
            });
            quote! {
                fn arm(&self) -> Option<&'static str> {
                    match self {
                        #(#match_arms,)*
                    }
                }
            }
        }
        _ => quote! {},
    };

    let (_, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics_with_bounds typeuri::Named for #struct_name #ty_generics #where_clause {
            fn typename() -> &'static str { #typename_impl }
            fn typehash() -> u64 { #typehash_impl }
            #arm_impl
        }
    };

    TokenStream::from(expanded)
}
