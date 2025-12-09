/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro2::TokenStream as TokenStream2;
use quote::format_ident;
use quote::quote;
use syn::Data;
use syn::DeriveInput;
use syn::Fields;
use syn::Index;
use syn::Type;
use syn::parse_macro_input;
use syn::parse_quote;

/// Generates a [`struct_diff_patch::Diff`] implementation for a struct.
/// The patch type will match the one expected by the struct's [`struct_diff_patch::Patch`] implementation,
/// as derived by the [`Patch`] derive macro.
///
/// For example,
///
/// ```ignore
/// #[derive(Diff)]
/// struct MyStruct {
///     name: String,
///     count: u32,
/// }
/// ```
///
/// will generate the following [`struct_diff_patch::Diff`] implementation:
///
/// ```ignore
/// impl struct_diff_patch::Diff for MyStruct
///   where
///       String: struct_diff_patch::Diff,
///       u32: struct_diff_patch::Diff,
///   {
///       type Patch = (
///           <String as struct_diff_patch::Diff>::Patch,
///           <u32 as struct_diff_patch::Diff>::Patch,
///       );
///       fn diff(&self, other: &Self) -> Self::Patch {
///           (self.name.diff(&other.name), self.count.diff(&other.count))
///       }
///   }
/// ```
#[proc_macro_derive(Diff)]
pub fn derive_diff(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_diff(input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Derives a [`struct_diff_patch::Patch`] implementation for a struct.
/// The patch type will match the one expected by the struct's [`struct_diff_patch::Diff`] implementation,
/// as derived by the [`Diff`] derive macro.
///
/// For example,
///
/// ```ignore
/// #[derive(Patch)]
/// struct MyStruct {
///     name: String,
///     count: u32,
/// }
/// ```
///
/// will generate the following:
///
/// ```ignore
/// impl struct_diff_patch::Patch<MyStruct>
///   for (<String as struct_diff_patch::Diff>::Patch, <u32 as struct_diff_patch::Diff>::Patch)
///   where
///       String: struct_diff_patch::Diff,
///       u32: struct_diff_patch::Diff,
///   {
///       fn apply(self, value: &mut MyStruct) -> struct_diff_patch::Result<()> {
///           let (field_patch_0, field_patch_1) = self;
///           field_patch_0.apply(&mut value.name)?;
///           field_patch_1.apply(&mut value.count)?;
///           Ok(())
///       }
///   }
/// ```
#[proc_macro_derive(Patch)]
pub fn derive_patch(input: TokenStream) -> TokenStream {
    let _ = parse_macro_input!(input as DeriveInput);
    TokenStream::new()
}

fn expand_diff(input: DeriveInput) -> syn::Result<TokenStream2> {
    let DeriveInput {
        ident,
        generics,
        data,
        ..
    } = input;
    let crate_path = crate_path();

    let (_, ty_generics, _) = generics.split_for_impl();
    let mut impl_generics = generics.clone();

    let struct_tokens = match data {
        Data::Struct(data_struct) => data_struct,
        _ => {
            return Err(syn::Error::new(
                Span::call_site(),
                "Diff derive currently supports only structs",
            ));
        }
    };

    let (patch_type, diff_expr, apply_body, field_types) =
        build_struct_parts(&struct_tokens.fields, &crate_path);

    if !field_types.is_empty() {
        let where_clause_impl = impl_generics.make_where_clause();
        for field_ty in field_types {
            where_clause_impl
                .predicates
                .push(parse_quote! { #field_ty: #crate_path::Diff });
        }
    }

    let (impl_generics_tokens, _, where_clause_tokens) = impl_generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics_tokens #crate_path::Diff for #ident #ty_generics #where_clause_tokens {
            type Patch = #patch_type;

            fn diff(&self, other: &Self) -> Self::Patch {
                #diff_expr
            }
        }

        impl #impl_generics_tokens #crate_path::Patch<#ident #ty_generics> for #patch_type #where_clause_tokens {
            fn apply(self, value: &mut #ident #ty_generics) -> #crate_path::Result<()> {
                #apply_body
                Ok(())
            }
        }
    })
}

fn build_struct_parts(
    fields: &Fields,
    crate_path: &syn::Path,
) -> (TokenStream2, TokenStream2, TokenStream2, Vec<Type>) {
    match fields {
        Fields::Named(named) => {
            let names: Vec<_> = named
                .named
                .iter()
                .map(|field| field.ident.clone().expect("named field"))
                .collect();
            let types: Vec<_> = named.named.iter().map(|field| field.ty.clone()).collect();

            let patch_types: Vec<_> = types
                .iter()
                .map(|ty| quote! { <#ty as #crate_path::Diff>::Patch })
                .collect();

            let diff_fields: Vec<_> = names
                .iter()
                .map(|name| quote! { self.#name.diff(&other.#name) })
                .collect();

            let binding_names: Vec<_> = names
                .iter()
                .enumerate()
                .map(|(pos, _)| format_ident!("field_patch_{pos}"))
                .collect();

            let apply_steps = binding_names
                .iter()
                .zip(names.iter())
                .map(|(binding, name)| quote! { #binding.apply(&mut value.#name)?; });

            let patch_type = if patch_types.len() > 0 {
                quote! { ( #( #patch_types ),* , ) }
            } else {
                quote! { () }
            };

            let diff_expr = if diff_fields.len() > 0 {
                quote! { ( #( #diff_fields ),* , ) }
            } else {
                quote! { () }
            };

            let apply_body = if binding_names.is_empty() {
                quote! {
                    let _ = self;
                    let _ = value;
                }
            } else {
                quote! {
                    let ( #( #binding_names ),* , ) = self;
                    #( #apply_steps )*
                }
            };

            (patch_type, diff_expr, apply_body, types)
        }
        Fields::Unnamed(unnamed) => {
            let types: Vec<_> = unnamed
                .unnamed
                .iter()
                .map(|field| field.ty.clone())
                .collect();
            let indices: Vec<_> = (0..types.len()).map(Index::from).collect();
            let patch_fields: Vec<_> = types
                .iter()
                .map(|ty| quote! { <#ty as #crate_path::Diff>::Patch })
                .collect();

            let diff_fields: Vec<_> = indices
                .iter()
                .map(|idx| quote! { self.#idx.diff(&other.#idx) })
                .collect();

            let binding_names: Vec<_> = indices
                .iter()
                .enumerate()
                .map(|(pos, _)| format_ident!("field_patch_{pos}"))
                .collect();

            let apply_steps = binding_names
                .iter()
                .zip(indices.iter())
                .map(|(binding, idx)| quote! { #binding.apply(&mut value.#idx)?; });

            let patch_type = if patch_fields.len() > 0 {
                quote! { ( #( #patch_fields ),* , ) }
            } else {
                quote! { () }
            };
            let diff_expr = if diff_fields.len() > 0 {
                quote! { ( #( #diff_fields ),* , ) }
            } else {
                quote! { () }
            };

            let apply_body = if binding_names.is_empty() {
                quote! {
                    let _ = self;
                    let _ = value;
                }
            } else {
                quote! {
                    let ( #( #binding_names ),* , ) = self;
                    #( #apply_steps )*
                }
            };

            (patch_type, diff_expr, apply_body, types)
        }
        Fields::Unit => {
            let patch_type = quote! { () };
            let diff_expr = quote! { () };
            let apply_body = quote! {
                let _ = self;
                let _ = value;
            };
            (patch_type, diff_expr, apply_body, Vec::new())
        }
    }
}

fn crate_path() -> syn::Path {
    syn::parse_quote!(struct_diff_patch)
    // TODO: get proc-macro-crate into third-party
    // match crate_name("struct_diff_patch") {
    //     Ok(FoundCrate::Itself) => syn::parse_quote!(crate),
    //     Ok(FoundCrate::Name(name)) => {
    //         let ident = Ident::new(&name, Span::call_site());
    //         syn::parse_quote!(#ident)
    //     }
    //     Err(_) => syn::parse_quote!(struct_diff_patch),
    // }
}
