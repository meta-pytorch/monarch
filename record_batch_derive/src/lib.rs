/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Derive macro for generating Arrow RecordBatch buffers from row structs.
//!
//! # Example
//!
//! ```ignore
//! #[derive(RecordBatchRow)]
//! struct Span {
//!     id: u64,
//!     name: String,
//!     timestamp: i64,
//!     parent_id: Option<u64>,
//! }
//! ```
//!
//! This generates:
//! - `SpanBuffer` struct with `Vec<T>` for each field
//! - `insert(&mut self, row: Span)` method
//! - `schema() -> SchemaRef` method
//! - `impl RecordBatchBuffer` with `len()`, `is_empty()`, and `to_record_batch()` methods
//!
//! The `RecordBatchBuffer` trait must be in scope where the derive is used.

use proc_macro::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::parse_macro_input;
use syn::DeriveInput;
use syn::Field;
use syn::Fields;
use syn::Type;

/// Derive macro for generating Arrow RecordBatch buffer types.
#[proc_macro_derive(RecordBatchRow)]
pub fn derive_record_batch_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let buffer_name = format_ident!("{}Buffer", name);

    let fields = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("RecordBatchRow only supports named fields"),
        },
        _ => panic!("RecordBatchRow only supports structs"),
    };

    let field_info: Vec<FieldInfo> = fields.iter().map(FieldInfo::from_field).collect();

    let buffer_fields = field_info.iter().map(|f| {
        let name = &f.name;
        let vec_ty = &f.vec_type;
        quote! { #name: #vec_ty }
    });

    let insert_pushes = field_info.iter().map(|f| {
        let name = &f.name;
        quote! { self.#name.push(row.#name); }
    });

    let schema_fields = field_info.iter().map(|f| {
        let field_name_str = f.name.to_string();
        let nullable = f.nullable;
        let data_type = &f.arrow_data_type;
        quote! {
            datafusion::arrow::datatypes::Field::new(#field_name_str, #data_type, #nullable)
        }
    });

    let column_conversions = field_info.iter().map(|f| {
        let name = &f.name;
        let array_conversion = &f.array_conversion;
        quote! {
            std::sync::Arc::new(#array_conversion(std::mem::take(&mut self.#name)))
        }
    });

    let first_field = &field_info[0].name;

    let expanded = quote! {
        #[derive(Default)]
        pub struct #buffer_name {
            #(#buffer_fields,)*
        }

        impl #buffer_name {
            pub fn insert(&mut self, row: #name) {
                #(#insert_pushes)*
            }

            pub fn schema() -> datafusion::arrow::datatypes::SchemaRef {
                std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
                    #(#schema_fields,)*
                ]))
            }
        }

        impl RecordBatchBuffer for #buffer_name {
            fn len(&self) -> usize {
                self.#first_field.len()
            }

            fn is_empty(&self) -> bool {
                self.#first_field.is_empty()
            }

            fn to_record_batch(&mut self) -> anyhow::Result<datafusion::arrow::record_batch::RecordBatch> {
                let schema = #buffer_name::schema();
                let columns: Vec<datafusion::arrow::array::ArrayRef> = vec![
                    #(#column_conversions,)*
                ];
                Ok(datafusion::arrow::record_batch::RecordBatch::try_new(schema, columns)?)
            }
        }
    };

    TokenStream::from(expanded)
}

struct FieldInfo {
    name: syn::Ident,
    vec_type: proc_macro2::TokenStream,
    nullable: bool,
    arrow_data_type: proc_macro2::TokenStream,
    array_conversion: proc_macro2::TokenStream,
}

impl FieldInfo {
    fn from_field(field: &Field) -> Self {
        let name = field.ident.clone().expect("field must have name");
        let (inner_ty, nullable) = extract_option_inner(&field.ty);

        let (vec_type, arrow_data_type, array_conversion) = if nullable {
            let vec_ty = quote! { Vec<Option<#inner_ty>> };
            let (data_type, array_conv) = get_arrow_type_and_conversion(inner_ty);
            (vec_ty, data_type, array_conv)
        } else {
            let vec_ty = quote! { Vec<#inner_ty> };
            let (data_type, array_conv) = get_arrow_type_and_conversion(inner_ty);
            (vec_ty, data_type, array_conv)
        };

        FieldInfo {
            name,
            vec_type,
            nullable,
            arrow_data_type,
            array_conversion,
        }
    }
}

fn extract_option_inner(ty: &Type) -> (&Type, bool) {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if segment.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return (inner, true);
                    }
                }
            }
        }
    }
    (ty, false)
}

fn get_arrow_type_and_conversion(
    ty: &Type,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let type_str = quote!(#ty).to_string().replace(' ', "");

    match type_str.as_str() {
        "u64" => (
            quote! { datafusion::arrow::datatypes::DataType::UInt64 },
            quote! { datafusion::arrow::array::UInt64Array::from },
        ),
        "u32" => (
            quote! { datafusion::arrow::datatypes::DataType::UInt32 },
            quote! { datafusion::arrow::array::UInt32Array::from },
        ),
        "i64" => (
            quote! { datafusion::arrow::datatypes::DataType::Int64 },
            quote! { datafusion::arrow::array::Int64Array::from },
        ),
        "i32" => (
            quote! { datafusion::arrow::datatypes::DataType::Int32 },
            quote! { datafusion::arrow::array::Int32Array::from },
        ),
        "String" => (
            quote! { datafusion::arrow::datatypes::DataType::Utf8 },
            quote! { datafusion::arrow::array::StringArray::from },
        ),
        "bool" => (
            quote! { datafusion::arrow::datatypes::DataType::Boolean },
            quote! { datafusion::arrow::array::BooleanArray::from },
        ),
        "f64" => (
            quote! { datafusion::arrow::datatypes::DataType::Float64 },
            quote! { datafusion::arrow::array::Float64Array::from },
        ),
        "f32" => (
            quote! { datafusion::arrow::datatypes::DataType::Float32 },
            quote! { datafusion::arrow::array::Float32Array::from },
        ),
        _ => panic!("unsupported type: {}", type_str),
    }
}
