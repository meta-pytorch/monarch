/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Defines macros used by the [`hyperactor`] crate.

#![feature(proc_macro_def_site)]
#![deny(missing_docs)]

extern crate proc_macro;

use convert_case::Case;
use convert_case::Casing;
use indoc::indoc;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::ToTokens;
use quote::format_ident;
use quote::quote;
use syn::Attribute;
use syn::Data;
use syn::DataEnum;
use syn::DataStruct;
use syn::DeriveInput;
use syn::Expr;
use syn::ExprLit;
use syn::Field;
use syn::Fields;
use syn::Ident;
use syn::Index;
use syn::ItemFn;
use syn::ItemImpl;
use syn::Lit;
use syn::Meta;
use syn::MetaNameValue;
use syn::Token;
use syn::Type;
use syn::bracketed;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::parse_macro_input;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;

const REPLY_VARIANT_ERROR: &str = indoc! {r#"
`call` message expects a typed port ref (`OncePortRef` or `PortRef`) or handle (`OncePortHandle` or `PortHandle`) argument in the last position

= help: use `MyCall(Arg1Type, Arg2Type, .., OncePortRef<ReplyType>)`
= help: use `MyCall(Arg1Type, Arg2Type, .., OncePortHandle<ReplyType>)`
"#};

const REPLY_USAGE_ERROR: &str = indoc! {r#"
`call` message expects at most one `reply` argument

= help: use `MyCall(Arg1Type, Arg2Type, .., #[reply] OncePortRef<ReplyType>)`
= help: use `MyCall(Arg1Type, Arg2Type, .., #[reply] OncePortHandle<ReplyType>)`
"#};

enum FieldFlag {
    None,
    Reply,
}

/// Represents a variant of an enum.
#[allow(dead_code)]
enum Variant {
    /// A named variant (i.e., `MyVariant { .. }`).
    Named {
        enum_name: Ident,
        name: Ident,
        field_names: Vec<Ident>,
        field_types: Vec<Type>,
        field_flags: Vec<FieldFlag>,
        is_struct: bool,
        generics: syn::Generics,
    },
    /// An anonymous variant (i.e., `MyVariant(..)`).
    Anon {
        enum_name: Ident,
        name: Ident,
        field_types: Vec<Type>,
        field_flags: Vec<FieldFlag>,
        is_struct: bool,
        generics: syn::Generics,
    },
}

impl Variant {
    /// The number of fields in the variant.
    fn len(&self) -> usize {
        self.field_types().len()
    }

    /// Returns whether this variant was defined as a struct.
    fn is_struct(&self) -> bool {
        match self {
            Variant::Named { is_struct, .. } => *is_struct,
            Variant::Anon { is_struct, .. } => *is_struct,
        }
    }

    /// The name of the enum containing the variant.
    fn enum_name(&self) -> &Ident {
        match self {
            Variant::Named { enum_name, .. } => enum_name,
            Variant::Anon { enum_name, .. } => enum_name,
        }
    }

    /// The name of the variant itself.
    fn name(&self) -> &Ident {
        match self {
            Variant::Named { name, .. } => name,
            Variant::Anon { name, .. } => name,
        }
    }

    /// The generics of the variant itself.
    #[allow(dead_code)]
    fn generics(&self) -> &syn::Generics {
        match self {
            Variant::Named { generics, .. } => generics,
            Variant::Anon { generics, .. } => generics,
        }
    }

    /// The snake_name of the variant itself.
    fn snake_name(&self) -> Ident {
        Ident::new(
            &self.name().to_string().to_case(Case::Snake),
            self.name().span(),
        )
    }

    /// The variant's qualified name.
    fn qualified_name(&self) -> proc_macro2::TokenStream {
        let enum_name = self.enum_name();
        let name = self.name();

        if self.is_struct() {
            quote! { #enum_name }
        } else {
            quote! { #enum_name::#name }
        }
    }

    /// Names of the fields in the variant. Anonymous variants are named
    /// according to their position in the argument list.
    fn field_names(&self) -> Vec<Ident> {
        match self {
            Variant::Named { field_names, .. } => field_names.clone(),
            Variant::Anon { field_types, .. } => (0usize..field_types.len())
                .map(|idx| format_ident!("arg{}", idx))
                .collect(),
        }
    }

    /// The types of the fields int the variant.
    fn field_types(&self) -> &Vec<Type> {
        match self {
            Variant::Named { field_types, .. } => field_types,
            Variant::Anon { field_types, .. } => field_types,
        }
    }

    /// Return the field flags for this variant.
    fn field_flags(&self) -> &Vec<FieldFlag> {
        match self {
            Variant::Named { field_flags, .. } => field_flags,
            Variant::Anon { field_flags, .. } => field_flags,
        }
    }

    /// The constructor for the variant, using the field names directly.
    fn constructor(&self) -> proc_macro2::TokenStream {
        let qualified_name = self.qualified_name();
        let field_names = self.field_names();
        match self {
            Variant::Named { .. } => quote! { #qualified_name { #(#field_names),* } },
            Variant::Anon { .. } => quote! { #qualified_name(#(#field_names),*) },
        }
    }
}

struct ReplyPort {
    is_handle: bool,
    is_once: bool,
}

impl ReplyPort {
    fn from_last_segment(last_segment: &proc_macro2::Ident) -> ReplyPort {
        ReplyPort {
            is_handle: last_segment == "PortHandle" || last_segment == "OncePortHandle",
            is_once: last_segment == "OncePortHandle" || last_segment == "OncePortRef",
        }
    }

    fn open_op(&self) -> proc_macro2::TokenStream {
        if self.is_once {
            quote! { hyperactor::mailbox::open_once_port }
        } else {
            quote! { hyperactor::mailbox::open_port }
        }
    }

    fn rx_modifier(&self) -> proc_macro2::TokenStream {
        if self.is_once {
            quote! {}
        } else {
            quote! { mut }
        }
    }
}

/// Represents a message that can be sent to a handler, each message is associated with
/// a variant.
#[allow(clippy::large_enum_variant)]
enum Message {
    /// A call message is a request-response message, the last argument is
    /// a [`hyperactor::OncePortRef`] or [`hyperactor::OncePortHandle`].
    Call {
        variant: Variant,
        /// Tells whether the reply argument is a handle.
        reply_port: ReplyPort,
        /// The underlying return type (i.e., the type of the reply port).
        return_type: Type,
        /// the log level for generated instrumentation for handlers of this message.
        log_level: Option<Ident>,
    },
    OneWay {
        variant: Variant,
        /// the log level for generated instrumentation for handlers of this message.
        log_level: Option<Ident>,
    },
}

impl Message {
    fn new(span: Span, variant: Variant, log_level: Option<Ident>) -> Result<Self, syn::Error> {
        match &variant
            .field_flags()
            .iter()
            .zip(variant.field_types())
            .filter_map(|(flag, ty)| match flag {
                FieldFlag::Reply => Some(ty),
                FieldFlag::None => None,
            })
            .collect::<Vec<&Type>>()[..]
        {
            [] => Ok(Self::OneWay { variant, log_level }),
            [reply_port_ty] => {
                let syn::Type::Path(type_path) = reply_port_ty else {
                    return Err(syn::Error::new(span, REPLY_VARIANT_ERROR));
                };
                let Some(last_segment) = type_path.path.segments.last() else {
                    return Err(syn::Error::new(span, REPLY_VARIANT_ERROR));
                };
                if last_segment.ident != "OncePortRef"
                    && last_segment.ident != "OncePortHandle"
                    && last_segment.ident != "PortRef"
                    && last_segment.ident != "PortHandle"
                {
                    return Err(syn::Error::new_spanned(last_segment, REPLY_VARIANT_ERROR));
                }
                let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments else {
                    return Err(syn::Error::new_spanned(last_segment, REPLY_VARIANT_ERROR));
                };
                let Some(syn::GenericArgument::Type(return_ty)) = args.args.first() else {
                    return Err(syn::Error::new_spanned(&args.args, REPLY_VARIANT_ERROR));
                };
                let reply_port = ReplyPort::from_last_segment(&last_segment.ident);
                let return_type = return_ty.clone();
                Ok(Self::Call {
                    variant,
                    reply_port,
                    return_type,
                    log_level,
                })
            }
            _ => Err(syn::Error::new(span, REPLY_USAGE_ERROR)),
        }
    }

    /// The arguments of this message.
    fn args(&self) -> Vec<(Ident, Type)> {
        match self {
            Message::Call { variant, .. } => variant
                .field_names()
                .into_iter()
                .zip(variant.field_types().clone())
                .take(variant.len() - 1)
                .collect(),
            Message::OneWay { variant, .. } => variant
                .field_names()
                .into_iter()
                .zip(variant.field_types().clone())
                .collect(),
        }
    }

    fn variant(&self) -> &Variant {
        match self {
            Message::Call { variant, .. } => variant,
            Message::OneWay { variant, .. } => variant,
        }
    }

    fn reply_port_position(&self) -> Option<usize> {
        self.variant()
            .field_flags()
            .iter()
            .position(|flag| matches!(flag, FieldFlag::Reply))
    }

    /// The reply port argument of this message.
    fn reply_port_arg(&self) -> Option<(Ident, Type)> {
        match self {
            Message::Call { variant, .. } => {
                let pos = self.reply_port_position()?;
                Some((
                    variant.field_names()[pos].clone(),
                    variant.field_types()[pos].clone(),
                ))
            }
            Message::OneWay { .. } => None,
        }
    }
}

fn parse_log_level(attrs: &[Attribute]) -> Result<Option<Ident>, syn::Error> {
    let level: Option<String> = match attrs.iter().find(|attr| attr.path().is_ident("log_level")) {
        Some(attr) => {
            let Ok(meta) = attr.meta.require_list() else {
                return Err(syn::Error::new(
                    Span::call_site(),
                    indoc! {"
                            `log_level` attribute must specify level. Supported levels = error, warn, info, debug, trace

                            = help use `#[log_level(info)]` or `#[log_level(error)]`
                        "},
                ));
            };
            let parsed = meta.parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)?;
            if parsed.len() != 1 {
                return Err(syn::Error::new(
                    Span::call_site(),
                    indoc! {"
                            `log_level` attribute must specify exactly one level

                            = help use `#[log_level(warn)]` or `#[log_level(info)]`
                        "},
                ));
            };
            Some(parsed.first().unwrap().to_string())
        }
        None => None,
    };

    if level.is_none() {
        return Ok(None);
    }
    let level = level.unwrap();

    match level.as_str() {
        "error" | "warn" | "info" | "debug" | "trace" => {}
        _ => {
            return Err(syn::Error::new(
                Span::call_site(),
                indoc! {"
                            `log_level` attribute must be one of 'error, warn, info, debug, trace'

                            = help use `#[log_level(warn)]` or `#[log_level(info)]`
                        "},
            ));
        }
    }

    Ok(Some(Ident::new(
        level.to_ascii_uppercase().as_str(),
        Span::call_site(),
    )))
}

fn parse_field_flag(field: &Field) -> FieldFlag {
    for attr in field.attrs.iter() {
        match &attr.meta {
            syn::Meta::Path(path) if path.is_ident("reply") => return FieldFlag::Reply,
            _ => {}
        }
    }
    FieldFlag::None
}

/// Parse a message enum or struct into its constituent messages.
fn parse_messages(input: DeriveInput) -> Result<Vec<Message>, syn::Error> {
    match &input.data {
        Data::Enum(data_enum) => {
            let mut messages = Vec::new();

            for variant in &data_enum.variants {
                let name = variant.ident.clone();
                let attrs = &variant.attrs;

                let message_variant = match &variant.fields {
                    syn::Fields::Unnamed(fields_) => Variant::Anon {
                        enum_name: input.ident.clone(),
                        name,
                        field_types: fields_
                            .unnamed
                            .iter()
                            .map(|field| field.ty.clone())
                            .collect(),
                        field_flags: fields_.unnamed.iter().map(parse_field_flag).collect(),
                        is_struct: false,
                        generics: input.generics.clone(),
                    },
                    syn::Fields::Named(fields_) => Variant::Named {
                        enum_name: input.ident.clone(),
                        name,
                        field_names: fields_
                            .named
                            .iter()
                            .map(|field| field.ident.clone().unwrap())
                            .collect(),
                        field_types: fields_.named.iter().map(|field| field.ty.clone()).collect(),
                        field_flags: fields_.named.iter().map(parse_field_flag).collect(),
                        is_struct: false,
                        generics: input.generics.clone(),
                    },
                    _ => {
                        return Err(syn::Error::new_spanned(
                            variant,
                            indoc! {r#"
                                `Handler` currently only supports named or tuple struct variants

                                = help use `MyCall(Arg1Type, Arg2Type, ..)`,
                                = help use `MyCall { arg1: Arg1Type, arg2: Arg2Type, .. }`,
                                = help use `MyCall(Arg1Type, Arg2Type, .., #[reply] OncePortRef<ReplyType>)`
                                = help use `MyCall { arg1: Arg1Type, arg2: Arg2Type, .., reply: #[reply] OncePortRef<ReplyType>}`
                                = help use `MyCall(Arg1Type, Arg2Type, .., #[reply] OncePortHandle<ReplyType>)`
                                = help use `MyCall { arg1: Arg1Type, arg2: Arg2Type, .., reply: #[reply] OncePortHandle<ReplyType>}`
                              "#},
                        ));
                    }
                };
                let log_level = parse_log_level(attrs)?;

                messages.push(Message::new(
                    variant.fields.span(),
                    message_variant,
                    log_level,
                )?);
            }

            Ok(messages)
        }
        Data::Struct(data_struct) => {
            let struct_name = input.ident.clone();
            let attrs = &input.attrs;

            let message_variant = match &data_struct.fields {
                syn::Fields::Unnamed(fields_) => Variant::Anon {
                    enum_name: struct_name.clone(),
                    name: struct_name,
                    field_types: fields_
                        .unnamed
                        .iter()
                        .map(|field| field.ty.clone())
                        .collect(),
                    field_flags: fields_.unnamed.iter().map(parse_field_flag).collect(),
                    is_struct: true,
                    generics: input.generics.clone(),
                },
                syn::Fields::Named(fields_) => Variant::Named {
                    enum_name: struct_name.clone(),
                    name: struct_name,
                    field_names: fields_
                        .named
                        .iter()
                        .map(|field| field.ident.clone().unwrap())
                        .collect(),
                    field_types: fields_.named.iter().map(|field| field.ty.clone()).collect(),
                    field_flags: fields_.named.iter().map(parse_field_flag).collect(),
                    is_struct: true,
                    generics: input.generics.clone(),
                },
                syn::Fields::Unit => Variant::Anon {
                    enum_name: struct_name.clone(),
                    name: struct_name,
                    field_types: Vec::new(),
                    field_flags: Vec::new(),
                    is_struct: true,
                    generics: input.generics.clone(),
                },
            };

            let log_level = parse_log_level(attrs)?;
            let message = Message::new(data_struct.fields.span(), message_variant, log_level)?;

            Ok(vec![message])
        }
        _ => Err(syn::Error::new_spanned(
            input,
            "handlers can only be derived for enums and structs",
        )),
    }
}

/// Derive a custom handler trait for given an enum containing tuple
/// structs.  The handler trait defines a method corresponding
/// to each of the enum's variants, and a `handle` function
/// that dispatches messages to the correct method.  The macro
/// supports two messaging patterns: "call" and "oneway". A call is a
/// request-response message; a [`hyperactor::mailbox::OncePortRef`] or
/// [`hyperactor::mailbox::OncePortHandle`] in the last position is used
/// to send the return value.
///
/// The macro also derives a client trait that can be automatically implemented
/// by specifying [`HandleClient`] for `ActorHandle<Actor>` and [`RefClient`]
/// for `ActorRef<Actor>` accordingly. We require two implementations because
/// not `ActorRef`s require that its message type is serializable.
///
/// The associated [`hyperactor_macros::handler`] macro can be used to add
/// a dispatching handler directly to an [`hyperactor::Actor`].
///
/// # Example
///
/// The following example creates a "shopping list" actor responsible for
/// maintaining a shopping list.
///
/// ```
/// use std::collections::HashSet;
/// use std::time::Duration;
///
/// use async_trait::async_trait;
/// use hyperactor::Actor;
/// use hyperactor::HandleClient;
/// use hyperactor::Handler;
/// use hyperactor::Instance;
/// use hyperactor::Named;
/// use hyperactor::OncePortRef;
/// use hyperactor::RefClient;
/// use hyperactor::proc::Proc;
/// use serde::Deserialize;
/// use serde::Serialize;
///
/// #[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
/// enum ShoppingList {
///     // Oneway messages dispatch messages asynchronously, with no reply.
///     Add(String),
///     Remove(String),
///
///     // Call messages dispatch a request, expecting a reply to the
///     // provided port, which must be in the last position.
///     Exists(String, #[reply] OncePortRef<bool>),
///
///     List(#[reply] OncePortRef<Vec<String>>),
/// }
///
/// // Define an actor.
/// #[derive(Debug)]
/// #[hyperactor::export(
///     spawn = true,
///     handlers = [
///         ShoppingList,
///     ],
/// )]
/// struct ShoppingListActor(HashSet<String>);
///
/// #[async_trait]
/// impl Actor for ShoppingListActor {
///     type Params = ();
///
///     async fn new(_params: ()) -> Result<Self, anyhow::Error> {
///         Ok(Self(HashSet::new()))
///     }
/// }
///
/// // ShoppingListHandler is the trait generated by derive(Handler) above.
/// // We implement the trait here for the actor, defining a handler for
/// // each ShoppingList message.
/// //
/// // The `forward` attribute installs a handler that forwards messages
/// // to the `ShoppingListHandler` implementation directly. This can also
/// // be done manually:
/// //
/// // ```ignore
/// //<ShoppingListActor as ShoppingListHandler>
/// //     ::handle(self, comm, message).await
/// // ```
/// #[async_trait]
/// #[hyperactor::forward(ShoppingList)]
/// impl ShoppingListHandler for ShoppingListActor {
///     async fn add(&mut self, _cx: &Context<Self>, item: String) -> Result<(), anyhow::Error> {
///         eprintln!("insert {}", item);
///         self.0.insert(item);
///         Ok(())
///     }
///
///     async fn remove(&mut self, _cx: &Context<Self>, item: String) -> Result<(), anyhow::Error> {
///         eprintln!("remove {}", item);
///         self.0.remove(&item);
///         Ok(())
///     }
///
///     async fn exists(
///         &mut self,
///         _cx: &Context<Self>,
///         item: String,
///     ) -> Result<bool, anyhow::Error> {
///         Ok(self.0.contains(&item))
///     }
///
///     async fn list(&mut self, _cx: &Context<Self>) -> Result<Vec<String>, anyhow::Error> {
///         Ok(self.0.iter().cloned().collect())
///     }
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<(), anyhow::Error> {
///     let mut proc = Proc::local();
///
///     // Spawn our actor, and get a handle for rank 0.
///     let shopping_list_actor: hyperactor::ActorHandle<ShoppingListActor> =
///         proc.spawn("shopping", ()).await?;
///
///     // We join the system, so that we can send messages to actors.
///     let client = proc.attach("client").unwrap();
///
///     // todo: consider making this a macro to remove the magic names
///
///     // Derive(Handler) generates client methods, which call the
///     // remote handler provided an actor instance,
///     // the destination actor, and the method arguments.
///
///     shopping_list_actor.add(&client, "milk".into()).await?;
///     shopping_list_actor.add(&client, "eggs".into()).await?;
///
///     println!(
///         "got milk? {}",
///         shopping_list_actor.exists(&client, "milk".into()).await?
///     );
///     println!(
///         "got yoghurt? {}",
///         shopping_list_actor
///             .exists(&client, "yoghurt".into())
///             .await?
///     );
///
///     shopping_list_actor.remove(&client, "milk".into()).await?;
///     println!(
///         "got milk now? {}",
///         shopping_list_actor.exists(&client, "milk".into()).await?
///     );
///
///     println!(
///         "shopping list: {:?}",
///         shopping_list_actor.list(&client).await?
///     );
///
///     let _ = proc
///         .destroy_and_wait::<()>(Duration::from_secs(1), None)
///         .await?;
///     Ok(())
/// }
/// ```
#[proc_macro_derive(Handler, attributes(reply))]
pub fn derive_handler(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name: Ident = input.ident.clone();
    let (_, ty_generics, _) = input.generics.split_for_impl();

    let messages = match parse_messages(input.clone()) {
        Ok(messages) => messages,
        Err(err) => return TokenStream::from(err.to_compile_error()),
    };

    // Trait definition methods for the handler.
    let mut handler_trait_methods = Vec::new();

    // The arms of the match used in the message dispatcher.
    let mut match_arms = Vec::new();

    // Trait implemented by clients.
    let mut client_trait_methods = Vec::new();

    let global_log_level = parse_log_level(&input.attrs).ok().unwrap_or(None);

    for message in &messages {
        match message {
            Message::Call {
                variant,
                reply_port,
                return_type,
                log_level,
            } => {
                let (arg_names, arg_types): (Vec<_>, Vec<_>) = message.args().into_iter().unzip();
                let variant_name_snake = variant.snake_name();
                let variant_name_snake_deprecated =
                    format_ident!("{}_deprecated", variant_name_snake);
                let enum_name = variant.enum_name();
                let _variant_qualified_name = variant.qualified_name();
                let log_level = match (&global_log_level, log_level) {
                    (_, Some(local)) => local.clone(),
                    (Some(global), None) => global.clone(),
                    _ => Ident::new("DEBUG", Span::call_site()),
                };
                let _log_level = if reply_port.is_handle {
                    quote! {
                        tracing::Level::#log_level
                    }
                } else {
                    quote! {
                        tracing::Level::TRACE
                    }
                };
                let log_message = quote! {
                        hyperactor::metrics::ACTOR_MESSAGES_RECEIVED.add(1, hyperactor::kv_pairs!(
                            "rpc" => "call",
                            "actor_id" => hyperactor::context::Mailbox::mailbox(cx).actor_id().to_string(),
                            "message_type" => stringify!(#enum_name),
                            "variant" => stringify!(#variant_name_snake),
                        ));
                };

                handler_trait_methods.push(quote! {
                    #[doc = "The generated handler method for this enum variant."]
                    async fn #variant_name_snake(
                        &mut self,
                        cx: &hyperactor::Context<Self>,
                        #(#arg_names: #arg_types),*)
                        -> Result<#return_type, hyperactor::anyhow::Error>;
                });

                client_trait_methods.push(quote! {
                    #[doc = "The generated client method for this enum variant."]
                    async fn #variant_name_snake(
                        &self,
                        cx: &impl hyperactor::context::Actor,
                        #(#arg_names: #arg_types),*)
                        -> Result<#return_type, hyperactor::anyhow::Error>;

                    #[doc = "The DEPRECATED DO NOT USE generated client method for this enum variant."]
                    async fn #variant_name_snake_deprecated(
                        &self,
                        cx: &impl hyperactor::context::Actor,
                        #(#arg_names: #arg_types),*)
                        -> Result<#return_type, hyperactor::anyhow::Error>;
                });

                let (reply_port_arg, _) = message.reply_port_arg().unwrap();
                let constructor = variant.constructor();
                let result_ident = Ident::new("result", Span::mixed_site());
                let construct_result_future = quote! { use hyperactor::Message; let #result_ident = self.#variant_name_snake(cx, #(#arg_names),*).await?; };
                if reply_port.is_handle {
                    match_arms.push(quote! {
                        #constructor => {
                            #log_message
                            // TODO: should we propagate this error (to supervision), or send it back as an "RPC error"?
                            // This would require Result<Result<..., in order to handle RPC errors.
                            #construct_result_future
                            #reply_port_arg.send(#result_ident).map_err(hyperactor::anyhow::Error::from)
                        }
                    });
                } else {
                    match_arms.push(quote! {
                        #constructor => {
                            #log_message
                            // TODO: should we propagate this error (to supervision), or send it back as an "RPC error"?
                            // This would require Result<Result<..., in order to handle RPC errors.
                            #construct_result_future
                            #reply_port_arg.send(cx, #result_ident).map_err(hyperactor::anyhow::Error::from)
                        }
                    });
                }
            }
            Message::OneWay { variant, log_level } => {
                let (arg_names, arg_types): (Vec<_>, Vec<_>) = message.args().into_iter().unzip();
                let variant_name_snake = variant.snake_name();
                let variant_name_snake_deprecated =
                    format_ident!("{}_deprecated", variant_name_snake);
                let enum_name = variant.enum_name();
                let log_level = match (&global_log_level, log_level) {
                    (_, Some(local)) => local.clone(),
                    (Some(global), None) => global.clone(),
                    _ => Ident::new("TRACE", Span::call_site()),
                };
                let _log_level = quote! {
                    tracing::Level::#log_level
                };
                let log_message = quote! {
                        hyperactor::metrics::ACTOR_MESSAGES_RECEIVED.add(1, hyperactor::kv_pairs!(
                            "rpc" => "call",
                            "actor_id" => hyperactor::context::Mailbox::mailbox(cx).actor_id().to_string(),
                            "message_type" => stringify!(#enum_name),
                            "variant" => stringify!(#variant_name_snake),
                        ));
                };

                handler_trait_methods.push(quote! {
                    #[doc = "The generated handler method for this enum variant."]
                    async fn #variant_name_snake(
                        &mut self,
                        cx: &hyperactor::Context<Self>,
                        #(#arg_names: #arg_types),*)
                        -> Result<(), hyperactor::anyhow::Error>;
                });

                client_trait_methods.push(quote! {
                    #[doc = "The generated client method for this enum variant."]
                    async fn #variant_name_snake(
                        &self,
                        cx: &impl hyperactor::context::Actor,
                        #(#arg_names: #arg_types),*)
                        -> Result<(), hyperactor::anyhow::Error>;

                    #[doc = "The DEPRECATED DO NOT USE generated client method for this enum variant."]
                    async fn #variant_name_snake_deprecated(
                        &self,
                        cx: &impl hyperactor::context::Actor,
                        #(#arg_names: #arg_types),*)
                        -> Result<(), hyperactor::anyhow::Error>;
                });

                let constructor = variant.constructor();

                match_arms.push(quote! {
                    #constructor => {
                        #log_message
                        self.#variant_name_snake(cx, #(#arg_names),*).await
                    },
                });
            }
        }
    }

    let handler_trait_name = format_ident!("{}Handler", name);
    let client_trait_name = format_ident!("{}Client", name);

    // We impose additional constraints on the generics in the implementation;
    // but the trait itself should not impose additional constraints:

    let mut handler_generics = input.generics.clone();
    for param in handler_generics.type_params_mut() {
        param.bounds.push(syn::parse_quote!(serde::Serialize));
        param
            .bounds
            .push(syn::parse_quote!(for<'de> serde::Deserialize<'de>));
        param.bounds.push(syn::parse_quote!(Send));
        param.bounds.push(syn::parse_quote!(Sync));
        param.bounds.push(syn::parse_quote!(std::fmt::Debug));
        param.bounds.push(syn::parse_quote!(hyperactor::Named));
    }
    let (handler_impl_generics, _, _) = handler_generics.split_for_impl();
    let (client_impl_generics, _, _) = input.generics.split_for_impl();

    let expanded = quote! {
        #[doc = "The custom handler trait for this message type."]
        #[hyperactor::async_trait::async_trait]
        pub trait #handler_trait_name #handler_impl_generics: hyperactor::Actor + Send + Sync  {
            #(#handler_trait_methods)*

            #[doc = "Handle the next message."]
            async fn handle(
                &mut self,
                cx: &hyperactor::Context<Self>,
                message: #name #ty_generics,
            ) -> hyperactor::anyhow::Result<()>  {
                 // Dispatch based on message type.
                 match message {
                     #(#match_arms)*
                }
            }
        }

        #[doc = "The custom client trait for this message type."]
        #[hyperactor::async_trait::async_trait]
        pub trait #client_trait_name #client_impl_generics: Send + Sync  {
            #(#client_trait_methods)*
        }
    };

    TokenStream::from(expanded)
}

/// Derives a client implementation on `ActorHandle<Actor>`.
/// See [`Handler`] documentation for details.
#[proc_macro_derive(HandleClient, attributes(log_level))]
pub fn derive_handle_client(input: TokenStream) -> TokenStream {
    derive_client(input, true)
}

/// Derives a client implementation on `ActorRef<Actor>`.
/// See [`Handler`] documentation for details.
#[proc_macro_derive(RefClient, attributes(log_level))]
pub fn derive_ref_client(input: TokenStream) -> TokenStream {
    derive_client(input, false)
}

fn derive_client(input: TokenStream, is_handle: bool) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident.clone();

    let messages = match parse_messages(input.clone()) {
        Ok(messages) => messages,
        Err(err) => return TokenStream::from(err.to_compile_error()),
    };

    // The client implementation methods.
    let mut impl_methods = Vec::new();

    let send_message = if is_handle {
        quote! { self.send(message)? }
    } else {
        quote! { self.send(cx, message)? }
    };
    let global_log_level = parse_log_level(&input.attrs).ok().unwrap_or(None);

    for message in &messages {
        match message {
            Message::Call {
                variant,
                reply_port,
                return_type,
                log_level,
            } => {
                let (arg_names, arg_types): (Vec<_>, Vec<_>) = message.args().into_iter().unzip();
                let variant_name_snake = variant.snake_name();
                let variant_name_snake_deprecated =
                    format_ident!("{}_deprecated", variant_name_snake);
                let enum_name = variant.enum_name();

                let (reply_port_arg, _) = message.reply_port_arg().unwrap();
                let constructor = variant.constructor();
                let log_level = match (&global_log_level, log_level) {
                    (_, Some(local)) => local.clone(),
                    (Some(global), None) => global.clone(),
                    _ => Ident::new("DEBUG", Span::call_site()),
                };
                let log_level = if is_handle {
                    quote! {
                        tracing::Level::#log_level
                    }
                } else {
                    quote! {
                        tracing::Level::TRACE
                    }
                };
                let log_message = quote! {
                        hyperactor::metrics::ACTOR_MESSAGES_SENT.add(1, hyperactor::kv_pairs!(
                            "rpc" => "call",
                            "actor_id" => hyperactor::context::Mailbox::mailbox(cx).actor_id().to_string(),
                            "message_type" => stringify!(#enum_name),
                            "variant" => stringify!(#variant_name_snake),
                        ));

                };
                let open_port = reply_port.open_op();
                let rx_mod = reply_port.rx_modifier();
                if reply_port.is_handle {
                    impl_methods.push(quote! {
                        #[hyperactor::instrument(level=#log_level, rpc = "call", message_type=#name)]
                        async fn #variant_name_snake(
                            &self,
                            cx: &impl hyperactor::context::Actor,
                            #(#arg_names: #arg_types),*)
                            -> Result<#return_type, hyperactor::anyhow::Error> {
                            let (#reply_port_arg, #rx_mod reply_receiver) =
                                #open_port::<#return_type>(cx);
                            let message = #constructor;
                            #log_message;
                            #send_message;
                            reply_receiver.recv().await.map_err(hyperactor::anyhow::Error::from)
                        }

                        #[hyperactor::instrument(level=#log_level, rpc = "call", message_type=#name)]
                        async fn #variant_name_snake_deprecated(
                            &self,
                            cx: &impl hyperactor::context::Actor,
                            #(#arg_names: #arg_types),*)
                            -> Result<#return_type, hyperactor::anyhow::Error> {
                            let (#reply_port_arg, #rx_mod reply_receiver) =
                                #open_port::<#return_type>(cx);
                            let message = #constructor;
                            #log_message;
                            #send_message;
                            reply_receiver.recv().await.map_err(hyperactor::anyhow::Error::from)
                        }
                    });
                } else {
                    impl_methods.push(quote! {
                        #[hyperactor::instrument(level=#log_level, rpc="call", message_type=#name)]
                        async fn #variant_name_snake(
                            &self,
                            cx: &impl hyperactor::context::Actor,
                            #(#arg_names: #arg_types),*)
                            -> Result<#return_type, hyperactor::anyhow::Error> {
                            let (#reply_port_arg, #rx_mod reply_receiver) =
                                #open_port::<#return_type>(cx);
                            let #reply_port_arg = #reply_port_arg.bind();
                            let message = #constructor;
                            #log_message;
                            #send_message;
                            reply_receiver.recv().await.map_err(hyperactor::anyhow::Error::from)
                        }

                        #[hyperactor::instrument(level=#log_level, rpc="call", message_type=#name)]
                        async fn #variant_name_snake_deprecated(
                            &self,
                            cx: &impl hyperactor::context::Actor,
                            #(#arg_names: #arg_types),*)
                            -> Result<#return_type, hyperactor::anyhow::Error> {
                            let (#reply_port_arg, #rx_mod reply_receiver) =
                                #open_port::<#return_type>(cx);
                            let #reply_port_arg = #reply_port_arg.bind();
                            let message = #constructor;
                            #log_message;
                            #send_message;
                            reply_receiver.recv().await.map_err(hyperactor::anyhow::Error::from)
                        }
                    });
                }
            }
            Message::OneWay { variant, log_level } => {
                let (arg_names, arg_types): (Vec<_>, Vec<_>) = message.args().into_iter().unzip();
                let variant_name_snake = variant.snake_name();
                let variant_name_snake_deprecated =
                    format_ident!("{}_deprecated", variant_name_snake);
                let enum_name = variant.enum_name();
                let constructor = variant.constructor();
                let log_level = match (&global_log_level, log_level) {
                    (_, Some(local)) => local.clone(),
                    (Some(global), None) => global.clone(),
                    _ => Ident::new("DEBUG", Span::call_site()),
                };
                let _log_level = if is_handle {
                    quote! {
                        tracing::Level::TRACE
                    }
                } else {
                    quote! {
                        tracing::Level::#log_level
                    }
                };
                let log_message = quote! {
                    hyperactor::metrics::ACTOR_MESSAGES_SENT.add(1, hyperactor::kv_pairs!(
                        "rpc" => "oneway",
                        "actor_id" => self.actor_id().to_string(),
                        "message_type" => stringify!(#enum_name),
                        "variant" => stringify!(#variant_name_snake),
                    ));
                };
                impl_methods.push(quote! {
                    async fn #variant_name_snake(
                        &self,
                        cx: &impl hyperactor::context::Actor,
                        #(#arg_names: #arg_types),*)
                        -> Result<(), hyperactor::anyhow::Error> {
                        let message = #constructor;
                        #log_message;
                        #send_message;
                        Ok(())
                    }

                    async fn #variant_name_snake_deprecated(
                        &self,
                        cx: &impl hyperactor::context::Actor,
                        #(#arg_names: #arg_types),*)
                        -> Result<(), hyperactor::anyhow::Error> {
                        let message = #constructor;
                        #log_message;
                        #send_message;
                        Ok(())
                    }
                });
            }
        }
    }

    let trait_name = format_ident!("{}Client", name);

    let (_, ty_generics, _) = input.generics.split_for_impl();

    // Add a new generic parameter 'A'
    let actor_ident = Ident::new("A", proc_macro2::Span::from(proc_macro::Span::def_site()));
    let mut trait_generics = input.generics.clone();
    trait_generics.params.insert(
        0,
        syn::GenericParam::Type(syn::TypeParam {
            ident: actor_ident.clone(),
            attrs: vec![],
            colon_token: None,
            bounds: Punctuated::new(),
            eq_token: None,
            default: None,
        }),
    );

    for param in trait_generics.type_params_mut() {
        if param.ident == actor_ident {
            continue;
        }
        param.bounds.push(syn::parse_quote!(serde::Serialize));
        param
            .bounds
            .push(syn::parse_quote!(for<'de> serde::Deserialize<'de>));
        param.bounds.push(syn::parse_quote!(Send));
        param.bounds.push(syn::parse_quote!(Sync));
        param.bounds.push(syn::parse_quote!(std::fmt::Debug));
        param.bounds.push(syn::parse_quote!(hyperactor::Named));
    }

    let (impl_generics, _, _) = trait_generics.split_for_impl();

    let expanded = if is_handle {
        quote! {
            #[hyperactor::async_trait::async_trait]
            impl #impl_generics #trait_name #ty_generics for hyperactor::ActorHandle<#actor_ident>
              where #actor_ident: hyperactor::Handler<#name #ty_generics> {
                #(#impl_methods)*
            }
        }
    } else {
        quote! {
            #[hyperactor::async_trait::async_trait]
            impl #impl_generics #trait_name #ty_generics for hyperactor::ActorRef<#actor_ident>
              where #actor_ident: hyperactor::actor::RemoteHandles<#name #ty_generics> {
                #(#impl_methods)*
            }
        }
    };

    TokenStream::from(expanded)
}

const FORWARD_ARGUMENT_ERROR: &str = indoc! {r#"
`forward` expects the message type that is being forwarded

= help: use `#[forward(MessageType)]`
"#};

/// Forward messages of the provided type to this handler implementation.
#[proc_macro_attribute]
pub fn forward(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr_args = parse_macro_input!(attr with Punctuated::<syn::PathSegment, syn::Token![,]>::parse_terminated);
    if attr_args.len() != 1 {
        return TokenStream::from(
            syn::Error::new_spanned(attr_args, FORWARD_ARGUMENT_ERROR).to_compile_error(),
        );
    }

    let message_type = attr_args.first().unwrap();
    let input = parse_macro_input!(item as ItemImpl);

    let self_type = match *input.self_ty {
        syn::Type::Path(ref type_path) => {
            let segment = type_path.path.segments.last().unwrap();
            segment.clone() //ident.clone()
        }
        _ => {
            return TokenStream::from(
                syn::Error::new_spanned(input.self_ty, "`forward` argument must be a type")
                    .to_compile_error(),
            );
        }
    };

    let trait_name = match input.trait_ {
        Some((_, ref trait_path, _)) => trait_path.segments.last().unwrap().clone(),
        None => {
            return TokenStream::from(
                syn::Error::new_spanned(input.self_ty, "no trait in implementation block")
                    .to_compile_error(),
            );
        }
    };

    let expanded = quote! {
        #input

        #[hyperactor::async_trait::async_trait]
        impl hyperactor::Handler<#message_type> for #self_type {
            async fn handle(
                &mut self,
                cx: &hyperactor::Context<Self>,
                message: #message_type,
            ) -> hyperactor::anyhow::Result<()> {
                <Self as #trait_name>::handle(self, cx, message).await
            }
        }
    };

    TokenStream::from(expanded)
}

/// Use this macro in place of tracing::instrument to prevent spamming our tracing table.
/// We set a default level of INFO while always setting ERROR if the function returns Result::Err giving us
/// consistent and high quality structured logs. Because this wraps around tracing::instrument, all parameters
/// mentioned in https://fburl.com/9jlkb5q4 should be valid. For functions that don't return a [`Result`] type, use
/// [`instrument_infallible`]
///
/// ```
/// #[telemetry::instrument]
/// async fn yolo() -> anyhow::Result<i32> {
///     Ok(420)
/// }
/// ```
#[proc_macro_attribute]
pub fn instrument(args: TokenStream, input: TokenStream) -> TokenStream {
    let args =
        parse_macro_input!(args with Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated);
    let input = parse_macro_input!(input as ItemFn);
    let output = quote! {
        #[hyperactor::tracing::instrument(err, skip_all, #args)]
        #input
    };

    TokenStream::from(output)
}

/// Use this macro in place of tracing::instrument to prevent spamming our tracing table.
/// Because this wraps around tracing::instrument, all parameters mentioned in
/// https://fburl.com/9jlkb5q4 should be valid.
///
/// ```
/// #[telemetry::instrument]
/// async fn yolo() -> i32 {
///     420
/// }
/// ```
#[proc_macro_attribute]
pub fn instrument_infallible(args: TokenStream, input: TokenStream) -> TokenStream {
    let args =
        parse_macro_input!(args with Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated);
    let input = parse_macro_input!(input as ItemFn);

    let output = quote! {
        #[hyperactor::tracing::instrument(skip_all, #args)]
        #input
    };

    TokenStream::from(output)
}

/// Derive the [`hyperactor::data::Named`] trait for a struct with the
/// provided type URI. The name of the type is its fully-qualified Rust
/// path. The name may be overridden by providing a string value for the
/// `name` attribute.
///
/// In addition to deriving [`hyperactor::data::Named`], this macro will
/// register the type using the [`hyperactor::register_type`] macro for
/// concrete types. This behavior can be overridden by providing a literal
/// booolean for the `register` attribute.
///
/// This also requires the type to implement [`serde::Serialize`]
/// and [`serde::Deserialize`].
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
    // By default, register concrete types.
    let mut register = !has_generics;

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
                        } else if path.is_ident("register") {
                            if let Lit::Bool(flag) = expr_lit.lit {
                                register = flag.value;
                            } else {
                                return TokenStream::from(
                                    syn::Error::new_spanned(path, "invalid registration flag")
                                        .to_compile_error(),
                                );
                            }
                        } else {
                            return TokenStream::from(
                                syn::Error::new_spanned(
                                    path,
                                    "unsupported attribute (only `name` or `register` is supported)",
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
            param
                .bounds
                .push(syn::parse_quote!(hyperactor::data::Named));
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
                hyperactor::data::intern_typename!(Self, #format_string, #(#type_param_idents),*)
            },
            quote! {
                hyperactor::cityhasher::hash(Self::typename())
            },
        )
    } else {
        (
            typename,
            quote! {
                static TYPEHASH: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
                    hyperactor::cityhasher::hash(<#struct_name as hyperactor::data::Named>::typename())
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

    // Try to register the type so we can get runtime TypeInfo.
    // We can only do this for concrete types.
    //
    // TODO: explore making type hashes "structural", so that we
    // can derive generic type hashes and reconstruct their runtime
    // TypeInfos.
    let registration = if register {
        quote! {
            hyperactor::register_type!(#struct_name);
        }
    } else {
        quote! {
            // Registration not requested
        }
    };

    let (_, ty_generics, where_clause) = input.generics.split_for_impl();
    // Ideally we would compute the has directly in the macro itself, however, we don't
    // have access to the fully expanded pathname here as we use the intrinsic std::module_path!() macro.
    let expanded = quote! {
        impl #impl_generics_with_bounds hyperactor::data::Named for #struct_name #ty_generics #where_clause {
            fn typename() -> &'static str { #typename_impl }
            fn typehash() -> u64 { #typehash_impl }
            #arm_impl
        }

        #registration
    };

    TokenStream::from(expanded)
}

struct HandlerSpec {
    ty: Type,
    cast: bool,
}

impl Parse for HandlerSpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ty: Type = input.parse()?;

        if input.peek(syn::token::Brace) {
            let content;
            syn::braced!(content in input);
            let key: Ident = content.parse()?;
            content.parse::<Token![=]>()?;
            let expr: Expr = content.parse()?;

            let cast = if key == "cast" {
                if let Expr::Lit(ExprLit {
                    lit: Lit::Bool(b), ..
                }) = expr
                {
                    b.value
                } else {
                    return Err(syn::Error::new_spanned(expr, "expected boolean for `cast`"));
                }
            } else {
                return Err(syn::Error::new_spanned(
                    key,
                    "unsupported field (expected `cast`)",
                ));
            };

            Ok(HandlerSpec { ty, cast })
        } else if input.is_empty() || input.peek(Token![,]) {
            Ok(HandlerSpec { ty, cast: false })
        } else {
            // Something unexpected follows the type
            let unexpected: proc_macro2::TokenTree = input.parse()?;
            Err(syn::Error::new_spanned(
                unexpected,
                "unexpected token after type — expected `{ ... }` or nothing",
            ))
        }
    }
}

impl HandlerSpec {
    fn add_indexed(handlers: Vec<HandlerSpec>) -> Vec<Type> {
        let mut tys = Vec::new();
        for HandlerSpec { ty, cast } in handlers {
            if cast {
                let wrapped = quote! { hyperactor::message::IndexedErasedUnbound<#ty> };
                let wrapped_ty: Type = syn::parse2(wrapped).unwrap();
                tys.push(wrapped_ty);
            }
            tys.push(ty);
        }
        tys
    }
}

/// Attribute Struct for [`fn export`] macro.
struct ExportAttr {
    spawn: bool,
    handlers: Vec<HandlerSpec>,
}

impl Parse for ExportAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut spawn = false;
        let mut handlers: Vec<HandlerSpec> = vec![];

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;

            if key == "spawn" {
                let expr: Expr = input.parse()?;
                if let Expr::Lit(ExprLit {
                    lit: Lit::Bool(b), ..
                }) = expr
                {
                    spawn = b.value;
                } else {
                    return Err(syn::Error::new_spanned(
                        expr,
                        "expected boolean for `spawn`",
                    ));
                }
            } else if key == "handlers" {
                let content;
                bracketed!(content in input);
                let raw_handlers = content.parse_terminated(HandlerSpec::parse, Token![,])?;
                handlers = raw_handlers.into_iter().collect();
            } else {
                return Err(syn::Error::new_spanned(
                    key,
                    "unexpected key in `#[export(...)]`. Only supports `spawn` and `handlers`",
                ));
            }

            // optional trailing comma
            let _ = input.parse::<Token![,]>();
        }

        Ok(ExportAttr { spawn, handlers })
    }
}

/// Exports handlers for this actor. The set of exported handlers
/// determine the messages that may be sent to remote references of
/// the actor ([`hyperaxtor::ActorRef`]). Only messages that implement
/// [`hyperactor::RemoteMessage`] may be exported.
///
/// Additionally, an exported actor may be remotely spawned,
/// indicated by `spawn = true`. Such actors must also ensure that
/// their parameter type implements [`hyperactor::RemoteMessage`].
///
/// # Example
///
/// In the following example, `MyActor` can be spawned remotely. It also has
/// exports handlers for two message types, `MyMessage` and `MyOtherMessage`.
/// Consequently, `ActorRef`s of the actor's type may dispatch messages of these
/// types.
///
/// ```ignore
/// #[export(
///     spawn = true,
///     handlers = [
///         MyMessage,
///         MyOtherMessage,
///     ],
/// )]
/// struct MyActor {}
/// ```
#[proc_macro_attribute]
pub fn export(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(item as DeriveInput);
    let data_type_name = &input.ident;

    let ExportAttr { spawn, handlers } = parse_macro_input!(attr as ExportAttr);
    let tys = HandlerSpec::add_indexed(handlers);

    let mut handles = Vec::new();
    let mut bindings = Vec::new();
    let mut type_registrations = Vec::new();

    for ty in &tys {
        handles.push(quote! {
            impl hyperactor::actor::RemoteHandles<#ty> for #data_type_name {}
        });
        bindings.push(quote! {
            ports.bind::<#ty>();
        });
        type_registrations.push(quote! {
            hyperactor::register_type!(#ty);
        });
    }

    let mut expanded = quote! {
        #input

        impl hyperactor::actor::Referable for #data_type_name {}

        #(#handles)*

        #(#type_registrations)*

        // Always export the `Signal` type.
        impl hyperactor::actor::RemoteHandles<hyperactor::actor::Signal> for #data_type_name {}

        impl hyperactor::actor::Binds<#data_type_name> for #data_type_name {
            fn bind(ports: &hyperactor::proc::Ports<Self>) {
                #(#bindings)*
            }
        }

        // TODO: just use Named derive directly here.
        impl hyperactor::data::Named for #data_type_name {
            fn typename() -> &'static str { concat!(std::module_path!(), "::", stringify!(#data_type_name)) }
        }
    };

    if spawn {
        expanded.extend(quote! {
            hyperactor::remote!(#data_type_name);
        });
    }

    TokenStream::from(expanded)
}

/// Represents the full input to [`fn behavior`].
struct BehaviorInput {
    behavior: Ident,
    generics: syn::Generics,
    handlers: Vec<HandlerSpec>,
}

impl syn::parse::Parse for BehaviorInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let behavior: Ident = input.parse()?;
        let generics: syn::Generics = input.parse()?;
        let _: Token![,] = input.parse()?;
        let raw_handlers = input.parse_terminated(HandlerSpec::parse, Token![,])?;
        let handlers = raw_handlers.into_iter().collect();
        Ok(BehaviorInput {
            behavior,
            generics,
            handlers,
        })
    }
}

/// Create a [`Referable`] definition, handling a specific set of message types.
/// Behaviors are used to create an [`ActorRef`] without having to depend on the
/// actor's implementation. If the message type need to be cast, add `castable`
/// flag to those types. e.g. the following example creates a behavior with 5
/// message types, and 4 of which need to be cast.
///
/// ```
/// hyperactor::behavior!(
///     TestActorBehavior,
///     TestMessage { castable = true },
///     () {castable = true },
///     MyGeneric<()> {castable = true },
///     u64,
/// );
/// ```
///
/// This macro also supports generic behaviors:
/// ```
/// hyperactor::behavior!(
///     TestBehavior<T>,
///     Message<T> { castable = true },
///     u64,
/// );
/// ```
#[proc_macro]
pub fn behavior(input: TokenStream) -> TokenStream {
    let BehaviorInput {
        behavior,
        generics,
        handlers,
    } = parse_macro_input!(input as BehaviorInput);
    let tys = HandlerSpec::add_indexed(handlers);

    // Add bounds to generics for Named, Serialize, Deserialize
    let mut bounded_generics = generics.clone();
    for param in bounded_generics.type_params_mut() {
        param.bounds.push(syn::parse_quote!(hyperactor::Named));
        param.bounds.push(syn::parse_quote!(serde::Serialize));
        param.bounds.push(syn::parse_quote!(std::marker::Send));
        param.bounds.push(syn::parse_quote!(std::marker::Sync));
        param.bounds.push(syn::parse_quote!(std::fmt::Debug));
        // Note: lifetime parameters are not *actually* hygienic.
        // https://github.com/rust-lang/rust/issues/54727
        let lifetime =
            syn::Lifetime::new("'hyperactor_behavior_de", proc_macro2::Span::mixed_site());
        param
            .bounds
            .push(syn::parse_quote!(for<#lifetime> serde::Deserialize<#lifetime>));
    }

    // Split the generics for use in different contexts
    let (impl_generics, ty_generics, where_clause) = bounded_generics.split_for_impl();

    // Create a combined generics for the Binds impl that includes both A and the behavior's generics
    let mut binds_generics = bounded_generics.clone();
    binds_generics.params.insert(
        0,
        syn::GenericParam::Type(syn::TypeParam {
            attrs: vec![],
            ident: Ident::new("A", proc_macro2::Span::call_site()),
            colon_token: None,
            bounds: Punctuated::new(),
            eq_token: None,
            default: None,
        }),
    );
    let (binds_impl_generics, _, _) = binds_generics.split_for_impl();

    // Determine typename and typehash implementation based on whether we have generics
    let type_params: Vec<_> = bounded_generics.type_params().collect();
    let has_generics = !type_params.is_empty();

    let (typename_impl, typehash_impl) = if has_generics {
        // Create format string with placeholders for each generic parameter
        let placeholders = vec!["{}"; type_params.len()].join(", ");
        let placeholders_format_string = format!("<{}>", placeholders);
        let format_string = quote! { concat!(std::module_path!(), "::", stringify!(#behavior), #placeholders_format_string) };

        let type_param_idents: Vec<_> = type_params.iter().map(|p| &p.ident).collect();
        (
            quote! {
                hyperactor::data::intern_typename!(Self, #format_string, #(#type_param_idents),*)
            },
            quote! {
                hyperactor::cityhasher::hash(Self::typename())
            },
        )
    } else {
        (
            quote! {
                concat!(std::module_path!(), "::", stringify!(#behavior))
            },
            quote! {
                static TYPEHASH: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
                    hyperactor::cityhasher::hash(<#behavior as hyperactor::data::Named>::typename())
                });
                *TYPEHASH
            },
        )
    };

    let type_param_idents = generics.type_params().map(|p| &p.ident).collect::<Vec<_>>();

    let expanded = quote! {
        #[doc = "The generated behavior struct."]
        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        pub struct #behavior #impl_generics #where_clause {
            _phantom: std::marker::PhantomData<(#(#type_param_idents),*)>
        }

        impl #impl_generics hyperactor::Named for #behavior #ty_generics #where_clause {
            fn typename() -> &'static str {
                #typename_impl
            }

            fn typehash() -> u64 {
                #typehash_impl
            }
        }

        impl #impl_generics hyperactor::actor::Referable for #behavior #ty_generics #where_clause {}

        impl #binds_impl_generics hyperactor::actor::Binds<A> for #behavior #ty_generics
        where
            A: hyperactor::Actor #(+ hyperactor::Handler<#tys>)*,
            #where_clause
        {
            fn bind(ports: &hyperactor::proc::Ports<A>) {
                #(
                    ports.bind::<#tys>();
                )*
            }
        }

        #(
            impl #impl_generics hyperactor::actor::RemoteHandles<#tys> for #behavior #ty_generics #where_clause {}
        )*
    };

    TokenStream::from(expanded)
}

fn include_in_bind_unbind(field: &Field) -> syn::Result<bool> {
    let mut is_included = false;
    for attr in &field.attrs {
        if attr.path().is_ident("binding") {
            // parse #[binding(include)] and look for exactly "include"
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("include") {
                    is_included = true;
                    Ok(())
                } else {
                    let path = meta.path.to_token_stream().to_string().replace(' ', "");
                    Err(meta.error(format_args!("unknown binding variant attribute `{}`", path)))
                }
            })?
        }
    }
    Ok(is_included)
}

/// The field accessor in struct or enum variant.
/// e.g.:
///   struct NamedStruct { foo: u32 } => FieldAccessor::Named(Ident::new("foo", Span::call_site()))
///   struct UnnamedStruct(u32) => FieldAccessor::Unnamed(Index::from(0))
enum FieldAccessor {
    Named(Ident),
    Unnamed(Index),
}

/// Result of parsing a field in a struct, or a enum variant.
struct ParsedField {
    accessor: FieldAccessor,
    ty: Type,
    included: bool,
}

impl From<&ParsedField> for (Ident, Type) {
    fn from(field: &ParsedField) -> Self {
        let field_ident = match &field.accessor {
            FieldAccessor::Named(ident) => ident.clone(),
            FieldAccessor::Unnamed(i) => {
                Ident::new(&format!("f{}", i.index), proc_macro2::Span::call_site())
            }
        };
        (field_ident, field.ty.clone())
    }
}

fn collect_all_fields(fields: &Fields) -> syn::Result<Vec<ParsedField>> {
    match fields {
        Fields::Named(named) => named
            .named
            .iter()
            .map(|f| {
                let accessor = FieldAccessor::Named(f.ident.clone().unwrap());
                Ok(ParsedField {
                    accessor,
                    ty: f.ty.clone(),
                    included: include_in_bind_unbind(f)?,
                })
            })
            .collect(),
        Fields::Unnamed(unnamed) => unnamed
            .unnamed
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let accessor = FieldAccessor::Unnamed(Index::from(i));
                Ok(ParsedField {
                    accessor,
                    ty: f.ty.clone(),
                    included: include_in_bind_unbind(f)?,
                })
            })
            .collect(),
        Fields::Unit => Ok(Vec::new()),
    }
}

fn gen_struct_items<F>(
    fields: &Fields,
    make_item: F,
    is_mutable: bool,
) -> syn::Result<Vec<proc_macro2::TokenStream>>
where
    F: Fn(proc_macro2::TokenStream, Type) -> proc_macro2::TokenStream,
{
    let borrow = if is_mutable {
        quote! { &mut }
    } else {
        quote! { & }
    };
    let items: Vec<_> = collect_all_fields(fields)?
        .into_iter()
        .filter(|f| f.included)
        .map(
            |ParsedField {
                 accessor,
                 ty,
                 included,
             }| {
                assert!(included);
                let field_accessor = match accessor {
                    FieldAccessor::Named(ident) => quote! { #borrow self.#ident },
                    FieldAccessor::Unnamed(index) => quote! { #borrow self.#index },
                };
                make_item(field_accessor, ty)
            },
        )
        .collect();
    Ok(items)
}

/// Generate the field accessor for a enum variant in pattern matching. e.g.
/// the <GENERATED> parts in the following example:
///
///   match my_enum {
///     // e.g. MyEnum::Tuple(_, f1, f2, _)
///     MyEnum::Tuple(<GENERATED>) => { ... }
///     // e.g. MyEnum::Struct { field0: _, field1 }
///     MyEnum::Struct(<GENERATED>) => { ... }
///   }
fn gen_enum_field_accessors(all_fields: &[ParsedField]) -> Vec<proc_macro2::TokenStream> {
    all_fields
        .iter()
        .map(
            |ParsedField {
                 accessor,
                 ty: _,
                 included,
             }| {
                match accessor {
                    FieldAccessor::Named(ident) => {
                        if *included {
                            quote! { #ident }
                        } else {
                            quote! { #ident: _ }
                        }
                    }
                    FieldAccessor::Unnamed(i) => {
                        if *included {
                            let ident = Ident::new(
                                &format!("f{}", i.index),
                                proc_macro2::Span::call_site(),
                            );
                            quote! { #ident }
                        } else {
                            quote! { _ }
                        }
                    }
                }
            },
        )
        .collect()
}

/// Generate all the parts for enum variants. e.g. the <GENERATED> part in the
/// following example:
///
///   match my_enum {
///      <GENERATED>
///   }
fn gen_enum_arms<F>(data: &DataEnum, make_item: F) -> syn::Result<Vec<proc_macro2::TokenStream>>
where
    F: Fn(proc_macro2::TokenStream, Type) -> proc_macro2::TokenStream,
{
    data.variants
        .iter()
        .map(|variant| {
            let name = &variant.ident;
            let all_fields = collect_all_fields(&variant.fields)?;
            let field_accessors = gen_enum_field_accessors(&all_fields);
            let included_fields = all_fields.iter().filter(|f| f.included).collect::<Vec<_>>();
            let items = included_fields
                .iter()
                .map(|f| {
                    let (accessor, ty) = <(Ident, Type)>::from(*f);
                    make_item(quote! { #accessor }, ty)
                })
                .collect::<Vec<_>>();

            Ok(match &variant.fields {
                Fields::Named(_) => {
                    quote! { Self::#name { #(#field_accessors),* } => { #(#items)* } }
                }
                Fields::Unnamed(_) => {
                    quote! { Self::#name( #(#field_accessors),* ) => { #(#items)* } }
                }
                Fields::Unit => quote! { Self::#name => { #(#items)* } },
            })
        })
        .collect()
}

/// Derive a custom implementation of [`hyperactor::message::Bind`] trait for
/// a struct or enum. This macro is normally used in tandem with [`fn derive_unbind`]
/// to make the applied struct or enum castable.
///
/// Specifically, the derived implementation iterates through fields annotated
/// with `#[binding(include)]` based on their order of declaration in the struct
/// or enum. These fields' types must implement `Bind` trait as well. During the
/// iteration, parameters from `bindings` are bound to these fields.
///
/// # Example
///
/// This macro supports named and unamed structs and enums. Below are examples
/// of the supported types:
///
/// ```
/// #[derive(Bind, Unbind)]
/// struct MyNamedStruct {
///     field0: u64,
///     field1: MyReply,
///     #[binding(include)]
/// nnnn     field2: PortRef<MyReply>,
///     field3: bool,
///     #[binding(include)]
///     field4: hyperactor::PortRef<u64>,
/// }
///
/// #[derive(Bind, Unbind)]
/// struct MyUnamedStruct(
///     u64,
///     MyReply,
///     #[binding(include)] hyperactor::PortRef<MyReply>,
///     bool,
///     #[binding(include)] PortRef<u64>,
/// );
///
/// #[derive(Bind, Unbind)]
/// enum MyEnum {
///     Unit,
///     NoopTuple(u64, bool),
///     NoopStruct {
///         field0: u64,
///         field1: bool,
///     },
///     Tuple(
///         u64,
///         MyReply,
///         #[binding(include)] PortRef<MyReply>,
///         bool,
///         #[binding(include)] hyperactor::PortRef<u64>,
///     ),
///     Struct {
///         field0: u64,
///         field1: MyReply,
///         #[binding(include)]
///         field2: PortRef<MyReply>,
///         field3: bool,
///         #[binding(include)]
///         field4: hyperactor::PortRef<u64>,
///     },
/// }
/// ```
///
/// The following shows what derived `Bind`` and `Unbind`` implementations for
/// `MyNamedStruct` will look like. The implementations of other types are
/// similar, and thus are not shown here.
/// ```ignore
/// impl Bind for MyNamedStruct {
/// fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()> {
///     Bind::bind(&mut self.field2, bindings)?;
///     Bind::bind(&mut self.field4, bindings)?;
///     Ok(())
/// }
///
/// impl Unbind for MyNamedStruct {
///     fn unbind(&self, bindings: &mut Bindings) -> anyhow::Result<()> {
///         Unbind::unbind(&self.field2, bindings)?;
///         Unbind::unbind(&self.field4, bindings)?;
///         Ok(())
///     }
/// }
/// ```
#[proc_macro_derive(Bind, attributes(binding))]
pub fn derive_bind(input: TokenStream) -> TokenStream {
    fn make_item(field_accessor: proc_macro2::TokenStream, _ty: Type) -> proc_macro2::TokenStream {
        quote! {
            hyperactor::message::Bind::bind(#field_accessor, bindings)?;
        }
    }

    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let inner = match &input.data {
        Data::Struct(DataStruct { fields, .. }) => {
            match gen_struct_items(fields, make_item, true) {
                Ok(collects) => {
                    quote! { #(#collects)* }
                }
                Err(e) => {
                    return TokenStream::from(e.to_compile_error());
                }
            }
        }
        Data::Enum(data_enum) => match gen_enum_arms(data_enum, make_item) {
            Ok(arms) => {
                quote! { match self { #(#arms),* } }
            }
            Err(e) => {
                return TokenStream::from(e.to_compile_error());
            }
        },
        _ => panic!("Bind can only be derived for structs and enums"),
    };
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let expand = quote! {
        #[automatically_derived]
        impl #impl_generics hyperactor::message::Bind for #name #ty_generics #where_clause {
            fn bind(&mut self, bindings: &mut hyperactor::message::Bindings) -> anyhow::Result<()> {
                #inner
                Ok(())
            }
        }
    };
    TokenStream::from(expand)
}

/// Derive a custom implementation of [`hyperactor::message::Unbind`] trait for
/// a struct or enum. This macro is normally used in tandem with [`fn derive_bind`]
/// to make the applied struct or enum castable.
///
/// Specifically, the derived implementation iterates through fields annoated
/// with `#[binding(include)]` based on their order of declaration in the struct
/// or enum. These fields' types must implement `Unbind` trait as well. During
/// the iteration, parameters from these fields are extracted and stored in
/// `bindings`.
///
/// # Example
///
/// See [`fn derive_bind`]'s documentation for examples.
#[proc_macro_derive(Unbind, attributes(binding))]
pub fn derive_unbind(input: TokenStream) -> TokenStream {
    fn make_item(field_accessor: proc_macro2::TokenStream, _ty: Type) -> proc_macro2::TokenStream {
        quote! {
            hyperactor::message::Unbind::unbind(#field_accessor, bindings)?;
        }
    }

    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let inner = match &input.data {
        Data::Struct(DataStruct { fields, .. }) => match gen_struct_items(fields, make_item, false)
        {
            Ok(collects) => {
                quote! { #(#collects)* }
            }
            Err(e) => {
                return TokenStream::from(e.to_compile_error());
            }
        },
        Data::Enum(data_enum) => match gen_enum_arms(data_enum, make_item) {
            Ok(arms) => {
                quote! { match self { #(#arms),* } }
            }
            Err(e) => {
                return TokenStream::from(e.to_compile_error());
            }
        },
        _ => panic!("Unbind can only be derived for structs and enums"),
    };
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let expand = quote! {
        #[automatically_derived]
        impl #impl_generics hyperactor::message::Unbind for #name #ty_generics #where_clause {
            fn unbind(&self, bindings: &mut hyperactor::message::Bindings) -> anyhow::Result<()> {
                #inner
                Ok(())
            }
        }
    };
    TokenStream::from(expand)
}

/// Derives the `Actor` trait for a struct. By default, generates an implementation
/// with no params (`type Params = ()` and `async fn new(_params: ()) -> Result<Self, anyhow::Error>`).
/// This requires that the Actor implements [`Default`].
///
/// If the `#[actor(passthrough)]` attribute is specified, generates an implementation
/// with where the parameter type is `Self`
/// (`type Params = Self` and `async fn new(instance: Self) -> Result<Self, anyhow::Error>`).
///
/// # Examples
///
/// Default behavior:
/// ```
/// #[derive(Actor, Default)]
/// struct MyActor(u64);
/// ```
///
/// Generates:
/// ```ignore
/// #[async_trait]
/// impl Actor for MyActor {
///     type Params = ();
///
///     async fn new(_params: ()) -> Result<Self, anyhow::Error> {
///         Ok(Default::default())
///     }
/// }
/// ```
///
/// Passthrough behavior:
/// ```
/// #[derive(Actor, Default)]
/// #[actor(passthrough)]
/// struct MyActor(u64);
/// ```
///
/// Generates:
/// ```ignore
/// #[async_trait]
/// impl Actor for MyActor {
///     type Params = Self;
///
///     async fn new(instance: Self) -> Result<Self, anyhow::Error> {
///         Ok(instance)
///     }
/// }
/// ```
#[proc_macro_derive(Actor, attributes(actor))]
pub fn derive_actor(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let is_passthrough = input.attrs.iter().any(|attr| {
        if attr.path().is_ident("actor") {
            if let Ok(meta) = attr.parse_args_with(
                syn::punctuated::Punctuated::<syn::Ident, syn::Token![,]>::parse_terminated,
            ) {
                return meta.iter().any(|ident| ident == "passthrough");
            }
        }
        false
    });

    let expanded = if is_passthrough {
        quote! {
            #[hyperactor::async_trait::async_trait]
            impl #impl_generics hyperactor::Actor for #name #ty_generics #where_clause {
                type Params = Self;

                async fn new(instance: Self) -> Result<Self, hyperactor::anyhow::Error> {
                    Ok(instance)
                }
            }
        }
    } else {
        quote! {
            #[hyperactor::async_trait::async_trait]
            impl #impl_generics hyperactor::Actor for #name #ty_generics #where_clause {
                type Params = ();

                async fn new(_params: ()) -> Result<Self, hyperactor::anyhow::Error> {
                    Ok(Default::default())
                }
            }
        }
    };

    TokenStream::from(expanded)
}

// Helper function for common parsing and validation
fn parse_observe_function(
    attr: TokenStream,
    item: TokenStream,
) -> syn::Result<(ItemFn, String, String)> {
    let input = syn::parse::<ItemFn>(item)?;

    if input.sig.asyncness.is_none() {
        return Err(syn::Error::new(
            input.sig.span(),
            "observe macros can only be applied to async functions",
        ));
    }

    let fn_name_str = input.sig.ident.to_string();
    let module_name_str = syn::parse::<syn::LitStr>(attr)?.value();

    Ok((input, fn_name_str, module_name_str))
}

// Helper function for creating telemetry identifiers and setup code
fn create_telemetry_setup(
    module_name_str: &str,
    fn_name_str: &str,
    include_error: bool,
) -> (Ident, Ident, Option<Ident>, proc_macro2::TokenStream) {
    let module_and_fn = format!("{}_{}", module_name_str, fn_name_str);
    let latency_ident = Ident::new("latency", Span::from(proc_macro::Span::def_site()));

    let success_ident = Ident::new("success", Span::from(proc_macro::Span::def_site()));

    let error_ident = if include_error {
        Some(Ident::new(
            "error",
            Span::from(proc_macro::Span::def_site()),
        ))
    } else {
        None
    };

    let error_declaration = if let Some(ref error_ident) = error_ident {
        quote! {
            hyperactor_telemetry::declare_static_counter!(#error_ident, concat!(#module_and_fn, ".error"));
        }
    } else {
        quote! {}
    };

    let setup_code = quote! {
        use hyperactor_telemetry;
        hyperactor_telemetry::declare_static_timer!(#latency_ident, concat!(#module_and_fn, ".latency"), hyperactor_telemetry::TimeUnit::Micros);
        hyperactor_telemetry::declare_static_counter!(#success_ident, concat!(#module_and_fn, ".success"));
        #error_declaration
    };

    (latency_ident, success_ident, error_ident, setup_code)
}

/// A procedural macro that automatically injects telemetry code into async functions
/// that return a Result type.
///
/// This macro wraps async functions and adds instrumentation to measure:
/// 1. Latency - how long the function takes to execute
/// 2. Error counter - function error count
/// 3. Success counter - function completion count
///
/// # Example
///
/// ```rust
/// use hyperactor_actor::observe_result;
///
/// #[observe_result("my_module")]
/// async fn process_request(user_id: &str) -> Result<String, Error> {
///     // Function implementation
///     // Telemetry will be automatically collected
/// }
/// ```
#[proc_macro_attribute]
pub fn observe_result(attr: TokenStream, item: TokenStream) -> TokenStream {
    let (input, fn_name_str, module_name_str) = match parse_observe_function(attr, item) {
        Ok(parsed) => parsed,
        Err(err) => return err.to_compile_error().into(),
    };

    let fn_name = &input.sig.ident;
    let vis = &input.vis;
    let args = &input.sig.inputs;
    let return_type = &input.sig.output;
    let body = &input.block;
    let attrs = &input.attrs;
    let generics = &input.sig.generics;

    let (latency_ident, success_ident, error_ident, telemetry_setup) =
        create_telemetry_setup(&module_name_str, &fn_name_str, true);
    let error_ident = error_ident.unwrap();

    let result_ident = Ident::new("result", Span::from(proc_macro::Span::def_site()));

    // Generate the instrumented function
    let expanded = quote! {
        #(#attrs)*
        #vis async fn #fn_name #generics(#args) #return_type {
            #telemetry_setup

            let kv_pairs = hyperactor_telemetry::kv_pairs!("function" => #fn_name_str.clone());
            let _timer = #latency_ident.start(kv_pairs);

            let #result_ident = async #body.await;

            match &#result_ident {
                Ok(_) => {
                    #success_ident.add(
                        1,
                        hyperactor_telemetry::kv_pairs!("function" => #fn_name_str.clone())
                    );
                }
                Err(_) => {
                    #error_ident.add(
                        1,
                        hyperactor_telemetry::kv_pairs!("function" => #fn_name_str.clone())
                    );
                }
            }

            #result_ident
        }
    };

    expanded.into()
}

/// A procedural macro that automatically injects telemetry code into async functions
/// that do not return a Result type.
///
/// This macro wraps async functions and adds instrumentation to measure:
/// 1. Latency - how long the function takes to execute
/// 2. Success counter - function completion count
///
/// # Example
///
/// ```rust
/// use hyperactor_actor::observe_async;
///
/// #[observe_async("my_module")]
/// async fn process_data(data: &str) -> String {
///     // Function implementation
///     // Telemetry will be automatically collected
/// }
/// ```
#[proc_macro_attribute]
pub fn observe_async(attr: TokenStream, item: TokenStream) -> TokenStream {
    let (input, fn_name_str, module_name_str) = match parse_observe_function(attr, item) {
        Ok(parsed) => parsed,
        Err(err) => return err.to_compile_error().into(),
    };

    let fn_name = &input.sig.ident;
    let vis = &input.vis;
    let args = &input.sig.inputs;
    let return_type = &input.sig.output;
    let body = &input.block;
    let attrs = &input.attrs;
    let generics = &input.sig.generics;

    let (latency_ident, success_ident, _, telemetry_setup) =
        create_telemetry_setup(&module_name_str, &fn_name_str, false);

    let return_ident = Ident::new("ret", Span::from(proc_macro::Span::def_site()));

    // Generate the instrumented function
    let expanded = quote! {
        #(#attrs)*
        #vis async fn #fn_name #generics(#args) #return_type {
            #telemetry_setup

            let kv_pairs = hyperactor_telemetry::kv_pairs!("function" => #fn_name_str.clone());
            let _timer = #latency_ident.start(kv_pairs);

            let #return_ident = async #body.await;

            #success_ident.add(
                1,
                hyperactor_telemetry::kv_pairs!("function" => #fn_name_str.clone())
            );
            #return_ident
        }
    };

    expanded.into()
}

/// Derive the [`hyperactor::attrs::AttrValue`] trait for a struct or enum.
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
/// ```
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
        impl #impl_generics hyperactor::attrs::AttrValue for #name #ty_generics #where_clause {
            fn display(&self) -> String {
                self.to_string()
            }

            fn parse(value: &str) -> Result<Self, anyhow::Error> {
                value.parse().map_err(|e| anyhow::anyhow!("failed to parse {}: {}", stringify!(#name), e))
            }
        }
    })
}
