/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use proc_macro::TokenStream;
use quote::quote;
use syn::Expr;
use syn::ItemFn;
use syn::Lit;
use syn::MetaNameValue;
use syn::parse_macro_input;
use syn::parse_quote;

/// A test macro that wraps tokio::test and adds a configurable timeout.
///
/// # Examples
///
/// ```rust
/// #[async_timed_test(timeout_secs = 5)]
/// async fn my_test() {
///     // Test that should complete within 5 seconds
///     tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
/// }
/// ```
#[proc_macro_attribute]
pub fn async_timed_test(attr: TokenStream, input: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as MetaNameValue);
    let input_fn = parse_macro_input!(input as ItemFn);

    if !attr.path.is_ident("timeout_secs") {
        return TokenStream::from(
            syn::Error::new_spanned(attr.path, "only timeout_secs allowed as argument")
                .to_compile_error(),
        );
    }
    let timeout_secs = match attr.value {
        Expr::Lit(ref lit) => match &lit.lit {
            Lit::Int(val) => val.base10_parse::<u64>().unwrap(),
            _ => {
                return TokenStream::from(
                    syn::Error::new_spanned(
                        attr.value,
                        "unexpected value for timeout_secs, please pass an integer literal",
                    )
                    .to_compile_error(),
                );
            }
        },
        _ => {
            return TokenStream::from(
                syn::Error::new_spanned(
                    attr.value,
                    "unexpected value for timeout_secs, please pass an integer literal",
                )
                .to_compile_error(),
            );
        }
    };

    let fn_block = &input_fn.block;
    let fn_attrs = &input_fn.attrs;
    let fn_vis = &input_fn.vis;
    let sig = &input_fn.sig;
    let fn_name = &sig.ident;
    let output = &sig.output;

    if sig.asyncness.is_none() {
        return TokenStream::from(
            syn::Error::new_spanned(sig, "test function must be async").to_compile_error(),
        );
    }

    let output = quote! {
        #[test]
        #(#fn_attrs)*
        #fn_vis fn #fn_name() #output {
            use tokio::runtime::Runtime;
            use std::sync::mpsc::{channel, RecvTimeoutError};
            use std::thread;
            use std::time::Duration;

            let (result_tx, result_rx) = channel();

            // Create a separate thread to drive the test runtime. This is to
            // ensure that even if the runtime gets stuck somehow, we will still
            // be able to enforce the timeout.
            thread::spawn(move || {
                let test_rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(8)
                    .enable_all()
                    .build()
                    .unwrap();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    test_rt.block_on(async #fn_block)
                }));
                let _ = result_tx.send(result);
            });

            // Wait with timeout - guaranteed to fire independently of test runtime
            match result_rx.recv_timeout(Duration::from_secs(#timeout_secs)) {
                Ok(result) => match result {
                    Ok(test_result) => test_result,
                    Err(panic) => std::panic::resume_unwind(panic),
                },
                Err(RecvTimeoutError::Timeout) => {
                    panic!("test timed out after {} seconds", #timeout_secs);
                },
                Err(RecvTimeoutError::Disconnected) => {
                    panic!("test thread panicked without sending result");
                }
            }
        }
    };

    output.into()
}

/// Wrap a test body with a Linux child process leak guard.
///
/// The crate using this macro must define
/// `crate::testing::ChildProcessGuard` on Linux.
#[proc_macro_attribute]
pub fn assert_no_process_leak(attr: TokenStream, input: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return quote!(compile_error!("assert_no_process_leak does not accept arguments");).into();
    }

    let mut input_fn = parse_macro_input!(input as ItemFn);
    let fn_block = input_fn.block;
    input_fn.block = Box::new(parse_quote!({
        #[cfg(target_os = "linux")]
        let _child_process_guard = crate::testing::ChildProcessGuard::new();
        #fn_block
    }));
    quote!(#input_fn).into()
}
