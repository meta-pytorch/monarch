/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Operation-context carrier for the Python endpoint machinery.
//!
//! Stamps `OPERATION_ENDPOINT` / `OPERATION_ADVERB` on outgoing
//! requests so failure surfaces (mailbox log, undeliverable
//! formatter) can name the operation.
//!
//! ## Operation-context invariants (OC-*)
//!
//! - **OC-1 (request-originated).** Stamped at the caller's
//!   request-send site. Only the caller knows the qualified
//!   endpoint and adverb.
//!
//! - **OC-2 (filtered carry).** Only keys marked
//!   `@meta(OPERATION_CONTEXT_HEADER = true)` ride this path.
//!   Adding another `OPERATION_*` key joins the vocabulary by
//!   declaration alone.
//!
//! - **OC-3 (carry continuity).** Captured headers are preserved
//!   onto the reply envelope at the `Port` capture site via
//!   `send_with_headers`.

use hyperactor::mailbox::headers::OPERATION_ADVERB;
use hyperactor::mailbox::headers::OPERATION_ENDPOINT;
use hyperactor_config::Attrs;
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::OPERATION_CONTEXT_HEADER;
use hyperactor_config::attrs::stamp_marked_attrs_into_flattrs;

/// Build an `Attrs` carrier populated with the operation-context keys
/// an endpoint-send site wants to stamp onto its outgoing request.
///
/// Callers supply the values they know in scope at the send site
/// (qualified endpoint name, adverb token). Any `None` is simply
/// omitted — the marker-driven stamp filters on presence.
pub fn build_operation_context_attrs(
    endpoint: Option<String>,
    adverb: Option<&'static str>,
) -> Attrs {
    let mut attrs = Attrs::new();
    if let Some(ep) = endpoint {
        attrs.set(OPERATION_ENDPOINT, ep);
    }
    if let Some(a) = adverb {
        attrs.set(OPERATION_ADVERB, a.to_string());
    }
    attrs
}

/// Stamp the operation-context subset of `attrs` onto `headers` using
/// the shared marker-driven mechanism. Only entries whose declared
/// key carries `@meta(OPERATION_CONTEXT_HEADER = true)` are written;
/// anything else in `attrs` is silently skipped (OC-2).
pub fn stamp_operation_context(headers: &mut Flattrs, attrs: &Attrs) {
    stamp_marked_attrs_into_flattrs(headers, attrs, OPERATION_CONTEXT_HEADER);
}

#[cfg(test)]
mod tests {
    use hyperactor_config::attrs::copy_marked_flattrs;

    use super::*;

    /// OC-2: the shared marker-driven copy propagates exactly the
    /// `OPERATION_CONTEXT_HEADER`-marked keys from the source headers
    /// onto the destination. Unrelated declared `Flattrs` entries
    /// (e.g. `SEND_TIMESTAMP`) are excluded even though they're
    /// present on the source.
    #[test]
    fn test_oc2_filter_rejects_unmarked_keys() {
        let mut src = Flattrs::new();
        src.set(OPERATION_ENDPOINT, "training.buffer.sample()".to_string());
        src.set(
            hyperactor::mailbox::headers::SEND_TIMESTAMP,
            std::time::SystemTime::UNIX_EPOCH,
        );

        let mut dst = Flattrs::new();
        copy_marked_flattrs(&mut dst, &src, OPERATION_CONTEXT_HEADER);

        assert_eq!(
            dst.get(OPERATION_ENDPOINT),
            Some("training.buffer.sample()".to_string()),
            "OC-2: marked key must cross the carry"
        );
        assert!(
            dst.get(hyperactor::mailbox::headers::SEND_TIMESTAMP)
                .is_none(),
            "OC-2: unmarked key must not cross the carry"
        );
    }

    /// OC-1: the caller-side helper assembles the operation-context
    /// attrs from values known at the request-send site, rather
    /// than synthesized at the reply site. `build_operation_context_attrs`
    /// is the vehicle for that origination step.
    #[test]
    fn test_oc1_build_attrs_populates_supplied_fields() {
        let attrs = build_operation_context_attrs(
            Some("training.buffer.sample()".to_string()),
            Some("call_one"),
        );
        assert_eq!(
            attrs.get(OPERATION_ENDPOINT),
            Some(&"training.buffer.sample()".to_string())
        );
        assert_eq!(attrs.get(OPERATION_ADVERB), Some(&"call_one".to_string()));
    }

    /// OC-1: fields the request site cannot supply must not be
    /// fabricated downstream. `build_operation_context_attrs` omits
    /// them when the caller passes `None`.
    #[test]
    fn test_oc1_build_attrs_omits_none_fields() {
        let attrs =
            build_operation_context_attrs(Some("training.buffer.sample()".to_string()), None);
        assert_eq!(
            attrs.get(OPERATION_ENDPOINT),
            Some(&"training.buffer.sample()".to_string())
        );
        assert!(attrs.get(OPERATION_ADVERB).is_none());
    }

    /// OC-2: the stamp helper writes only the
    /// `OPERATION_CONTEXT_HEADER`-marked keys onto the outgoing
    /// headers. Same mechanism as the OC-2 copy test above, on the
    /// `Attrs -> Flattrs` direction.
    #[test]
    fn test_oc2_stamp_writes_marked_keys_to_headers() {
        let attrs = build_operation_context_attrs(
            Some("training.buffer.sample()".to_string()),
            Some("call_one"),
        );
        let mut headers = Flattrs::new();
        stamp_operation_context(&mut headers, &attrs);

        assert_eq!(
            headers.get(OPERATION_ENDPOINT),
            Some("training.buffer.sample()".to_string())
        );
        assert_eq!(headers.get(OPERATION_ADVERB), Some("call_one".to_string()));
    }
}
