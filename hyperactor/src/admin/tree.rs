/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ASCII tree rendering for proc and actor hierarchies.

use crate::Proc;
use crate::reference::ActorId;

/// Renders an ASCII tree of all actors in a proc (without URLs).
pub fn format_proc_tree(proc: &Proc) -> String {
    format_proc_tree_with_urls(proc, None)
}

/// Renders an ASCII tree of all actors in a proc, optionally with URLs.
pub fn format_proc_tree_with_urls(proc: &Proc, base_url: Option<&str>) -> String {
    let mut nodes: Vec<(ActorId, usize)> = Vec::new();
    proc.traverse(&mut |cell, depth| {
        nodes.push((cell.actor_id().clone(), depth));
    });
    render_tree(&nodes, base_url)
}

/// URL-encode a string for use in a URL path component.
fn url_encode_path(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 3);
    for c in s.chars() {
        match c {
            // Unreserved characters (RFC 3986)
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                result.push(c);
            }
            // Encode everything else
            _ => {
                for byte in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", byte));
                }
            }
        }
    }
    result
}

/// Renders a list of (actor_id, depth) pairs as an ASCII tree.
fn render_tree(nodes: &[(ActorId, usize)], base_url: Option<&str>) -> String {
    let mut output = String::new();
    for (i, (actor_id, depth)) in nodes.iter().enumerate() {
        let url_suffix = base_url
            .map(|base| {
                let encoded = url_encode_path(&actor_id.to_string());
                format!("  ->  {}/{}", base.trim_end_matches('/'), encoded)
            })
            .unwrap_or_default();

        if *depth == 0 {
            output.push_str(&format!("{}{}\n", actor_id, url_suffix));
        } else {
            // Find if this is the last sibling at this depth
            let is_last = nodes[i + 1..]
                .iter()
                .take_while(|(_, d)| *d >= *depth)
                .all(|(_, d)| *d > *depth);

            let mut prefix = String::new();
            for d in 1..*depth {
                // Check if there are more siblings at depth d after this node
                let has_more_at_d = nodes[i + 1..].iter().any(|(_, dd)| *dd == d);
                prefix.push_str(if has_more_at_d { "│   " } else { "    " });
            }
            prefix.push_str(if is_last { "└── " } else { "├── " });
            output.push_str(&format!("{}{}{}\n", prefix, actor_id, url_suffix));
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_empty_tree() {
        let nodes: Vec<(ActorId, usize)> = Vec::new();
        let result = render_tree(&nodes, None);
        assert_eq!(result, "");
    }

    #[test]
    fn test_url_encode_path() {
        assert_eq!(url_encode_path("simple"), "simple");
        assert_eq!(url_encode_path("with[brackets]"), "with%5Bbrackets%5D");
        assert_eq!(url_encode_path("with,comma"), "with%2Ccomma");
        assert_eq!(url_encode_path("with@at"), "with%40at");
        assert_eq!(url_encode_path("with:colon"), "with%3Acolon");
    }
}
