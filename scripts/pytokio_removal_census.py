#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Checked inventory of the remaining ``pytokio`` surface.

Temporary migration tooling for the Stage 6 pytokio removal. It reconciles the
legacy sites discovered in Monarch source with the reviewed rows in
``pytokio_removal_census.toml``, so that no later diff can add, remove, or
reclassify a pytokio boundary without an explicit manifest edit.

Two modes:

``--check``
    Fail on an unknown hit, a stale row, a duplicate or missing identifier, a
    total mismatch, a missing required field, an import member-set change, an
    undeclared or unowned transition, or a malformed migration revision. This
    is the only landing gate.

``--summary``
    Print active and tombstoned entry counts by category, file, and owning
    transition.

Run from the fbsource repository root. Stdlib only, read-only. Deleted in
Stage 6.8 once the independent searches are zero.

For operational usage -- what the sections mean, what each behavior field
records, how states and transitions work, and the drift and update workflow --
read the guide at the top of ``pytokio_removal_census.toml``. That guide is the
reference for maintaining the data; this docstring is the reference for
maintaining the code. The two are deliberately not duplicates.

Implementation guide
====================

Pipeline. ``check`` and ``summarize`` both drive the same front half:

    load manifest
      -> enumerate bounded sources        (source_files)
      -> language-specific discovery      (scan_rust, scan_python, scan_stub,
                                           scan_text)
      -> normalize and collapse hits      (collapse, applied by discover)
      -> validate schema and transitions  (validate_rows, validate_transitions,
                                           validate_matrix)
      -> reconcile rows against hits      (reconcile)
      -> validate totals                  (validate_totals)
      -> report                           (error list, or summarize)

``discover`` owns the first three steps and returns normalized hits.
Everything after it is pure comparison between those hits and the manifest.

The Hit model. A ``Hit`` is one discovered occurrence: path, symbol, operation,
line, and optionally the imported ``members`` set or a regex ``capture``.
Identity is ``(path, symbol, operation)`` -- the ``locator`` -- and the line is
carried for diagnostics only. Keeping the line out of identity is what lets an
unrelated edit move code without invalidating rows.

Rust lexical views. Rust has no stdlib parser here, so ``lex_rust`` does the
one job a regex cannot: it walks the whole file once and returns two views of
it, both the same length and with the same newline positions as the source, so
an offset in either maps back to a line.

    scannable   comments blanked, string and char literals kept. This is what
                patterns match against, so prose in a ``//``, ``///`` or
                ``/* */`` comment is never inventoried as a call site, while a
                symbol named inside a string literal -- a config key, say --
                still is.
    structural  comments *and* literal contents blanked. This is what block
                depth is counted from, so a brace inside a string or a ``'{'``
                char cannot close a block early.

Using one view for both jobs is what produced the two classes of bug this
replaced: matching raw lines inventoried documentation, and counting braces
from a naively comment-stripped line let a ``//`` inside a string discard the
line's closing brace. Whole-file rather than per-line because block comments
nest, and both block comments and raw strings carry state across newlines.

Rust ownership parsing. ``rust_symbols`` consumes the structural view and
returns the qualified enclosing symbol for every line, so a hit can be
attributed without a second pass. It tracks ``impl`` and ``trait`` blocks on a
stack keyed by brace depth and resolves each impl header through
``parse_impl_header``, which skips balanced generics and splits on the ``for``
at depth zero. The result is ``Type::method``, ``<Type as Trait>::method``, or
``Trait::method`` for a trait default body -- which is what distinguishes two
methods sharing a bare name, and what keeps a default method's identity stable
only while it stays in its trait.

``unsafe impl`` and ``default impl`` are recognized as impl headers. They are
common, and matching only a bare ``impl`` would attribute their methods as free
functions.

Headers that span lines are buffered until their opening brace. This parsing
fails closed on purpose: an unbalanced generic or a header that never
terminates raises ``CensusError`` rather than guessing, because a wrong
qualifier would silently attribute a block's methods to another type and the
manifest would then encode that mistake.

``scan_rust`` matches over the whole scannable view rather than line by line,
under ``re.MULTILINE`` so a leading ``^`` still anchors per line. Whole-file
matching is what lets a pattern reach a macro's arguments on the lines below
the line that opens it, and capture from them. Each match is attributed to the
symbol owning the line where it starts.

Python identity. Calls and imports are found through ``ast`` rather than line
matching, so a construct split across lines is found and a match inside a
string or comment is not. Relative imports are resolved against the file's
repository path before comparison, and matched by dotted suffix, because the
resolved name carries the directories above the Python package root that the
configured literal does not. Ignoring ``node.level`` would make every relative
pytokio import invisible. A call site's symbol is its enclosing scope plus the
unparsed call target and first argument, ``scope[target(arg)]``. Both halves
matter: one function can hold two distinct roots, and one function can take the
inner task of several different receivers. ``ast.unparse`` is used so that
subscripts and chained calls survive; a lossy renderer would merge sites that
are genuinely distinct.

Narrower textual scanners. Stubs and documents have no call graph to walk.
``scan_stub`` matches declarations, and ``scan_text`` records at most one hit
per document, because a document is inventoried as "this file mentions the
legacy surface", not once per mention. Both are deliberately weaker than the
AST path and are used only where an AST would not apply.

Normalization. Some operations are reference-shaped rather than site-shaped.
``collapse`` reduces those to one hit per surface: operations listed in
``symbol_capture_operations`` collapse per file and captured symbol, so a
helper is inventoried as "which files use it" and a second reference in the
same file does not create a second row; operations listed in
``per_file_operations`` collapse to one hit per file. Everything else passes
through untouched. ``collapse`` is defined among the scanners but runs last,
from ``discover``.

Every alternative of a ``symbol_capture_operations`` pattern must carry a
capture group, and ``collapse`` raises when a matched hit has none. Falling
back to the enclosing symbol -- the obvious-looking default -- would key two
distinct sites in one scope to the same locator and silently merge them, which
is a false negative in the central bijection.

Scope. ``source_files`` enumerates the Monarch roots only. Downstream callers
of the public pytokio API are a stated exclusion, swept with code search rather
than content-scanned here; the manifest guide records why and what it costs.

Four kinds of checking, which are easy to confuse:

    schema validation      (validate_rows, validate_matrix) -- is each row
                           well formed, are its enum values declared, is its
                           identity unique?
    transition validation  (validate_transitions) -- does the owning transition
                           declare this site, does it permit the state, and is
                           the provenance revision well formed and consistent
                           with the state?
    reconciliation         (reconcile) -- does the source agree with the
                           manifest, in both directions?
    totals                 (validate_totals) -- does the expected aggregate
                           count agree, and does every configured operation
                           have a total? Reconciliation is what catches a
                           net-zero swap, reporting the missing locator and
                           the unknown one. Totals are an independent
                           aggregate and parity check alongside it, not a
                           backstop for that case.

``check`` withholds rows that failed the required-field gate from the
validators that index those fields. Those rows are already reported; passing
them on would raise ``KeyError`` and replace the accumulated error list with a
traceback.

Tombstones participate in the first two and not the third. A migrated or
removed_upstream row is still schema- and transition-validated, and still holds
its locator against reuse, but ``reconcile`` skips it, because by definition its
source hit is gone. That is what makes a completed migration auditable without
making it look like drift.

Adding a new tracked operation. Five edits, all in one diff:

    1. a pattern under the appropriate ``[config.patterns.*]`` table in the
       manifest, plus ``operation_paths`` scoping if the symbol has an
       unrelated homonym elsewhere in the tree;
    2. an expected count in ``[totals]``, and a ``[total_units]`` entry if it
       is counted by file rather than by hit;
    3. one ``[[site]]`` row per discovered hit, with a category drawn from
       ``schema.categories``;
    4. ownership: each new row listed in the ``owns`` array of the transition
       that removes it; and
    5. a counterexample fixture in ``test_pytokio_removal_census.py`` proving
       the new operation fails when it should -- an unexpected hit, a missing
       hit, or a mis-scoped homonym.

Pattern literals stay in the manifest rather than here, so this file does not
itself register as pytokio residue in the source-zero gates. Do not move them
into the Python.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
import tomllib
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

MANIFEST = Path(__file__).with_suffix(".toml")

# Rows whose semantic classification carries migration risk, and which
# therefore must state the full set of behavior fields.
BEHAVIOR_KINDS = frozenset({"native_producer", "coroutine_root"})

REQUIRED_FIELDS = (
    "id",
    "category",
    "language",
    "path",
    "symbol",
    "operation",
    "scope",
    "state",
    "transition",
)

REQUIRED_BEHAVIOR_FIELDS = (
    "return_surface",
    "consumer",
    "driver",
    "start_point",
    "abandonment",
    "eager_effect",
    "drop_behavior",
    "unobserved_error",
    "disposition",
    "oracle",
    "semantic_class",
)

# Reviewed facts a coroutine-root row carries beyond the behavior set: what
# public API the coroutine backs, who calls it, and what it does first.
REQUIRED_COROUTINE_ROOT_FIELDS = (
    "public_operation",
    "caller_contexts",
    "first_side_effect",
)

REQUIRED_MATRIX_FIELDS = ("id", "disposition", "execution_state")

REVISION = re.compile(r"^D\d{6,}$")

LEGACY = "legacy"
TOMBSTONES = frozenset({"migrated", "removed_upstream"})
VALID_STATES = frozenset({LEGACY}) | TOMBSTONES

IMPL_START = re.compile(r"^\s*(?:unsafe\s+|default\s+)*impl\b")
TRAIT_START = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:unsafe\s+)?trait\s+(?P<name>[A-Za-z_]\w*)"
)
FN_RE = re.compile(r"\bfn\s+(?P<name>[A-Za-z_]\w*)")


# --------------------------------------------------------------------------
# Discovery: Rust ownership parsing
# --------------------------------------------------------------------------


def lex_rust(text: str) -> tuple[str, str]:
    """Two aligned lexical views of a whole Rust file.

    Both results have the same length and newline positions as the input, so an
    offset in either maps back to a source line. Removed characters become
    spaces rather than disappearing.

    ``scannable`` blanks comments but keeps string and char literal contents,
    so a tracked symbol named inside a string -- a config key, say -- is still
    inventoried. ``structural`` additionally blanks literal contents, so a
    brace inside a string or a ``'{'`` char cannot corrupt block-depth
    accounting.

    Whole-file rather than per-line because block comments, nested block
    comments and multi-line raw strings all carry state across newlines. A
    per-line pass reads the second line of ``/* ... */`` as code.
    """
    scannable: list[str] = []
    structural: list[str] = []
    index = 0
    length = len(text)

    def emit(chunk: str, keep: bool) -> None:
        blanked = "".join(c if c == "\n" else " " for c in chunk)
        scannable.append(chunk if keep else blanked)
        structural.append(blanked)

    while index < length:
        char = text[index]
        if char == "/" and text.startswith("//", index):
            end = text.find("\n", index)
            end = length if end == -1 else end
            emit(text[index:end], keep=False)
            index = end
            continue
        if char == "/" and text.startswith("/*", index):
            depth = 0
            end = index
            while end < length:
                if text.startswith("/*", end):
                    depth += 1
                    end += 2
                    continue
                if text.startswith("*/", end):
                    depth -= 1
                    end += 2
                    if not depth:
                        break
                    continue
                end += 1
            emit(text[index:end], keep=False)
            index = end
            continue
        raw = _raw_string_span(text, index)
        if raw is not None:
            emit(text[index:raw], keep=True)
            index = raw
            continue
        if char == '"':
            end = index + 1
            while end < length:
                if text[end] == "\\":
                    end += 2
                    continue
                if text[end] == '"':
                    end += 1
                    break
                end += 1
            # Literal text stays scannable but is blanked structurally.
            scannable.append(text[index:end])
            structural.append("".join(c if c == "\n" else " " for c in text[index:end]))
            index = end
            continue
        if char == "'" and _is_char_literal(text, index):
            end = index + 1
            while end < length:
                if text[end] == "\\":
                    end += 2
                    continue
                if text[end] == "'":
                    end += 1
                    break
                end += 1
            scannable.append(text[index:end])
            structural.append(" " * (end - index))
            index = end
            continue
        scannable.append(char)
        structural.append(char)
        index += 1
    return "".join(scannable), "".join(structural)


def _raw_string_span(text: str, index: int) -> int | None:
    """End offset of a raw string starting at ``index``, or ``None``.

    Handles ``r"..."`` and ``r#*"..."#*``. Raw strings honour no escapes, so a
    trailing backslash must not swallow the closing quote.
    """
    cursor = index
    if text[cursor] == "b":
        cursor += 1
    if cursor >= len(text) or text[cursor] != "r":
        return None
    cursor += 1
    hashes = 0
    while cursor < len(text) and text[cursor] == "#":
        hashes += 1
        cursor += 1
    if cursor >= len(text) or text[cursor] != '"':
        return None
    terminator = '"' + "#" * hashes
    end = text.find(terminator, cursor + 1)
    if end == -1:
        raise CensusError(f"unterminated raw string at offset {index}")
    return end + len(terminator)


def _is_char_literal(text: str, index: int) -> bool:
    """Whether the quote at ``index`` opens a char literal or a lifetime.

    Exact shapes rather than a lookahead heuristic. A quote followed by a
    backslash is an escaped char literal -- ``'\\n'``, ``'\\u{1f600}'`` -- and is
    scanned to its closing quote. Otherwise it is a char literal only when it
    closes immediately after one codepoint, ``'a'``.

    Everything else is a lifetime. This matters for adjacent lifetimes:
    ``'a, 'b`` offers a second quote four characters along, and treating that
    as a closing quote blanks the span between them, corrupting the impl
    header that encloses it.
    """
    if index + 1 >= len(text):
        return False
    if text[index + 1] == "\\":
        return True
    return index + 2 < len(text) and text[index + 2] == "'"


def _closes_generic(text: str, index: int) -> bool:
    """Whether ``text[index]`` closes a generic bracket.

    The ``>`` of a ``->`` return arrow is not a bracket. Counting it closes a
    generic early and silently mis-slices the header around it, so every
    generic-depth counter in this module goes through here rather than testing
    the character directly.
    """
    return text[index] == ">" and not (index and text[index - 1] == "-")


def _skip_generics(text: str, pos: int) -> int:
    """Index just past a balanced ``<...>`` at ``pos``, or ``pos`` if absent."""
    if pos >= len(text) or text[pos] != "<":
        return pos
    depth = 0
    for index in range(pos, len(text)):
        if text[index] == "<":
            depth += 1
        elif _closes_generic(text, index):
            depth -= 1
            if depth == 0:
                return index + 1
    raise CensusError(f"unbalanced generics in impl header: {text.strip()[:80]!r}")


def _split_for(text: str) -> tuple[str, str]:
    """Split an impl header on the ``for`` that is outside any generic."""
    depth = 0
    index = 0
    while index < len(text):
        char = text[index]
        if char == "<":
            depth += 1
        elif _closes_generic(text, index):
            depth -= 1
        elif depth == 0 and text.startswith(" for ", index):
            return text[:index].strip(), text[index + 5 :].strip()
        index += 1
    return "", text.strip()


def _split_top_level(text: str) -> list[str]:
    """Split a generic argument list on the commas outside nested brackets."""
    parts: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(text):
        if char == "<":
            depth += 1
        elif _closes_generic(text, index):
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(text[start:index])
            start = index + 1
    parts.append(text[start:])
    return [part.strip() for part in parts if part.strip()]


def normalize_type(name: str) -> str:
    """Canonical form of a type or trait reference, generic arguments retained.

    Generic arguments are part of identity. ``ActorMeshRef<PythonActor>`` and
    ``ActorMeshRef<OtherActor>`` are separate impls with separate method
    bodies; erasing the argument to a bare ``ActorMeshRef`` would give two
    producers one locator and silently merge them, so moving a producer from
    one specialization to the other would pass the gate unnoticed.

    Whitespace is canonicalized so that reformatting a header is not a move.
    Lifetime arguments are dropped: ``Thing<'a>`` and ``Thing<'b>`` are the same
    type, so keeping them would add churn without distinguishing anything.
    """
    text = " ".join(name.split())
    head, bracket, _ = text.partition("<")
    head = head.strip()
    if not bracket:
        return head
    if not text.endswith(">"):
        raise CensusError(f"unbalanced generics in header: {name.strip()[:80]!r}")
    inner = text[len(head) + 1 : -1]
    kept = [
        normalize_type(argument)
        for argument in _split_top_level(inner)
        if not argument.startswith("'")
    ]
    if not kept:
        return head
    return f"{head}<{', '.join(kept)}>"


def parse_impl_header(text: str) -> str:
    """Qualifier for an impl header, handling generic and nested-generic forms.

    ``impl<T: Into<U>> Trait<T> for Foo<T>`` yields ``<Foo<T> as Trait<T>>``.
    The impl's own parameter list is skipped; the arguments applied to the
    trait and the self type are kept, because they are what distinguishes two
    specializations. An unparseable header raises rather than silently
    attributing the block's methods to the wrong type.
    """
    body = text.strip()
    body = body[body.index("impl") + 4 :]  # IMPL_START guarantees a match
    body = body[_skip_generics(body, len(body) - len(body.lstrip())) :].strip()
    body = body.split("{")[0].split(" where ")[0].strip()
    trait, typename = _split_for(body)

    if not typename:
        raise CensusError(f"unparseable impl header: {text.strip()[:80]!r}")
    if trait:
        return f"<{normalize_type(typename)} as {normalize_type(trait)}>"
    return normalize_type(typename)


def parse_trait_header(text: str) -> str:
    """Qualifier for a trait header: the trait name with its generics.

    A default method body belongs to the trait that declares it, so
    ``trait Proto<T>: Send`` yields ``Proto<T>``. Supertrait bounds and where
    clauses are not part of the identity and are cut. An unparseable header
    raises for the same reason an impl header does.
    """
    match = TRAIT_START.match(text.strip())
    if not match:
        raise CensusError(f"unparseable trait header: {text.strip()[:80]!r}")
    body = text.strip()[match.end("name") :]
    body = body.split("{")[0].split(" where ")[0]
    depth = 0
    for index, char in enumerate(body):
        if char == "<":
            depth += 1
        elif _closes_generic(body, index):
            depth -= 1
        elif char == ":" and depth == 0:
            body = body[:index]
            break
    return normalize_type(match.group("name") + body)


# --------------------------------------------------------------------------
# Model
# --------------------------------------------------------------------------


class CensusError(Exception):
    """Manifest or discovery inconsistency."""


@dataclass(frozen=True)
class Hit:
    """One discovered occurrence of a tracked pattern.

    Identity is ``locator()``, not the whole record. ``line`` is diagnostic
    only; ``members`` is set for imports and ``capture`` for patterns with a
    capture group, both consumed during normalization.
    """

    path: str
    symbol: str
    operation: str
    line: int
    members: tuple[str, ...] = field(default=())
    capture: str = ""

    def locator(self) -> tuple[str, str, str]:
        """Identity key. Deliberately excludes the line number."""
        return (self.path, self.symbol, self.operation)


# --------------------------------------------------------------------------
# Discovery: manifest and source enumeration
# --------------------------------------------------------------------------


def load_manifest(path: Path) -> dict:
    """Parse the manifest. A malformed file raises out of ``tomllib``."""
    with open(path, "rb") as handle:
        return tomllib.load(handle)


def source_files(root: Path, config: dict) -> list[Path]:
    """Every in-scope source file under the configured roots.

    ``build/`` holds an untracked copy of the Python tree produced by
    setuptools. Scanning it would double every count, so exclusions are
    mandatory rather than advisory.
    """
    excluded = tuple(config["exclude"])
    suffixes = tuple(config["suffixes"])
    found: list[Path] = []
    for rel in config["roots"]:
        base = root / rel
        if not base.exists():
            raise CensusError(f"configured root is missing: {rel}")
        for path in sorted(base.rglob("*")):
            if path.suffix not in suffixes or not path.is_file():
                continue
            posix = path.relative_to(root).as_posix()
            if any(fragment in posix for fragment in excluded):
                continue
            found.append(path)
    return found


def rust_symbols(structural_lines: list[str]) -> list[str]:
    """Qualified enclosing symbol for every line of a Rust file.

    Takes the ``structural`` view from ``lex_rust``, in which comments and
    literal contents are blanked, so no brace inside a string or comment
    reaches the depth accounting.

    Tracks ``impl`` blocks by brace depth so that ``LocalPort::resolve_and_send``
    and ``DroppingPort::resolve_and_send`` are distinct identities. A trait impl
    renders as ``<Type as Trait>::method``, matching how the two are told apart
    when reading the source. A ``trait`` block is tracked the same way, so a
    default method body qualifies as ``Trait::method`` rather than a bare name
    that any same-named free function would collide with.
    """
    out: list[str] = []
    impls: list[tuple[int, str]] = []  # (depth at entry, qualifier)
    current_fn = ""
    depth = 0
    pending = ""  # an impl or trait header still accumulating across lines
    pending_kind = ""
    for stripped in structural_lines:
        while impls and depth < impls[-1][0]:
            impls.pop()
            current_fn = ""

        header = ""
        kind = ""
        if pending:
            pending = f"{pending} {stripped.strip()}"
            if "{" in stripped:
                header, kind, pending = pending, pending_kind, ""
            elif stripped.rstrip().endswith(";") or len(pending) > 2000:
                # A declaration rather than a block, or a runaway. Attributing
                # the following methods to a guessed type would be worse than
                # refusing.
                raise CensusError(
                    f"unterminated {pending_kind} header: {pending[:80]!r}"
                )
        else:
            for candidate, matcher in (("impl", IMPL_START), ("trait", TRAIT_START)):
                if not matcher.match(stripped):
                    continue
                if "{" in stripped:
                    header, kind = stripped, candidate
                elif stripped.rstrip().endswith(";"):
                    # A bare declaration, not a block. It owns no methods.
                    header = ""
                else:
                    pending, pending_kind = stripped.strip(), candidate
                break

        if header:
            parse = parse_impl_header if kind == "impl" else parse_trait_header
            impls.append((depth + 1, parse(header)))
            current_fn = ""

        fn_match = FN_RE.search(stripped)
        if fn_match:
            current_fn = fn_match.group("name")

        qualifier = impls[-1][1] if impls else ""
        if current_fn and qualifier:
            out.append(f"{qualifier}::{current_fn}")
        elif current_fn:
            out.append(current_fn)
        else:
            out.append("<module>")

        if not pending:
            depth += stripped.count("{") - stripped.count("}")
    return out


def scan_rust(path: Path, rel: str, operations: dict[str, str]) -> list[Hit]:
    """Match every configured Rust pattern against one file.

    Matching runs over the whole ``scannable`` view rather than line by line,
    so a construct spanning newlines -- a macro invocation whose arguments sit
    on following lines -- is matched, and a pattern can capture from them. Each
    match is attributed to the symbol owning the line where the match *starts*.
    re.MULTILINE keeps a leading ^ anchored to each line rather than to
    the file.

    Because the view blanks comments, prose in a doc comment is not inventoried
    as a call site; because it keeps literals, a symbol named inside a string
    still is. ``finditer`` records two constructions in one place separately. A
    file whose ownership cannot be parsed raises rather than yielding hits with
    guessed symbols.
    """
    text = path.read_text(encoding="utf-8")
    scannable, structural = lex_rust(text)
    symbols = rust_symbols(structural.split("\n"))
    starts = _line_starts(scannable)
    hits: list[Hit] = []
    for name, literal in operations.items():
        for match in re.finditer(literal, scannable, re.MULTILINE):
            index = bisect_right(starts, match.start()) - 1
            groups = [g for g in match.groups() if g]
            hits.append(
                Hit(
                    rel,
                    symbols[index],
                    name,
                    index + 1,
                    capture=groups[0] if groups else "",
                )
            )
    return hits


def _line_starts(text: str) -> list[int]:
    """Offset of the first character of each line, for offset-to-line lookup."""
    starts = [0]
    for index, char in enumerate(text):
        if char == "\n":
            starts.append(index + 1)
    return starts


# --------------------------------------------------------------------------
# Normalization (runs last, from discover)
# --------------------------------------------------------------------------


def collapse(hits: list[Hit], config: dict) -> list[Hit]:
    """Reduce reference-style operations to one hit per surface.

    A helper such as ``is_tokio_thread`` is inventoried as "which files use
    it", not as every occurrence, so a row stays meaningful when a file gains a
    second reference. The captured group names the symbol.

    Every alternative of a ``symbol_capture_operations`` pattern must carry a
    capture group. A capture-less alternative would key on the *enclosing*
    symbol instead, silently merging two distinct sites that happen to share a
    scope -- a false negative in the bijection. That fails closed here rather
    than falling back.
    """
    captured = frozenset(config.get("symbol_capture_operations", ()))
    per_file = frozenset(config.get("per_file_operations", ()))

    out: list[Hit] = []
    seen: set[tuple[str, str, str]] = set()
    for hit in hits:
        if hit.operation in captured:
            if not hit.capture:
                raise CensusError(
                    f"{hit.path}:{hit.line}: operation {hit.operation} is a "
                    "symbol-capture operation but the matched alternative "
                    "produced no capture group; add one to the pattern"
                )
            symbol = hit.capture
            key = (hit.path, symbol, hit.operation)
        elif hit.operation in per_file:
            symbol = "<file>"
            key = (hit.path, symbol, hit.operation)
        else:
            out.append(hit)
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(Hit(hit.path, symbol, hit.operation, hit.line, hit.members))
    return out


# --------------------------------------------------------------------------
# Discovery: Python, stubs, and documents
# --------------------------------------------------------------------------


class PythonVisitor(ast.NodeVisitor):
    """Collect call and import sites with their enclosing symbol.

    Uses ``ast`` rather than line matching so that calls and imports split
    across lines are found, and so that a match inside a string or comment is
    not. Imports record their member set, because swapping one imported name
    for another leaves a directory count unchanged.
    """

    def __init__(self, rel: str, calls: dict[str, str], modules: dict[str, str]):
        self.rel = rel
        self.calls = calls
        self.modules = modules
        self.scope: list[str] = []
        self.hits: list[Hit] = []

    def _scoped(self) -> str:
        return ".".join(self.scope) if self.scope else "<module>"

    def _descend(self, node: ast.AST, name: str) -> None:
        self.scope.append(name)
        self.generic_visit(node)
        self.scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._descend(node, node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._descend(node, node.name)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._descend(node, node.name)

    def visit_Call(self, node: ast.Call) -> None:
        rendered = _render(node.func)

        for name, literal in self.calls.items():
            if rendered == literal or rendered.endswith("." + literal):
                self.hits.append(
                    Hit(self.rel, self._qualified(node), name, node.lineno)
                )
        self.generic_visit(node)

    def _qualified(self, node: ast.Call) -> str:
        """Enclosing scope, disambiguated by the wrapped callee.

        The receiver and the wrapped callee both matter. ``shutdown_context``
        wraps two different coroutines, and ``exec_command`` takes the inner
        task of three different receivers, so identity carries both.
        """
        scope = self._scoped()
        target = _unparse(node.func)
        if node.args:
            target = f"{target}({_unparse(node.args[0])})"
        return f"{scope}[{target}]"

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        resolved = self._resolved_module(node)
        relative = bool(node.level)
        for name, literal in self.modules.items():
            if _names_module(resolved, literal, relative):
                members = tuple(sorted(alias.name for alias in node.names))
                self.hits.append(
                    Hit(self.rel, self._scoped(), name, node.lineno, members)
                )
            elif self._imports_submodule(node, resolved, literal, relative):
                self.hits.append(
                    Hit(self.rel, self._scoped(), name, node.lineno, ("<module>",))
                )
        self.generic_visit(node)

    def _resolved_module(self, node: ast.ImportFrom) -> str:
        """Absolute dotted module named by an ``ImportFrom``.

        ``node.level`` counts leading dots: one means the file's own package,
        each further dot climbs one level. Ignoring it would make every
        relative pytokio import invisible to the census. The result is rooted
        at the repository, not at a ``sys.path`` entry, so a relative import is
        matched by dotted suffix rather than equality.
        """
        if not node.level:
            return node.module or ""
        parts = self.rel.split("/")[:-1]
        climb = node.level - 1
        if climb > len(parts):
            raise CensusError(
                f"{self.rel}:{node.lineno}: relative import climbs above the "
                f"repository root ({node.level} levels)"
            )
        base = parts[: len(parts) - climb] if climb else parts
        tail = (node.module or "").split(".") if node.module else []
        return ".".join(base + tail)

    def _imports_submodule(
        self, node: ast.ImportFrom, resolved: str, literal: str, relative: bool
    ) -> bool:
        """``from pkg import pytokio`` binds the submodule from its parent."""
        parent, _, leaf = literal.rpartition(".")
        if not _names_module(resolved, parent, relative):
            return False
        return leaf in [a.name for a in node.names]

    def visit_Import(self, node: ast.Import) -> None:
        """``import pkg.pytokio`` binds the module itself, member set empty."""
        for name, literal in self.modules.items():
            for alias in node.names:
                if alias.name == literal:
                    self.hits.append(
                        Hit(self.rel, self._scoped(), name, node.lineno, ("<module>",))
                    )
        self.generic_visit(node)


def _names_module(resolved: str, literal: str, relative: bool) -> bool:
    """Whether a resolved import target names the configured module.

    An absolute import must match exactly. A relative one is resolved against
    the file's repository path, which carries a prefix -- the directories above
    the Python package root -- that the configured literal does not, so it
    matches on dotted suffix.
    """
    if resolved == literal:
        return True
    return relative and resolved.endswith("." + literal)


def _unparse(node: ast.AST) -> str:
    """Source form of an expression, so subscripts and calls survive.

    ``a[0].b()._take_inner`` must render whole; a lossy renderer would collapse
    the subscript and merge two distinct sites.
    """
    try:
        return ast.unparse(node)
    except Exception as err:  # pragma: no cover - defensive
        raise CensusError(f"cannot render expression: {err}") from err


def _render(node: ast.AST) -> str:
    """Dotted name for a call target, or "" when it is not a plain name.

    A chained call renders with empty parentheses -- ``a.b().c`` -- so that a
    method invoked on the result of another call is addressable. Mesh storage
    spawns take that shape.
    """
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _render(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    if isinstance(node, ast.Call):
        inner = _render(node.func)
        return f"{inner}()" if inner else ""
    return ""


def scan_python(
    path: Path, rel: str, calls: dict[str, str], modules: dict[str, str]
) -> list[Hit]:
    """Walk one Python file for configured calls and imports.

    A file that does not parse raises ``CensusError``; skipping it would let a
    site disappear from the census without failing the gate.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError as err:
        raise CensusError(f"{rel}: {err}") from err
    visitor = PythonVisitor(rel, calls, modules)
    visitor.visit(tree)
    return visitor.hits


def scan_text(path: Path, rel: str, operations: dict[str, str]) -> list[Hit]:
    """Plain-text scan, used for documentation surfaces."""
    lines = path.read_text(encoding="utf-8").splitlines()
    hits: list[Hit] = []
    for name, literal in operations.items():
        needle = re.compile(literal)
        for index, line in enumerate(lines):
            if needle.search(line):
                hits.append(Hit(rel, "<document>", name, index + 1))
                break  # one row per document, not per mention
    return hits


def scan_stub(path: Path, rel: str, operations: dict[str, str]) -> list[Hit]:
    """Declaration scan for binding stubs, which have no call sites."""
    lines = path.read_text(encoding="utf-8").splitlines()
    hits: list[Hit] = []
    for name, literal in operations.items():
        needle = re.compile(literal)
        for index, line in enumerate(lines):
            match = needle.search(line)
            if match:
                groups = [g for g in match.groups() if g]
                hits.append(
                    Hit(
                        rel,
                        groups[0] if groups else "<module>",
                        name,
                        index + 1,
                        capture=groups[0] if groups else "",
                    )
                )
    return hits


def discover(root: Path, config: dict) -> list[Hit]:
    """Every in-scope hit, normalized.

    Dispatches by suffix, then applies three filters in order: operations
    restricted by ``operation_paths`` are dropped outside their owning files;
    ``import_only_roots`` keeps just module imports, because a test is
    inventoried by whether it imports pytokio at all; and the defining module
    yields only the operations in ``defining_module_exempt``, since its own
    constructors are not migration targets. ``collapse`` runs last.
    """
    rust_ops = config["patterns"]["rust"]
    py_calls = config["patterns"]["python_calls"]
    py_modules = config["patterns"]["python_modules"]
    doc_ops = config["patterns"].get("docs", {})
    stub_ops = config["patterns"].get("python_stub", {})
    def_ops = config["patterns"].get("python_defs", {})
    defining = config.get("defining_module")
    defining_exempt = frozenset(config.get("defining_module_exempt", ()))
    import_only = tuple(config.get("import_only_roots", ()))
    doc_suffixes = tuple(config.get("doc_suffixes", ()))
    hits: list[Hit] = []
    for path in source_files(root, config):
        rel = path.relative_to(root).as_posix()
        if path.suffix in doc_suffixes:
            found = scan_text(path, rel, doc_ops)
        elif path.suffix == ".rs":
            found = scan_rust(path, rel, rust_ops)
        else:
            found = scan_python(path, rel, py_calls, py_modules)
            if path.suffix == ".pyi" and stub_ops:
                found += scan_stub(path, rel, stub_ops)
            if path.suffix == ".py" and def_ops:
                found += scan_stub(path, rel, def_ops)
        if rel.startswith(import_only):
            # Tests are inventoried by whether they import pytokio at all. Their
            # individual call sites belong to the suites that own them.
            found = [h for h in found if h.operation in py_modules]
        scoped = config.get("operation_paths", {})
        found = [
            h for h in found if h.operation not in scoped or rel in scoped[h.operation]
        ]
        if rel == defining:
            # pytokio.rs defines the types under deletion. Its own constructors
            # are not migration targets, but its exported symbols are.
            found = [h for h in found if h.operation in defining_exempt]
        hits.extend(found)
    return collapse(hits, config)


# --------------------------------------------------------------------------
# Validation: schema
# --------------------------------------------------------------------------


def validate_rows(rows: list[dict], schema: dict, config: dict) -> list[str]:
    """Per-row schema checks, returning every failure rather than the first.

    Enforces required fields, declared enum values, identifier uniqueness, and
    locator uniqueness across active rows *and* tombstones, so a completed
    migration still reserves its identity. Behavior rows additionally must
    carry the full field set, and coroutine-root rows the three root-only
    facts on top of it. The obligation to pin an import member set is derived
    from the row's *operation* -- whether it is a configured Python module
    pattern -- not from its category, because nothing ties the two together.
    Provenance is state-dependent: a legacy row must
    not claim a ``transition_revision``, because that field records a move that
    has already happened.
    """
    errors: list[str] = []
    seen: set[str] = set()
    locators: dict[tuple[str, str, str], str] = {}
    categories = frozenset(schema["categories"])
    classes = frozenset(schema["semantic_classes"])
    dispositions = frozenset(schema["dispositions"])
    import_operations = frozenset(config.get("patterns", {}).get("python_modules", {}))
    for position, row in enumerate(rows):
        if "id" not in row:
            errors.append(f"row {position}: missing id")
            continue
        missing = [f for f in REQUIRED_FIELDS if f not in row]
        if missing:
            errors.append(f"{row['id']}: missing {', '.join(missing)}")
            continue
        if row["id"] in seen:
            errors.append(f"{row['id']}: duplicate id")
        seen.add(row["id"])
        if row["state"] not in VALID_STATES:
            errors.append(f"{row['id']}: invalid state {row['state']!r}")
        if row["category"] not in categories:
            errors.append(f"{row['id']}: unknown category {row['category']!r}")
        if row["state"] == LEGACY and row.get("transition_revision"):
            errors.append(
                f"{row['id']}: transition_revision is provenance for a completed "
                "move; a legacy row must not carry one"
            )
        locator = (row["path"], row["symbol"], row["operation"])
        if locator in locators:
            errors.append(
                f"{row['id']}: duplicate locator, already claimed by "
                f"{locators[locator]}; identity is unique across active rows "
                "and tombstones"
            )
        locators[locator] = row["id"]
        if row["operation"] in import_operations and not row.get("members"):
            errors.append(f"{row['id']}: an import row must record its member set")
        if "multiplicity" in row:
            errors.append(
                f"{row['id']}: multiplicity is not permitted; the inventory is "
                "one row per site"
            )
        if row["category"] not in BEHAVIOR_KINDS:
            continue
        absent = [f for f in REQUIRED_BEHAVIOR_FIELDS if not row.get(f)]
        if absent:
            errors.append(
                f"{row['id']}: behavior row missing {', '.join(sorted(absent))}"
            )
            continue
        if row["semantic_class"] not in classes:
            errors.append(
                f"{row['id']}: unknown semantic class {row['semantic_class']!r}"
            )
        if row["disposition"] not in dispositions:
            errors.append(f"{row['id']}: unknown disposition {row['disposition']!r}")
        if row["category"] == "coroutine_root":
            bare = [f for f in REQUIRED_COROUTINE_ROOT_FIELDS if not row.get(f)]
            if bare:
                errors.append(
                    f"{row['id']}: coroutine-root row missing {', '.join(sorted(bare))}"
                )
    return errors


# --------------------------------------------------------------------------
# Validation: transitions
# --------------------------------------------------------------------------


def validate_transitions(rows: list[dict], transitions: list[dict]) -> list[str]:
    """Enforce transition ownership and the allowed-state model.

    A site changes state only through the transition that declares it, so a
    migration cannot be recorded against a diff that does not own the site.
    ``allowed_states`` is required rather than defaulted: a transition that
    only ever migrates sites in place should not silently also permit
    ``removed_upstream``.
    """
    errors: list[str] = []
    declared: dict[str, dict] = {}
    for transition in transitions:
        if transition["id"] in declared:
            errors.append(f"transition {transition['id']}: duplicate id")
        states = transition.get("allowed_states")
        if not states:
            errors.append(f"transition {transition['id']}: must declare allowed_states")
        else:
            unknown = sorted(set(states) - TOMBSTONES)
            if unknown:
                errors.append(
                    f"transition {transition['id']}: allowed_states may only "
                    f"name tombstone states, got {', '.join(unknown)}"
                )
        declared[transition["id"]] = transition

    owned: dict[str, str] = {}
    for transition in transitions:
        for site_id in transition.get("owns", []):
            if site_id in owned:
                errors.append(
                    f"{site_id}: owned by both {owned[site_id]} and {transition['id']}"
                )
            owned[site_id] = transition["id"]

    known_ids = {row["id"] for row in rows if "id" in row}
    for site_id, transition_id in owned.items():
        if site_id not in known_ids:
            errors.append(f"transition {transition_id}: owns unknown site {site_id}")

    for row in rows:
        if "id" not in row or "transition" not in row:
            continue
        name = row["transition"]
        if name not in declared:
            errors.append(f"{row['id']}: undeclared transition {name!r}")
            continue
        if owned.get(row["id"]) != name:
            errors.append(
                f"{row['id']}: transition {name!r} does not declare ownership "
                "of this site"
            )
        allowed = declared[name].get("allowed_states", ())
        state = row["state"]
        if state != LEGACY:
            if state not in allowed:
                errors.append(
                    f"{row['id']}: transition {name!r} does not permit state {state!r}"
                )
            revision = row.get("transition_revision", "")
            if not REVISION.match(revision):
                errors.append(
                    f"{row['id']}: state {state!r} requires a Differential "
                    f"transition revision, got {revision!r}"
                )
        if row.get("amendment_revision"):
            if not REVISION.match(row["amendment_revision"]):
                errors.append(f"{row['id']}: malformed amendment revision")
            if state != LEGACY:
                errors.append(
                    f"{row['id']}: a current-behavior amendment cannot mark the "
                    "producer migrated; the declared transition is still required"
                )
    return errors


def validate_matrix(matrix: list[dict], schema: dict) -> list[str]:
    """The matrix is a fixed set of cases, not a free-form list.

    Both directions are checked: a case absent from ``schema.matrix_ids`` is
    rejected as undeclared, and a declared case with no row is rejected as
    missing, so the set cannot drift by addition or omission.
    """
    errors: list[str] = []
    expected_ids = list(schema["matrix_ids"])
    dispositions = frozenset(schema["matrix_dispositions"])
    states = frozenset(schema["matrix_execution_states"])

    seen: set[str] = set()
    for position, row in enumerate(matrix):
        missing = [f for f in REQUIRED_MATRIX_FIELDS if not row.get(f)]
        if missing:
            errors.append(f"matrix row {position}: missing {', '.join(missing)}")
            continue
        if row["id"] in seen:
            errors.append(f"matrix {row['id']}: duplicate id")
        seen.add(row["id"])
        if row["disposition"] not in dispositions:
            errors.append(
                f"matrix {row['id']}: unknown disposition {row['disposition']!r}"
            )
        if row["execution_state"] not in states:
            errors.append(
                f"matrix {row['id']}: unknown execution state "
                f"{row['execution_state']!r}"
            )

    unexpected = sorted(seen - set(expected_ids))
    absent = [i for i in expected_ids if i not in seen]
    for extra in unexpected:
        errors.append(f"matrix {extra}: not a declared matrix case")
    for gone in absent:
        errors.append(f"matrix {gone}: declared case is missing")
    return errors


# --------------------------------------------------------------------------
# Reconciliation: manifest against source
# --------------------------------------------------------------------------


def reconcile(hits: list[Hit], rows: list[dict]) -> list[str]:
    """Match discovered hits against manifest locators, one to one.

    The bijection is enforced in both directions. A row with no hit fails as
    stale or removed; a hit no row claims fails as unknown. Moving a site
    therefore surfaces as a matched pair of both errors. Tombstones are skipped,
    since their source hit is expected to be gone. Import rows additionally
    compare their recorded member set, because a file count alone would permit
    swapping one imported name for another; conversely a hit that carries
    members must meet a row that pins them, so the comparison cannot be
    skipped by omitting the key.
    """
    errors: list[str] = []
    by_locator: dict[tuple[str, str, str], list[Hit]] = defaultdict(list)
    for hit in hits:
        by_locator[hit.locator()].append(hit)

    claimed: set[tuple[str, str, str]] = set()
    for row in rows:
        if "id" not in row or row.get("state") in TOMBSTONES:
            continue
        locator = (row["path"], row["symbol"], row["operation"])
        found = by_locator.get(locator, [])
        if not found:
            errors.append(
                f"{row['id']}: no source hit for "
                f"{locator[0]}::{locator[1]} [{locator[2]}]"
            )
        elif len(found) != 1:
            lines = ", ".join(str(h.line) for h in found)
            errors.append(
                f"{row['id']}: expected 1 site, found {len(found)} at line(s) {lines}"
            )
        actual = sorted({m for hit in found for m in hit.members})
        if "members" in row:
            if found and actual != sorted(row["members"]):
                errors.append(
                    f"{row['id']}: import members changed; manifest "
                    f"{sorted(row['members'])}, source {actual}"
                )
        elif actual:
            errors.append(
                f"{row['id']}: source hit imports {actual} but the row pins no "
                "member set"
            )
        claimed.add(locator)

    for locator, found in sorted(by_locator.items()):
        if locator not in claimed:
            lines = ", ".join(str(h.line) for h in found)
            errors.append(
                f"unknown hit: {locator[0]}::{locator[1]} "
                f"[{locator[2]}] at line(s) {lines}"
            )
    return errors


# --------------------------------------------------------------------------
# Validation: totals
# --------------------------------------------------------------------------


def validate_totals(
    hits: list[Hit], totals: dict, units: dict, config: dict
) -> list[str]:
    """Compare manifest totals with discovery.

    An operation may be counted by hit or by distinct file. Imports are counted
    by file, because a module that imports pytokio twice is still one file to
    migrate; their member sets are checked separately.

    Coverage is cross-checked against the configured patterns in both
    directions first. Iterating ``totals`` alone would leave a newly configured
    operation with no independent aggregate check, and a total naming no
    configured operation checks nothing at all. A ``total_units`` key naming no
    configured operation is likewise a typo that would silently choose the
    wrong counting unit.
    """
    errors: list[str] = []
    configured = {
        operation
        for group in config.get("patterns", {}).values()
        for operation in group
    }
    for operation in sorted(configured - set(totals)):
        errors.append(f"operation {operation} is configured but has no total")
    for operation in sorted(set(totals) - configured):
        errors.append(f"total declared for unconfigured operation {operation}")
    for operation in sorted(set(units) - configured):
        errors.append(f"total_units declared for unconfigured operation {operation}")
    by_hit = Counter(hit.operation for hit in hits)
    by_file: dict[str, set[str]] = defaultdict(set)
    for hit in hits:
        by_file[hit.operation].add(hit.path)
    for operation, expected in totals.items():
        unit = units.get(operation, "hit")
        actual = len(by_file[operation]) if unit == "file" else by_hit.get(operation, 0)
        if actual != expected:
            errors.append(
                f"total mismatch for {operation}: "
                f"manifest {expected} {unit}s, source {actual}"
            )
    return errors


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------


def check(root: Path, manifest: dict) -> list[str]:
    """Run the full gate and return every failure found.

    Errors accumulate rather than short-circuiting, so one run shows the whole
    picture. An empty list means the manifest and the source agree. Rows that
    fail the required-field gate are reported and then withheld from the
    validators that index those fields.
    """
    rows = manifest["site"]
    config = manifest["config"]
    hits = discover(root, config)
    errors = validate_rows(rows, manifest["schema"], config)
    # Downstream validators index the required fields directly. A row that
    # failed the required-field gate is already reported; passing it on would
    # raise KeyError and replace the accumulated report with a traceback.
    well_formed = [r for r in rows if all(f in r for f in REQUIRED_FIELDS)]
    errors += validate_transitions(well_formed, manifest.get("transition", []))
    errors += validate_matrix(manifest.get("matrix", []), manifest["schema"])
    errors += reconcile(hits, well_formed)
    errors += validate_totals(
        hits, manifest["totals"], manifest.get("total_units", {}), config
    )
    return errors


def summarize(root: Path, manifest: dict) -> str:
    """Human-readable inventory, reporting entries rather than rows.

    Active and tombstoned entries are counted separately: tombstones are
    history, not remaining work.
    """
    rows = manifest["site"]
    hits = discover(root, manifest["config"])
    out: list[str] = ["pytokio removal census", ""]

    active = [r for r in rows if r.get("state") == LEGACY]
    tombstones = [r for r in rows if r.get("state") in TOMBSTONES]

    out.append(f"active entries {len(active)}")
    out.append(f"tombstoned entries {len(tombstones)}")

    out.append("")
    out.append("active entries by category")
    counts: Counter[str] = Counter()
    for row in active:
        counts[row["category"]] += 1
    for category in sorted(counts):
        out.append(f"  {category:<30} {counts[category]:>4}")

    out.append("")
    out.append("active entries by owning diff")
    owners: Counter[str] = Counter()
    for row in active:
        owners[row["transition"]] += 1
    for owner in sorted(owners):
        out.append(f"  {owner:<30} {owners[owner]:>4}")

    out.append("")
    out.append("discovered hits by file")
    for path, count in sorted(Counter(h.path for h in hits).items()):
        out.append(f"  {path:<70} {count:>4}")

    if tombstones:
        out.append("")
        out.append("tombstones")
        for row in tombstones:
            out.append(
                f"  {row['id']:<44} {row['state']:<18} "
                f"{row.get('transition_revision', '?')}"
            )

    return "\n".join(out)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns 0 when a requested check passed, 1 otherwise."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="validate the manifest")
    parser.add_argument("--summary", action="store_true", help="print totals")
    parser.add_argument(
        "--root", default=".", help="fbsource repository root (default: cwd)"
    )
    parser.add_argument("--manifest", default=str(MANIFEST))
    args = parser.parse_args(argv)

    if not args.check and not args.summary:
        parser.error("choose --check, --summary, or both")

    root = Path(args.root).resolve()
    manifest = load_manifest(Path(args.manifest))

    status = 0
    if args.summary:
        print(summarize(root, manifest))
        if args.check:
            print()

    if args.check:
        errors = check(root, manifest)
        if errors:
            print(f"census check failed with {len(errors)} error(s):", file=sys.stderr)
            for error in errors:
                print(f"  {error}", file=sys.stderr)
            status = 1
        else:
            print("census check passed")
    return status


if __name__ == "__main__":
    sys.exit(main())
