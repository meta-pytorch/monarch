#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Fixture coverage for the pytokio removal census checker.

Every later Stage 6 gate depends on this checker rejecting the failure modes
below, so the coverage is mandatory rather than a self-test. The fixtures build
synthetic source trees; they do not read Monarch source. The live repository
census runs as an explicit per-diff test-plan command, not from here.
"""

import tempfile
import tomllib
import unittest
from pathlib import Path

import pytokio_removal_census as census

BASE_CONFIG = """
[config]
roots = ["src"]
exclude = ["/build/"]
suffixes = [".rs", ".py", ".pyi", ".md"]
doc_suffixes = [".md"]
defining_module = "src/pytokio.rs"
defining_module_exempt = ["helper_symbol"]
symbol_capture_operations = ["helper_symbol"]
per_file_operations = ["pytokio_module_ref"]

[config.operation_paths]
scoped_only = ["src/owner.rs"]

[config.patterns.rust]
py_python_task_new = "\\\\bPyPythonTask::new\\\\b"
raw_python_task_new = "(?<!Py)\\\\bPythonTask::new\\\\b"
pytokio_module_ref = "crate::pytokio"
helper_symbol = "crate::pytokio::(\\\\w+)"
scoped_only = "\\\\bspawn_blocking\\\\b"

[config.patterns.python_calls]
from_coro = "Future._from_coro"
mesh_storage_spawn = "PythonTask.from_coroutine().spawn"

[config.patterns.python_modules]
pytokio_import = "pkg.pytokio"

[config.patterns.docs]
doc_legacy_surface = "pytokio"

# Every configured operation needs a total: validate_totals enforces exact
# parity with the pattern set, so an operation with no aggregate check cannot
# exist.
[totals]
py_python_task_new = 1
raw_python_task_new = 0
pytokio_module_ref = 0
helper_symbol = 0
scoped_only = 0
from_coro = 0
mesh_storage_spawn = 0
pytokio_import = 0
doc_legacy_surface = 0

[schema]
categories = [
  "native_producer",
  "coroutine_root",
  "module_import",
  "helper_residue",
]
semantic_classes = [
  "deferred_side_effect",
  "ready_no_op_wrapper",
  "already_started_lazy_observer",
]
dispositions = [
  "intentionally become eager",
  "replace with a direct value",
  "replace with the bridge future directly",
]
matrix_dispositions = ["preserve", "intentional change"]
matrix_execution_states = ["green_in_6.0b", "activates_in_6.1"]
matrix_ids = []
"""

PRODUCER_ROW = """
[[site]]
id = "np.one"
category = "native_producer"
language = "rust"
path = "src/a.rs"
symbol = "Thing::make"
operation = "py_python_task_new"
scope = "production"
state = "legacy"
transition = "6.2"
return_surface = "PyPythonTask"
consumer = "caller"
driver = "await"
start_point = "lazy"
abandonment = "possible"
eager_effect = "starts a side effect"
drop_behavior = "nothing runs"
unobserved_error = "dropped"
disposition = "intentionally become eager"
semantic_class = "deferred_side_effect"
oracle = ["fbcode//monarch/scripts:test_pytokio_removal_census::test_baseline_passes"]
"""

TRANSITION = """
[[transition]]
id = "6.2"
allowed_states = ["migrated", "removed_upstream"]
owns = ["np.one"]
"""

RUST_ONE_PRODUCER = """
impl Thing {
    fn make() -> PyResult<PyPythonTask> {
        PyPythonTask::new(async move { Ok(()) })
    }
}
"""


class Fixture:
    """A synthetic repository root plus manifest."""

    def __init__(self, stack: tempfile.TemporaryDirectory):
        self.root = Path(stack.name)
        (self.root / "src").mkdir()

    def write(self, rel: str, body: str) -> None:
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)

    def manifest(
        self, extra: str = "", base: str = "", totals: dict | None = None
    ) -> dict:
        """Assemble a manifest. ``totals`` patches counts a fixture changes.

        Totals cannot be overridden in ``extra``: TOML rejects a second
        ``[totals]`` header, and the parity check rejects omitting one.
        """
        parsed = tomllib.loads(
            (base or BASE_CONFIG) + PRODUCER_ROW + TRANSITION + extra
        )
        if totals:
            parsed["totals"].update(totals)
        return parsed

    def check(self, manifest: dict) -> list[str]:
        return census.check(self.root, manifest)


class CensusCheckerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.stack = tempfile.TemporaryDirectory()
        self.addCleanup(self.stack.cleanup)
        self.fixture = Fixture(self.stack)
        self.fixture.write("src/a.rs", RUST_ONE_PRODUCER)

    def assert_reports(self, errors: list[str], fragment: str) -> None:
        joined = "\n".join(errors)
        self.assertIn(fragment, joined, f"expected {fragment!r} in:\n{joined}")

    def test_baseline_passes(self) -> None:
        self.assertEqual(self.fixture.check(self.fixture.manifest()), [])

    # -- discovery -------------------------------------------------------

    def test_impl_qualified_identity_separates_same_named_methods(self) -> None:
        """Two methods with one bare name are distinct sites."""
        self.fixture.write(
            "src/a.rs",
            "impl Left {\n    fn go() { PyPythonTask::new(async {}); }\n}\n"
            "impl Right {\n    fn go() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            sorted(h.symbol for h in hits if h.operation == "py_python_task_new"),
            ["Left::go", "Right::go"],
        )

    def test_trait_impl_qualifies_with_trait_name(self) -> None:
        self.fixture.write(
            "src/a.rs",
            "impl Proto for Mesh {\n    fn stop() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["<Mesh as Proto>::stop"],
        )

    def test_wrapped_and_raw_constructors_are_separate_operations(self) -> None:
        """26 wrapped and six raw constructors are tracked apart."""
        self.fixture.write(
            "src/a.rs",
            "impl Thing {\n"
            "    fn make() { PyPythonTask::new(async {}); }\n"
            "    fn raw() { PythonTask::new(fut); }\n"
            "}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        kinds = {h.operation: h.symbol for h in hits if "task_new" in h.operation}
        self.assertEqual(
            kinds,
            {"py_python_task_new": "Thing::make", "raw_python_task_new": "Thing::raw"},
        )

    def test_python_roots_disambiguated_by_wrapped_callee(self) -> None:
        """One enclosing function may hold two distinct roots."""
        self.fixture.write(
            "src/m.py",
            "def shutdown_context():\n"
            "    if done:\n"
            "        return Future._from_coro(_noop())\n"
            "    return Future._from_coro(_shutdown_sequence())\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            sorted(h.symbol for h in hits if h.operation == "from_coro"),
            [
                "shutdown_context[Future._from_coro(_noop())]",
                "shutdown_context[Future._from_coro(_shutdown_sequence())]",
            ],
        )

    def test_multiline_python_call_and_import_found(self) -> None:
        """``ast`` finds calls and imports that a line matcher would miss."""
        self.fixture.write(
            "src/m.py",
            "from pkg.pytokio import (\n    PythonTask,\n)\n"
            "def go():\n"
            "    return Future._from_coro(\n        thing(),\n    )\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        found = {(h.operation, h.symbol) for h in hits}
        self.assertIn(("from_coro", "go[Future._from_coro(thing())]"), found)
        self.assertIn(("pytokio_import", "<module>"), found)

    def test_plain_import_form_is_found(self) -> None:
        """``import pkg.pytokio`` binds the module and must be inventoried."""
        self.fixture.write("src/m.py", "import pkg.pytokio\n")
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        imports = [h for h in hits if h.operation == "pytokio_import"]
        self.assertEqual([h.members for h in imports], [("<module>",)])

    def test_rust_qualified_form_found(self) -> None:
        self.fixture.write(
            "src/a.rs",
            "impl Thing {\n    fn make() { crate::pytokio::PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertIn("py_python_task_new", {h.operation for h in hits})

    def test_helper_symbol_collapses_per_file_and_symbol(self) -> None:
        self.fixture.write(
            "src/h.rs",
            "use crate::pytokio::send_result;\n"
            "fn a() { crate::pytokio::send_result(x); }\n"
            "fn b() { crate::pytokio::to_py_error(y); }\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        helpers = sorted(h.symbol for h in hits if h.operation == "helper_symbol")
        self.assertEqual(helpers, ["send_result", "to_py_error"])

    def test_module_reference_collapses_per_file(self) -> None:
        self.fixture.write(
            "src/h.rs", "use crate::pytokio::A;\nuse crate::pytokio::B;\n"
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        refs = [h for h in hits if h.operation == "pytokio_module_ref"]
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0].symbol, "<file>")

    def test_documents_yield_one_row_each(self) -> None:
        self.fixture.write("src/d.md", "pytokio appears\nand pytokio again\n")
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            len([h for h in hits if h.operation == "doc_legacy_surface"]), 1
        )

    def test_chained_call_is_addressable(self) -> None:
        """Mesh storage spawns invoke a method on another call's result."""
        self.fixture.write(
            "src/m.py",
            "def make():\n    return PythonTask.from_coroutine(task()).spawn()\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "mesh_storage_spawn"],
            ["make[PythonTask.from_coroutine(task()).spawn]"],
        )

    def test_operation_scoped_to_owning_file(self) -> None:
        """A symbol with an unrelated homonym is scoped to its owner."""
        self.fixture.write("src/owner.rs", "fn a() { spawn_blocking(x); }\n")
        self.fixture.write("src/other.rs", "fn b() { spawn_blocking(y); }\n")
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        scoped = [h.path for h in hits if h.operation == "scoped_only"]
        self.assertEqual(scoped, ["src/owner.rs"])

    def test_two_matches_on_one_line_are_both_found(self) -> None:
        """A single line may hold more than one construction."""
        self.fixture.write(
            "src/a.rs",
            "impl Thing {\n    fn make() { PyPythonTask::new(a); PyPythonTask::new(b); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            len([h for h in hits if h.operation == "py_python_task_new"]), 2
        )

    def test_multiline_impl_header_is_handled(self) -> None:
        """An impl header split across lines still owns its methods."""
        self.fixture.write(
            "src/a.rs",
            "impl VeryLongTrait\n    for Thing\n{\n"
            "    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["<Thing as VeryLongTrait>::make"],
        )

    def test_impl_prefixed_macro_is_not_an_impl(self) -> None:
        """``impl_foo!(...)`` is a macro, not an impl block."""
        self.fixture.write(
            "src/a.rs",
            "impl_something!(\n  a, b,\n);\n"
            "impl Thing {\n    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["Thing::make"],
        )

    def test_multiline_generic_impl_is_recognized(self) -> None:
        """``impl<T>`` split across lines still owns its methods."""
        self.fixture.write(
            "src/a.rs",
            "impl<T: Clone>\n    Trait<T> for Foo<T>\n{\n"
            "    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["<Foo<T> as Trait<T>>::make"],
        )

    def test_nested_generics_in_impl_header(self) -> None:
        self.assertEqual(
            census.parse_impl_header("impl<T: Into<U>> Proto<T, V<W>> for Bar<T> {"),
            "<Bar<T> as Proto<T, V<W>>>",
        )

    def test_unbalanced_generics_fail_closed(self) -> None:
        with self.assertRaises(census.CensusError):
            census.parse_impl_header("impl<T: Into<U> Trait for Foo {")

    def test_unterminated_impl_header_fails_closed(self) -> None:
        self.fixture.write("src/a.rs", "impl Thing\n" + "    where T: X\n" * 400)
        with self.assertRaises(census.CensusError):
            census.discover(self.fixture.root, self.fixture.manifest()["config"])

    def test_receiver_is_part_of_python_identity(self) -> None:
        """Same-named calls on different receivers are distinct sites."""
        self.fixture.write(
            "src/m.py",
            "def go():\n"
            "    a = Future._from_coro(one.call())\n"
            "    b = Future._from_coro(two.call())\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            sorted(h.symbol for h in hits if h.operation == "from_coro"),
            [
                "go[Future._from_coro(one.call())]",
                "go[Future._from_coro(two.call())]",
            ],
        )

    def test_subscript_receiver_is_rendered_whole(self) -> None:
        """A lossy renderer would collapse the subscript and merge two sites."""
        self.fixture.write(
            "src/m.py",
            "def go():\n"
            "    a = Future._from_coro(items[0].run())\n"
            "    b = Future._from_coro(items[1].run())\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            sorted(h.symbol for h in hits if h.operation == "from_coro"),
            [
                "go[Future._from_coro(items[0].run())]",
                "go[Future._from_coro(items[1].run())]",
            ],
        )

    def test_parent_module_import_is_detected(self) -> None:
        """``from pkg import pytokio`` binds the submodule."""
        self.fixture.write("src/m.py", "from pkg import pytokio\n")
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.members for h in hits if h.operation == "pytokio_import"],
            [("<module>",)],
        )

    def test_excluded_tree_is_not_scanned(self) -> None:
        """build/ mirrors the source tree and would double every count."""
        self.fixture.write("src/build/copy.rs", RUST_ONE_PRODUCER)
        self.assertEqual(self.fixture.check(self.fixture.manifest()), [])

    def test_defining_module_producers_are_not_migration_targets(self) -> None:
        self.fixture.write("src/pytokio.rs", RUST_ONE_PRODUCER)
        self.assertEqual(self.fixture.check(self.fixture.manifest()), [])

    def test_line_movement_does_not_change_identity(self) -> None:
        manifest = self.fixture.manifest()
        self.assertEqual(self.fixture.check(manifest), [])
        self.fixture.write("src/a.rs", "// pad\n" * 40 + RUST_ONE_PRODUCER)
        self.assertEqual(self.fixture.check(manifest), [])

    # -- discovery: Rust lexical views -----------------------------------

    def test_line_comment_is_not_a_call_site(self) -> None:
        """Prose naming a tracked symbol is documentation, not a site."""
        self.fixture.write(
            "src/a.rs",
            "impl Thing {\n"
            "    /// Returns a PyPythonTask::new wrapper eventually.\n"
            "    fn make() { PyPythonTask::new(async {}); }\n"
            "}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        found = [h for h in hits if h.operation == "py_python_task_new"]
        self.assertEqual([(h.symbol, h.line) for h in found], [("Thing::make", 3)])

    def test_block_comment_spanning_lines_is_not_scanned(self) -> None:
        """A per-line pass reads the second line of a block comment as code."""
        self.fixture.write(
            "src/a.rs",
            "/*\n PyPythonTask::new\n PyPythonTask::new\n*/\n"
            "impl Thing {\n    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            len([h for h in hits if h.operation == "py_python_task_new"]), 1
        )

    def test_nested_block_comment_terminates_at_the_outer_close(self) -> None:
        """Rust block comments nest; a naive scan resumes one level too early."""
        self.fixture.write(
            "src/a.rs",
            "/* outer /* inner */ PyPythonTask::new */\n"
            "impl Thing {\n    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            len([h for h in hits if h.operation == "py_python_task_new"]), 1
        )

    def test_url_in_string_does_not_start_a_comment(self) -> None:
        """The // in a URL must not discard the rest of the line."""
        self.fixture.write(
            "src/a.rs",
            "impl Thing {\n"
            '    fn make() { let _ = "https://x/y"; PyPythonTask::new(async {}); }\n'
            "}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["Thing::make"],
        )

    def test_brace_in_string_does_not_skew_block_depth(self) -> None:
        """A literal brace would close the impl early and misattribute."""
        self.fixture.write(
            "src/a.rs",
            'impl Alpha {\n    fn keep() { let _ = "}"; }\n'
            "    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["Alpha::make"],
        )

    def test_brace_in_char_literal_does_not_skew_block_depth(self) -> None:
        self.fixture.write(
            "src/a.rs",
            "impl Alpha {\n    fn keep() { let _ = '{'; }\n"
            "    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["Alpha::make"],
        )

    def test_raw_string_contents_are_inert(self) -> None:
        """A raw string honours no escapes and may hold braces and slashes."""
        self.fixture.write(
            "src/a.rs",
            'impl Alpha {\n    fn keep() { let _ = r#"} // "#; }\n'
            "    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["Alpha::make"],
        )

    def test_lifetime_is_not_read_as_a_char_literal(self) -> None:
        self.fixture.write(
            "src/a.rs",
            "impl<'a> Thing<'a> {\n    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["Thing::make"],
        )

    def test_unsafe_impl_qualifies_its_methods(self) -> None:
        """A bare impl matcher attributes these to a free function name."""
        self.fixture.write(
            "src/a.rs",
            "unsafe impl Proto for Thing {\n"
            "    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["<Thing as Proto>::make"],
        )

    def test_default_impl_qualifies_its_methods(self) -> None:
        self.fixture.write(
            "src/a.rs",
            "default impl Thing {\n    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["Thing::make"],
        )

    def test_trait_default_method_is_qualified_by_its_trait(self) -> None:
        """An unqualified name would survive a move to another trait."""
        self.fixture.write(
            "src/a.rs",
            "pub(crate) trait Proto: Send {\n"
            "    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["Proto::make"],
        )

    def test_trait_default_and_free_function_are_distinct(self) -> None:
        self.fixture.write(
            "src/a.rs",
            "trait Proto {\n    fn make() { PyPythonTask::new(async {}); }\n}\n"
            "fn make() { PyPythonTask::new(async {}); }\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            sorted(h.symbol for h in hits if h.operation == "py_python_task_new"),
            ["Proto::make", "make"],
        )

    def test_pattern_matches_across_lines(self) -> None:
        """A macro names its subject below the line that opens it."""
        config = BASE_CONFIG.replace(
            'helper_symbol = "crate::pytokio::(\\\\w+)"',
            'helper_symbol = "make_exception!\\\\s*\\\\(\\\\s*(\\\\w+)"',
        )
        self.fixture.write("src/a.rs", "make_exception!(\n    Boom,\n);\n")
        hits = census.discover(
            self.fixture.root, self.fixture.manifest(base=config)["config"]
        )
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "helper_symbol"], ["Boom"]
        )

    def test_capture_operation_without_a_capture_fails_closed(self) -> None:
        """Keying on the enclosing symbol would merge two distinct sites."""
        config = BASE_CONFIG.replace(
            'helper_symbol = "crate::pytokio::(\\\\w+)"',
            'helper_symbol = "crate::pytokio::helper_symbol"',
        )
        self.fixture.write("src/a.rs", "fn f() { crate::pytokio::helper_symbol(); }\n")
        with self.assertRaises(census.CensusError) as caught:
            census.discover(
                self.fixture.root, self.fixture.manifest(base=config)["config"]
            )
        self.assertIn("no capture group", str(caught.exception))

    # -- discovery: Python imports ---------------------------------------

    def test_relative_import_is_discovered(self) -> None:
        """Reading only node.module makes every relative import invisible."""
        self.fixture.write("src/pkg/__init__.py", "")
        self.fixture.write("src/pkg/m.py", "from .pytokio import PythonTask\n")
        hits = census.discover(
            self.fixture.root,
            self.fixture.manifest(totals={"pytokio_import": 1})["config"],
        )
        found = [h for h in hits if h.operation == "pytokio_import"]
        self.assertEqual([h.members for h in found], [("PythonTask",)])

    def test_parent_relative_import_is_discovered(self) -> None:
        self.fixture.write("src/pkg/sub/m.py", "from ..pytokio import PythonTask\n")
        hits = census.discover(
            self.fixture.root,
            self.fixture.manifest(totals={"pytokio_import": 1})["config"],
        )
        self.assertEqual(
            [h.members for h in hits if h.operation == "pytokio_import"],
            [("PythonTask",)],
        )

    def test_relative_import_above_the_root_fails(self) -> None:
        self.fixture.write("src/m.py", "from ....... pytokio import PythonTask\n")
        with self.assertRaises(census.CensusError) as caught:
            census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertIn("climbs above the repository root", str(caught.exception))

    def test_unrelated_relative_import_is_not_a_hit(self) -> None:
        self.fixture.write("src/pkg/m.py", "from .other import Thing\n")
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual([h for h in hits if h.operation == "pytokio_import"], [])

    def test_import_only_root_drops_call_sites(self) -> None:
        """A test suite is inventoried by whether it imports pytokio at all."""
        config = BASE_CONFIG.replace(
            'defining_module = "src/pytokio.rs"',
            'import_only_roots = ["src/tests"]\ndefining_module = "src/pytokio.rs"',
        )
        self.fixture.write(
            "src/tests/t.py",
            "from pkg.pytokio import PythonTask\nFuture._from_coro(x)\n",
        )
        manifest = self.fixture.manifest(base=config, totals={"pytokio_import": 1})
        hits = census.discover(self.fixture.root, manifest["config"])
        self.assertEqual(
            sorted({h.operation for h in hits if h.path.startswith("src/tests")}),
            ["pytokio_import"],
        )

    # -- discovery: generic specialization and trait headers -------------

    def test_generic_specializations_are_distinct_owners(self) -> None:
        """Erasing the argument would give two impls one locator."""
        self.fixture.write(
            "src/a.rs",
            "impl Proto for Ref<PythonActor> {\n"
            "    fn go() { PyPythonTask::new(async {}); }\n}\n"
            "impl Proto for Ref<OtherActor> {\n"
            "    fn go() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            sorted(h.symbol for h in hits if h.operation == "py_python_task_new"),
            [
                "<Ref<OtherActor> as Proto>::go",
                "<Ref<PythonActor> as Proto>::go",
            ],
        )

    def test_moving_a_producer_between_specializations_fails_the_check(self) -> None:
        """The counterexample: erased identities would pass this silently."""
        self.fixture.write(
            "src/a.rs",
            "impl Proto for Ref<PythonActor> {\n"
            "    fn make() { PyPythonTask::new(async {}); }\n}\n"
            "impl Proto for Ref<OtherActor> {\n}\n",
        )
        row = (
            '\n[[site]]\nid = "np.spec"\ncategory = "native_producer"\n'
            'language = "rust"\npath = "src/a.rs"\n'
            'symbol = "<Ref<PythonActor> as Proto>::make"\n'
            'operation = "py_python_task_new"\nscope = "production"\n'
            'state = "legacy"\ntransition = "6.2"\n'
            'return_surface = "PyPythonTask"\nconsumer = "caller"\n'
            'driver = "await"\nstart_point = "lazy"\nabandonment = "possible"\n'
            'eager_effect = "starts a side effect"\n'
            'drop_behavior = "nothing runs"\nunobserved_error = "dropped"\n'
            'disposition = "intentionally become eager"\n'
            'semantic_class = "deferred_side_effect"\n'
            'oracle = ["fbcode//monarch/scripts:test_pytokio_removal_census::test_baseline_passes"]\n'
        )
        manifest = self.fixture.manifest(row, totals={"py_python_task_new": 1})
        manifest["site"] = [r for r in manifest["site"] if r["id"] != "np.one"]
        manifest["transition"][0]["owns"] = ["np.spec"]
        self.assertEqual(self.fixture.check(manifest), [])

        # Move the producer to the other specialization. Nothing else changes:
        # same file, same method name, same count.
        self.fixture.write(
            "src/a.rs",
            "impl Proto for Ref<PythonActor> {\n}\n"
            "impl Proto for Ref<OtherActor> {\n"
            "    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        errors = self.fixture.check(manifest)
        self.assert_reports(errors, "<Ref<PythonActor> as Proto>::make")
        self.assert_reports(errors, "<Ref<OtherActor> as Proto>::make")

    def test_lifetime_argument_is_not_part_of_identity(self) -> None:
        """Thing<'a> and Thing<'b> are one type; keeping them is churn."""
        self.assertEqual(census.parse_impl_header("impl<'a> Thing<'a> {"), "Thing")

    def test_generic_whitespace_is_normalized(self) -> None:
        """Reformatting a header must not read as a move."""
        self.assertEqual(
            census.parse_impl_header("impl Proto for Ref< PythonActor > {"),
            census.parse_impl_header("impl Proto for Ref<PythonActor> {"),
        )

    def test_multiline_traits_sharing_a_method_name_stay_distinct(self) -> None:
        """A trait header may span lines exactly as an impl header may."""
        self.fixture.write(
            "src/a.rs",
            "pub trait Alpha<T>:\n    Send + Sync\n{\n"
            "    fn go() { PyPythonTask::new(async {}); }\n}\n"
            "trait Beta<T>\n    : Send\n{\n"
            "    fn go() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            sorted(h.symbol for h in hits if h.operation == "py_python_task_new"),
            ["Alpha<T>::go", "Beta<T>::go"],
        )

    def test_unterminated_trait_header_fails_closed(self) -> None:
        """A runaway header must refuse rather than guess an owner."""
        self.fixture.write("src/a.rs", "trait Alpha\n" + "    where T: X\n" * 400)
        with self.assertRaises(census.CensusError) as caught:
            census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertIn("unterminated trait header", str(caught.exception))

    def test_malformed_trait_header_fails_closed(self) -> None:
        with self.assertRaises(census.CensusError):
            census.parse_trait_header("trait Alpha<T: Bound {")

    def test_trait_supertraits_are_not_part_of_identity(self) -> None:
        self.assertEqual(
            census.parse_trait_header("pub(crate) trait Proto: Send + Sync {"), "Proto"
        )

    # -- discovery: lifetimes and return arrows --------------------------

    def test_adjacent_lifetimes_stay_structural_text(self) -> None:
        """The second lifetime's quote is not a closing quote."""
        header = "impl<'a, 'b> Trait for Foo<'a, 'b> {"
        self.assertEqual(census.lex_rust(header)[1], header)

    def test_adjacent_lifetimes_do_not_corrupt_the_qualifier(self) -> None:
        """Blanking the span between them yielded ``<Foo<b> as Trait>``."""
        self.fixture.write(
            "src/a.rs",
            "impl<'a, 'b> Trait for Foo<'a, 'b> {\n"
            "    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["<Foo as Trait>::make"],
        )

    def test_elided_lifetime_pair_keeps_the_trait_bare(self) -> None:
        """The live shape: ``impl FromPyObject<'_, '_> for T``.

        Parsed through the lexer, which is where the corruption arose: the
        blanked span left ``FromPyObject<_>`` as the trait.
        """
        header = "impl FromPyObject<'_, '_> for PyDuration {"
        self.assertEqual(
            census.parse_impl_header(census.lex_rust(header)[1]),
            "<PyDuration as FromPyObject>",
        )

    def test_char_literals_are_still_blanked_structurally(self) -> None:
        """A brace in a char literal must not reach block-depth accounting."""
        for source in ("let d = '{';", "let e = '\\u{1f600}';"):
            with self.subTest(source=source):
                self.assertNotIn("{", census.lex_rust(source)[1])

    def test_escaped_char_literal_is_scanned_to_its_close(self) -> None:
        self.assertEqual(census.lex_rust("let b = '\\n';")[1], "let b =     ;")

    def test_return_arrow_in_impl_binder_does_not_close_the_generic(self) -> None:
        """The ``>`` of ``->`` closed the binder early, mis-slicing the head."""
        self.assertEqual(
            census.parse_impl_header("impl<F: Fn() -> u32> Proto for Foo<F> {"),
            "<Foo<F> as Proto>",
        )

    def test_return_arrow_in_nested_generic_argument(self) -> None:
        self.assertEqual(
            census.parse_impl_header("impl Proto for Foo<Box<dyn Fn() -> u32>> {"),
            "<Foo<Box<dyn Fn() -> u32>> as Proto>",
        )

    def test_return_arrow_in_trait_binder(self) -> None:
        """The trait header splits on its supertrait colon at depth zero."""
        self.assertEqual(
            census.parse_trait_header("trait Alpha<F: Fn() -> u32>: Send {"),
            "Alpha<F: Fn() -> u32>",
        )

    def test_return_arrow_binder_attributes_methods_correctly(self) -> None:
        """End to end: the mis-sliced header produced a garbage owner."""
        self.fixture.write(
            "src/a.rs",
            "impl<F: Fn() -> u32> Proto for Foo<F> {\n"
            "    fn make() { PyPythonTask::new(async {}); }\n}\n",
        )
        hits = census.discover(self.fixture.root, self.fixture.manifest()["config"])
        self.assertEqual(
            [h.symbol for h in hits if h.operation == "py_python_task_new"],
            ["<Foo<F> as Proto>::make"],
        )

    # -- reconciliation --------------------------------------------------

    def test_unexpected_producer_fails(self) -> None:
        self.fixture.write("src/b.rs", RUST_ONE_PRODUCER)
        self.assert_reports(
            self.fixture.check(self.fixture.manifest()), "unknown hit: src/b.rs"
        )

    def test_missing_producer_fails(self) -> None:
        self.fixture.write("src/a.rs", "fn make() {}\n")
        self.assert_reports(
            self.fixture.check(self.fixture.manifest()), "no source hit for src/a.rs"
        )

    def test_locator_matching_multiple_sites_fails(self) -> None:
        self.fixture.write(
            "src/a.rs",
            "impl Thing {\n    fn make() {\n"
            "        PyPythonTask::new(async {});\n"
            "        PyPythonTask::new(async {});\n"
            "    }\n}\n",
        )
        manifest = self.fixture.manifest()
        manifest["totals"]["py_python_task_new"] = 2
        self.assert_reports(self.fixture.check(manifest), "expected 1 site, found 2")

    def test_total_mismatch_fails(self) -> None:
        """Totals check the expected aggregate count for an operation.

        A net-zero swap is caught by reconciliation, not here.
        """
        manifest = self.fixture.manifest()
        manifest["totals"]["py_python_task_new"] = 5
        self.assert_reports(self.fixture.check(manifest), "total mismatch")

    def test_import_member_swap_fails(self) -> None:
        """A directory count alone would permit this swap."""
        self.fixture.write("src/m.py", "from pkg.pytokio import PythonTask, Shared\n")
        extra = (
            '\n[[site]]\nid = "im.m"\ncategory = "module_import"\n'
            'language = "python"\npath = "src/m.py"\nsymbol = "<module>"\n'
            'operation = "pytokio_import"\nscope = "production"\n'
            'state = "legacy"\ntransition = "6.2"\n'
            'members = ["PythonTask", "Shared"]\n'
        )
        manifest = self.fixture.manifest(extra, totals={"pytokio_import": 1})
        manifest["transition"][0]["owns"].append("im.m")
        self.assertEqual(self.fixture.check(manifest), [])

        self.fixture.write("src/m.py", "from pkg.pytokio import PythonTask, Handle\n")
        self.assert_reports(self.fixture.check(manifest), "import members changed")

    def test_hit_with_members_and_unpinned_row_fails(self) -> None:
        """Miscategorising an import row must not skip the member check."""
        self.fixture.write("src/m.py", "from pkg.pytokio import PythonTask\n")
        extra = (
            '\n[[site]]\nid = "im.m"\ncategory = "helper_residue"\n'
            'language = "python"\npath = "src/m.py"\nsymbol = "<module>"\n'
            'operation = "pytokio_import"\nscope = "production"\n'
            'state = "legacy"\ntransition = "6.2"\n'
        )
        manifest = self.fixture.manifest(extra, totals={"pytokio_import": 1})
        manifest["transition"][0]["owns"].append("im.m")
        errors = self.fixture.check(manifest)
        self.assert_reports(errors, "must record its member set")
        self.assert_reports(errors, "the row pins no member set")

    # -- schema ----------------------------------------------------------

    def test_duplicate_active_locator_fails(self) -> None:
        """Two active rows must not claim one site."""
        manifest = self.fixture.manifest()
        clone = dict(manifest["site"][0])
        clone["id"] = "np.two"
        manifest["site"].append(clone)
        manifest["transition"][0]["owns"].append("np.two")
        self.assert_reports(
            self.fixture.check(manifest), "identity is unique across active rows"
        )

    def test_locator_unique_against_tombstones(self) -> None:
        """A tombstone still owns its locator; a new row may not reuse it."""
        manifest = self.fixture.manifest()
        clone = dict(manifest["site"][0])
        clone["id"] = "np.ghosted"
        clone["state"] = "migrated"
        clone["transition_revision"] = "D123456"
        manifest["site"].append(clone)
        manifest["transition"][0]["owns"].append("np.ghosted")
        self.assert_reports(
            self.fixture.check(manifest), "identity is unique across active rows"
        )

    def test_multiplicity_is_rejected_anywhere(self) -> None:
        manifest = self.fixture.manifest()
        manifest["site"][0]["multiplicity"] = 2
        self.assert_reports(
            self.fixture.check(manifest), "multiplicity is not permitted"
        )

    def test_import_row_requires_members(self) -> None:
        self.fixture.write("src/m.py", "from pkg.pytokio import PythonTask\n")
        extra = (
            '\n[[site]]\nid = "im.m"\ncategory = "module_import"\n'
            'language = "python"\npath = "src/m.py"\nsymbol = "<module>"\n'
            'operation = "pytokio_import"\nscope = "production"\n'
            'state = "legacy"\ntransition = "6.2"\n'
        )
        manifest = self.fixture.manifest(extra)
        manifest["transition"][0]["owns"].append("im.m")
        self.assert_reports(self.fixture.check(manifest), "must record its member set")

    def test_unknown_category_fails(self) -> None:
        manifest = self.fixture.manifest()
        manifest["site"][0]["category"] = "invented"
        self.assert_reports(self.fixture.check(manifest), "unknown category")

    def test_unknown_semantic_class_fails(self) -> None:
        manifest = self.fixture.manifest()
        manifest["site"][0]["semantic_class"] = "invented"
        self.assert_reports(self.fixture.check(manifest), "unknown semantic class")

    def test_unknown_disposition_fails(self) -> None:
        manifest = self.fixture.manifest()
        manifest["site"][0]["disposition"] = "invented"
        self.assert_reports(self.fixture.check(manifest), "unknown disposition")

    def test_transition_revision_on_legacy_row_fails(self) -> None:
        """Provenance belongs to a completed move, not a pending one."""
        manifest = self.fixture.manifest()
        manifest["site"][0]["transition_revision"] = "D123456"
        self.assert_reports(
            self.fixture.check(manifest), "a legacy row must not carry one"
        )

    def test_duplicate_id_fails(self) -> None:
        manifest = self.fixture.manifest(PRODUCER_ROW)
        self.assert_reports(self.fixture.check(manifest), "duplicate id")

    def test_missing_id_fails(self) -> None:
        manifest = self.fixture.manifest()
        del manifest["site"][0]["id"]
        self.assert_reports(self.fixture.check(manifest), "missing id")

    def test_incomplete_abandonment_data_fails(self) -> None:
        manifest = self.fixture.manifest()
        del manifest["site"][0]["drop_behavior"]
        self.assert_reports(
            self.fixture.check(manifest), "behavior row missing drop_behavior"
        )

    def test_reviewed_facts_are_required(self) -> None:
        """Each behavior row still states its class, disposition, and oracle.

        These are what the row is reviewed on, so their absence must fail.
        """
        for missing in ("semantic_class", "disposition", "oracle"):
            with self.subTest(field=missing):
                manifest = self.fixture.manifest()
                del manifest["site"][0][missing]
                self.assert_reports(
                    self.fixture.check(manifest),
                    f"behavior row missing {missing}",
                )

    def test_owning_transition_is_required(self) -> None:
        manifest = self.fixture.manifest()
        del manifest["site"][0]["transition"]
        self.assert_reports(self.fixture.check(manifest), "missing transition")

    def test_malformed_amendment_revision_fails(self) -> None:
        manifest = self.fixture.manifest()
        manifest["site"][0]["amendment_revision"] = "nope"
        self.assert_reports(
            self.fixture.check(manifest), "malformed amendment revision"
        )

    def test_behavior_row_may_not_use_multiplicity(self) -> None:
        """Behavior-bearing sites are one row each."""
        manifest = self.fixture.manifest()
        manifest["site"][0]["multiplicity"] = 2
        self.assert_reports(
            self.fixture.check(manifest), "multiplicity is not permitted"
        )

    # -- totals ----------------------------------------------------------

    def test_configured_operation_without_a_total_fails(self) -> None:
        """Every configured operation needs its independent aggregate check."""
        manifest = self.fixture.manifest()
        del manifest["totals"]["pytokio_import"]
        self.assert_reports(
            self.fixture.check(manifest),
            "operation pytokio_import is configured but has no total",
        )

    def test_total_for_unconfigured_operation_fails(self) -> None:
        """A renamed pattern would otherwise leave a total checking nothing."""
        manifest = self.fixture.manifest()
        manifest["totals"]["typo_operation"] = 3
        self.assert_reports(
            self.fixture.check(manifest),
            "total declared for unconfigured operation typo_operation",
        )

    def test_total_units_for_unconfigured_operation_fails(self) -> None:
        """A typo here silently selects the wrong counting unit."""
        manifest = self.fixture.manifest()
        manifest["total_units"] = {"typo_operation": "file"}
        self.assert_reports(
            self.fixture.check(manifest),
            "total_units declared for unconfigured operation typo_operation",
        )

    def test_file_counted_total_ignores_repeat_imports(self) -> None:
        """A module importing pytokio twice is still one file to migrate."""
        self.fixture.write(
            "src/m.py",
            "from pkg.pytokio import PythonTask\n"
            "def later():\n    from pkg.pytokio import Shared\n",
        )
        rows = "".join(
            f'\n[[site]]\nid = "im.m.{name}"\ncategory = "module_import"\n'
            f'language = "python"\npath = "src/m.py"\nsymbol = "{symbol}"\n'
            'operation = "pytokio_import"\nscope = "production"\n'
            f'state = "legacy"\ntransition = "6.2"\nmembers = ["{member}"]\n'
            for name, symbol, member in [
                ("module", "<module>", "PythonTask"),
                ("later", "later", "Shared"),
            ]
        )
        manifest = self.fixture.manifest(rows, totals={"pytokio_import": 1})
        manifest["total_units"] = {"pytokio_import": "file"}
        manifest["transition"][0]["owns"].extend(["im.m.module", "im.m.later"])
        self.assertEqual(self.fixture.check(manifest), [])

        # The same inventory counted by hit is two, not one.
        manifest["total_units"] = {}
        self.assert_reports(self.fixture.check(manifest), "manifest 1 hits, source 2")

    def test_file_counted_total_mismatch_fails(self) -> None:
        manifest = self.fixture.manifest()
        manifest["total_units"] = {"pytokio_import": "file"}
        manifest["totals"]["pytokio_import"] = 2
        self.assert_reports(self.fixture.check(manifest), "manifest 2 files, source 0")

    def test_coroutine_root_missing_root_fields_fails(self) -> None:
        """The three root-only reviewed facts are not optional."""
        manifest = self.fixture.manifest()
        row = dict(manifest["site"][0])
        row["category"] = "coroutine_root"
        row["public_operation"] = "Actor.call"
        row["caller_contexts"] = "endpoint"
        manifest["site"][0] = row
        self.assert_reports(
            self.fixture.check(manifest),
            "coroutine-root row missing first_side_effect",
        )

    def test_coroutine_root_with_all_root_fields_passes(self) -> None:
        manifest = self.fixture.manifest()
        row = dict(manifest["site"][0])
        row["category"] = "coroutine_root"
        row["public_operation"] = "Actor.call"
        row["caller_contexts"] = "endpoint"
        row["first_side_effect"] = "sends a message"
        manifest["site"][0] = row
        self.assertEqual(self.fixture.check(manifest), [])

    def test_malformed_row_reports_rather_than_crashing(self) -> None:
        """Downstream validators index the required fields directly."""
        manifest = self.fixture.manifest()
        row = dict(manifest["site"][0])
        del row["symbol"]
        manifest["site"].append(row)
        errors = self.fixture.check(manifest)
        self.assert_reports(errors, "missing symbol")

    def test_malformed_row_does_not_suppress_other_errors(self) -> None:
        """Withholding the bad row must not stop the rest of the gate."""
        manifest = self.fixture.manifest()
        broken = dict(manifest["site"][0])
        broken["id"] = "np.broken"
        del broken["path"]
        manifest["site"].append(broken)
        manifest["totals"]["py_python_task_new"] = 9
        errors = self.fixture.check(manifest)
        self.assert_reports(errors, "np.broken: missing path")
        self.assert_reports(errors, "total mismatch for py_python_task_new")

    # -- transitions -----------------------------------------------------

    def test_undeclared_transition_fails(self) -> None:
        manifest = self.fixture.manifest()
        manifest["site"][0]["transition"] = "6.9"
        self.assert_reports(self.fixture.check(manifest), "undeclared transition '6.9'")

    def test_transition_must_declare_ownership(self) -> None:
        manifest = self.fixture.manifest()
        manifest["transition"][0]["owns"] = []
        self.assert_reports(
            self.fixture.check(manifest), "does not declare ownership of this site"
        )

    def test_two_transitions_cannot_own_one_site(self) -> None:
        manifest = self.fixture.manifest()
        manifest["transition"].append(
            {"id": "6.3c", "allowed_states": ["migrated"], "owns": ["np.one"]}
        )
        self.assert_reports(self.fixture.check(manifest), "owned by both")

    def test_transition_owning_unknown_site_fails(self) -> None:
        manifest = self.fixture.manifest()
        manifest["transition"][0]["owns"].append("np.ghost")
        self.assert_reports(self.fixture.check(manifest), "owns unknown site np.ghost")

    def test_state_change_requires_differential_revision(self) -> None:
        manifest = self.fixture.manifest()
        manifest["site"][0]["state"] = "migrated"
        manifest["site"][0]["transition_revision"] = "nope"
        self.fixture.write("src/a.rs", "fn make() {}\n")
        manifest["totals"]["py_python_task_new"] = 0
        self.assert_reports(
            self.fixture.check(manifest), "requires a Differential transition revision"
        )

    def test_disallowed_state_fails(self) -> None:
        manifest = self.fixture.manifest()
        manifest["transition"][0]["allowed_states"] = ["migrated"]
        manifest["site"][0]["state"] = "removed_upstream"
        manifest["site"][0]["transition_revision"] = "D123456"
        self.fixture.write("src/a.rs", "fn make() {}\n")
        manifest["totals"]["py_python_task_new"] = 0
        self.assert_reports(self.fixture.check(manifest), "does not permit state")

    def test_amendment_cannot_mark_producer_migrated(self) -> None:
        """A bug fix amends behavior; it does not discharge the migration."""
        manifest = self.fixture.manifest()
        row = manifest["site"][0]
        row["amendment_revision"] = "D999999"
        row["state"] = "migrated"
        row["transition_revision"] = "D888888"
        self.fixture.write("src/a.rs", "fn make() {}\n")
        manifest["totals"]["py_python_task_new"] = 0
        self.assert_reports(self.fixture.check(manifest), "amendment cannot mark the")

    def test_direct_removal_models_one_site_drop(self) -> None:
        """The modeled D116091231 change removes only Direct."""
        self.fixture.write(
            "src/a.rs",
            RUST_ONE_PRODUCER
            + "\nimpl Actor {\n    fn handle_direct() { PythonTask::new(fut); }\n}\n",
        )
        direct = (
            '\n[[site]]\nid = "np.direct"\ncategory = "native_producer"\n'
            'language = "rust"\npath = "src/a.rs"\nsymbol = "Actor::handle_direct"\n'
            'operation = "raw_python_task_new"\nscope = "production"\n'
            'state = "legacy"\ntransition = "6.2"\n'
            'return_surface = "PythonTask"\nconsumer = "waiter"\n'
            'driver = "tokio::spawn"\nstart_point = "eager"\n'
            'abandonment = "none"\neager_effect = "observes already-running work"\n'
            'drop_behavior = "n/a"\nunobserved_error = "kill signal"\n'
            'disposition = "replace with the bridge future directly"\n'
            'semantic_class = "already_started_lazy_observer"\n'
            'oracle = ["fbcode//monarch/scripts:test_pytokio_removal_census::test_baseline_passes"]\n'
        )
        manifest = self.fixture.manifest(direct)
        manifest["transition"][0]["owns"].append("np.direct")
        manifest["totals"]["raw_python_task_new"] = 1
        self.assertEqual(self.fixture.check(manifest), [])

        self.fixture.write("src/a.rs", RUST_ONE_PRODUCER)
        manifest["site"][1]["state"] = "removed_upstream"
        manifest["site"][1]["transition_revision"] = "D116091231"
        manifest["totals"]["raw_python_task_new"] = 0
        self.assertEqual(self.fixture.check(manifest), [])

    def test_transition_without_allowed_states_fails(self) -> None:
        """Defaulting to both tombstones grants a permission never reviewed."""
        manifest = self.fixture.manifest()
        del manifest["transition"][0]["allowed_states"]
        self.assert_reports(self.fixture.check(manifest), "must declare allowed_states")

    def test_transition_allowed_states_must_be_tombstones(self) -> None:
        manifest = self.fixture.manifest()
        manifest["transition"][0]["allowed_states"] = ["legacy"]
        self.assert_reports(
            self.fixture.check(manifest), "may only name tombstone states"
        )

    # -- Future matrix ---------------------------------------------------

    def test_matrix_duplicate_id_fails(self) -> None:
        row = (
            '\n[[matrix]]\nid = "fm.x"\ndisposition = "preserve"\n'
            'execution_state = "green_in_6.0b"\n'
        )
        manifest = self.fixture.manifest(row + row)
        manifest["schema"]["matrix_ids"] = ["fm.x"]
        self.assert_reports(self.fixture.check(manifest), "matrix fm.x: duplicate id")

    def test_matrix_missing_id_fails(self) -> None:
        manifest = self.fixture.manifest(
            '\n[[matrix]]\ndisposition = "preserve"\n'
            'execution_state = "green_in_6.0b"\n'
        )
        manifest["schema"]["matrix_ids"] = ["fm.x"]
        self.assert_reports(self.fixture.check(manifest), "matrix row 0: missing id")

    def test_matrix_unexpected_id_fails(self) -> None:
        manifest = self.fixture.manifest(
            '\n[[matrix]]\nid = "fm.rogue"\ndisposition = "preserve"\n'
            'execution_state = "green_in_6.0b"\n'
        )
        manifest["schema"]["matrix_ids"] = []
        self.assert_reports(
            self.fixture.check(manifest), "matrix fm.rogue: not a declared matrix case"
        )

    def test_matrix_declared_case_missing_fails(self) -> None:
        manifest = self.fixture.manifest()
        manifest["schema"]["matrix_ids"] = ["fm.absent"]
        self.assert_reports(
            self.fixture.check(manifest), "matrix fm.absent: declared case is missing"
        )

    def test_matrix_renamed_id_reports_both_directions(self) -> None:
        """Renaming a case is an undeclared id and a missing declared one."""
        manifest = self.fixture.manifest(
            '\n[[matrix]]\nid = "fm.renamed"\ndisposition = "preserve"\n'
            'execution_state = "green_in_6.0b"\n'
        )
        manifest["schema"]["matrix_ids"] = ["fm.original"]
        errors = self.fixture.check(manifest)
        self.assert_reports(errors, "matrix fm.renamed: not a declared matrix case")
        self.assert_reports(errors, "matrix fm.original: declared case is missing")

    def test_matrix_missing_disposition_fails(self) -> None:
        manifest = self.fixture.manifest(
            '\n[[matrix]]\nid = "fm.x"\nexecution_state = "green_in_6.0b"\n'
        )
        manifest["schema"]["matrix_ids"] = ["fm.x"]
        self.assert_reports(
            self.fixture.check(manifest), "matrix row 0: missing disposition"
        )

    def test_matrix_invalid_disposition_fails(self) -> None:
        manifest = self.fixture.manifest(
            '\n[[matrix]]\nid = "fm.x"\ndisposition = "invented"\n'
            'execution_state = "green_in_6.0b"\n'
        )
        manifest["schema"]["matrix_ids"] = ["fm.x"]
        self.assert_reports(self.fixture.check(manifest), "unknown disposition")

    def test_matrix_missing_execution_state_fails(self) -> None:
        manifest = self.fixture.manifest(
            '\n[[matrix]]\nid = "fm.x"\ndisposition = "preserve"\n'
        )
        manifest["schema"]["matrix_ids"] = ["fm.x"]
        self.assert_reports(
            self.fixture.check(manifest), "matrix row 0: missing execution_state"
        )

    def test_matrix_invalid_execution_state_fails(self) -> None:
        manifest = self.fixture.manifest(
            '\n[[matrix]]\nid = "fm.x"\ndisposition = "preserve"\n'
            'execution_state = "invented"\n'
        )
        manifest["schema"]["matrix_ids"] = ["fm.x"]
        self.assert_reports(self.fixture.check(manifest), "unknown execution state")

    # -- oracle references ------------------------------------------------

    def _behavior_row(self, manifest):
        """The synthetic producer row, which is a behavior row."""
        return manifest["site"][0]

    def _tombstoned_producer(self, manifest, oracle):
        """Append a tombstoned behavior row carrying ``oracle``.

        Its locator names no source file, so reconciliation -- which skips
        tombstones -- neither expects a hit nor reports an unknown one.
        """
        row = dict(manifest["site"][0])
        row.update(
            id="np.gone",
            path="src/deleted.rs",
            symbol="Gone::make",
            state="removed_upstream",
            transition_revision="D111111",
            oracle=oracle,
        )
        manifest["site"].append(row)
        manifest["transition"][0]["owns"].append("np.gone")
        return manifest

    def test_oracle_canonical_single_reference_passes(self) -> None:
        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = [
            "fbcode//monarch/scripts:test_pytokio_removal_census::test_baseline_passes"
        ]
        self.assertEqual(self.fixture.check(manifest), [])

    def test_oracle_multiple_qualified_references_pass(self) -> None:
        """Conjunctive references, module-qualified and class-qualified."""
        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = [
            "fbcode//monarch/monarch_hyperactor:monarch_hyperactor-unittest"
            "::pickle::tests::resolve_fills_slots_in_order",
            "fbcode//monarch/python/tests:test_job::TestJob::test_exec_command",
        ]
        self.assertEqual(self.fixture.check(manifest), [])

    def test_oracle_legacy_label_accepted_on_active_row(self) -> None:
        """Prose is still accepted while rows are being converted."""
        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = "endpoint reply coverage"
        self.assertEqual(self.fixture.check(manifest), [])

    def test_oracle_legacy_label_accepted_on_tombstone(self) -> None:
        """A deleted path has no live test to name, so its label stays."""
        manifest = self._tombstoned_producer(
            self.fixture.manifest(), "test_actor_driver_characterization"
        )
        self.assertEqual(self.fixture.check(manifest), [])

    def test_oracle_scalar_reference_attempts_fail(self) -> None:
        """Any bare string reaching for the canonical form is a half-finished
        conversion, valid or not.

        Accepting a *malformed* attempt as prose would silently swallow the
        typo, so the prefix alone decides, and the diagnostic names the syntax
        problem as well as the shape."""
        cases = [
            ("fbcode//monarch/scripts:target::test_name", None),
            ("fbcode//monarch/scripts:target::9bad", "malformed test name"),
            ("fbcode//monarch/scripts:target", "no '::'"),
            ("fbcode//monarch/scripts:target::test name", "whitespace"),
            ("  fbcode//monarch/scripts:target::test_name", "whitespace"),
        ]
        for reference, syntax in cases:
            with self.subTest(reference=reference):
                manifest = self.fixture.manifest()
                self._behavior_row(manifest)["oracle"] = reference
                errors = self.fixture.check(manifest)
                self.assert_reports(errors, "canonical references go in a list")
                if syntax is not None:
                    self.assert_reports(errors, syntax)

    def test_oracle_target_names_follow_buck_grammar(self) -> None:
        """Buck's grammar, not a guessed subset.

        A narrower class silently rejects legal targets; a laxer one blesses
        ``...``, which Buck reserves. Both failures are invisible here because
        the checker never resolves the label.
        """
        for name in (
            ".",
            "..",
            "foo+bar",
            "a@b!c=d~e",
            "with.dots-and_score",
            "back\\slash",
        ):
            with self.subTest(name=name):
                manifest = self.fixture.manifest()
                self._behavior_row(manifest)["oracle"] = [
                    f"fbcode//monarch/scripts:{name}::test_name"
                ]
                self.assertEqual(self.fixture.check(manifest), [])

        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = [
            "fbcode//monarch/scripts:...::test_name"
        ]
        self.assert_reports(self.fixture.check(manifest), "reserved target name")

    def test_oracle_target_name_boundary_rules(self) -> None:
        """The three rules Buck applies beyond its character set.

        Each is a boundary rather than a shape, so none of them is caught by
        the character class alone.
        """
        limit = census.TARGET_NAME_MAX_LEN

        # `_eqsb_` is Buck's substitution for `=`; a written name may not carry it.
        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = [
            "fbcode//monarch/scripts:a_eqsb_b::test_name"
        ]
        self.assert_reports(self.fixture.check(manifest), "reserved substring")

        # Exactly at the limit is legal; one over is not.
        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = [
            f"fbcode//monarch/scripts:{'x' * limit}::test_name"
        ]
        self.assertEqual(self.fixture.check(manifest), [])

        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = [
            f"fbcode//monarch/scripts:{'x' * (limit + 1)}::test_name"
        ]
        self.assert_reports(self.fixture.check(manifest), f"over the {limit} limit")

    def test_oracle_prose_inside_list_is_named_directly(self) -> None:
        """Prose in the list gets its own diagnostic.

        Falling through to the reference check would blame whatever the label
        trips first -- usually a space -- instead of the real mistake. An entry
        carrying '::' is a botched reference, not prose, and still gets the
        reference diagnostic.
        """
        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = ["endpoint reply coverage"]
        self.assert_reports(
            self.fixture.check(manifest), "a legacy prose label stays a bare string"
        )

        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = [
            "fbcode//monarch/scripts:test_pytokio_removal_census::test_baseline_passes",
            "lifecycle_coverage",
        ]
        self.assert_reports(self.fixture.check(manifest), "oracle[1]")

        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = ["//monarch/scripts:target::test_name"]
        self.assert_reports(self.fixture.check(manifest), "malformed target")

    def test_oracle_whitespace_only_label_fails(self) -> None:
        """A blank label names nothing, and was passing because it is truthy."""
        for blank in ("   ", "\t", " \n "):
            with self.subTest(blank=repr(blank)):
                manifest = self.fixture.manifest()
                self._behavior_row(manifest)["oracle"] = blank
                self.assert_reports(self.fixture.check(manifest), "oracle is empty")

    def test_oracle_ordinary_prose_label_still_passes(self) -> None:
        """The contrast case: prose that does not reach for the canonical form
        is untouched by the prefix rule."""
        for label in ("endpoint reply coverage", "lifecycle edge behavior"):
            with self.subTest(label=label):
                manifest = self.fixture.manifest()
                self._behavior_row(manifest)["oracle"] = label
                self.assertEqual(self.fixture.check(manifest), [])

    def test_oracle_empty_list_reports_shape_not_missing_field(self) -> None:
        """The empty list is present but unusable; the truthiness gate would
        otherwise misreport it as an absent field."""
        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = []
        errors = self.fixture.check(manifest)
        self.assert_reports(errors, "oracle is an empty list")
        joined = "\n".join(errors)
        self.assertNotIn("behavior row missing", joined)

    def test_oracle_absent_still_reports_missing_field(self) -> None:
        """Shape checking must not swallow the absent-field diagnostic."""
        manifest = self.fixture.manifest()
        del self._behavior_row(manifest)["oracle"]
        self.assert_reports(self.fixture.check(manifest), "behavior row missing oracle")

    def test_oracle_wrong_top_level_type_fails(self) -> None:
        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = 7
        self.assert_reports(self.fixture.check(manifest), "oracle is int")

    def test_oracle_non_string_member_fails(self) -> None:
        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = [7]
        self.assert_reports(self.fixture.check(manifest), "oracle[0] is int")

    def test_oracle_duplicate_references_fail(self) -> None:
        reference = (
            "fbcode//monarch/scripts:test_pytokio_removal_census::test_baseline_passes"
        )
        manifest = self.fixture.manifest()
        self._behavior_row(manifest)["oracle"] = [reference, reference]
        self.assert_reports(self.fixture.check(manifest), "oracle[1] repeats")

    def test_oracle_malformed_references_fail(self) -> None:
        """Each malformed shape is rejected, and named in the diagnostic."""
        cases = [
            ("//monarch/scripts:target::test_name", "malformed target"),
            ("fbcode//:target::test_name", "malformed package path"),
            ("fbcode//monarch/scripts::test_name", "malformed target"),
            ("fbcode//monarch/scripts:target", "no '::'"),
            ("fbcode//monarch/scripts:target::", "malformed test name"),
            ("fbcode//monarch/scripts:target::9test", "malformed test name"),
            ("fbcode//monarch/scripts:target::test name", "whitespace"),
            ("fbcode//monarch/scripts:tar get::test_name", "whitespace"),
            ("fbcode///pkg:target::test", "malformed package path"),
            ("fbcode//pkg//sub:target::test", "malformed package path"),
            ("fbcode//pkg/../sub:target::test", "malformed package path"),
            ("fbcode//pkg/./sub:target::test", "malformed package path"),
            ("fbcode//pkg/:target::test", "malformed package path"),
            ("fbcode//pkg:...::test", "reserved target name"),
            ("fbcode//pkg:tar get::test", "whitespace"),
        ]
        for reference, expected in cases:
            with self.subTest(reference=reference):
                manifest = self.fixture.manifest()
                self._behavior_row(manifest)["oracle"] = [reference]
                self.assert_reports(self.fixture.check(manifest), expected)


if __name__ == "__main__":
    unittest.main()
