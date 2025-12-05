# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import multiprocessing as mp
import os
import re
import shutil
import sys
import threading
from multiprocessing.connection import Connection
from typing import Optional

import pytest

_CHILD_ENV = "PYTEST_SPAWN_ISOLATE_CHILD"

# 256-color tan ANSI color
_TAN = "\x1b[38;5;180m"
# 256-color orange ANSI color
_ORANGE = "\x1b[38;5;208m"
_RESET = "\x1b[0m"
_DEFAULT_FG = "\x1b[39m"

_SGR_PATTERN = re.compile(r"\x1b\[([0-9;]*)m")


def get_term_width_from_reporter(pyfuncitem: pytest.Function):
    try:
        tr = pyfuncitem.config.pluginmanager.get_plugin("terminalreporter")
        if tr is not None:
            # Internal attribute, but reflects what pytest itself is using.
            return tr._screen_width
    except Exception:
        pass
    # Fallback if no reporter for some reason
    return shutil.get_terminal_size(fallback=(80, 24)).columns


def spawn_isolate(func=None, *, timeout=None, env=None):
    """
    Decorator: @spawn_isolate or @spawn_isolate(timeout=10, env={"KEY": "value"})
    """
    mark = pytest.mark.spawn_isolate(timeout=timeout, env=env)
    if func is None:
        return mark
    return mark(func)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "spawn_isolate(timeout=None, env=None): run this test in a spawned subprocess",
    )


class _PipeWriter:
    """
    File-like object for sys.stdout/sys.stderr in the child.
    Sends every write() chunk over a multiprocessing.Connection.
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def write(self, data: str) -> int:
        if not data:
            return 0
        try:
            self._conn.send(data)
        except (EOFError, OSError):
            pass
        return len(data)

    def flush(self) -> None:
        pass

    def isatty(self) -> bool:
        return False


def _run_single_test_in_child(
    child_args: list[str],
    out_conn: Connection,
    err_conn: Connection,
    result_q: "mp.Queue[int]",
    env: Optional[dict[str, str]] = None,
) -> None:
    """
    Runs pytest for a single nodeid in a fresh spawned process.
    Streams stdout/stderr to parent while running.
    """
    os.environ[_CHILD_ENV] = "1"

    if env:
        os.environ.update(env)

    sys.stdout = _PipeWriter(out_conn)
    sys.stderr = _PipeWriter(err_conn)

    exit_code = 1
    try:
        exit_code = pytest.main(child_args)
    finally:
        try:
            out_conn.close()
        except Exception:
            pass
        try:
            err_conn.close()
        except Exception:
            pass

    result_q.put(exit_code)


def _drain_conn(
    conn: Connection,
    buf_list: list[str],
    stop_event: threading.Event,
    poll_interval: float = 0.05,
) -> None:
    """
    Parent-side reader thread.
    Uses poll() because closing the connection doesn't consistently
    break out of a blocking recv() call (it can just block forever).
    Accumulates raw text chunks in buf_list.
    """
    try:
        while not stop_event.is_set():
            if conn.poll(poll_interval):
                try:
                    chunk = conn.recv()
                except (EOFError, OSError):
                    break

                if chunk:
                    buf_list.append(chunk)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _resets_foreground(codes_str: str) -> bool:
    """Check if an SGR escape sequence leaves the foreground in a reset state.

    Returns True only if the sequence resets the foreground to default and
    doesn't set a new foreground color afterwards. This correctly handles
    sequences like ESC[0;31m (reset then red) where we should NOT re-apply
    our default color since red was explicitly set.
    """
    if not codes_str:
        return True  # ESC[m is a full reset

    codes = []
    for c in codes_str.split(";"):
        c = c.strip()
        if c:
            try:
                codes.append(int(c))
            except ValueError:
                pass

    if not codes:
        return True  # Empty codes means reset

    # Iterate backwards to find the last foreground-affecting code
    i = len(codes) - 1
    while i >= 0:
        code = codes[i]

        # Check if we're inside a 38;5;n or 38;2;r;g;b sequence
        if i >= 2 and codes[i - 2] == 38 and codes[i - 1] == 5:
            return False  # 256-color foreground was set
        if i >= 4 and codes[i - 4] == 38 and codes[i - 3] == 2:
            return False  # True-color foreground was set

        if code == 0 or code == 39:
            return True
        if 30 <= code <= 37 or 90 <= code <= 97:
            return False
        if code == 38:
            return False

        i -= 1

    return False


def _colorize_child_text(text: str, color: str) -> str:
    """
    Colorize child output by making `color` the default foreground.

    The color is applied at the start, after any newline, and after any escape
    sequence that resets the foreground (code 0 or 39). This ensures color
    persists even when output is processed line-by-line (as buck does).
    Explicit colors from pytest (like red for failures) will show through
    temporarily, but tan resumes after each reset or newline.
    """
    if not text:
        return text

    result = [color]  # Start with our color as the default
    pos = 0

    for match in _SGR_PATTERN.finditer(text):
        start, end = match.span()
        codes_str = match.group(1)

        # Add text before this escape sequence, with color after each newline
        if start > pos:
            chunk = text[pos:start]
            # Insert color code after each newline so color persists across lines
            chunk = chunk.replace("\n", f"\n{color}")
            result.append(chunk)

        # Add the escape sequence itself
        result.append(match.group(0))

        # If this resets the foreground, re-apply our color
        if _resets_foreground(codes_str):
            result.append(color)

        pos = end

    # Add remaining text, with color after each newline
    if pos < len(text):
        chunk = text[pos:]
        chunk = chunk.replace("\n", f"\n{color}")
        result.append(chunk)

    # Reset foreground to default (not full reset, to preserve other attributes)
    result.append(_DEFAULT_FG)

    return "".join(result)


def _write_sep(text: str, sep: str = "=", color: str = "", fullwidth: int = 80) -> str:
    """
    Format a separator line with centered text, similar to pytest's write_sep.
    If color is provided, it applies to both the separator and the text.
    """
    terminal_width = fullwidth

    # Calculate padding
    text_with_spaces = f" {text} "
    text_len = len(text_with_spaces)
    sep_len = (terminal_width - text_len) // 2

    # Ensure minimum separator length
    if sep_len < 3:
        # If text is too long, add minimum separators on each side
        sep_len = 3

    # Build the line
    line = sep * sep_len + text_with_spaces + sep * sep_len

    # Adjust for odd widths (only if we're not already over width)
    if len(line) < terminal_width:
        line += sep

    # Apply color if specified
    if color:
        line = f"{color}{line}{_RESET}"

    return line


def _emit_child_output(
    header: str,
    child_out: str,
    child_err: str,
    header_color: str = "",
    output_color: str = "",
    fullwidth: int = 80,
) -> None:
    """
    Print child output in the parent:
      - Tint non-colored lines with output_color (default tan)
      - Add extra blank lines before and after the whole block
      - Optionally colorize header/footer with header_color
    """
    # Apply output_color if specified, otherwise use tan as default
    color_to_use = output_color if output_color else _TAN
    child_out = _colorize_child_text(child_out, color_to_use)
    child_err = _colorize_child_text(child_err, color_to_use)

    # Direct prints to stdout/stderr (ANSI preserved on most terminals)
    sys.stdout.write("\n")
    sys.stdout.write(
        _write_sep(header, color=header_color, fullwidth=fullwidth, sep="v")
    )
    sys.stdout.write("\n\n")
    if child_out:
        sys.stdout.write(child_out)
    if child_err:
        sys.stdout.write(child_err)
    sys.stdout.write("\n")
    sys.stdout.write(
        _write_sep(f"end {header}", color=header_color, fullwidth=fullwidth, sep="^")
    )
    sys.stdout.write("\n\n")
    sys.stdout.flush()


def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> Optional[bool]:
    # Don't recurse inside the child process itself.
    if os.environ.get(_CHILD_ENV) == "1":
        return None

    marker = pyfuncitem.get_closest_marker("spawn_isolate")
    if marker is None:
        return None

    timeout = marker.kwargs.get("timeout", None)
    env = marker.kwargs.get("env", None)
    nodeid = pyfuncitem.nodeid

    # Get the absolute file path and construct a nodeid that works from any directory
    # The nodeid format is: relative/path/to/file.py::ClassName::test_method
    # We need to replace the relative path with absolute path
    fspath = str(pyfuncitem.path)

    # Extract the test specifier (everything after the filename in nodeid)
    # First, find where the .py file ends in nodeid
    if ".py::" in nodeid:
        test_specifier = nodeid.split(".py::", 1)[1]
        absolute_nodeid = f"{fspath}::{test_specifier}"
    elif nodeid.endswith(".py"):
        absolute_nodeid = fspath
    elif "::" in nodeid:
        # Handle module-level tests where nodeid might be like "module/::test_name"
        test_specifier = nodeid.split("::", 1)[1]
        absolute_nodeid = f"{fspath}::{test_specifier}"
    else:
        # Fallback: just use the nodeid as-is
        absolute_nodeid = nodeid

    # Start from original CLI args, but strip any explicit paths/nodeids and --pyargs.
    invocation_params = pyfuncitem.config.invocation_params
    if invocation_params is None:
        orig_args = []
    else:
        orig_args = list(invocation_params.args)

    filtered = []
    skip_next = False
    for a in orig_args:
        if skip_next:
            skip_next = False
            continue
        if a == nodeid:
            continue
        if a.endswith(".py") or "::" in a:
            continue
        # Skip --pyargs and its module argument since we're using an absolute path
        if a == "--pyargs":
            skip_next = True
            continue
        # Skip any --color arg so we can force --color=yes
        if a.startswith("--color"):
            continue
        # Skip any --timeout arg so we can set our own from the marker
        if a.startswith("--timeout"):
            continue
        filtered.append(a)

    child_args = filtered + [absolute_nodeid]

    # Force colors in the child so we get pytest's native coloring
    child_args.insert(0, "--color=yes")

    # Pass timeout to child pytest so pytest-timeout can produce useful stack traces
    if timeout is not None:
        child_args.insert(0, f"--timeout={timeout}")

    ctx = mp.get_context("spawn")

    out_recv, out_send = ctx.Pipe(duplex=False)
    err_recv, err_send = ctx.Pipe(duplex=False)

    result_q = ctx.Queue()

    out_chunks = []
    err_chunks = []
    stop_event = threading.Event()

    t_out = threading.Thread(
        target=_drain_conn, args=(out_recv, out_chunks, stop_event), daemon=True
    )
    t_err = threading.Thread(
        target=_drain_conn, args=(err_recv, err_chunks, stop_event), daemon=True
    )
    t_out.start()
    t_err.start()

    p = ctx.Process(
        target=_run_single_test_in_child,
        args=(child_args, out_send, err_send, result_q, env),
    )
    p.start()

    # Parent timeout is a fallback - give child time to produce its own timeout error
    # with useful stack traces before the parent kills it
    parent_timeout = None
    if timeout is not None:
        parent_timeout = timeout + 30  # 30s buffer for child to report its own timeout
    p.join(parent_timeout)

    timed_out = p.is_alive()
    if timed_out:
        p.terminate()
        p.join()

    # Tell reader threads to stop and give them a moment to exit.
    stop_event.set()
    t_out.join(timeout=1.0)
    t_err.join(timeout=1.0)

    # If they're somehow still alive, close the pipes to force EOF.
    if t_out.is_alive():
        try:
            out_recv.close()
        except Exception:
            pass
    if t_err.is_alive():
        try:
            err_recv.close()
        except Exception:
            pass

    child_out = "".join(out_chunks)
    child_err = "".join(err_chunks)

    fullwidth = get_term_width_from_reporter(pyfuncitem)

    if timed_out:
        _emit_child_output(
            header=f"spawn_isolate TIMEOUT for {nodeid} (partial child output)",
            child_out=child_out,
            child_err=child_err,
            header_color=_ORANGE,
            output_color=_TAN,
            fullwidth=fullwidth,
        )
        pytest.fail(
            f"spawn_isolate: parent timeout after {parent_timeout}s for {nodeid}"
        )

    if result_q.empty():
        _emit_child_output(
            header=f"spawn_isolate ERROR for {nodeid} (no exit code; child output)",
            child_out=child_out,
            child_err=child_err,
            header_color=_ORANGE,
            output_color=_TAN,
            fullwidth=fullwidth,
        )
        pytest.fail("spawn_isolate: child produced no result")

    exit_code = result_q.get()

    if exit_code != 0:
        _emit_child_output(
            header=f"spawn_isolate FAIL for {nodeid} (child output)",
            child_out=child_out,
            child_err=child_err,
            header_color=_ORANGE,
            output_color=_TAN,
            fullwidth=fullwidth,
        )
        pytest.fail(f"spawn_isolate: child exit code {exit_code}")

    # We handled this pyfunc call entirely via the child.
    return True
