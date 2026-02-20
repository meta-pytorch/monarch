## Important considerations

- Follow the style of the surrounding code.
- Avoid comments that just describe what the code does. Use comments only when the implementation is subtle.
- Document all public functions and classes.
- Communicate succinctly and clearly. No flowery prose. No "you're absolutely right!" No emojis. Minimize tokens. Telegraph. Get to the point. Use noun phrases appropriately.
- Within meta, use `./check lint` to apply formatting and linting rules. Fix *new* reported issues.
- Always fix Rust and Python type errors reported by `./check typecheck`.
- Never commit code that does not pass the type checkers (rustc or pyre).
- Classes and functions should be named clearly, but succinctly. Prefer shorter names and succinct noun phrases.
- For large changes, include a "walkthrough" in the commit message, so that the reviewer can approach the change efficiently.
- When refactoring code, we don't care about backwards compatibility within the implementation crates. Update all usages within 'monarch', but don't worry about breaking potential external customers. Treat the 'monarch' project as a monorepo.

## Design imperatives

- In Rust, make illegal state *unrepresentable*. For example, if you find structs with Option<> that are always Some in certain contexts but not in others, consider using an enum instead to explicitly enumerate the legal states of the data structure.
- Do NOT engage in defensive coding. If the program is in an illegal state (e.g., violated some invariant), *panic* instead of returning errors. In Rust use panic! and .unwrap() for these cases.
- Where appropriate, embrace the actor model: use actors for concurrency, fault tolerance, and messaging.  Use the supervision tree model for fault tolerance. Actors can be organized into a tree, where failures propagate up the tree. The root actor is the supervisor of all other actors.
- In Rust, use `hyperactor::clock::RealClock` for time operations instead of `tokio::time` directly:
  ```rust
  use hyperactor::clock::Clock;
  use hyperactor::clock::RealClock;

  RealClock.sleep(duration).await;    // not tokio::time::sleep
  RealClock.timeout(duration, fut).await;  // not tokio::time::timeout
  ```

## Style

- For prose (docs, comments, commit messages, etc.), adhere to Strunk & White. Use the Oxford comma. Use "you" and "we" to refer to the reader and the author, respectively.
- Do NOT use decoration in code comments. If structure is necessary, use Markdown (Rust), or reStructuredText (Python). Keep it SIMPLE.
- Do NOT add headers (e.g., in comments) to denote different sections of code; only use modules, structs, impl blocks, etc. for organization.
- Use "that" for restrictive clauses (essential to meaning, no commas) and "which" for non-restrictive clauses (additional info, set off by commas). "The actor that crashed must be restarted" (identifies a specific actor) vs "The actor, which was created yesterday, is still running" (adds extra detail about an already-identified actor).
- In Rust code, error messages are concise lowercase sentences without trailing punctuation


## Workflow

- Prefer `./check typecheck` for quick Rust and Python type checking
- Run `arc autocargo -p monarch` after BUCK/TARGET edits
- Tip: `arc sanity` runs all unittests directly affected by changes
- Run relevant tests after making large changes

## Overview

Monarch is a distributed programming framework for PyTorch based on scalable actor messaging. It provides remote actors with scalable messaging, fault tolerance through supervision trees, point-to-point RDMA transfers, and distributed tensors.

**If you are writing code that uses the Monarch Python API, read `docs/DOCS_INDEX.md` first for an index of tutorials, API docs, and examples.**

**Key Components:**
- **Rust Core**: The core actor system, messaging, RDMA, and tensor operations are implemented in Rust
- **Python API**: Python bindings expose the functionality through a simple API
- **Hyperactor System**: The underlying actor mesh implementation
- **Tensor Engine**: Optional GPU/RDMA support for distributed tensors (can be disabled for CPU-only builds)

## Repository Structure

This is part of the Meta fbsource monorepo. The Monarch codebase is located in `fbcode/monarch/`.

### Main Directories

- `python/monarch/` - Python package source code
  - `actor/` - Actor API and lifecycle management
  - `rdma/` - RDMA memory management
  - `common/` - Common utilities and C++ extensions
  - `gradient/` - Gradient generation for distributed training
  - `config/` - Configuration management
- `hyperactor*/` - Rust crates implementing the core actor system
- `monarch_*/` - Rust crates for specific functionality (RDMA, messages, types, etc.)
- `examples/` - Example code demonstrating Monarch features
- `python/tests/` - Python unit tests
- `docs/` - Sphinx-based documentation
- `scripts/` - Build and setup scripts
- `tools/` - Development tools

### Key Rust Crates (in Cargo workspace)

- `hyperactor` - Core actor implementation
- `hyperactor_mesh` - Actor mesh management
- `monarch_extension` - PyO3 Python bindings
- `monarch_rdma` - RDMA functionality
- `monarch_tensor_worker` - Distributed tensor operations
- `torch-sys2`, `torch-sys-cuda` - PyTorch C++ bindings

## Build System

Monarch uses a **dual build system**:

### OSS Build (pip/uv + setuptools-rust)

For external/open-source development:

```bash
# Build with tensor_engine (CUDA/GPU support) - default
uv sync
python setup.py bdist_wheel

# Build without tensor_engine (CPU-only)
USE_TENSOR_ENGINE=0 uv sync
USE_TENSOR_ENGINE=0 python setup.py bdist_wheel

# Development installation
pip install -e .
# or
uv sync
```

**Environment Variables:**
- `USE_TENSOR_ENGINE=0` - Build without CUDA/tensor support (CPU-only)
- `MONARCH_BUILD_MESH_ONLY=1` - Skip building legacy process_allocator binary (default)
- `MONARCH_PACKAGE_NAME` - Override package name (default: `torchmonarch`)
- `MONARCH_VERSION` - Override version (default: `0.0.1`)
- `ENABLE_MESSAGE_LOGGING` - Enable hyperactor message logging

**PyTorch Index Configuration:**
The project uses PyTorch from specific indices (see `pyproject.toml`). Default is `pytorch-cu128`. To change:
```bash
uv sync --extra-index-url https://download.pytorch.org/whl/cu126
```

### Meta Internal Build (Buck2)

For Meta internal development:

```bash
# Quick Rust type checking (like cargo check, much faster than full build)
arc rust-check fbcode//monarch/...

# Build targets with Buck2
buck2 build @fbcode//mode/dev-nosan fbcode//monarch/...

# Run tests
buck2 test @fbcode//mode/dev-nosan fbcode//monarch/...
```

The `check` script provides a unified workflow for linting, typechecking, and testing.

## Common Development Tasks

### Building the Project

**OSS (Outside Meta):**
```bash
# Full build with GPU support (requires CUDA, torch, RDMA libraries)
uv sync
python setup.py bdist_wheel

# CPU-only build (no CUDA/RDMA required)
USE_TENSOR_ENGINE=0 uv sync
USE_TENSOR_ENGINE=0 pip install -e .
```

**Meta Internal:**
```bash
# Use the check script for comprehensive checks
./check                   # lint, typecheck, test, autocargo
./check typecheck         # Typechecking only
./check lint              # Format and lint only
./check test              # Test only

# Or use Buck2 directly
buck2 build @fbcode//mode/dev-nosan fbcode//monarch/python/monarch:monarch_lib
```

### Running Tests

**Python Tests (OSS):**
```bash
# Install test dependencies
uv sync --extra test

# Run all tests (skip Meta-internal only tests)
uv run pytest python/tests/ -v -m "not oss_skip"

# Run specific test file
uv run pytest python/tests/_monarch/test_actor_mesh.py -v

# Run tests in parallel
uv run pytest python/tests/ -v -m "not oss_skip" -n auto
```

**Rust Tests (OSS):**
```bash
# IMPORTANT: Activate Python environment first (Rust binaries link against Python)
uv sync  # Creates and activates venv
uv run cargo nextest run  # Run with nextest

# Or with standard cargo test
cargo test
```

**Meta Internal:**
```bash
# Run Buck tests
./check test
# or
buck2 test @fbcode//mode/dev-nosan fbcode//monarch/...

# Run single test
buck2 test @fbcode//mode/dev-nosan fbcode//monarch/python/tests:test_actor_mesh
```

### Linting and Formatting

**Meta Internal:**
```bash
# Format changed files
arc f

# Run all lints and formatters
./check lint

# Type checking
arc pyre check-changed-targets
```

**OSS:**
```bash
# Python linting (flake8 config in .flake8)
flake8 python/

# Rust formatting
cargo fmt

# Rust linting
cargo clippy
```

### Building Documentation

```bash
cd docs

# Install dependencies
pip install -r requirements.txt

# Build all documentation (includes Python API docs, Rust docs, examples)
make html

# View the results
open build/html/index.html

# Clean build
make clean
```

The documentation system:
- Auto-generates Python API docs from docstrings
- Integrates Rust `cargo doc` output
- Includes mdBook narrative documentation
- Processes examples with Sphinx Gallery

## Architecture Notes

### Actor System Design

Monarch implements a hierarchical actor model:
- **Actors** are lightweight units of computation with mailboxes
- **Meshes** are collections of actors that can receive broadcast messages
- **Supervision Trees** handle fault tolerance - failures propagate up the tree
- **Endpoints** are methods decorated with `@endpoint` that can be called remotely

### Build Configuration

The `setup.py` detects:
1. **PyTorch installation** - locates libtorch, includes, and detects C++11 ABI
2. **CUDA availability** - checks `CUDA_HOME` or searches for `nvcc`
3. **Tensor Engine Flag** - uses `USE_TENSOR_ENGINE` env var to enable/disable GPU features

**Rust Features:**
- `tensor_engine` - Enables CUDA, RDMA, and distributed tensor support
- `extension-module` - Always enabled for Python bindings

### C++ Extensions

When `tensor_engine` is enabled, two C++ extensions are built:
- `monarch.common._C` - Core C++ utilities interfacing with PyTorch
- `monarch.gradient._gradient_generator` - Gradient computation for distributed training

These link against libtorch and must match the C++11 ABI of the installed PyTorch.

### Python Environment Requirements

**Rust builds require an active Python environment** because PyO3 links against Python libraries. Always activate your conda/venv/uv environment before running `cargo` commands, or use `uv run cargo ...`.

## Testing Notes

### Test Organization

- `python/tests/_monarch/` - Tests for the main Monarch API
- `python/tests/_src/` - Tests for internal implementation details
- Rust tests are co-located with source code in each crate

### Test Markers

- `@pytest.mark.oss_skip` - Skip in OSS CI (Meta-internal dependencies)

### Test Timeouts

Default pytest timeout is 5 minutes (configured in `pyproject.toml`).

## Common Pitfalls

1. **Rust Python Linking Errors**: If you see "could not find native static library `python3.12`", activate your Python environment first
2. **C++11 ABI Mismatches**: The build auto-detects PyTorch's ABI, but mismatches cause runtime errors
3. **CUDA Version Mismatches**: Ensure your CUDA installation matches the PyTorch index (e.g., cu128 = CUDA 12.8)
4. **Missing tensor_engine**: If you get import errors for RDMA/distributed tensors, rebuild with `USE_TENSOR_ENGINE=1`

## Development Workflow

### OSS Contribution Workflow

1. Make changes to Rust or Python code
2. Build: `uv sync && python setup.py develop`
3. Test: `uv run pytest python/tests/ -v -m "not oss_skip"`
4. Run Rust tests: `uv run cargo nextest run`
5. Format: `cargo fmt` (Rust), ensure `.flake8` compliance (Python)

### Meta Internal Workflow

1. Make changes
2. Run checks: `./check` (or `./check lint`, `./check test`, etc.)
3. Update autocargo if needed: The `check` script runs `arc autocargo -p monarch`
4. Commit with proper Sapling commit message format

## Configuration Files

- `pyproject.toml` - Python package metadata, dependencies, pytest config, uv sources
- `setup.py` - Build configuration, extension definitions, environment detection
- `Cargo.toml` - Rust workspace definition
- `.cargo/config.toml` - Rust build flags (`tracing_unstable`)
- `rust-toolchain` - Pinned to `nightly-2025-12-05`
- `.flake8` - Python linting configuration (max-line-length: 256)
- `docs/source/conf.py` - Sphinx documentation configuration

## Related Documentation

- Full documentation: https://meta-pytorch.org/monarch/
- README.md - Installation instructions and overview
- docs/DOCS_INDEX.md - Index of tutorials, API docs, and examples for using Monarch from Python
- docs/DOCUMENTATION_GUIDE.md - How to contribute to documentation
