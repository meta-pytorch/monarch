# Monarch Documentation System Guide

This guide explains how the Monarch project's documentation system works and how to contribute to it.

## Quick Start - Building Documentation

### Building Locally

For most users, this is all you need to get started:

```bash
# From project root
make -C docs html

# View the results
open docs/build/html/index.html
```

The generated documentation will be available in `docs/build/html/`.

The Makefile installs the `docs` dependency group from `uv.lock` with
`uv sync --locked`, then runs Sphinx in that environment. It fails if the
lockfile needs an update. Update dependencies explicitly before building.

### CI/CD Builds

The documentation is automatically built and deployed via GitHub Actions:

- **Trigger**: On pushes to main branch and pull requests
- **Location**: `.github/workflows/doc_build.yml`
- **Process**: Full environment setup → Monarch build → Documentation generation → Deployment

### Development Builds

For faster iteration during development:

```bash
# Clean previous builds
make -C docs clean

# Build with CI environment (if you have full Monarch built)
CI=true make -C docs html

# Standard build (uses mocked imports for unavailable Rust bindings)
make -C docs html
```

### Updating Documentation Dependencies

Python documentation dependencies are declared in the root `pyproject.toml`
under `[dependency-groups].docs` and locked in `uv.lock`. Mermaid's JavaScript
version is set by `mermaid_version` in `docs/source/conf.py`.

From the project root:

```bash
# Upgrade Python documentation packages within their declared constraints
uv lock --upgrade-group docs

# Check the latest Mermaid JavaScript release, then update mermaid_version
npm view mermaid version

# Install the updated dependencies and rebuild
make -C docs clean
make -C docs html

# Check the lockfile and lint the changes
uv lock --check
arc lint
```

The PyTorch theme constrains Sphinx and PyData Sphinx Theme versions. A newer
version of either may require an updated theme first.

## Overview

The Monarch documentation system is built using **Sphinx** (for Python documentation) with additional support for **Rust documentation** (via `cargo doc`) and **mdBook** (for narrative books). The system automatically generates API documentation, includes examples, and supports multiple output formats.

## Documentation Structure

```
docs/
├── Makefile              # Main build commands
├── source/
│   ├── conf.py          # Sphinx configuration
│   ├── index.md         # Main documentation homepage
│   ├── get_started.md   # Getting started guide
│   ├── rust-api.md      # Rust API overview page
│   ├── _ext/            # Custom Sphinx extensions
│   │   └── public_api_generator.py  # Generates Python API docs
│   ├── api/             # Generated Python API documentation
│   ├── books/           # mdBook-based narrative documentation
│   │   └── hyperactor-book/  # The Hyperactor book
│   └── examples/        # Python example files
└── build/               # Generated documentation output
```

## How the Documentation System Works

### 1. Python API Documentation

The Python API documentation is automatically generated from the codebase using a multi-step process:

1. **Public API Detection**: The system reads the `__all__` list and `_public_api` dictionary from the main `monarch` package
2. **Categorization**: APIs are automatically categorized into logical groups (Core Components, Mesh Management, Data Operations, etc.)
3. **Sphinx Generation**: Uses Sphinx's `autodoc` extension to extract docstrings and generate formatted documentation
4. **Custom Extension**: The `public_api_generator.py` extension handles the organization and template generation

**Key Files:**
- `docs/source/_ext/public_api_generator.py` - Custom extension for API doc generation
- `docs/source/conf.py` - Lines 50-90 handle autodoc configuration
- Generated output: `docs/source/api/index.rst`

### 2. Rust API Documentation

Rust documentation is generated using `cargo doc` and integrated into the Sphinx build:

1. **Cargo Doc Generation**: `cargo doc --workspace --no-deps` generates HTML documentation for all Rust crates
2. **Integration**: The generated docs are copied to `docs/source/target/doc/` and served alongside Sphinx docs
3. **Navigation Page**: `docs/source/rust-api.md` provides organized access to all crate docs

**Build Process:**
```bash
# Generate Rust documentation
cargo doc --workspace --no-deps

# Copy to docs directory
mkdir -p docs/source/target docs/build/html/rust-api
cp -r target/doc docs/source/target/
```

### 3. Books (mdBook)

Narrative documentation is created using **mdBook** for rich, book-like content:

- **Location**: `docs/source/books/hyperactor-book/`
- **Configuration**: `book.toml` defines book metadata and build settings
- **Content**: Markdown files in the `src/` directory organized by `SUMMARY.md`
- **Integration**: Books are built separately and integrated into the main documentation site

### 4. Examples

Python examples are automatically processed by **Sphinx Gallery**:
- **Source**: `docs/source/examples/`
- **Processing**: Sphinx Gallery converts `.py` files into documented examples
- **Output**: Generated gallery in `docs/build/html/generated/examples/`

## How to Add to the Documentation

### Adding to Python API Documentation

1. **Ensure proper docstrings**: Add comprehensive docstrings to your Python functions/classes:
   ```python
   def my_function(param: str) -> int:
       """
       Brief description of the function.

       Args:
           param: Description of the parameter

       Returns:
           Description of return value

       Example:
           >>> my_function("test")
           42
       """
       return 42
   ```

2. **Update `__all__` and `_public_api`**: If adding new public APIs, update the package's `__all__` list and `_public_api` dictionary in the main `__init__.py` file.

3. **Rebuild documentation**: The API docs will be automatically regenerated on the next build.

### Adding to Rust API Documentation

1. **Add comprehensive doc comments** to your Rust code:
   ```rust
   /// Brief description of the function.
   ///
   /// # Arguments
   ///
   /// * `param` - Description of the parameter
   ///
   /// # Returns
   ///
   /// Description of return value
   ///
   /// # Examples
   ///
   /// ```
   /// let result = my_function("test");
   /// assert_eq!(result, 42);
   /// ```
   pub fn my_function(param: &str) -> i32 {
       42
   }
   ```

2. **Update crate documentation**: Add or update the crate-level documentation in `lib.rs`:
   ```rust
   //! # My Crate
   //!
   //! Description of what this crate does.
   ```

3. **Rebuild Rust docs**: Run `cargo doc --workspace --no-deps` to regenerate documentation.

### Adding to Books

1. **Navigate to the book directory**: `docs/source/books/hyperactor-book/`

2. **Create or edit markdown files**: Add content to existing files or create new ones in the `src/` directory.

3. **Update the table of contents**: Edit `src/SUMMARY.md` to include new pages:
   ```markdown
   # Summary

   - [Introduction](introduction.md)
   - [New Chapter](new_chapter.md)
   ```

4. **Build the book**: Run `mdbook build` in the book directory to generate HTML.

### Adding Examples

1. **Create Python example files** in `docs/source/examples/`:
   ```python
   """
   Title of the Example
   ====================

   Description of what this example demonstrates.
   """

   import monarch

   # Your example code here
   print("Hello, Monarch!")
   ```

2. **Follow naming conventions**: Use descriptive filenames that will make sense in the generated gallery.

3. **Include comprehensive docstrings**: The first docstring becomes the example description.

## Build Process Details

The documentation build process includes several steps:

1. **Environment Setup**: The build script `scripts/build_monarch_for_docs.sh` ensures Monarch is properly built with Rust bindings
2. **API Generation**: Custom extensions generate Python API documentation
3. **Rust Integration**: Cargo doc output is copied and integrated
4. **Sphinx Build**: Main documentation is generated using Sphinx
5. **Asset Copying**: Static files and generated docs are organized for serving

### CI/CD Integration

The documentation is automatically built and deployed via GitHub Actions (see `.github/workflows/doc_build.yml`):

- **Trigger**: On pushes to main branch and pull requests
- **Process**: Full environment setup, Monarch build, documentation generation
- **Output**: Deployed to documentation hosting service

## Configuration Details

### Sphinx Configuration (`conf.py`)

Key configuration sections:

- **Extensions**: Includes `sphinx.ext.autodoc`, `myst_parser`, `sphinx_gallery.gen_gallery`, and custom extensions
- **Theme**: Uses `pytorch_sphinx_theme2` for consistent PyTorch ecosystem styling
- **API Generation**: Custom `public_api_generator` extension with automatic categorization
- **Mock Imports**: Environment-dependent mocking for Rust bindings during local development

### Build Dependencies

**Python packages** (from the `docs` dependency group in `pyproject.toml`):
- Sphinx and extensions
- Theme packages
- MyST parser for Markdown
- Sphinx Gallery for examples
- Build tools and NumPy for the Monarch build used by docs CI

**System dependencies**:
- Rust toolchain (for `cargo doc`)
- mdBook (for book generation)
- Complete Monarch build environment

## Troubleshooting

### Common Issues

1. **Import Errors**: After syncing the docs dependencies, build Monarch with `uv run --frozen --no-sync bash scripts/build_monarch_for_docs.sh` from the project root
2. **Missing Rust Docs**: Run `cargo doc --workspace --no-deps` before building
3. **Theme Issues**: Check that all theme dependencies are installed
4. **Build Failures**: Use `make -C docs clean` then `make -C docs html` from the project root for a fresh build

### Environment Differences

- **CI Environment**: Uses real Monarch imports, no mocking required
- **Local Development**: May use mocked imports for unavailable Rust bindings
- **Debug Mode**: Set `CI=true` environment variable to disable mocking locally

### Getting Help

- Check the GitHub Actions logs for CI build issues
- Examine `scripts/handle_sphinx_errors.sh` for error handling
- Review Sphinx documentation for advanced configuration options

This documentation system provides comprehensive, automatically-generated documentation that stays in sync with the codebase while supporting rich narrative content through books and examples.
