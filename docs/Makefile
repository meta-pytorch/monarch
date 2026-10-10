# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
UV_SYNC       ?= uv sync --locked --group docs --no-dev --inexact --no-install-project --project ..
SPHINXBUILD   ?= uv run --frozen --no-sync --project .. sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

# Put it first so that "make" without argument is like "make help".
help:
	@$(UV_SYNC)
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# Generate example documentation from Python files
generate-examples:
	@echo "Generating example documentation..."
	@cd "$(SOURCEDIR)" && python GenerateExamples.py

# Override html target to run generate-examples first and include books
html:
	@$(UV_SYNC)
	@echo "Building HTML documentation with books directory..."
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile html

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(UV_SYNC)
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

clean:
	@echo "Cleaning up build and generated directories..."
	@rm -rf "$(BUILDDIR)"
	@rm -rf "$(SOURCEDIR)/generated"
