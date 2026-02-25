#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set -e  # Exit on any error

echo "========================================="
echo "Building Monarch for Documentation"
echo "========================================="

# Set CI environment variable for Sphinx configuration
export CI=true
# BUILD MONARCH COMPLETELY - This is critical for API documentation
echo "Building Monarch with Rust bindings..."
export MONARCH_BUILD_MESH_ONLY=0
python -m pip install -e ".[kubernetes]" --no-build-isolation

# Verify Monarch installation and imports
echo "Verifying Monarch installation..."
python -c "import torch; print(f'PyTorch version: {torch.__version__}')" || {
    echo "ERROR: PyTorch import failed"; exit 1;
}

python -c "import monarch; print('Monarch imported successfully')" || {
    echo "ERROR: Monarch import failed"; exit 1;
}

# Test critical modules that were showing up blank
echo "Testing critical API modules..."
python -c "
import sys

modules = [
    'monarch.fetch',
    'monarch.gradient_generator',
    'monarch.notebook'
]

failed_modules = []

for mod in modules:
    try:
        __import__(mod)
        print(f'{mod} imported successfully')

        # Test that we can access actual functions/classes
        module = sys.modules[mod]
        if hasattr(module, '__all__'):
            attrs = module.__all__
        else:
            attrs = [attr for attr in dir(module) if not attr.startswith('_')]

        attr_list = attrs[:5] if len(attrs) > 5 else attrs
        suffix = '...' if len(attrs) > 5 else ''
        print(f'   Available: {attr_list}{suffix}')

    except Exception as e:
        print(f'ERROR: {mod} failed: {e}')
        failed_modules.append(mod)
        import traceback
        traceback.print_exc()

if failed_modules:
    print(f'FAILED MODULES: {failed_modules}')
    exit(1)
else:
    print('All critical modules verified successfully!')
"

# Test Sphinx autodoc compatibility
echo "Testing Sphinx autodoc compatibility..."
python -c "
import sphinx.ext.autodoc
print('Sphinx autodoc available')

# Test autodoc can handle our modules
from sphinx.application import Sphinx
from sphinx.util.docutils import docutils_namespace
import tempfile
import os

# Create minimal sphinx app for testing
with tempfile.TemporaryDirectory() as tmpdir:
    srcdir = os.path.join(tmpdir, 'source')
    outdir = os.path.join(tmpdir, 'build')
    doctreedir = os.path.join(tmpdir, 'doctrees')
    os.makedirs(srcdir)

    # Write minimal conf.py
    with open(os.path.join(srcdir, 'conf.py'), 'w') as f:
        f.write('extensions = [\"sphinx.ext.autodoc\"]\n')
        f.write('autodoc_mock_imports = []\n')  # No mocking in CI

    with docutils_namespace():
        try:
            app = Sphinx(srcdir, srcdir, outdir, doctreedir, 'html', confoverrides={})
            print('Sphinx app created successfully')
            # Note: cleanup() method doesn't exist in newer Sphinx versions
            # The app will be cleaned up automatically when the context exits
        except Exception as e:
            print(f'ERROR: Sphinx setup failed: {e}')
            exit(1)
"

echo "Monarch build and verification complete!"
