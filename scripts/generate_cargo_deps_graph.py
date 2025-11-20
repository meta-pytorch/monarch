#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Generate a Graphviz dependency graph from Rust Cargo.toml files.

This script scans a directory tree for Cargo.toml files, extracts dependencies
between local crates (those with 'path' dependencies), and generates a .dot file
visualizing the dependency relationships.
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, TypedDict


class DependencyInfo(TypedDict):
    name: str
    path: str
    type: str


class TomlParseResult(TypedDict):
    package_name: str | None
    dependencies: List[DependencyInfo]


def parse_toml_simple(content: str) -> TomlParseResult:
    """
    Simple TOML parser that extracts package name and path dependencies.
    This is a minimal parser focused on what we need for dependency extraction.
    """
    result: TomlParseResult = {"package_name": None, "dependencies": []}

    in_dependencies = False
    in_dev_dependencies = False
    in_build_dependencies = False

    for line in content.split("\n"):
        line = line.strip()

        # Skip comments and empty lines
        if not line or line.startswith("#"):
            continue

        # Check for section headers
        if line.startswith("["):
            in_dependencies = line == "[dependencies]"
            in_dev_dependencies = line == "[dev-dependencies]"
            in_build_dependencies = line == "[build-dependencies]"

            # Extract package name
            if line.startswith("[package]"):
                in_dependencies = False
            continue

        # Extract package name
        if "name =" in line and result["package_name"] is None:
            match = re.search(r'name\s*=\s*"([^"]+)"', line)
            if match:
                result["package_name"] = match.group(1)

        # Extract path dependencies
        if in_dependencies or in_dev_dependencies or in_build_dependencies:
            # Match patterns like: crate_name = { path = "../path" }
            # Note: crate names can contain hyphens, so we use [\w-]+ instead of \w+
            path_match = re.search(r'([\w-]+)\s*=\s*\{[^}]*path\s*=\s*"([^"]+)"', line)
            if path_match:
                dep_name = path_match.group(1)
                dep_path = path_match.group(2)
                dep_type = (
                    "dev"
                    if in_dev_dependencies
                    else ("build" if in_build_dependencies else "normal")
                )
                result["dependencies"].append(
                    {"name": dep_name, "path": dep_path, "type": dep_type}
                )

    return result


def find_cargo_tomls(root_dir: Path) -> List[Path]:
    """Find all Cargo.toml files in the directory tree."""
    cargo_files = []
    for dirpath, _dirnames, filenames in os.walk(root_dir):
        if "Cargo.toml" in filenames:
            cargo_files.append(Path(dirpath) / "Cargo.toml")
    return cargo_files


def extract_dependencies(
    root_dir: Path,
    include_dev: bool = False,
    include_build: bool = False,
    verbose: bool = False,
) -> Tuple[Dict[str, str], Dict[str, List[Tuple[str, str]]]]:
    """
    Extract dependency information from all Cargo.toml files.

    Returns:
        - crate_map: Dict mapping crate names to their directory paths
        - dependencies: Dict mapping crate names to lists of (dependency_name, dep_type) tuples
    """
    cargo_files = find_cargo_tomls(root_dir)

    # Map crate name to its directory
    crate_map: Dict[str, str] = {}
    # Map crate name to list of its dependencies
    dependencies: Dict[str, List[Tuple[str, str]]] = {}

    for cargo_path in cargo_files:
        try:
            with open(cargo_path, "r") as f:
                content = f.read()

            parsed = parse_toml_simple(content)

            # Skip workspace manifests and files without package names
            package_name = parsed["package_name"]
            if not package_name:
                if verbose:
                    print(
                        f"Skipping {cargo_path} (no package name, likely workspace manifest)",
                        file=sys.stderr,
                    )
                continue

            # Type narrowing: package_name is guaranteed to be str here
            package_dir = str(cargo_path.parent.relative_to(root_dir))

            crate_map[package_name] = package_dir

            if verbose:
                print(f"Found crate '{package_name}' at {package_dir}", file=sys.stderr)

            # Collect dependencies
            deps = []
            for dep in parsed["dependencies"]:
                dep_type = dep["type"]

                # Filter based on dependency type
                if dep_type == "dev" and not include_dev:
                    if verbose:
                        print(
                            f"  Skipping dev-dependency: {dep['name']}", file=sys.stderr
                        )
                    continue
                if dep_type == "build" and not include_build:
                    if verbose:
                        print(
                            f"  Skipping build-dependency: {dep['name']}",
                            file=sys.stderr,
                        )
                    continue

                deps.append((dep["name"], dep_type))
                if verbose:
                    print(
                        f"  Found {dep_type}-dependency: {dep['name']}", file=sys.stderr
                    )

            if deps:
                dependencies[package_name] = deps

        except Exception as e:
            print(f"Warning: Failed to parse {cargo_path}: {e}", file=sys.stderr)
            import traceback

            if verbose:
                traceback.print_exc()
            continue

    return crate_map, dependencies


def filter_local_dependencies(
    crate_map: Dict[str, str], dependencies: Dict[str, List[Tuple[str, str]]]
) -> Dict[str, List[Tuple[str, str]]]:
    """
    Filter dependencies to only include those that are local crates (present in crate_map).
    """
    filtered = {}
    for crate, deps in dependencies.items():
        local_deps = [
            (dep_name, dep_type) for dep_name, dep_type in deps if dep_name in crate_map
        ]
        if local_deps:
            filtered[crate] = local_deps
    return filtered


def generate_graphviz(
    crate_map: Dict[str, str],
    dependencies: Dict[str, List[Tuple[str, str]]],
    output_file: Path,
    title: str = "Cargo Dependencies",
):
    """Generate a Graphviz .dot file from the dependency information."""

    # Collect all crates (even those without dependencies)
    all_crates = set(crate_map.keys())

    with open(output_file, "w") as f:
        f.write("digraph cargo_dependencies {\n")
        f.write("    rankdir=LR;\n")
        f.write("    node [shape=box, style=rounded];\n")
        f.write('    labelloc="t";\n')
        f.write(f'    label="{title}";\n')
        f.write("    \n")

        # Define nodes
        f.write("    // Nodes\n")
        for crate_name in sorted(all_crates):
            f.write(f'    "{crate_name}";\n')

        f.write("    \n")

        # Define edges
        f.write("    // Dependencies\n")
        for crate, deps in sorted(dependencies.items()):
            for dep_name, dep_type in sorted(deps):
                # Use different colors/styles for different dependency types
                if dep_type == "dev":
                    style = " [color=blue, style=dashed]"
                elif dep_type == "build":
                    style = " [color=green, style=dotted]"
                else:
                    style = ""

                f.write(f'    "{crate}" -> "{dep_name}"{style};\n')

        f.write("}\n")

    print(f"Generated {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate a Graphviz dependency graph from Rust Cargo.toml files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate graph for current directory (includes build-dependencies by default)
  %(prog)s

  # Generate graph for specific directory
  %(prog)s /path/to/rust/project

  # Include dev dependencies and exclude build dependencies
  %(prog)s --include-dev --exclude-build

  # Custom output file
  %(prog)s -o my_deps.dot

  # Enable verbose output for debugging
  %(prog)s -v
        """,
    )

    parser.add_argument(
        "directory",
        nargs="?",
        default=".",
        help="Root directory to scan for Cargo.toml files (default: current directory)",
    )

    parser.add_argument(
        "-o",
        "--output",
        default="cargo_deps.dot",
        help="Output .dot file path (default: cargo_deps.dot)",
    )

    parser.add_argument(
        "--include-dev",
        action="store_true",
        help="Include dev-dependencies in the graph (shown as dashed blue lines)",
    )

    parser.add_argument(
        "--exclude-build",
        action="store_true",
        help="Exclude build-dependencies from the graph (by default they are included as dotted green lines)",
    )

    parser.add_argument(
        "--title",
        default="Cargo Dependencies",
        help='Graph title (default: "Cargo Dependencies")',
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output for debugging",
    )

    args = parser.parse_args()

    root_dir = Path(args.directory).resolve()

    if not root_dir.exists():
        print(f"Error: Directory {root_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning {root_dir} for Cargo.toml files...")

    # Extract dependencies
    crate_map, dependencies = extract_dependencies(
        root_dir,
        include_dev=args.include_dev,
        include_build=not args.exclude_build,
        verbose=args.verbose,
    )

    print(f"Found {len(crate_map)} crates")

    # Filter to only local dependencies
    local_deps = filter_local_dependencies(crate_map, dependencies)

    print(f"Found {sum(len(deps) for deps in local_deps.values())} local dependencies")

    # Generate graphviz file
    output_path = Path(args.output)
    generate_graphviz(crate_map, local_deps, output_path, args.title)

    print("\nTo visualize the graph, run:")
    print(f"  dot -Tpng {output_path} -o {output_path.stem}.png")
    print(f"  dot -Tsvg {output_path} -o {output_path.stem}.svg")
    print(f"  dot -Tpdf {output_path} -o {output_path.stem}.pdf")


if __name__ == "__main__":
    main()
