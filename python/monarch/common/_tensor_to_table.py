# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import Callable, List, Optional

import torch


def tensor_to_table(
    tensor: torch.Tensor,
    format_data: Callable,
    axis_labels: Optional[List[List[str]]] = None,
    axis_names: Optional[List[str]] = None,
    format_spec: str = ".4f",
    table_format: str = "grid",
) -> str:
    """
    Convert a tensor into formatted tables with generic dimension handling.

    Parameters:
    -----------
    tensor : torch.Tensor or np.ndarray
        Input tensor to be converted (1D, 2D, or 3D)
    axis_labels : list of lists, optional
        Labels for each axis, ordered from outer to inner dimension
        For 1D: [column_labels]
        For 2D: [row_labels, column_labels]
        For 3D: [depth_labels, row_labels, column_labels]
    axis_names : list, optional
        Names for each axis, ordered from outer to inner dimension
        For 1D: [column_name]
        For 2D: [row_name, column_name]
        For 3D: [depth_name, row_name, column_name]
    format_spec : str, optional
        Format specification for numbers (default: ".4f")
    table_format : str, optional
        Table format style for tabulate (default: "grid")

    Returns:
    --------
    str : Formatted table string
    """
    import numpy as np
    from tabulate import tabulate

    assert tensor.dtype == torch.int
    # Convert tensor to numpy for easier manipulation
    data = tensor.detach().cpu().numpy()

    # Normalize dimensions
    orig_ndim = data.ndim
    if data.ndim == 1:
        data = data.reshape(1, 1, -1)
    elif data.ndim == 2:
        data = data.reshape(1, *data.shape)
    elif data.ndim > 3:
        raise ValueError("Input tensor must be 1D, 2D, or 3D")

    # Get tensor dimensions
    depth, rows, cols = data.shape

    # Generate or validate labels for each dimension
    if axis_labels is None:
        axis_labels = []

    # Pad or truncate axis_labels based on tensor dimensions
    ndim = orig_ndim
    while len(axis_labels) < ndim:
        dim_size = data.shape[-(len(axis_labels) + 1)]
        axis_labels.insert(0, [f"D{len(axis_labels)}_{i + 1}" for i in range(dim_size)])
    axis_labels = axis_labels[-ndim:]

    # Convert to internal format (depth, rows, cols)
    all_labels = [None] * 3
    if ndim == 1:
        all_labels = [["1"], ["1"], axis_labels[0]]
    elif ndim == 2:
        all_labels = [["1"], axis_labels[0], axis_labels[1]]
    else:
        all_labels = axis_labels

    # Handle axis names similarly
    if axis_names is None:
        axis_names = []

    # Pad or truncate axis_names based on tensor dimensions
    while len(axis_names) < ndim:
        axis_names.insert(0, f"Dimension {len(axis_names)}")
    axis_names = axis_names[-ndim:]

    # Convert to internal format (depth, rows, cols)
    all_names = [None] * 3
    if ndim == 1:
        all_names = [None, None, axis_names[0]]
    elif ndim == 2:
        all_names = [None, axis_names[0], axis_names[1]]
    else:
        all_names = axis_names

    # Format output
    tables = []
    for d in range(depth):
        # Format slice data
        formatted_data = [[format_data(x) for x in row] for row in data[d]]

        # Add row labels except for 1D tensors
        if orig_ndim > 1:
            formatted_data = [
                [all_labels[1][i]] + row for i, row in enumerate(formatted_data)
            ]

        # Create slice header for 3D tensors
        if orig_ndim == 3:
            slice_header = (
                f"\n{all_names[0]}: {all_labels[0][d]}\n"
                if d > 0
                else f"{all_names[0]}: {all_labels[0][d]}\n"
            )
        else:
            slice_header = ""

        # Create table
        headers = [""] + all_labels[2] if orig_ndim > 1 else all_labels[2]
        table = tabulate(
            formatted_data,
            headers=headers,
            tablefmt=table_format,
            stralign="right",
            numalign="right",
        )

        # Add axis labels
        lines = table.split("\n")

        # Add column axis name for all dimensions on first slice
        if d == 0 and all_names[2]:
            if orig_ndim == 1:
                # For 1D, center the column name over the entire table
                col_label = f"{all_names[2]:^{len(lines[0])}}"
            else:
                # For 2D and 3D, account for row labels
                total_width = len(lines[0])
                y_axis_width = max(len(label) for label in all_labels[1]) + 4
                data_width = total_width - y_axis_width
                col_label = f"{' ' * y_axis_width}{all_names[2]:^{data_width}}"
            lines.insert(0, col_label)

        # Add row axis name (only for 2D and 3D tensors)
        if orig_ndim > 1 and all_names[1]:
            label_lines = lines[1:] if d == 0 and all_names[2] else lines
            max_label_length = len(all_names[1])
            padded_label = f"{all_names[1]:>{max_label_length}} │"

            if d == 0 and all_names[2]:
                lines[0] = f"{' ' * (max_label_length + 2)}{lines[0]}"

            for i, line in enumerate(label_lines):
                if i == len(label_lines) // 2:
                    lines[i + (1 if d == 0 and all_names[2] else 0)] = (
                        f"{padded_label}{line}"
                    )
                else:
                    lines[i + (1 if d == 0 and all_names[2] else 0)] = (
                        f"{' ' * max_label_length} │{line}"
                    )

        tables.append(slice_header + "\n".join(lines))

    return "\n".join(tables)
