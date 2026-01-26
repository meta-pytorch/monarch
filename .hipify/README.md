# Hipify Custom Mappings

This directory contains custom mapping files for `hipify_torch` to handle project-specific CUDA to HIP conversions that the standard hipify tool doesn't cover.

## Files

- `rdmaxcel_custom_map.json` - Custom mappings for `rdmaxcel-sys` crate
  - Converts RDMA-specific CUDA driver API wrapper function pointer names
  - Handles types that hipify_torch doesn't recognize (e.g., `CUmemRangeHandleType`)

## Format

Each JSON file should have the following structure:

```json
{
  "custom_map": {
    "cuda_name": "hip_name",
    ...
  }
}
```

## Usage

The custom mapping files are automatically used during the build process when hipifying CUDA sources. See `build_utils::run_hipify_torch()` and crate-specific `build.rs` files for implementation details.
