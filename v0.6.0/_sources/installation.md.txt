## Installation

### Prerequisites

Before installing Monarch, ensure you have:

- Linux or macOS.
  - The CPU-only tensor engine builds on both; GPU features require Linux with a supported GPU toolchain
- Python 3.10 or later
- Optional: CUDA-compatible GPU(s) for distributed tensor and RDMA features
- Basic familiarity with PyTorch


See [README](https://github.com/meta-pytorch/monarch?tab=readme-ov-file#installation) for install instructions.

## Next Steps

Now that you've got the basics, you can:

1. Read the [getting started](./generated/examples/getting_started) guide to understand the core concepts.
2. Check out the [Examples](./generated/examples/index) directory for more detailed demonstrations
3. Explore the [API documentation](api/index) for a complete reference

## Troubleshooting

If you encounter issues:

- Make sure your CUDA environment is properly set up
- Check that you're using a compatible version of PyTorch
- Verify that all dependencies are installed correctly
- Consult the [GitHub repository](https://github.com/meta-pytorch/monarch) for known issues
- If you don't find an existing issue that matches, please [file a new one](https://github.com/meta-pytorch/monarch/issues/new)
