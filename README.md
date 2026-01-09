# Deprecation Notice

The torchmonarch-nightly package is deprecated. Please use [torchmonarch](https://pypi.org/project/torchmonarch)
instead.
Nightly builds will be published there, and can be installed with `pip install --pre torchmonarch`.
The version format is <major>.<minor>.<patch>.dev<date>

# Monarch 🦋

**Monarch** is a distributed programming framework for PyTorch based on scalable
actor messaging. It provides:

1. Remote actors with scalable messaging: Actors are grouped into collections
   called meshes and messages can be broadcast to all members.
2. Fault tolerance through supervision trees: Actors and processes form a tree
   and failures propagate up the tree, providing good default error behavior and
   enabling fine-grained fault recovery.
3. Point-to-point RDMA transfers: cheap registration of any GPU or CPU memory in
   a process, with the one-sided transfers based on libibverbs
4. Distributed tensors: actors can work with tensor objects sharded across
   processes

Monarch code imperatively describes how to create processes and actors using a
simple python API:

```python
from monarch.actor import Actor, endpoint, this_host

# spawn 8 trainer processes one for each gpu
training_procs = this_host().spawn_procs({"gpus": 8})


# define the actor to run on each process
class Trainer(Actor):
    @endpoint
    def train(self, step: int): ...


# create the trainers
trainers = training_procs.spawn("trainers", Trainer)

# tell all the trainers to take a step
fut = trainers.train.call(step=0)

# wait for all trainers to complete
fut.get()
```

The
[introduction to monarch concepts](https://meta-pytorch.org/monarch/generated/examples/getting_started.html)
provides an introduction to using these features.

> ⚠️ **Early Development Warning** Monarch is currently in an experimental
> stage. You should expect bugs, incomplete features, and APIs that may change
> in future versions. The project welcomes bugfixes, but to make sure things are
> well coordinated you should discuss any significant change before starting the
> work. It's recommended that you signal your intention to contribute in the
> issue tracker, either by filing a new issue or by claiming an existing one.

## 📖 Documentation

View Monarch's hosted documentation
[at this link](https://meta-pytorch.org/monarch/).

## Installation

### Installing from Pre-built Wheels

Monarch provides pre-built wheels that work regardless of what version of
PyTorch you have installed:

#### Stable

```sh
pip install torchmonarch
```

#### Nightly

```sh
pip install --pre torchmonarch
```

Or install a specific nightly version:

```sh
pip install torchmonarch==0.3.0.dev20260106
```

### Build and Install from Source

**Note**: Building from source requires additional system dependencies. These
are needed at **build time** only, not at runtime.

Monarch uses `uv` for fast, reliable Python package management. If you don't
have `uv` installed:

```sh
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
# Or on macOS
brew install uv
```

**Configuring PyTorch Index**: By default, Monarch builds with PyTorch from the
`pytorch-cu128` index (CUDA 12.8). To use a different CUDA version:

- Edit `[tool.uv.sources]` in `pyproject.toml` to point to a different index
  (e.g., `pytorch-cu126`, `pytorch-cu130`, or `pytorch-cpu`)
- Or use `--extra-index-url` when running uv:
  ```sh
  uv sync --extra-index-url https://download.pytorch.org/whl/cu126
  ```

#### Understanding Tensor Engine

Monarch includes
[distributed tensor](https://meta-pytorch.org/monarch/generated/examples/getting_started.html#distributed-tensors)
and
[RDMA](https://meta-pytorch.org/monarch/generated/examples/getting_started.html#point-to-point-rdma)
APIs. Since these are hardware-specific, it can be useful to develop with a
lighter-weight version of Monarch (actors only) by setting
`USE_TENSOR_ENGINE=0`.

By default, Monarch builds with tensor_engine enabled. To build without it:

```sh
USE_TENSOR_ENGINE=0 uv sync
```

**Note**: Building without tensor_engine means you won't have access to the
distributed tensor or RDMA APIs.

#### Build Dependencies by Platform

##### On Fedora distributions

```sh
# Install nightly rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup toolchain install nightly
rustup default nightly

# Install non-python dependencies
sudo dnf install libunwind -y

# Install the correct cuda and cuda-toolkit versions for your machine
sudo dnf install cuda-toolkit-12-8 cuda-12-8

# Install clang-devel, nccl-devel, and libstdc++-static
sudo dnf install clang-devel libnccl-devel libstdc++-static

# Install RDMA libraries (needed for tensor_engine builds)
sudo dnf install -y libibverbs rdma-core libmlx5 libibverbs-devel rdma-core-devel

# Clone and sync dependencies
git clone https://github.com/meta-pytorch/monarch.git
cd monarch

# Install in development mode with all dependencies
uv sync

# Or install without tensor_engine
USE_TENSOR_ENGINE=0 uv sync

# Verify installation
uv run python -c "from monarch import actor; print('Monarch installed successfully')"
```

##### On Ubuntu distributions

```sh
# Install nightly rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
rustup toolchain install nightly
rustup default nightly

# Install Ubuntu-specific system dependencies
sudo apt install -y ninja-build libunwind-dev clang

# Set clang as the default C/C++ compiler
export CC=clang
export CXX=clang++

# Install the correct cuda and cuda-toolkit versions for your machine
sudo apt install -y cuda-toolkit-12-8 cuda-12-8

# Install RDMA libraries (needed for tensor_engine builds)
sudo apt install -y rdma-core libibverbs1 libmlx5-1 libibverbs-dev

# Clone and sync dependencies
git clone https://github.com/meta-pytorch/monarch.git
cd monarch

# Install in development mode with all dependencies
uv sync

# Or install without tensor_engine (CPU-only)
USE_TENSOR_ENGINE=0 uv sync

# Verify installation
uv run python -c "from monarch import actor; print('Monarch installed successfully')"
```

##### On non-CUDA machines

You can also build Monarch on non-CUDA machines (e.g., macOS laptops) for
CPU-only usage.

Note that this does not support tensor_engine, which requires CUDA and RDMA
libraries.

```sh
# Install nightly rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup toolchain install nightly
rustup default nightly

# Clone and sync dependencies (without tensor_engine)
git clone https://github.com/meta-pytorch/monarch.git
cd monarch

# Install without tensor engine (CPU-only)
USE_TENSOR_ENGINE=0 uv sync

# Verify installation
uv run python -c "from monarch import actor; print('Monarch installed successfully')"
```

#### Alternative: Using pip

If you prefer to use pip instead of uv:

```sh
# After installing system dependencies (see above)

# Install build dependencies

# Build and install Monarch
pip install .

# Or for development
pip install -e .

# Without tensor_engine
USE_TENSOR_ENGINE=0 pip install -e .
```

## Running examples

Check out the `examples/` directory for demonstrations of how to use Monarch's
APIs.

We'll be adding more examples as we stabilize and polish functionality!

## Running tests

We have both Rust and Python unit tests. Rust tests are run with `cargo-nextest`
and Python tests are run with `pytest`.

### Rust tests

**Important:** Monarch's Rust code uses PyO3 to interface with Python, which
means the Rust binaries need to link against Python libraries. Before running
Rust tests, you need to have a Python environment activated (conda, venv, or
uv):

```sh
# If using uv (recommended)
uv sync  # This creates and activates a virtual environment
uv run cargo nextest run  # Run tests within the uv environment

# Or if using conda
conda activate monarchenv
cargo nextest run

# Or if using venv
source .venv/bin/activate
cargo nextest run
```

Without an active Python environment, you'll get Python linking errors like:

```
error: could not find native static library `python3.12`, perhaps an -L flag is missing?
```

**Installing cargo-nextest:**

```sh
# We use cargo-nextest to run our tests, as they provide strong process isolation
# between every test.
# Here we install it from source, but you can instead use a pre-built binary described
# here: https://nexte.st/docs/installation/pre-built-binaries/
cargo install cargo-nextest --locked
```

cargo-nextest supports all of the filtering flags of "cargo test".

### Python tests

```sh
# Install test dependencies (if not already installed via uv sync)
uv sync --extra test

# Run unit tests with uv
uv run pytest python/tests/ -v -m "not oss_skip"

# Or if using pip
pip install -e '.[test]'
pytest python/tests/ -v -m "not oss_skip"
```

## License

Monarch is BSD-3 licensed, as found in the [LICENSE](LICENSE) file.
