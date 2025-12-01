# Monarch 🦋

**Monarch** is a distributed programming framework for PyTorch based on scalable
actor messaging. It provides:

1. Remote actors with scalable messaging: Actors are grouped into collections called meshes and messages can be broadcast to all members.
2. Fault tolerance through supervision trees: Actors and processes form a tree and failures propagate up the tree, providing good default error behavior and enabling fine-grained fault recovery.
3. Point-to-point RDMA transfers: cheap registration of any GPU or CPU memory in a process, with the one-sided transfers based on libibverbs
4. Distributed tensors: actors can work with tensor objects sharded across processes

Monarch code imperatively describes how to create processes and actors using a simple python API:

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


The [introduction to monarch concepts](https://meta-pytorch.org/monarch/generated/examples/getting_started.html) provides an introduction to using these features.

> ⚠️ **Early Development Warning** Monarch is currently in an experimental
> stage. You should expect bugs, incomplete features, and APIs that may change
> in future versions. The project welcomes bugfixes, but to make sure things are
> well coordinated you should discuss any significant change before starting the
> work. It's recommended that you signal your intention to contribute in the
> issue tracker, either by filing a new issue or by claiming an existing one.

## 📖 Documentation

View Monarch's hosted documentation [at this link](https://meta-pytorch.org/monarch/).

## Installation
Note for running distributed tensors and RDMA, the local torch version must match the version that monarch was built with.
Stable and nightly distributions require libmxl and libibverbs (runtime).

## Fedora
`sudo dnf install -y libibverbs rdma-core libmlx5 libibverbs-devel rdma-core-devel`

## Ubuntu
`sudo apt install -y rdma-core libibverbs1 libmlx5-1 libibverbs-dev`

### Stable

`pip install torchmonarch`

torchmonarch stable is built with the latest stable torch.

### Nightly
`pip install torchmonarch-nightly`

torchmonarch-nightly is built with torch nightly.

### Build and Install from Source

If you're building Monarch from source, you should be building it with the nightly PyTorch as well for ABI compatibility.


#### On Fedora distributions

```sh

# Create and activate the conda environment
conda create -n monarchenv python=3.10 -y
conda activate monarchenv

# Install nightly rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup toolchain install nightly
rustup default nightly

# Install non-python dependencies
conda install libunwind -y

# Install the correct cuda and cuda-toolkit versions for your machine
sudo dnf install cuda-toolkit-12-8 cuda-12-8

# Install clang-dev and nccl-dev
sudo dnf install clang-devel libnccl-devel
# Or, in some environments, the following may be necessary instead
conda install -c conda-forge clangdev nccl
conda update -n monarchenv --all -c conda-forge -y

# If you are building with RDMA support, build monarch with `USE_TENSOR_ENGINE=1 pip install --no-build-isolation .` and dnf install the following packages
sudo dnf install -y libibverbs rdma-core libmlx5 libibverbs-devel rdma-core-devel

# Install build dependencies
pip install --pre torch --index-url https://download.pytorch.org/whl/nightly/cu128
pip install -r build-requirements.txt
# Install test dependencies
pip install -r python/tests/requirements.txt

# Build and install Monarch
pip install --no-build-isolation -v .
# or setup for development
pip install --no-build-isolation -v -e .

# Verify installation
pip list | grep monarch
```

#### On Ubuntu distributions

```sh
# Clone the repository and navigate to it
git clone https://github.com/meta-pytorch/monarch.git
cd monarch

# Install nightly rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
rustup toolchain install nightly
rustup default nightly

# Install Ubuntu-specific system dependencies
sudo apt install -y ninja-build
sudo apt install -y libunwind-dev
sudo apt install -y clang

# Set clang as the default C/C++ compiler
export CC=clang
export CXX=clang++

# Install the correct cuda and cuda-toolkit versions for your machine
sudo apt install -y cuda-toolkit-12-8 cuda-12-8

# Install build dependencies
pip install --pre torch --index-url https://download.pytorch.org/whl/nightly/cu128
pip install -r build-requirements.txt
# Install test dependencies
pip install -r python/tests/requirements.txt

# Build and install Monarch (with tensor engine support)
pip install --no-build-isolation .

# or
# Build and install Monarch (without tensor engine support)
USE_TENSOR_ENGINE=0 pip install --no-build-isolation .

# or setup for development
pip install --no-build-isolation -e .

# Verify installation
pip list | grep monarch
```

#### On non-CUDA machines

You can also build Monarch to run on non-CUDA machines, e.g. locally on a MacOS system.

Note that this does not support tensor engine, which is tied to CUDA and RDMA (via ibverbs).


```sh

# Create and activate the conda environment
conda create -n monarchenv python=3.10 -y
conda activate monarchenv

# Install nightly rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup toolchain install nightly
rustup default nightly

# Install build dependencies
pip install --pre torch --index-url https://download.pytorch.org/whl/nightly/cpu
pip install -r build-requirements.txt
# Install test dependencies
pip install -r python/tests/requirements.txt

# Build and install Monarch
USE_TENSOR_ENGINE=0 pip install --no-build-isolation .
# or setup for development
USE_TENSOR_ENGINE=0 pip install --no-build-isolation -e .

# Verify installation
pip list | grep monarch
```


## Running examples

Check out the `examples/` directory for demonstrations of how to use Monarch's APIs.

We'll be adding more examples as we stabilize and polish functionality!

## Running tests

We have both Rust and Python unit tests. Rust tests are run with `cargo-nextest`
and Python tests are run with `pytest`.

Rust tests:
```sh
# We use cargo-nextest to run our tests, as they can provide strong process isolation
# between every test.
# Here we install it from source, but you can instead use a pre-built binary described
# here: https://nexte.st/docs/installation/pre-built-binaries/
cargo install cargo-nextest --locked
cargo nextest run
```
cargo-nextest supports all of the filtering flags of "cargo test".

Python tests:
```sh
# Make sure to install test dependencies first
pip install -r python/tests/requirements.txt
# Run unit tests. consider -s for more verbose output
pytest python/tests/ -v -m "not oss_skip"
```

## License

Monarch is BSD-3 licensed, as found in the [LICENSE](LICENSE) file.
