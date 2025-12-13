# Monarch 🦋

**Monarch** is a distributed programming framework for PyTorch based on scalable
actor messaging. It provides:

1. Remote actors with scalable messaging: Actors are grouped into collections called meshes and messages can be broadcast to all members.
2. Fault tolerance through supervision trees: Actors and processes for a tree and failures propagate up the tree, providing good default error behavior and enabling fine-grained fault recovery.
3. Point-to-point RDMA transfers: cheap registration of any GPU or CPU memory in a process, with the one-sided tranfers based on libibverbs
4. Distributed tensors: actors can work with tensor objects sharded across processes

Monarch code imperatively describes how to create processes and actors using a simple python API:

    from monarch.actor import Actor, endpoint, this_host

    # spawn 8 trainer processes one for each gpu
    training_procs = this_host().spawn_procs({"gpus": 8})


    # define the actor to run on each process
    class Trainer(Actor):
        @endpoint
        def train(self, step: int): ...


    # create the trainers
    trainers = training_procs.spawn("trainers", Trainer)

    # tell all the trainers to to take a step
    fut = trainers.train.call(step=0)

    # wait for all trainers to complete
    fut.get()

> ⚠️ **Early Development Warning** Monarch is currently in an experimental
> stage. You should expect bugs, incomplete features, and APIs that may change
> in future versions. The project welcomes bugfixes, but to make sure things are
> well coordinated you should discuss any significant change before starting the
> work. It's recommended that you signal your intention to contribute in the
> issue tracker, either by filing a new issue or by claiming an existing one.

Note: Monarch is currently only supported on Linux systems

## Getting Started

Here are some suggested steps to get started with Monarch:

1. **Installation**: Check out the [Install guide](installation) for getting monarch installed.
2. **Getting Started**: The [getting started](./generated/examples/getting_started) provides an introduction to monarchs core API
2. **Explore Examples**: Review the [Examples](./generated/examples/index) to see Monarch in action
3. **Dive Deeper**: Explore the API Documentation for more detailed information:
    - [Python API](api/index)
    - [Rust API](rust-api)
4. **Deep Understanding of Actors**: Gain comprehensive knowledge of [Actors](actors), the foundational building blocks of Monarch.

```{toctree}
:maxdepth: 2
:caption: Contents
:hidden:
installation
./generated/examples/getting_started
./generated/examples/index
api/index
rust-api
actors
```

## License

Monarch is BSD-3 licensed, as found in the [LICENSE](https://github.com/meta-pytorch/monarch/blob/main/LICENSE) file.

## Community

We welcome contributions from the community! If you're interested in contributing, please:

1. Check the [GitHub repository](https://github.com/meta-pytorch/monarch)
2. Review existing issues or create a new one
3. Discuss your proposed changes before starting work
4. Submit a pull request with your changes

## Examples and blogs
- [Pytorch blog](https://pytorch.org/blog/introducing-pytorch-monarch/)
- [Demo notebook](https://github.com/meta-pytorch/monarch/blob/main/examples/presentation/presentation.ipynb)
- [DevX Pytorch tutorial](https://docs.pytorch.org/tutorials/intermediate/monarch_distributed_tutorial.html)
- [Lightning Monarch blog](https://lightning.ai/meta-ai/environments/large-scale-interactive-training-with-monarch)
- [Running on Kubernetes via SkyPilot](https://github.com/meta-pytorch/monarch/tree/main/examples/skypilot)
