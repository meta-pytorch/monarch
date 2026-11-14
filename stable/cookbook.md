# Monarch Cookbook

Task-oriented recipes for common Monarch patterns. Each recipe is small,
self-contained, and answers a single "how do I ...?" question.

Every snippet below is embedded directly from a test in
[`python/tests/test_cookbook.py`](https://github.com/meta-pytorch/monarch/blob/main/python/tests/test_cookbook.py)
and runs in CI, so the code here stays in sync with the API. To add a recipe,
write a test with a `# cookbook: <slug>` / `# cookbook: end` marker pair and
embed the region here with `literalinclude`.

## Meshes and Slicing

### Get the rank-0 actor of a mesh

You often need the rank-0 actor of a mesh, for example to use it as the coordinator
of the SPMD job running on the mesh. Do:

[View source](https://github.com/meta-pytorch/monarch/blob/51656e5f33f4b832329734bfa6289a0cf321b5e5/python/tests/test_cookbook.py#L48-L49)

```
rank_0 = counters.flatten("rank").slice(rank=0)
await rank_0.increment.call_one()
```

- `flatten` collapses every dimension into one named dimension, so this works
regardless of the mesh's original shape. This is more robust than slicing by the
original dimension names (e.g. `generator.slice(hosts=0, gpus=0)`), which breaks
if those names change.
- `slice` then selects a single rank. The result is a singleton mesh, so
`call_one` applies.

See [Slicing Operations](actors.html#slicing-operations) in the Actors guide for the full slicing API.

### Scatter different data to groups of ranks

Sometimes you need to send different data to different groups of ranks. In RL,
for example, the controller owns the training data and must send each
data-parallel (DP) group of the trainer its own batch.

To do this, first find the DP groups by splitting the mesh into your trainer's
parallel dimensions:

[View source](https://github.com/meta-pytorch/monarch/blob/51656e5f33f4b832329734bfa6289a0cf321b5e5/python/tests/test_cookbook.py#L79-L82)

```
parallel_dims = {"pp": 2, "dp": 2, "tp": 2}
trainer = procs.spawn("trainer", Trainer, parallel_dims)
mesh = trainer.flatten("rank").split(rank=tuple(parallel_dims), **parallel_dims)
by_dp_rank = [mesh.slice(dp=i) for i in range(mesh.size("dp"))]
```

Optionally, check the layout once at startup:

[View source](https://github.com/meta-pytorch/monarch/blob/51656e5f33f4b832329734bfa6289a0cf321b5e5/python/tests/test_cookbook.py#L86-L89)

```
for dp_rank, trainer_at_dp_rank in enumerate(by_dp_rank):
 reported = set((await trainer_at_dp_rank.get_dp_rank.call()).values())
 if reported != {dp_rank}:
 raise RuntimeError(f"DP slice {dp_rank} holds DP ranks {reported}")
```

Then send each DP group its batch. `asyncio.gather` issues the calls
concurrently and returns one `ValueMesh` per group:

[View source](https://github.com/meta-pytorch/monarch/blob/51656e5f33f4b832329734bfa6289a0cf321b5e5/python/tests/test_cookbook.py#L93-L98)

```
await asyncio.gather(
 *[
 trainer_at_dp_rank.train_step.call(batches[dp_rank])
 for dp_rank, trainer_at_dp_rank in enumerate(by_dp_rank)
 ]
)
```

## Intra-mesh Communication

### Call a specific rank of your own mesh

An actor often needs to reach another rank of its own mesh -- for example, a
coordinator at rank 0. Give each actor a handle to its mesh after spawning, then
slice that handle to the target rank inside an endpoint:

[View source](https://github.com/meta-pytorch/monarch/blob/51656e5f33f4b832329734bfa6289a0cf321b5e5/python/tests/test_cookbook.py#L138-L139)

```
coordinator = self.mesh.flatten("rank").slice(rank=0)
return await coordinator.ping.call_one()
```

`self.mesh` is the actor's own `ActorMesh`. An actor cannot reference its own
mesh at construction time, so we pass the handle to every member through a
`set_mesh` endpoint once the mesh is spawned. Slicing that handle yields a
reference to the chosen rank, so the call is a direct point-to-point message.

### Return results asynchronously with a port

The previous recipe returns synchronously because `call` (and `call_one`) hand
back the endpoint's return value inline. A port is the asynchronous counterpart.
The coordinator opens a `Channel` and `broadcast`s the sending half (a `Port`) to
the other ranks, which send or stream results back to it whenever they are ready:

[View source](https://github.com/meta-pytorch/monarch/blob/51656e5f33f4b832329734bfa6289a0cf321b5e5/python/tests/test_cookbook.py#L170-L174)

```
port, receiver = Channel[int].open()
others = self.mesh.flatten("rank").slice(rank=slice(1, None))
others.produce.broadcast(port)
expected = others.size() * STREAM_LEN
return sorted([await receiver.recv() for _ in range(expected)])
```

Each rank's `produce` endpoint launches a background task and returns
immediately; the task sends its values to the port over time:

[View source](https://github.com/meta-pytorch/monarch/blob/51656e5f33f4b832329734bfa6289a0cf321b5e5/python/tests/test_cookbook.py#L180-L186)

```
rank = context().actor_instance.rank["gpus"]

async def stream() -> None:
 for step in range(STREAM_LEN):
 port.send(rank * 10 + step)

self.task = asyncio.create_task(stream())
```