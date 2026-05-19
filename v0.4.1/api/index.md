# Python API

Note

This documents Monarch's **public APIs** - the stable, supported interfaces.

The actor API for monarch is accessed through the [monarch.actor](monarch.actor.html) package.
The [monarch](monarch.html) package contains APIs related to computing with distributed tensors.
The [monarch.job](monarch.job.html) package provides a declarative interface for managing distributed job resources.
The [monarch.rdma](monarch.rdma.html) package provides RDMA support for high-performance networking.
The [monarch.spmd](monarch.spmd.html) package provides primitives for running torchrun-style SPMD scripts over Monarch meshes.

- [monarch.actor](monarch.actor.html)
- [Creating Actors](monarch.actor.html#creating-actors)

- [`HostMesh`](monarch.actor.html#monarch.actor.HostMesh)
- [`ProcMesh`](monarch.actor.html#monarch.actor.ProcMesh)
- [`get_or_spawn_controller()`](monarch.actor.html#monarch.actor.get_or_spawn_controller)
- [`this_host()`](monarch.actor.html#monarch.actor.this_host)
- [`this_proc()`](monarch.actor.html#monarch.actor.this_proc)
- [Defining Actors](monarch.actor.html#defining-actors)

- [`Actor`](monarch.actor.html#monarch.actor.Actor)
- [`endpoint()`](monarch.actor.html#monarch.actor.endpoint)
- [Messaging Actor](monarch.actor.html#messaging-actor)

- [`Endpoint`](monarch.actor.html#monarch.actor.Endpoint)
- [`Future`](monarch.actor.html#monarch.actor.Future)
- [`ValueMesh`](monarch.actor.html#monarch.actor.ValueMesh)
- [`ActorError`](monarch.actor.html#monarch.actor.ActorError)
- [`Accumulator`](monarch.actor.html#monarch.actor.Accumulator)
- [`send()`](monarch.actor.html#monarch.actor.send)
- [`Channel`](monarch.actor.html#monarch.actor.Channel)
- [`Port`](monarch.actor.html#monarch.actor.Port)
- [`PortReceiver`](monarch.actor.html#monarch.actor.PortReceiver)
- [`as_endpoint()`](monarch.actor.html#monarch.actor.as_endpoint)
- [Context API](monarch.actor.html#context-api)

- [`current_actor_name()`](monarch.actor.html#monarch.actor.current_actor_name)
- [`current_rank()`](monarch.actor.html#monarch.actor.current_rank)
- [`current_size()`](monarch.actor.html#monarch.actor.current_size)
- [`context()`](monarch.actor.html#monarch.actor.context)
- [`Context`](monarch.actor.html#monarch.actor.Context)
- [`Point`](monarch.actor.html#monarch.actor.Point)
- [`Extent`](monarch.actor.html#monarch.actor.Extent)
- [Supervision](monarch.actor.html#supervision)

- [`MeshFailure`](monarch.actor.html#monarch.actor.MeshFailure)
- [monarch.config](monarch.config.html)
- [Configuration API](monarch.config.html#configuration-api)

- [`configure()`](monarch.config.html#monarch.config.configure)
- [`configured()`](monarch.config.html#monarch.config.configured)
- [`get_global_config()`](monarch.config.html#monarch.config.get_global_config)
- [`get_runtime_config()`](monarch.config.html#monarch.config.get_runtime_config)
- [`clear_runtime_config()`](monarch.config.html#monarch.config.clear_runtime_config)
- [Configuration Keys](monarch.config.html#configuration-keys)

- [Performance and Transport](monarch.config.html#performance-and-transport)
- [Timeouts](monarch.config.html#timeouts)
- [Logging](monarch.config.html#logging)
- [Message Handling](monarch.config.html#message-handling)
- [Message Encoding](monarch.config.html#message-encoding)
- [Mesh Bootstrap](monarch.config.html#mesh-bootstrap)
- [Runtime and Buffering](monarch.config.html#runtime-and-buffering)
- [Actor Configuration](monarch.config.html#actor-configuration)
- [Mesh Configuration](monarch.config.html#mesh-configuration)
- [Mesh Admin](monarch.config.html#mesh-admin)
- [Mesh Attach](monarch.config.html#mesh-attach)
- [Remote Allocation](monarch.config.html#remote-allocation)
- [Validation and Error Handling](monarch.config.html#validation-and-error-handling)
- [Examples](monarch.config.html#examples)

- [Basic Configuration](monarch.config.html#basic-configuration)
- [Temporary Configuration (Testing)](monarch.config.html#temporary-configuration-testing)
- [Nested Overrides](monarch.config.html#nested-overrides)
- [Duration Formats](monarch.config.html#duration-formats)
- [Environment Variable Override](monarch.config.html#environment-variable-override)
- [See Also](monarch.config.html#see-also)
- [monarch.job](monarch.job.html)
- [Job Model](monarch.job.html#job-model)
- [Job State](monarch.job.html#job-state)

- [`JobState`](monarch.job.html#monarch.job.JobState)
- [Job Base Class](monarch.job.html#job-base-class)

- [`JobTrait`](monarch.job.html#monarch.job.JobTrait)
- [Job Implementations](monarch.job.html#job-implementations)

- [LocalJob](monarch.job.html#localjob)
- [ProcessJob](monarch.job.html#processjob)
- [SlurmJob](monarch.job.html#slurmjob)
- [KubernetesJob](monarch.job.html#kubernetesjob)
- [Serialization](monarch.job.html#serialization)

- [`job_load()`](monarch.job.html#monarch.job.job_load)
- [`job_loads()`](monarch.job.html#monarch.job.job_loads)
- [SPMD Jobs](monarch.job.html#spmd-jobs)

- [`serve()`](monarch.job.html#monarch.job.spmd.serve)
- [`SPMDJob`](monarch.job.html#monarch.job.spmd.SPMDJob)
- [monarch](monarch.html)

- [`Tensor`](monarch.html#monarch.Tensor)
- [`Stream`](monarch.html#monarch.Stream)
- [`remote()`](monarch.html#monarch.remote)
- [`coalescing()`](monarch.html#monarch.coalescing)
- [`get_active_mesh()`](monarch.html#monarch.get_active_mesh)
- [`no_mesh()`](monarch.html#monarch.no_mesh)
- [`to_mesh()`](monarch.html#monarch.to_mesh)
- [`slice_mesh()`](monarch.html#monarch.slice_mesh)
- [`get_active_stream()`](monarch.html#monarch.get_active_stream)
- [`reduce()`](monarch.html#monarch.reduce)
- [`reduce_()`](monarch.html#monarch.reduce_)
- [`call_on_shard_and_fetch()`](monarch.html#monarch.call_on_shard_and_fetch)
- [`fetch_shard()`](monarch.html#monarch.fetch_shard)
- [`inspect()`](monarch.html#monarch.inspect)
- [`show()`](monarch.html#monarch.show)
- [`grad_function()`](monarch.html#monarch.grad_function)
- [`grad_generator()`](monarch.html#monarch.grad_generator)
- [`timer()`](monarch.html#monarch.timer)
- [`world_mesh()`](monarch.html#monarch.world_mesh)
- [`function_resolvers()`](monarch.html#monarch.function_resolvers)
- [Types](monarch.html#types)

- [`Extent`](monarch.html#monarch.Extent)
- [`Shape`](monarch.html#monarch.Shape)
- [`Selection`](monarch.html#monarch.Selection)
- [`NDSlice`](monarch.html#monarch.NDSlice)
- [`OpaqueRef`](monarch.html#monarch.OpaqueRef)
- [`Future`](monarch.html#monarch.Future)
- [Distributed Computing](monarch.html#distributed-computing)

- [`RemoteProcessGroup`](monarch.html#monarch.RemoteProcessGroup)
- [Simulation](monarch.html#simulation)

- [`Simulator`](monarch.html#monarch.Simulator)
- [`set_meta()`](monarch.html#monarch.set_meta)
- [Builtins](monarch.html#module-monarch.builtins)
- [monarch.rdma](monarch.rdma.html)
- [RDMA Buffer](monarch.rdma.html#rdma-buffer)

- [`RDMABuffer`](monarch.rdma.html#monarch.rdma.RDMABuffer)
- [RDMA Actions](monarch.rdma.html#rdma-actions)

- [`RDMAAction`](monarch.rdma.html#monarch.rdma.RDMAAction)
- [Utility Functions](monarch.rdma.html#utility-functions)

- [`is_ibverbs_available()`](monarch.rdma.html#monarch.rdma.is_ibverbs_available)
- [`is_rdma_available()`](monarch.rdma.html#monarch.rdma.is_rdma_available)
- [`get_rdma_backend()`](monarch.rdma.html#monarch.rdma.get_rdma_backend)
- [monarch.spmd](monarch.spmd.html)

- [Environment Setup](monarch.spmd.html#environment-setup)
- [SPMD Actor](monarch.spmd.html#spmd-actor)