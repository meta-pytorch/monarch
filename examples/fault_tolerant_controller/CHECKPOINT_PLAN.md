# Controller checkpoint recovery plan

1. Publish a keeper actor. An independently bootstrapped controller process joins
   that token and supplies its `this_proc()` reference. The keeper actor spawns
   the application controller on that process, making the application controller
   its real supervised child. Token join alone does not adopt the bootstrap root.
2. Pass a reference to the keeper's host into the controller. Spawn trainer and
   generator process meshes and actors in the controller endpoint context, so
   both process and actor lifetimes belong to that controller. The host survives.
3. Store atomic, complete checkpoints in a keeper-owned temporary directory.
   Include trainer step/model and every generator RNG state. Crash only after
   additional uncommitted sampling, so recovery proves that RNG restoration works.
4. Kill the external controller process with SIGKILL. Require the keeper's
   controller-specific supervision event and natural termination of every old
   worker before launching a replacement. Safe emergency process cleanup makes
   the run fail; it must never manufacture a passing supervision result.
5. Restore into fresh worker processes, repeat recovery when requested, and
   compare the final checkpoint with an uninterrupted deterministic reference.
   Provide bounded join/run/cleanup deadlines, negative injections, run docs,
   and tests for checkpoint fidelity, repeated recovery, timeout, and cleanup.

The keeper and its host stay alive throughout. This example does not attempt
keeper migration, root adoption, or concurrent controller fencing.
The Controller actor is the client shutdown boundary: all application children
come from its context. Other actors on the joining `this_proc()` are ignored;
adopting that root/process is deferred Monarch core work.
