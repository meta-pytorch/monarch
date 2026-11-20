# Preserve workers across controller crashes

1. Add a separate CPU-only example. The root owns a Keeper actor; Keeper spawns
   two persistent worker actors and a Controller actor on each
   external bootstrap process. Token connection supplies references only;
   spawning Controller in Keeper's endpoint establishes actual supervision.
2. Keep deterministic step payloads and an operation ledger on each worker.
   Stable step IDs survive controller epochs; duplicate identical updates return
   the committed result, and changed payloads fail. Record active work and
   committed-but-unanswered operations to exercise lost replies.
3. Have the keeper driver kill several joined controller processes after seeded
   random delays while requests are outstanding, reap each process, and wait for
   Keeper's matching supervision event before admitting the next generation.
   Workers register controller destinations before work and tolerate selected
   recipient-loss failures for those sessions without advance crash
   notification. Unrelated worker failures remain visible.
4. Check worker process and actor identities after every restart, exact final
   per-worker optimizer values and ledger contents, late reply handling, and real
   controller supervision,
   worker-actor failure propagation, and complete cleanup. Bound all calls and the
   overall run; provide negative timeout/failure probes and a repeatable stress
   driver. Document private APIs, trust assumptions, and the keeper-failure limit.
5. Run formatting/type checks and real multi-process runs with multiple seeds,
   including negative probes, then commit this example independently.

The implemented model uses independent toy optimizer ledgers on two worker ranks;
it does not claim a transactional trainer/generator or real optimizer protocol.
The Controller actor is the client shutdown boundary; retained workers are
keeper-owned siblings. Unrelated actors on the joining `this_proc()` are ignored.
Adopting that root/process is deferred Monarch core work.
