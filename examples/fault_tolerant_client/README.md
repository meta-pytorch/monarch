# Fault-tolerant client

The keeper owns a trainer and two generators. The first client crashes during step 3. A replacement client reconnects to the same worker processes and resumes at step 3. The example then kills a generator. The client gets `SupervisionError`, and the keeper receives the worker failure.

This example uses Monarch’s private, experimental token API. Run it from the fbsource checkout on a trusted devvm.

```bash
buck2 run @fbcode//mode/dev-nosan fbcode//monarch/examples/fault_tolerant_client:fault_tolerant_client
```

A successful run ends with `[keeper] result: PASS`.
