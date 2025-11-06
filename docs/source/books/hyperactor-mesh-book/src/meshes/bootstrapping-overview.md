# Bootstrapping Overview

This section explains one concrete, working way to bring up a mesh — the shape shown in the unit test `bootstrap_cannonical_simple`. The later pages break it into layers:

1. get a proc + instance (control endpoint)
2. use the v0 process allocator and bootstrap handshake to get remote runtimes
3. turn those runtimes into real hosts
4. then spawn procs and actors on those hosts

For the full, runnable test, see the appendix.
