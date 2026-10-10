# Bootstrapping Overview

*One sentence:* the controller boots remote hosts, then spawns procs (**1 proc = 1 OS process**). Each host runs one **HostAgent**; **every proc runs its own ProcAgent** supervising that proc's user/service actors.

![High-level architecture](image/mesh-elements.png)
**Figure 1.** High-level architecture of the host, procs, and agents.

Legend: ◻︎ = proc · ○ = actor (service or user).

## What you're looking at

- **Controller process** (left): owns orchestration APIs and kicks off bootstrapping.
- **HostAgent** (one per host): manages host-local resources and the service proc.
- **ProcAgent** (one per proc): supervises that proc's actors (service + user).
- **Procs** (squares): runtime containers; *1 proc = 1 OS process*.

## Boot sequence (10k-ft)

1. Obtain a **proc + instance** (control endpoint).
2. Use the **v0 process allocator** and **bootstrap handshake** to get remote runtimes.
3. Promote those runtimes into real **hosts** (start HostAgent).
4. **Spawn procs and actors** on those hosts (start proc/ProcAgent, spawn actors).

*For the full, runnable test (see `bootstrap_canonical_simple`), see the appendix.*