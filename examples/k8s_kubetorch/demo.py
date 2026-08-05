"""
Kubetorch Monarch Integration Demo

This demo shows how to use Kubetorch to deploy Monarch workloads from outside
the Kubernetes cluster.
"""

# Monarch imports are wrapped in try-except because torchmonarch
# may not be installed locally - it may not install on the local
# OS and only needs to actually run on the cluster. We don't need
# this block if Monarch is installed locally.
try:
    from monarch.actor import Actor, endpoint
except ImportError:
    class Actor:
        pass

    def endpoint(fn):
        return fn


class CounterActor(Actor):
    """Simple counter actor that runs on Monarch workers.

    Must be defined at module level so kubetorch can import it by name.
    """

    def __init__(self, initial_value: int = 0):
        self.value = initial_value

    @endpoint
    def increment(self) -> int:
        self.value += 1
        return self.value

    @endpoint
    def add(self, amount: int) -> int:
        self.value += amount
        return self.value

    @endpoint
    def get_value(self) -> int:
        return self.value

    @endpoint
    def get_info(self) -> dict:
        import os
        import socket
        return {
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "value": self.value,
        }


def run_demo():
    import kubetorch as kt
    from monarch_kubetorch import KubernetesJob

    print("=" * 60)
    print("Kubetorch Monarch Integration Demo")
    print("=" * 60)

    print("[1] Creating KubernetesJob with 2 replicas...")
    job = KubernetesJob(
        compute=kt.Compute(
            cpus="1",
            replicas=2,
            memory="2Gi",
        ),
        name="monarch-demo",
        namespace="default",
    )

    try:
        print("[2] Getting job state...")
        state = job.state()
        workers = state.workers
        print(f"    Host mesh: {workers.sizes}")

        print("[3] Spawning processes...")
        proc_mesh = workers.spawn_procs(per_host={"procs": 1})
        print(f"    Proc mesh: {proc_mesh.sizes}")

        print("[4] Spawning CounterActor instances...")
        counters = proc_mesh.spawn("counters", CounterActor, initial_value=0)
        print(f"    Actor mesh: {counters.sizes}")

        print("[5] Getting actor info...")
        info = counters.get_info.call().get()
        for item in info.items():
            print(f"    {item}")

        print("[6] Calling increment 3 times...")
        for i in range(3):
            results = counters.increment.call().get()
            print(f"    After increment {i+1}: {list(results.values())}")

        print("[7] Calling add(10)...")
        results = counters.add.call(amount=10).get()
        print(f"    After add(10): {list(results.values())}")

        print("[8] Getting final values...")
        final = counters.get_value.call().get()
        print(f"    Final values: {list(final.values())}")

        print("=" * 60)
        print("Success! Monarch actors ran via Kubetorch.")
        print("=" * 60)

    finally:
        print("[9] Cleaning up...")
        job.kill()
        print("    Job terminated.")


if __name__ == "__main__":
    run_demo()
