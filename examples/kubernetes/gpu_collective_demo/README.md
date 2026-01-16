# GPU collective demo

This demo gives an example of running a GPU collective across ranks using native Monarch Kubernetes integration.

# How to run

```shell
# Run the worker mesh
kubectl apply -f manifests/gpu_mesh.yaml

# Start the controller
kubectl apply -f manifests/simple_controller.yaml

# Get a shell into the controller and run the collective across all workers
kubectl cp main.py monarch-tests/monarch-client-demo:/tmp/main.py
kubectl exec -it monarch-client-demo -n monarch-tests -- /bin/bash
# Inside the controller run this:
python /tmp/main.py --num_hosts=2 --num_gpus_per_host=4

```
