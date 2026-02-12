# GPU collective demo

This demo gives an example of running a GPU collective across ranks using native Monarch Kubernetes integration.

## Provision Monarch Hosts from Python

No worker YAML manifests needed. The Python script creates the MonarchMesh CRDs directly.

```shell
kubectl create namespace monarch-tests

# Create the service account for the controller and bind the cluster role to it:
kubectl apply -f manifests/client-rbac.yaml

# start the controller
kubectl apply -f manifests/simple_controller.yaml

# Get a shell into the controller and run the collective across all workers
kubectl cp main.py monarch-tests/monarch-client-demo:/tmp/main.py
kubectl exec -it monarch-client-demo -n monarch-tests -- \
  python /tmp/main.py --provision --num_hosts=2 --num_gpus_per_host=4
```

Cleanup:

```shell
kubectl delete -f manifests/client-rbac.yaml
kubectl delete -f manifests/simple_controller.yaml
```

## Provision Monarch Hosts with MonarchMesh YAML Manifests

```shell
kubectl create namespace monarch-tests

# Run the worker mesh
kubectl apply -f manifests/gpu_mesh.yaml

# Create the service account for the controller and bind the cluster role to it:
kubectl apply -f manifests/client-rbac.yaml

# Start the controller
kubectl apply -f manifests/simple_controller.yaml

# Get a shell into the controller and run the collective across all workers
kubectl cp main.py monarch-tests/monarch-client-demo:/tmp/main.py
kubectl exec -it monarch-client-demo -n monarch-tests -- \
  python /tmp/main.py --num_hosts=2 --num_gpus_per_host=4
```

Cleanup:

```shell
kubectl delete -f manifests/gpu_mesh.yaml
kubectl delete -f manifests/client-rbac.yaml
kubectl delete -f manifests/simple_controller.yaml
```
