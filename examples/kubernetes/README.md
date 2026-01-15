# Running Monarch on Kubernetes

This directory contains examples for running Monarch on Kubernetes. KubernetesJob supports Monarch hosts provisioned by MonarchMesh CRD.
It is also vendor-independent and lets users decide how they want to schedule and orchestrate Monarch hosts.

## Provision Monarch Hosts with MonarchMesh CRD and operator
Coming Soon!

## Provision Monarch Hosts with Third-Party Scheduler
### Volcano Scheduler
#### Prerequisites

- A Kubernetes cluster with [Volcano scheduler](https://volcano.sh/en/docs/installation/) installed
- `kubectl` configured to access the cluster
- The `monarch-tests` namespace created

#### Provisioning Monarch Hosts with Volcano Scheduler

The `volcano_workers.yaml` manifest launches Monarch worker pods using Volcano's gang scheduling. This ensures all pods in a mesh are scheduled together or not at all.

Volcano automatically adds labels to pods:
- `volcano.sh/job-name` - the Volcano Job name (used for pod discovery)
- `volcano.sh/task-index` - ordinal index (0, 1, 2, ...) for ordering workers

#### Deploy the workers

```bash
kubectl apply -f volcano_workers.yaml
```

#### Verify pods are running

```bash
kubectl get pods -n monarch-tests -l volcano.sh/job-name=mesh1
kubectl get pods -n monarch-tests -l volcano.sh/job-name=mesh2
```

#### Running the Example

Once the workers are running, execute the example script from a pod within the cluster:

```bash
python hello_kubernetes_job.py --volcano
```

The `--volcano` flag configures `KubernetesJob` to use Volcano's labels:
- `volcano.sh/job-name=<mesh-name>` for pod discovery
- `volcano.sh/task-index` for pod ordering

#### Cleanup

```bash
kubectl delete -f volcano_workers.yaml
```
