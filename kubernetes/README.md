# monarch-kubernetes

Contains a reference CRD and operator for [Monarch](https://github.com/meta-pytorch/monarch) workers.

This can be used to deploy Monarch workers to a Kubernetes cluster. The operator applies the CRD
to the cluster and applies labels so the controller can discover them.

# Current status

We have a CRD and an operator. This can be used to provision workers on the kubernetes cluster.
[KubernetesJob](https://github.com/meta-pytorch/monarch/blob/a933901cb6e3433c52cf990b48b1787dcf6a6fea/python/monarch/_src/job/kubernetes.py#L36)
can then be used to connect and create a proc mesh on the workers.

# Directory structure
operator/ - Contains the operator code that can apply the CRD to a cluster

images/ - Contains the Dockerfile for building the monarch worker image

# How to build


```
make generate
make manifests
# By default IMG=controller:latest so change that if you want to apply a different tag.
make docker-build CONTAINER_TOOL=podman
```

# License
This repo is BSD-3 licensed, as found in the LICENSE file.
