# Building Docker Images for OCI Example

## Building Nvidia Docker Image (with RDMA support)

```bash
cd monarch-oci/examples/kubernetes/oci_kubernetes_job/docker

podman machine start
podman build -f Dockerfile-nvidia-rdma -t my_monarch:0.4.1-rdma-cuda12.8 .
podman tag my_monarch:0.4.1-rdma-cuda12.8 ghcr.io/dochakov-oci/monarch-oci:monarch0.4.1-cuda12.8-rdma-01

podman login ghcr.io
podman push ghcr.io/dochakov-oci/monarch-oci:monarch0.4.1-cuda12.8-rdma-01
```

## Building AMD Docker Image

```bash
cd monarch-oci/examples/kubernetes/oci_kubernetes_job/docker

podman machine start
podman build -f Dockerfile-amd -t my_monarch:0.4.1-rocm7.2.1 .
podman tag my_monarch:0.4.1-rocm7.2.1 ghcr.io/dochakov-oci/monarch-oci:monarch0.4.1-rocm7.2.1-02

podman login ghcr.io
podman push ghcr.io/dochakov-oci/monarch-oci:monarch0.4.1-rocm7.2.1-02
```
