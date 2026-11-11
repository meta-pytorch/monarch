# Override on build from CI.
ARG PYTORCH_TAG=2.15.0-cuda13.2-cudnn9-runtime

# Build from latest pytorch stable image; should be relatively in sync with torchmonarch and pytorch.
FROM ghcr.io/pytorch/pytorch:${PYTORCH_TAG}

ARG MONARCH_VERSION
SHELL ["/bin/bash", "-c"]

# System dependencies.
RUN apt-get update -y && \
    apt-get install curl clang fuse3 liblzma-dev libunwind-dev libibverbs-dev librdmacm-dev protobuf-compiler rsync -y

# Install monarch w/ kubernetes and NVIDIA GPU telemetry.
RUN python -m pip install "torchmonarch[kubernetes]==${MONARCH_VERSION}" "nvidia-ml-py>=13.580.65,<14" --break-system-packages && \
    python -c "import pynvml"

# Install torchx-nightly w/ kubernetes.
RUN pip install torchx-nightly[kubernetes] --break-system-packages

# Install VIM (nice to have on client for debugging; not necessary).
RUN apt-get update && apt-get install vim -y

# Install kubectl (nice to have on client for debugging; not necessary).
RUN curl -LO https://dl.k8s.io/release/v1.37.2/bin/linux/amd64/kubectl && chmod +x kubectl && mv kubectl /usr/local/bin
