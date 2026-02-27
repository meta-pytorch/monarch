# Override on build from CI.
ARG PYTORCH_TAG=2.10.0-cuda12.8-cudnn9-runtime

# Build from latest pytorch stable image; should be relatively in sync with torchmonarch and pytorch.
FROM ghcr.io/pytorch/pytorch:${PYTORCH_TAG}

SHELL ["/bin/bash", "-c"]

# System dependencies.
RUN apt-get update -y && \
    apt-get install curl clang liblzma-dev libunwind-dev libibverbs-dev librdmacm-dev protobuf-compiler -y

# Install monarch w/ kubernetes.
RUN pip install torchmonarch[kubernetes] --break-system-packages

# Install torchx-nightly w/ kubernetes.
RUN pip install torchx-nightly[kubernetes] --break-system-packages

# Path
ENV LD_LIBRARY_PATH=/opt/conda/lib/python3.11/site-packages/torch/lib:/opt/conda/lib:$LD_LIBRARY_PATH

# Install VIM (nice to have on client for debugging; not necessary).
RUN apt-get update && apt-get install vim -y

# Install kubectl (nice to have on client for debugging; not necessary).
RUN curl -LO https://dl.k8s.io/release/v1.34.0/bin/linux/amd64/kubectl && chmod +x kubectl && mv kubectl /usr/local/bin

# Set /workspace as the working directory
WORKDIR /workspace

# Allow non-root user to update /workspace
RUN chgrp -R 0 /workspace && \
    chmod -R g=u /workspace

# Set container to run as non-root user
USER 1001
