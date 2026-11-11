# Build the manager binary
FROM golang:1.24 AS builder
ARG TARGETOS
ARG TARGETARCH

# Proxy settings passed at build time (optional)
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY

# Force IPv4 for Go network operations
ENV GODEBUG=netdns=go
ENV GOPROXY=https://proxy.golang.org,direct
ENV HTTP_PROXY=${HTTP_PROXY}
ENV HTTPS_PROXY=${HTTPS_PROXY}
ENV NO_PROXY=${NO_PROXY}

WORKDIR /workspace

# Copy source and build
COPY . .
# the GOARCH has no default value to allow the binary to be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -o manager ./cmd/main.go

# Use scratch as the base - works for statically compiled Go binaries
# No external image pull needed
FROM scratch
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=builder /etc/passwd /etc/passwd
USER 65532:65532

ENTRYPOINT ["/manager"]
