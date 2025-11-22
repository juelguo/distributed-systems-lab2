FROM golang:1.15-buster

WORKDIR /workspace

# Install build essentials (useful for some Go tooling) and keep the image lean.
RUN sed -i 's@deb.debian.org@archive.debian.org@g; s@security.debian.org@archive.debian.org@g' /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Default to an interactive shell so you can run `go test`, `make`, etc.
CMD ["/bin/bash"]