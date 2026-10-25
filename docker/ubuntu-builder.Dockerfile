# syntax=docker/dockerfile:1

# Base image pinned by tag *and* digest so a re-tagged or updated
# `ubuntu:22.04` cannot silently change the builder's libc/toolchain.
ARG UBUNTU_IMAGE=ubuntu:22.04@sha256:0a9887931b0e38af2e3487ed39e29436b0535c4f3f52e57745baaf79d1898956
FROM ${UBUNTU_IMAGE}

ARG DEBIAN_FRONTEND=noninteractive
ARG RUST_VERSION=1.97.1

# rustup bootstrap, pinned: the installer is fetched at an immutable archive URL
# and checked against a recorded sha256 before it runs. `sh.rustup.rs | sh`
# executes whatever the redirect currently serves.
ARG RUSTUP_VERSION=1.29.2
ARG RUSTUP_INIT_X86_64_SHA256=5be191e8c70a5007074254cb74d78dbfdd25cd5a61c27de7b8e5410122ec24f4
ARG RUSTUP_INIT_AARCH64_SHA256=a9c168dbea81a84671c6b7a5c1795cb0eb699020110d107ddfca427bd797e697

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    clang \
    cmake \
    curl \
    git \
    libclang-dev \
    pkg-config \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_HOME=/root/.cargo
ENV RUSTUP_HOME=/root/.rustup
ENV PATH="${CARGO_HOME}/bin:${PATH}"
ENV CXXFLAGS="-include cstdint"

RUN set -eux; \
    case "$(dpkg --print-architecture)" in \
      amd64) rustup_target=x86_64-unknown-linux-gnu; rustup_sha256="${RUSTUP_INIT_X86_64_SHA256}" ;; \
      arm64) rustup_target=aarch64-unknown-linux-gnu; rustup_sha256="${RUSTUP_INIT_AARCH64_SHA256}" ;; \
      *) echo "unsupported architecture $(dpkg --print-architecture)" >&2; exit 1 ;; \
    esac; \
    curl --proto '=https' --tlsv1.2 -fsSLo /tmp/rustup-init \
      "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/${rustup_target}/rustup-init"; \
    echo "${rustup_sha256}  /tmp/rustup-init" | sha256sum -c -; \
    chmod +x /tmp/rustup-init; \
    /tmp/rustup-init -y --profile minimal --default-toolchain "${RUST_VERSION}"; \
    rm -f /tmp/rustup-init

WORKDIR /workspace/kresko
