# syntax=docker/dockerfile:1.7
#
# Multi-stage build for media-pipeline.
#
# Stage 1 (builder): compiles the release binary from src/main.rs.
# Stage 2 (runtime): minimal Debian with ffmpeg + the binary.
#
# Build cache trick: copy only Cargo.toml/Cargo.lock first and run a
# dummy build to populate the registry/git crates. Source changes
# afterwards don't bust the deps layer. The stub src/main.rs is
# required because `cargo build` needs at least one source file.

# ---------- Build stage ----------
# Base image is `nightly` because a downstream crate uses the
# `edition2024` cargo feature (unstable on stable). The repo's
# `rust-toolchain.toml` pins `channel = "nightly"` so this base
# image's toolchain is exactly what `cargo` expects inside it.
FROM rustlang/rust:nightly-bookworm AS builder
WORKDIR /build

# Toolchain + build deps for openssl-sys (russh feature) and rusqlite-bundled.
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Deps layer: copy manifests and a placeholder source tree, build, throw
# away the placeholder. Subsequent builds with only src/ changes reuse this.
# The stub src/main.rs is required because `cargo build` needs at least
# one source file to resolve the [[bin]] target.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
    && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --bin media-pipeline \
    && rm -rf src target/release/deps/media-pipeline* target/release/media-pipeline

# Real source: now the actual code gets compiled against the cached deps.
COPY src ./src
RUN cargo build --release --bin media-pipeline \
    && strip target/release/media-pipeline

# ---------- Runtime stage ----------
FROM debian:bookworm-slim AS runtime

# Runtime libs only:
#   ffmpeg / ffprobe — transcode step
#   libssl3 / ca-certificates — russh + reqwest TLS
#   tini — PID 1 + signal forwarding
#   sqlite3 — bundled rusqlite doesn't need a system lib, but having
#             the CLI on hand is useful for debugging
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        libssl3 \
        ca-certificates \
        tini \
        sqlite3 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /etc/media-pipeline /staging /library /data

# Non-root user. UID 1000 matches the common "first interactive user"
# convention so bind-mounts from the host don't show files as owned by
# root. The pipeline writes to /staging, /library, and /data; /etc is
# expected to be bind-mounted read-only from the host (see
# docker-compose.yml), so we don't chown it.
RUN groupadd --gid 1000 pipeline \
    && useradd  --uid 1000 --gid pipeline --shell /bin/bash --create-home pipeline \
    && chown -R pipeline:pipeline /staging /library /data

COPY --from=builder /build/target/release/media-pipeline /usr/local/bin/media-pipeline

# Bake the default config into the image. The image is self-sufficient:
# it can run with no host bind-mount and rely on env-var overrides for
# deployment-specific values. Operators who want to change structural
# fields (categories, section IDs) bind-mount their own TOML over this
# path; otherwise the in-image config is the source of truth.
#
# We copy the .example file directly. It's the single source of truth —
# editing the .example changes both the docs operators read AND the
# image's runtime defaults.
COPY config/media-pipeline.toml.example /etc/media-pipeline/config.toml
RUN chown -R pipeline:pipeline /etc/media-pipeline

USER pipeline
WORKDIR /home/pipeline

# Entrypoint is the binary; CMD defaults to running the full pipeline
# against the baked-in config. Env vars override TOML fields at
# startup. Override the subcommand for one-off operations:
#   docker run ... media-pipeline status
#   docker run ... media-pipeline sync-only
#   docker run ... media-pipeline process-only
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/media-pipeline"]
CMD ["run", "--config", "/etc/media-pipeline/config.toml"]
