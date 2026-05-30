# ---------- Build stage ----------
FROM rust:1.78-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

# ---------- Runtime stage ----------
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    ffmpeg ffprobe libssl3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/media-pipeline /usr/local/bin/media-pipeline
ENTRYPOINT ["/usr/local/bin/media-pipeline"]
CMD ["run", "--config", "/etc/media-pipeline/config.toml"]
