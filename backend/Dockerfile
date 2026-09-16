FROM rust:1-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --workspace

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/api-server /usr/local/bin/api-server
COPY --from=builder /app/target/release/resolution-consumer /usr/local/bin/resolution-consumer
COPY --from=builder /app/target/release/final-consumer /usr/local/bin/final-consumer

WORKDIR /app
ENTRYPOINT []
