FROM rust:1.97-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
RUN cargo build --release --workspace

FROM debian:bookworm-slim AS server
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/blazechat-server /usr/local/bin/blazechat-server
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["blazechat-server"]

FROM debian:bookworm-slim AS bench
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/blazechat-bench /usr/local/bin/blazechat-bench
USER 65532:65532
ENTRYPOINT ["blazechat-bench"]
