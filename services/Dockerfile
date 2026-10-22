# syntax=docker/dockerfile:1
# One image for every Rust service; the container's command picks the binary
# (docs/design.md §10). Built from the workspace root (see .dockerignore).

FROM rust:1.99.0-trixie AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY services services
COPY director director
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bins \
 && mkdir /out \
 && find target/release -maxdepth 1 -type f -perm -u+x -exec cp {} /out/ \;

FROM debian:trixie-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
USER nobody
