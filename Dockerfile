# syntax=docker/dockerfile:1.7

FROM rust:1.97.1-bookworm AS build

WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release -p runnel-server \
    && cp target/release/runnel /tmp/runnel

FROM debian:bookworm-slim

RUN useradd --system --uid 10001 --create-home runnel \
    && mkdir -p /var/lib/runnel \
    && chown -R runnel:runnel /var/lib/runnel

COPY --from=build /tmp/runnel /usr/local/bin/runnel

USER runnel
VOLUME ["/var/lib/runnel"]
EXPOSE 4222 7000 8080

ENTRYPOINT ["runnel"]
CMD ["--data-dir", "/var/lib/runnel", "--listen", "0.0.0.0:4222", "--http-listen", "0.0.0.0:8080"]
