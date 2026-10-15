FROM node:22-bookworm-slim AS web

WORKDIR /web
COPY ruvio-ui/web/package.json ruvio-ui/web/package-lock.json ./
RUN npm ci
COPY ruvio-ui/web/ ./
RUN npm run build

FROM rust:1.97.1-slim-bookworm AS rust

RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY ruvio-client /src/ruvio-client
COPY ruvio-ui/Cargo.toml ruvio-ui/Cargo.lock /src/ruvio-ui/
COPY ruvio-ui/src /src/ruvio-ui/src
COPY --from=web /web/dist /src/ruvio-ui/web/dist
WORKDIR /src/ruvio-ui
RUN cargo build --locked --release

FROM debian:bookworm-slim

LABEL org.opencontainers.image.title="Ruvio UI" \
      org.opencontainers.image.description="Browser operator console for a Ruvio server." \
      org.opencontainers.image.source="https://github.com/salihcantekin/ruvio" \
      org.opencontainers.image.url="https://salihcantekin.github.io/ruvio/" \
      org.opencontainers.image.documentation="https://salihcantekin.github.io/ruvio/" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.authors="Salih Cantekin"

RUN useradd --system --uid 10001 --create-home ruvio
COPY --from=rust /src/ruvio-ui/target/release/ruvio-ui /usr/local/bin/ruvio-ui
COPY ruvio-ui/LICENSE /usr/local/share/ruvio-ui/LICENSE
USER ruvio
ENV RUVIO_UI_ADDR=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ruvio-ui"]
