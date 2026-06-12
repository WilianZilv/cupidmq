# CupidMQ master + dashboard (static served on the metrics HTTP port).

FROM node:22-alpine AS dashboard
WORKDIR /build/dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./
COPY assets/ /build/assets/
# Same origin as master HTTP — API routes live under /metrics*
ENV VITE_CUPIDMQ_METRICS=/metrics
RUN npm run build

FROM rust:1-bookworm AS rust
WORKDIR /build
COPY master/ ./master/
RUN cargo build --release --manifest-path master/Cargo.toml

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=rust /build/master/target/release/cupidmq /usr/local/bin/cupidmq
COPY --from=dashboard /build/dashboard/dist /app/dashboard
COPY docker/cupidmq.conf /app/cupidmq.conf

EXPOSE 9750 9752

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/bin/curl", "-fsS", "http://127.0.0.1:9752/health"]

CMD ["cupidmq", "--config", "/app/cupidmq.conf"]
