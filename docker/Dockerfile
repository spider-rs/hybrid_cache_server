# ---------- builder ----------
FROM rust:1.84-alpine AS builder

WORKDIR /app

# Build deps for rocksdb-sys on Alpine
RUN apk add --no-cache \
    build-base clang llvm-dev libclang \
    cmake git pkgconfig \
    openssl-dev ca-certificates \
    rocksdb-dev zstd-dev snappy-dev lz4-dev

COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main(){}" > src/main.rs && cargo build --release && rm -rf src

COPY . .
RUN cargo build --release

# ---------- runtime ----------
FROM alpine:3.20 AS runtime

WORKDIR /app

RUN apk add --no-cache \
    ca-certificates \
    libgcc libstdc++ \
    openssl \
    rocksdb

ARG BIN_NAME=hybrid_cache_server
COPY --from=builder /app/target/release/${BIN_NAME} /usr/local/bin/${BIN_NAME}

ENV CACHE_PORT=8080
ENV MEILI_HOST=http://meilisearch:7700
ENV MEILI_MASTER_KEY=masterKey
ENV MEILI_INDEX=hybrid_cache

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/hybrid_cache_server"]
