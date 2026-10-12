# The validation tool as one image, for linux/amd64 and linux/arm64.
#
# Stage 1 builds the Rust load engine. Stage 2 holds Python, its pinned
# packages, the tool and the engine binary. Nobody who runs the image needs
# Python, a virtual environment or Rust on their own machine.
#
# Build for one platform:  docker build -t byoc-s3-validation .
# Build for both, and push: see docs/run_a_validation.md, "Build the image".

# The engine compiles on the build machine's own architecture, for the
# target one. Compiling under emulation crashed the Rust compiler.
FROM --platform=$BUILDPLATFORM rust:1-slim-bookworm AS engine
ARG TARGETARCH
WORKDIR /src
COPY loadgen/Cargo.toml loadgen/Cargo.lock ./
COPY loadgen/src ./src
COPY loadgen/examples ./examples
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) target=x86_64-unknown-linux-gnu; cc=x86_64-linux-gnu-gcc; pkg="gcc-x86-64-linux-gnu libc6-dev-amd64-cross" ;; \
      arm64) target=aarch64-unknown-linux-gnu; cc=aarch64-linux-gnu-gcc; pkg="gcc-aarch64-linux-gnu libc6-dev-arm64-cross" ;; \
      *) echo "unsupported platform: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    if [ "$(dpkg --print-architecture)" != "$TARGETARCH" ]; then \
      apt-get update; apt-get install -y --no-install-recommends $pkg; rm -rf /var/lib/apt/lists/*; \
      triple=$(echo "$target" | tr a-z- A-Z_); \
      export "CARGO_TARGET_${triple}_LINKER=$cc" "CC_$(echo "$target" | tr - _)=$cc"; \
    fi; \
    rustup target add "$target"; \
    cargo build --release --locked --bin byoc-loadgen --target "$target"; \
    cp "target/$target/release/byoc-loadgen" /byoc-loadgen

FROM python:3.12-slim-bookworm
# tini forwards Ctrl-C to the whole process group, so the load engine stops
# cleanly and still writes its summary. tzdata lets reports show local time.
RUN apt-get update \
 && apt-get install -y --no-install-recommends tini tzdata \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY run_validation.py run_certification.py ./
COPY src ./src
COPY config ./config
COPY examples ./examples
# The engine sources stay in the image: every run records their checksum.
COPY loadgen/Cargo.toml loadgen/Cargo.lock ./loadgen/
COPY loadgen/src ./loadgen/src
COPY bin/byoc-s3 docker/entrypoint.sh ./bin/
COPY --from=engine /byoc-loadgen /usr/local/bin/byoc-loadgen
# The container runs as the caller's own user, so everything must be
# readable by anyone, whatever the permissions were on the build machine.
RUN chmod -R a+rX /app && chmod a+rx /app/bin/*

# The image name `print-wrapper` writes into the wrapper script.
ARG IMAGE=byoc-s3-validation:latest
# git is not in the image, so the build records the commit instead.
ARG SOURCE_COMMIT=unknown
ENV BYOC_IMAGE=$IMAGE \
    BYOC_SOURCE_COMMIT=$SOURCE_COMMIT \
    BYOC_LOADGEN=/usr/local/bin/byoc-loadgen \
    BYOC_CONTAINER=1 \
    HOME=/tmp \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
LABEL org.opencontainers.image.title="Datadog BYOC Logs S3 storage validation" \
      org.opencontainers.image.revision=$SOURCE_COMMIT
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/app/bin/entrypoint.sh"]
CMD ["--help"]
