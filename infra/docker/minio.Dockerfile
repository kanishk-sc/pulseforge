# The public Quay image for this exact release became inaccessible to fresh CI
# runners. Use the official release asset, verified by its published SHA-256,
# without changing the MinIO version or the existing data volume.
FROM alpine:3.22

RUN apk add --no-cache ca-certificates curl

ARG MINIO_RELEASE=RELEASE.2025-09-07T16-13-09Z
ARG TARGETARCH
RUN arch="${TARGETARCH:-amd64}" \
    && case "$arch" in \
         amd64) checksum=7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f ;; \
         arm64) checksum=5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d ;; \
         *) echo "unsupported MinIO architecture: $arch" >&2; exit 1 ;; \
       esac \
    && curl --fail --show-error --location --retry 3 \
      "https://github.com/minio/minio/releases/download/${MINIO_RELEASE}/minio.linux-${arch}.${MINIO_RELEASE}" \
      --output /usr/local/bin/minio \
    && echo "${checksum}  /usr/local/bin/minio" | sha256sum -c \
    && chmod 0755 /usr/local/bin/minio

EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/minio"]
