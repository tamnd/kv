# Consumed by GoReleaser: it copies the already cross-compiled binary out of the
# build context rather than compiling, so the image build is fast and uses the
# same static binary every other artifact ships.
#
# kv is a single static pure-Go binary with no runtime dependencies, so the image
# is a minimal base plus the binary and the certificate bundle the server needs
# to validate JWKS endpoints over TLS.
#
# GoReleaser builds one multi-platform image with buildx and stages each
# platform's binary under a $TARGETPLATFORM directory (e.g. linux/amd64/) in the
# build context, so the COPY line selects the right one through the automatic
# TARGETPLATFORM build arg.
FROM alpine:3.21

ARG TARGETPLATFORM

# ca-certificates for outbound HTTPS (OIDC/JWKS); tzdata for sane timestamps.
RUN apk add --no-cache ca-certificates tzdata \
 && mkdir -p /data

COPY $TARGETPLATFORM/kv /usr/bin/kv

WORKDIR /data

# Databases live under the mounted /data volume by default:
#
#   docker run -v "$PWD/data:/data" ghcr.io/tamnd/kv create /data/app.kv
#   docker run -p 8480:8480 -v "$PWD/data:/data" ghcr.io/tamnd/kv serve /data/app.kv --addr :8480 --insecure
#
VOLUME ["/data"]

EXPOSE 8480

ENTRYPOINT ["/usr/bin/kv"]
