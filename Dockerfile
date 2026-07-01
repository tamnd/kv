# Consumed by GoReleaser: it copies the already cross-compiled binary out of the
# build context rather than compiling, so the image build is fast and uses the
# same static binary every other artifact ships.
#
# kv is a single static pure-Go binary with no runtime dependencies. The image is
# a minimal base plus the binary.
#
# GoReleaser builds one multi-platform image with buildx and stages each
# platform's binary under a $TARGETPLATFORM directory (e.g. linux/amd64/) in the
# build context, so the COPY line selects the right one through the automatic
# TARGETPLATFORM build arg.
FROM alpine:3.21

ARG TARGETPLATFORM

# tzdata for sane timestamps; ca-certificates to keep the base well formed.
RUN apk add --no-cache ca-certificates tzdata \
 && mkdir -p /data

COPY $TARGETPLATFORM/kv /usr/bin/kv

WORKDIR /data

# kv serves one store over the Redis wire protocol. Point it at a data directory
# on the mounted volume and expose the RESP port, then talk to it with any Redis
# client:
#
#   docker run -p 6379:6379 -v "$PWD/data:/data" ghcr.io/tamnd/kv --addr :6379 --dir /data
#   redis-cli -p 6379 set greeting hello
#
VOLUME ["/data"]

EXPOSE 6379

ENTRYPOINT ["/usr/bin/kv"]
