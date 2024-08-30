##########
# NOTE: When building this image, there is an assumption that you are in the top level directory of the repository.
# $ docker build . -f ./Dockerfile -t zdm-proxy
##########

FROM golang:1.23-bullseye AS builder

ARG RELEASE_VERSION="0.0.0"
ARG RELEASE_TYPE="development"
ARG RELEASE_HASH=""

ARG GOOS="linux"
ARG GOARCH="amd64"

ENV GO111MODULE=on \
    CGO_ENABLED=1 \
    BUILD_VERSION=${RELEASE_VERSION}-${RELEASE_TYPE}-${RELEASE_HASH}-${GOOS}-${GOARCH}

# Move to working directory /build
WORKDIR /build

RUN --mount=type=bind,source=go.mod,target=/build/go.mod \
    --mount=type=bind,source=go.sum,target=/build/go.sum \
    go mod download -x

# Build the application
ENV VERSION_PKG=github.com/datastax/zdm-proxy/proxy/pkg/version
RUN --mount=type=bind,source=.,target=.,rw \
    go build  \
        -ldflags " \
          -X '${VERSION_PKG}.ReleaseVersion=${RELEASE_VERSION}' \
          -X '${VERSION_PKG}.ReleaseType=${RELEASE_TYPE}' \
          -X '${VERSION_PKG}.ReleaseHash=${RELEASE_HASH}' \
          -X '${VERSION_PKG}.ReleaseOS=${GOOS}' \
          -X '${VERSION_PKG}.ReleaseArch=${GOARCH}' \
        "  \
        -o /bin/zdm-proxy  \
        ./proxy

# Move to /dist directory as the place for resulting binary folder
WORKDIR /dist

# Copy binary from /build to /dist
RUN cp /bin/zdm-proxy zdm-proxy-v${BUILD_VERSION}

ENTRYPOINT ["tail", "-f", "/dev/null"]

# Build a small image
FROM alpine AS server

COPY --from=builder /bin/zdm-proxy /
COPY LICENSE /

ENV ZDM_PROXY_LISTEN_ADDRESS="0.0.0.0"
ENV ZDM_METRICS_ADDRESS="0.0.0.0"

# Command to run
ENTRYPOINT ["/zdm-proxy"]
