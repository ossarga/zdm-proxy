##########
# NOTE: When building this image, there is an assumption that you are in the top level directory of the repository.
#
# Build for deployment (runtime image):
# $ docker build . -f ./Dockerfile -t zdm-proxy
#
# Build-only (extract binary):
# $ docker build --target builder -t zdm-proxy-builder .
# $ docker run -d --name build-container zdm-proxy-builder
# $ docker cp build-container:/dist/zdm-proxy-v2.5.2-dev-abc123-linux-amd64 ./
#
# Custom version build:
# $ docker build --build-arg VERSION=2.6.4 --build-arg VERSION_TYPE=release -t zdm-proxy .
##########

FROM golang:1.24.2-bullseye AS builder

# Build arguments for version injection
ARG VERSION=0.0.0
ARG VERSION_TYPE=dev
ARG GIT_HASH=""
ARG GOOS=linux
ARG GOARCH=amd64

ENV GO111MODULE=on \
    CGO_ENABLED=1 \
    BUILD_VERSION="v${VERSION}-${VERSION_TYPE}-${GIT_HASH}-${GOOS}-${GOARCH}"

WORKDIR /build

RUN --mount=type=bind,source=go.mod,target=/build/go.mod \
    --mount=type=bind,source=go.sum,target=/build/go.sum \
    go mod download -x

ENV VERSION_PKG=github.com/datastax/zdm-proxy/proxy/pkg/version
RUN --mount=type=bind,source=.,target=.,rw \
    echo "=== Building ZDM Proxy ${BUILD_VERSION} ===" && \
    LDFLAGS="-X ${VERSION_PKG}.ZdmVersion=${VERSION} \
             -X ${VERSION_PKG}.ZdmVersionType=${VERSION_TYPE} \
             -X ${VERSION_PKG}.ZdmGitHash=${GIT_HASH} \
             -X ${VERSION_PKG}.ZdmBuildOs=${GOOS} \
             -X ${VERSION_PKG}.ZdmBuildArch=${GOARCH}" && \
    go build -ldflags "$LDFLAGS" -o zdm-proxy-${BUILD_VERSION} ./proxy && \
    echo "=== Successfully built zdm-proxy-${BUILD_VERSION} ==="

WORKDIR /dist
RUN cp /build/zdm-proxy-${BUILD_VERSION} ./zdm-proxy-${BUILD_VERSION} && \
    ln -s zdm-proxy-${BUILD_VERSION} ./main && \
    ls -la

# Keep builder running for build-only usage
ENTRYPOINT ["tail", "-f", "/dev/null"]

# Runtime image
FROM alpine AS runtime

COPY --from=builder /dist/main /
COPY LICENSE /

ENV ZDM_PROXY_LISTEN_ADDRESS="0.0.0.0"
ENV ZDM_METRICS_ADDRESS="0.0.0.0"

# Command to run
ENTRYPOINT ["/main"]
