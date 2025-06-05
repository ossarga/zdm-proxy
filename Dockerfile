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
# $ docker build  --build-arg BUILD_TYPE=custom -t zdm-proxy .
##########

FROM golang:1.26.5-bookworm AS builder

# Build arguments for version injection
ARG BUILD_TYPE=""
ARG GOOS=linux
ARG GOARCH=amd64

ARG GO111MODULE=on
ARG CGO_ENABLED=0

WORKDIR /build

RUN --mount=type=bind,source=go.mod,target=/build/go.mod \
    --mount=type=bind,source=go.sum,target=/build/go.sum \
    go mod download -x

ENV VERSION_PKG=github.com/datastax/zdm-proxy/proxy/pkg/version
RUN --mount=type=bind,source=.,target=/build,rw \
    chmod +x /build/scripts/version.sh && \
    BINARY_NAME=$(GOOS=${GOOS} GOARCH=${GOARCH} BUILD_TYPE=${BUILD_TYPE} /build/scripts/version.sh binary-name) && \
    VERSION_TAG=$(BUILD_TYPE=${BUILD_TYPE} /build/scripts/version.sh version-tag) && \
    echo "=== Building ZDM Proxy ${VERSION_TAG} ===" && \
    echo $(scripts/version.sh vars) && \
    LDFLAGS=$(GOOS=${GOOS} GOARCH=${GOARCH} BUILD_TYPE=${BUILD_TYPE} /build/scripts/version.sh ldflags) && \
    go build -ldflags "${LDFLAGS}" -o ${BINARY_NAME} ./proxy && \
    echo "=== Successfully built ${BINARY_NAME} ===" && \
    cp ${BINARY_NAME} /${BINARY_NAME} && \
    echo "BINARY_NAME=${BINARY_NAME}" > /build.env

WORKDIR /dist
RUN . /build.env && \
    mv /${BINARY_NAME} . && \
    ln -s ${BINARY_NAME} ./main && \
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
