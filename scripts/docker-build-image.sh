#!/bin/bash

docker build \
  --build-arg="BUILD_TYPE=${BUILD_TYPE}" \
  --tag=ossarga/zdm-proxy:2.5.3-linux \
  --no-cache \
  --progress=plain \
  --security-opt label=disable \
  --platform linux/amd64 \
  .