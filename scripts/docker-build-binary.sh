#!/bin/bash

set -e

docker build \
  --target=builder \
  --build-arg="BUILD_TYPE=${BUILD_TYPE}" \
  --tag=zdm-proxy:builder \
  --no-cache \
  --security-opt label=disable \
  --progress=plain \
  . \
  2>&1 | tee build.log

build_result="$?"
echo "Build result: $build_result"
[ $build_result = 0 ] || exit $build_result

zdm_proxy_build=$(grep "=== Successfully built zdm-proxy" build.log | cut -d' ' -f4)
echo "Build version: $zdm_proxy_build"
zdm_proxy_build_container="${zdm_proxy_build}-build-$(date +%s)"
echo "Build container: $zdm_proxy_build_container"

docker run --name "${zdm_proxy_build_container}" zdm-proxy:builder &
while true
do
  sleep 5
  docker_ps_info=$(docker ps -a | grep "${zdm_proxy_build_container}" | grep "Up" || true)
  [ -z "$docker_ps_info" ] || break
done

docker cp "${zdm_proxy_build_container}":/dist/"${zdm_proxy_build}" ./ \
&& docker stop "${zdm_proxy_build_container}" > /dev/null \
&& docker rm "${zdm_proxy_build_container}" > /dev/null \
&& md5 "${zdm_proxy_build}"