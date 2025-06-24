#!/bin/bash

set -e

zdm_proxy_build_container=""
build_log_file="build.log"

cleanup() {
  local exit_code=$?

  if [ -n "$zdm_proxy_build_container" ]
  then
    echo "Stopping and removing container: $zdm_proxy_build_container"
    docker stop "$zdm_proxy_build_container" >/dev/null 2>&1 || true
    docker rm "$zdm_proxy_build_container" >/dev/null 2>&1 || true
  fi

  if [ "$exit_code" -eq 0 ] && [ -f "$build_log_file" ]
  then
    echo "Removing build log: $build_log_file"
    rm "$build_log_file"
  fi

  exit $exit_code
}

trap cleanup EXIT SIGINT SIGTERM SIGKILL

docker build \
  --target=builder \
  --build-arg="BUILD_TYPE=${BUILD_TYPE}" \
  --tag=zdm-proxy:builder \
  --no-cache \
  --progress=plain \
  --security-opt label=disable \
  . \
  2>&1 | tee "$build_log_file"

build_result="$?"
echo "Build result: $build_result"
[ $build_result = 0 ] || exit $build_result

zdm_proxy_build=$(grep "=== Successfully built zdm-proxy" build.log | cut -d' ' -f4)
if [ -z "$zdm_proxy_build" ]
then
  echo "Failed to determine build version"
  exit 1
fi

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

docker cp "${zdm_proxy_build_container}":/dist/"${zdm_proxy_build}" ./
md5 "${zdm_proxy_build}"
cleanup
