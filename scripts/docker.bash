#!/usr/bin/env bash

DOCKER_REGISTERY="657871693752.dkr.ecr.us-east-1.amazonaws.com"

# cmd_build_base_copy will create containers by copying resources from the project directory
# this can speed up container building if your local enviroment can build binaries
# compatiable with the runtime enviroment; basically if you run ubuntu.
cmd_build_base_copy() {
  echo "Building containers by copy"
	docker build -t filecoin-base -f Dockerfile.devnet.copy-base .
}

# cmd_build_base_src will create containers by copying source into a container and executing
# all build steps required. This is the most reproducedable build but is slower.
cmd_build_base_src() {
  echo "Building containers from source"
	docker build -t filecoin-base -f Dockerfile.devnet.build-base .
}

cmd_build_all() {
  echo "Building all deploy images"
  docker build -t filecoin        -f Dockerfile.devnet.filecoin      --cache-from filecoin-base:latest .
  docker build -t filecoin-test   -f Dockerfile.devnet.filecoin-test --cache-from filecoin-base:latest .
  docker build -t filecoin-faucet -f Dockerfile.devnet.faucet        --cache-from filecoin-base:latest .
}

cmd_tag_all() {
  echo "Tagging all deploy images"

  local label="$1"

  docker tag filecoin:latest          $DOCKER_REGISTERY/filecoin:$label
  docker tag filecoin-test:latest     $DOCKER_REGISTERY/filecoin-tests:$label
  docker tag filecoin-faucet:latest   $DOCKER_REGISTERY/filecoin-faucet:$label
}

cmd_push_all() {
  echo "Pushing all images"

  local label="$1"

  docker push $DOCKER_REGISTERY/filecoin:$label
  docker push $DOCKER_REGISTERY/filecoin-tests:$label
  docker push $DOCKER_REGISTERY/filecoin-faucet:$label
}

main() {
  local cmd="$1"

  shift;

  case $cmd in
    "build-base:copy")
      cmd_build_base_copy $@
      ;;
    "build-base:src")
      cmd_build_base_src $@
      ;;
    "build-all")
      cmd_build_all $@
      ;;
    "tag-all")
      cmd_tag_all $@
      ;;
    "push-all")
      cmd_push_all $@
      ;;
  esac
}

main $@
