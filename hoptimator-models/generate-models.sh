#!/bin/sh

docker pull ghcr.io/kubernetes-client/java/crd-model-gen:v1.0.6

docker run \
  --rm \
  --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
  --mount type=bind,src="$(pwd)",dst="$(pwd)" \
  -ti \
  --network host \
  ghcr.io/kubernetes-client/java/crd-model-gen:v1.0.6 \
  /generate.sh -o "$(pwd)/hoptimator-models" -n "" -p "com.linkedin.hoptimator" \
  -u "$(pwd)/deploy/kafkatopics.crd.yaml" \
  -u "$(pwd)/deploy/subscriptions.crd.yaml" \
  && echo "done."
