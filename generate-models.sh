#!/bin/sh

docker pull ghcr.io/kubernetes-client/java/crd-model-gen:v1.0.6

docker run \
  --rm \
  --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
  --mount type=bind,src="$(pwd)",dst="$(pwd)" \
  -ti \
  --network host \
  ghcr.io/kubernetes-client/java/crd-model-gen:v1.0.6 \
  /generate.sh -o "$(pwd)/hoptimator-k8s" -n "" -p "com.linkedin.hoptimator.k8s" \
  -u "$(pwd)/hoptimator-k8s/src/main/resources/databases.crd.yaml" \
  -u "$(pwd)/hoptimator-k8s/src/main/resources/engines.crd.yaml" \
  -u "$(pwd)/hoptimator-k8s/src/main/resources/jobtemplates.crd.yaml" \
  -u "$(pwd)/hoptimator-k8s/src/main/resources/pipelines.crd.yaml" \
  -u "$(pwd)/hoptimator-k8s/src/main/resources/sqljobs.crd.yaml" \
  -u "$(pwd)/hoptimator-k8s/src/main/resources/subscriptions.crd.yaml" \
  -u "$(pwd)/hoptimator-k8s/src/main/resources/tabletemplates.crd.yaml" \
  -u "$(pwd)/hoptimator-k8s/src/main/resources/tabletriggers.crd.yaml" \
  -u "$(pwd)/hoptimator-k8s/src/main/resources/views.crd.yaml" \
  && echo "done."
