FROM eclipse-temurin:18
WORKDIR /home/
ADD ./hoptimator-operator-integration/build/distributions/hoptimator-operator-integration.tar ./
ADD ./etc/* ./
ENV POD_NAMESPACE_FILEPATH=/var/run/secrets/kubernetes.io/serviceaccount/namespace
ENTRYPOINT ["/bin/sh", "-c"]

