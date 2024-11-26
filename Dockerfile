FROM eclipse-temurin:18
WORKDIR /home/
ADD ./hoptimator-operator-integration/build/distributions/hoptimator-operator-integration.tar ./
ADD ./etc/* ./
ENTRYPOINT ["/bin/sh", "-c"]

