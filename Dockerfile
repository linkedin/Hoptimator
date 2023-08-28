FROM eclipse-temurin:18
WORKDIR /home/
ADD ./hoptimator-cli-integration/build/distributions/hoptimator-cli-integration.tar ./
ADD ./hoptimator-operator-integration/build/distributions/hoptimator-operator-integration.tar ./
ADD ./etc/* ./
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["./hoptimator-cli-integration/bin/hoptimator-cli-integration -n '' -p '' -u jdbc:calcite:model=model.yaml"]

