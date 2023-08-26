FROM eclipse-temurin:18
WORKDIR /home/
ADD ./hoptimator-cli/run.sh ./hoptimator
ADD ./hoptimator-cli/build/libs/hoptimator-cli-all.jar ./hoptimator-cli-all.jar
ADD ./hoptimator-operator-integration/build/distributions/hoptimator-operator-integration.tar ./
ADD ./etc/* ./
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["./hoptimator -n '' -p '' -u jdbc:calcite:model=model.yaml"]

