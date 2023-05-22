FROM eclipse-temurin:18
WORKDIR /home/
ADD ./hoptimator-cli/run.sh ./hoptimator
ADD ./hoptimator-operator/run.sh ./hoptimator-operator
ADD ./hoptimator-cli/build/libs/hoptimator-cli-all.jar ./hoptimator-cli-all.jar
ADD ./hoptimator-operator/build/libs/hoptimator-operator-all.jar ./hoptimator-operator-all.jar
ADD ./etc/* ./
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["./hoptimator -n '' -p '' -u jdbc:calcite:model=model.yaml"]

