FROM eclipse-temurin:18
WORKDIR /home/
COPY ./hoptimator-cli/run.sh ./hoptimator
COPY ./hoptimator-operator/run.sh ./hoptimator-operator
COPY ./hoptimator-cli/build/libs/hoptimator-cli-all.jar ./hoptimator-cli-all.jar
COPY ./hoptimator-operator/build/libs/hoptimator-operator-all.jar ./hoptimator-operator-all.jar
COPY ./test-model.yaml ./test-model.yaml
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["./hoptimator"]
