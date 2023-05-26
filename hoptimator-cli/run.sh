#!/bin/bash

java \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.time=ALL-UNNAMED \
  -classpath "/opt/plugins/*/lib/*:./hoptimator-cli-all.jar" \
  $JAVA_OPTS \
  com.linkedin.hoptimator.HoptimatorCliApp --verbose=true -nn hoptimator "$@"
