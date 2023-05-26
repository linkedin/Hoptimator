#!/bin/bash

java \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.time=ALL-UNNAMED \
  -classpath "/opt/plugins/*/lib/*:./hoptimator-operator-all.jar" \
  $JAVA_OPTS \
  com.linkedin.hoptimator.operator.HoptimatorOperatorApp "$@"
