package com.linkedin.hoptimator.operator.flink;

import com.linkedin.hoptimator.catalog.Resource;

import java.util.List;

/** Along with the FlinkDeployment template, generates FlinkDeployment YAML. */
public class FlinkDeployment extends Resource {

  public FlinkDeployment(List<String> sql) {
    super("FlinkDeployment");
    export("sql", sql);
  }
}
