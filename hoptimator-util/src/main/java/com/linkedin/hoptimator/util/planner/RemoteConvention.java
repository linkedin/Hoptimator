package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.Engine;
import org.apache.calcite.plan.Convention;


public class RemoteConvention extends Convention.Impl {

  private final Engine engine;

  RemoteConvention(String name, Engine engine) {
    super(name, RemoteRel.class);
    this.engine = engine;
  }

  Engine engine() {
    return engine;
  }
}
