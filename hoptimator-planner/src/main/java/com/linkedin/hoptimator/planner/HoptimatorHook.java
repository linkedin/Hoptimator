package com.linkedin.hoptimator.planner;

/** For testing purposes */
public final class HoptimatorHook {

  public static final HoptimatorHook YAML_PLAN = new HoptimatorHook();
  public static final HoptimatorHook QUERY_PLAN = new HoptimatorHook();

  private String lastRun = "NOTHING YET";

  private HoptimatorHook() {
  }

  public void run(String s) {
    lastRun = s;
  }

  public String run() {
    return lastRun;
  }
}
