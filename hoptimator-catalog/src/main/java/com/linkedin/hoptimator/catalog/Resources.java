package com.linkedin.hoptimator.catalog;

/** Common Resources */
public final class Resources {

  private Resources() {
  }

  public static class KafkaTopic extends Resource {
    public KafkaTopic(String name) {
      super(name, "KafkaTopic");
    }
  }

  public static class BrooklinDatastream extends Resource {
    public BrooklinDatastream(String name) {
      super(name, "BrooklinDatastream");
    }
  }

  /** Anything that can run SQL, e.g. a Flink job */
  public static class SqlJob extends Resource {
    public SqlJob(String sql) {
      super("SqlJob");
      export("sql", sql);
    }
  }
}
