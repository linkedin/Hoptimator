package com.linkedin.hoptimator.catalog;

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

  public static class SqlJob extends Resource {
    public SqlJob(String sql) {
      super("SqlJob");
      export("sql", sql);
    }
  }
}
