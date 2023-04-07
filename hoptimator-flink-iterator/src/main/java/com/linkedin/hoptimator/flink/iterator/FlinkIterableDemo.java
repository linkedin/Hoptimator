package com.linkedin.hoptimator.flink.iterator;

public final class FlinkIterableDemo {

  private FlinkIterableDemo() {
  }

  public static void main(String[] args) {
    FlinkIterable iter = new FlinkIterable(
        "CREATE TEMPORARY VIEW FOO (FOO) AS SELECT 'bar';"
      + "SELECT * FROM FOO", 10_000);
    iter.field(0).forEach(x -> System.out.println(x));
  }
}
