package com.linkedin.hoptimator.kafka;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.jdbc.QuidemTestBase;


public class TestSqlScripts extends QuidemTestBase {

  @Test
  @Tag("integration")
  public void kafkaDdlScript() throws Exception {
    run("kafka-ddl.id", "hints=kafka.partitions=4,flink.parallelism=2,KAFKA.source.k1=v1,KAFKA.sink.k2=v2");
  }
}
