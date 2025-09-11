package com.linkedin.hoptimator.kafka;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.jdbc.QuidemTestBase;


public class TestSqlScripts extends QuidemTestBase {

  @Test
  @Tag("integration")
  public void kafkaDdlScript() throws Exception {
    run("kafka-ddl.id", "hints=kafka.partitions=4,flink.parallelism=2,kafka.source.k1=v1,kafka.sink.k2=v2");
  }

  @Test
  @Tag("integration")
  public void kafkaDdlScriptBeamJob() throws Exception {
    run("kafka-ddl-beam.id", "hints=flink.app.type=BEAM");
  }
}
