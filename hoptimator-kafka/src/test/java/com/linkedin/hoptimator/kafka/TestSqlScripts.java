package com.linkedin.hoptimator.kafka;

import com.linkedin.hoptimator.jdbc.QuidemTestBase;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class TestSqlScripts extends QuidemTestBase {

  @Test
  @Tag("integration")
  public void kafkaDdlScript() throws Exception {
    run("kafka-ddl.id");
  }
}
