package com.linkedin.hoptimator.logical;

import com.linkedin.hoptimator.jdbc.QuidemTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@Tag("integration")
public class TestSqlScripts extends QuidemTestBase {

  @Test
  public void logicalTableDdlScript() throws Exception {
    run("logical-ddl.id");
  }
}
