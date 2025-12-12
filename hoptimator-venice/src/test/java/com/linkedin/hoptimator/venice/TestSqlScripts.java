package com.linkedin.hoptimator.venice;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.jdbc.QuidemTestBase;


@Tag("integration")
public class TestSqlScripts extends QuidemTestBase {

  @Test
  public void veniceDdlInsertPartialScript() throws Exception {
    run("venice-ddl-insert-partial.id");
  }

  @Test
  public void veniceDdlCreateTableScript() throws Exception {
    run("venice-ddl-create-table.id");
  }
}
