package com.linkedin.hoptimator.pinot;

import com.linkedin.hoptimator.jdbc.QuidemTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@Tag("integration")
public class TestSqlScripts extends QuidemTestBase {

  @Test
  public void pinotDdlCreateTableScript() throws Exception {
    run("pinot-ddl-create-table.id");
  }

  @Test
  public void pinotAllTypesScript() throws Exception {
    run("pinot-all-types.id");
  }

  @Test
  public void pinotNativeDescribeScript() throws Exception {
    run("pinot-native-describe.id");
  }
}
