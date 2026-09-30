package com.linkedin.hoptimator.util.planner;

import java.nio.charset.StandardCharsets;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static java.util.Objects.requireNonNull;



/**
 * Tests for {@link HoptimatorSqlDialect}.
 */
public class HoptimatorSqlDialectTest {

  private static RelDataType varcharWithCharset() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    return typeFactory.createTypeWithCharsetAndCollation(
        typeFactory.createSqlType(SqlTypeName.VARCHAR),
        StandardCharsets.ISO_8859_1,
        SqlCollation.IMPLICIT);
  }

  @Test
  public void omitsCharacterSetInCastSpec() {
    // Regression: generating pipeline SQL with a charset-bearing cast produced
    // `CAST(... AS VARCHAR CHARACTER SET `ISO-8859-1`)`, which downstream engines (e.g. Flink)
    // reject because their string literals use a different charset (UTF-16LE).
    SqlNode castSpec = requireNonNull(HoptimatorSqlDialect.DEFAULT.getCastSpec(varcharWithCharset()));
    assertTrue(castSpec.toString().contains("VARCHAR"), "Should still emit VARCHAR. Got: " + castSpec);
    assertFalse(castSpec.toString().contains("CHARACTER SET"),
        "Generated cast must not include a CHARACTER SET clause. Got: " + castSpec);
  }

  @Test
  public void stockAnsiDialectIncludesCharacterSet() {
    // Documents the upstream Calcite behavior this dialect intentionally overrides.
    SqlNode castSpec = requireNonNull(AnsiSqlDialect.DEFAULT.getCastSpec(varcharWithCharset()));
    assertTrue(castSpec.toString().contains("CHARACTER SET"),
        "Stock ANSI dialect is expected to emit CHARACTER SET. Got: " + castSpec);
  }
}
