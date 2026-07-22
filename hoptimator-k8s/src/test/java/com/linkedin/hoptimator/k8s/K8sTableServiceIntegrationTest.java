package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.jdbc.HoptimatorDdlUtils;
import com.linkedin.hoptimator.jdbc.TableService;
import org.apache.avro.Schema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests using the SQL-free {@link TableService} direct API. ADS is a read-only demo ({@code demodb})
 * schema with no dedicated store deployer, so this covers what the direct path supports there — Avro schema
 * derivation, dry-run rendering, and error handling — rather than store resolution.
 *
 * <p>{@code TableService} is connection-free: it takes a {@link Properties} bag and log hooks, never a JDBC
 * connection, and resolves K8s Database config from CRDs via the default context. So these tests pass an empty
 * {@code Properties} and no hooks rather than opening a HoptimatorConnection.
 */
@Tag("integration")
public class K8sTableServiceIntegrationTest {

  private static final Properties PROPS = new Properties();
  private static final List<java.util.function.Consumer<String>> NO_HOOKS = Collections.emptyList();

  @Test
  void createTableDerivesRowType() throws SQLException {
    Schema schema = record("ts-newtable", nullable("i", Schema.Type.INT), nullable("s", Schema.Type.STRING));
    try {
      HoptimatorDdlUtils.SpecifyResult result = TableService.create(PROPS, NO_HOOKS,
          List.of("ADS", "ts-newtable"), schema, Map.of(), true, false);
      assertEquals(List.of("i", "s"), result.sinkRowType.getFieldNames());
      assertEquals(SqlTypeName.INTEGER, result.sinkRowType.getField("i", false, false).getType().getSqlTypeName());
      assertEquals(SqlTypeName.VARCHAR, result.sinkRowType.getField("s", false, false).getType().getSqlTypeName());
    } finally {
      TableService.delete(PROPS, List.of("ADS", "ts-newtable"));
    }
  }

  @Test
  void dryRunDerivesRowTypeWithoutMutation() throws SQLException {
    Schema schema = record("ts-dryruntable", nullable("i", Schema.Type.INT));
    HoptimatorDdlUtils.SpecifyResult result = TableService.create(PROPS, NO_HOOKS,
        List.of("ADS", "ts-dryruntable"), schema, Map.of(), false, true);
    assertEquals(List.of("i"), result.sinkRowType.getFieldNames());
  }

  @Test
  void createInUnknownDatabaseFails() {
    Schema schema = record("t", nullable("i", Schema.Type.INT));
    assertThatThrownBy(() -> TableService.create(PROPS, NO_HOOKS,
        List.of("NOSUCHDB", "t"), schema, Map.of(), true, false))
        .isInstanceOf(SQLException.class);
  }

  private static Schema record(String name, Schema.Field... fields) {
    return Schema.createRecord(name.replaceAll("\\W", "_"), null, "com.linkedin.hoptimator.test", false,
        Arrays.asList(fields));
  }

  private static Schema.Field nullable(String name, Schema.Type type) {
    Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type));
    return new Schema.Field(name, union, null, Schema.Field.NULL_DEFAULT_VALUE);
  }
}
