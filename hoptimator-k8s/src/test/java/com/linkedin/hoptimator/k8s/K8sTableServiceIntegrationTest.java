package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDdlUtils;
import com.linkedin.hoptimator.jdbc.TableService;
import org.apache.avro.Schema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Java port of the {@code create or replace table ads."newtable" ...} scenario from
 * {@code k8s-ddl.id}: creates a table via the SQL-free {@link TableService} direct API (a table
 * path + an Avro schema) instead of quidem SQL, and asserts the resolved row type. Requires the
 * integration environment, hence {@code @Tag}.
 */
@Tag("integration")
public class K8sTableServiceIntegrationTest {

  private static final String URL = "jdbc:hoptimator://catalogs=k8s";

  private HoptimatorConnection connection;

  @BeforeEach
  void setUp() throws SQLException {
    connection = (HoptimatorConnection) DriverManager.getConnection(URL);
  }

  @AfterEach
  void tearDown() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  @Test
  void createTableFromAvroSchema() throws SQLException {
    Schema schema = Schema.createRecord("newtable", null, "com.linkedin.hoptimator.test", false,
        Arrays.asList(
            nullable("i", Schema.Type.INT),
            nullable("s", Schema.Type.STRING)));
    try {
      HoptimatorDdlUtils.SpecifyResult result =
          TableService.create(connection.connectionProperties(), connection.logHooks(), List.of("ADS", "newtable"), schema, Map.of(), true, false);

      assertEquals(List.of("i", "s"), result.sinkRowType.getFieldNames());
      assertEquals(SqlTypeName.INTEGER, result.sinkRowType.getField("i", false, false).getType().getSqlTypeName());
      assertEquals(SqlTypeName.VARCHAR, result.sinkRowType.getField("s", false, false).getType().getSqlTypeName());
    } finally {
      TableService.delete(connection.connectionProperties(), List.of("ADS", "newtable"));
    }
  }

  @Test
  void dryRunProducesSpecsWithoutMutation() throws SQLException {
    Schema schema = Schema.createRecord("dryruntable", null, "com.linkedin.hoptimator.test", false,
        Arrays.asList(nullable("i", Schema.Type.INT)));

    HoptimatorDdlUtils.SpecifyResult result =
        TableService.create(connection.connectionProperties(), connection.logHooks(), List.of("ADS", "dryruntable"), schema, Map.of(), false, true);

    // Dry-run returns the rendered specs and resolved schema, but deploys nothing.
    assertEquals(List.of("i"), result.sinkRowType.getFieldNames());
  }

  private static Schema.Field nullable(String name, Schema.Type type) {
    Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type));
    return new Schema.Field(name, union, null, Schema.Field.NULL_DEFAULT_VALUE);
  }
}
