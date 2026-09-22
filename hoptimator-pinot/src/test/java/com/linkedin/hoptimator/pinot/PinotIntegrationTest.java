package com.linkedin.hoptimator.pinot;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test against a real Pinot controller (see {@code deploy/docker/pinot}).
 *
 * <p>Provisions a table via the controller REST API, then verifies both the client read surface and
 * the JDBC catalog resolve it, before tearing it down. Point at a different controller with
 * {@code -Dpinot.controllerUrl=...}.
 */
@Tag("integration")
public class PinotIntegrationTest {

  private static final String CONTROLLER_URL =
      System.getProperty("pinot.controllerUrl", "http://localhost:9000");
  private static final String TABLE = "hoptimator_it_table";

  @Test
  void tableLifecycle() throws Exception {
    try (PinotControllerClient client = new PinotControllerClient(CONTROLLER_URL)) {
      client.addSchema(schema(), true);
      client.addTable(offlineTableConfig());
      try {
        assertThat(client.listTables()).contains(TABLE);
        assertThat(client.tableExists(TABLE)).isTrue();

        Schema resolved = client.getSchema(TABLE);
        assertThat(resolved).isNotNull();
        assertThat(resolved.getColumnNames()).contains("id", "ts");

        // JDBC catalog path resolves the same table's row type.
        assertThat(resolveViaJdbc().getFieldNames()).contains("id", "ts");
      } finally {
        client.dropTable(TABLE);
        client.dropSchema(TABLE);
      }
      assertThat(client.tableExists(TABLE)).isFalse();
    }
  }

  private RelDataType resolveViaJdbc() throws Exception {
    Properties props = new Properties();
    try (Connection conn = DriverManager.getConnection(
        "jdbc:pinot://controllerUrl=" + CONTROLLER_URL, props)) {
      CalciteConnection calciteConnection = conn.unwrap(CalciteConnection.class);
      SchemaPlus pinotSchema = calciteConnection.getRootSchema().subSchemas().get("PINOT");
      Table table = pinotSchema == null ? null : pinotSchema.tables().get(TABLE);
      assertThat(table).isNotNull();
      return table.getRowType(calciteConnection.getTypeFactory());
    }
  }

  private static Schema schema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(TABLE)
        .addDimensionField("id", FieldSpec.DataType.STRING)
        .addDateTimeField("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  private static TableConfig offlineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE)
        .setTimeColumnName("ts")
        .setNumReplicas(1)
        .build();
  }
}
