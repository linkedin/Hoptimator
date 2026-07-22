package com.linkedin.hoptimator.logical;

import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.TableService;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Java port of {@code logical-offline-ddl.id}: creating a logical table with an offline tier
 * (LOGICAL-OFFLINE = ads-database → ads-catalog-database, no Venice) really deploys, and
 * auto-creates a paused {@code TableTrigger} for the offline tier. Verified by querying the
 * {@code k8s.table_triggers} metadata table, mirroring the quidem's SELECTs. Table names differ from
 * the quidem's. Requires the integration environment.
 */
@Tag("integration")
public class LogicalOfflineTableServiceIntegrationTest {

  private static final String DB = "LOGICAL-OFFLINE";
  private static final String TABLE = "tsoffline";

  private HoptimatorConnection connection;

  @BeforeEach
  void setUp() throws SQLException {
    connection = (HoptimatorConnection) DriverManager.getConnection("jdbc:hoptimator://catalogs=k8s");
  }

  @AfterEach
  void tearDown() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  @Test
  void offlineLogicalTableCreatesPausedTrigger() throws SQLException {
    try {
      // Real create: auto-creates the offline tier physical table + a paused TableTrigger.
      create(nullable("ID", Schema.Type.LONG), nullable("NAME", Schema.Type.STRING));

      // The implicit offline trigger exists, points at the offline physical catalog/schema, paused.
      List<Map<String, String>> triggers = triggersFor(TABLE);
      assertThat(triggers).hasSize(1);
      assertThat(triggers.get(0)).containsEntry("CATALOG", "ADS_CATALOG");
      assertThat(triggers.get(0)).containsEntry("SCHEMA", "ADS");
      assertThat(triggers.get(0)).containsEntry("PAUSED", "true");

      // CREATE OR REPLACE (add a column) preserves the trigger.
      create(nullable("ID", Schema.Type.LONG), nullable("NAME", Schema.Type.STRING), nullable("EXTRA", Schema.Type.STRING));
      assertThat(triggersFor(TABLE)).hasSize(1);

      // Drop cascades: the trigger is removed.
      TableService.delete(connection.connectionProperties(), List.of(DB, TABLE));
      assertThat(triggersFor(TABLE)).isEmpty();
    } catch (SQLException | RuntimeException e) {
      TableService.delete(connection.connectionProperties(), List.of(DB, TABLE));
      throw e;
    }
  }

  private void create(Schema.Field... fields) throws SQLException {
    Schema schema = Schema.createRecord(TABLE, null, "com.linkedin.hoptimator.test", false, Arrays.asList(fields));
    TableService.create(connection.connectionProperties(), connection.logHooks(),
        List.of(DB, TABLE), schema, Map.of(), true, false);
  }

  /** Queries k8s.table_triggers for triggers whose TABLE equals {@code table} (any case). */
  private List<Map<String, String>> triggersFor(String table) throws SQLException {
    List<Map<String, String>> rows = new ArrayList<>();
    try (Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery(
            "select name, catalog, schema, \"TABLE\", paused from \"k8s\".table_triggers")) {
      while (rs.next()) {
        String t = rs.getString("TABLE");
        if (t != null && t.equalsIgnoreCase(table)) {
          rows.add(Map.of(
              "NAME", String.valueOf(rs.getString("NAME")),
              "CATALOG", String.valueOf(rs.getString("CATALOG")),
              "SCHEMA", String.valueOf(rs.getString("SCHEMA")),
              "PAUSED", String.valueOf(rs.getBoolean("PAUSED"))));
        }
      }
    }
    return rows;
  }

  private static Schema.Field nullable(String name, Schema.Type type) {
    Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type));
    return new Schema.Field(name, union, null, Schema.Field.NULL_DEFAULT_VALUE);
  }
}
