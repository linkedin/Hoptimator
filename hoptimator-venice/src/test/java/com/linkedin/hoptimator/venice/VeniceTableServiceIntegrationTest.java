package com.linkedin.hoptimator.venice;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.DatabaseConfigResolvers;
import com.linkedin.hoptimator.jdbc.DirectDeploymentContext;
import com.linkedin.hoptimator.jdbc.HoptimatorDdlUtils;
import com.linkedin.hoptimator.jdbc.TableService;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import org.apache.avro.Schema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests using the SQL-free {@link TableService} direct API.
 * Exercises Venice CREATE TABLE key/value schema derivation, backward-compatible evolution and validation failures.
 */
@Tag("integration")
public class VeniceTableServiceIntegrationTest {

  private static final String SCHEMA = "VENICE";

  @Test
  void storeLifecycle() throws SQLException {
    String store = "directapi-store";
    try {
      // Create with a record (KEY_ prefixed) key plus value fields.
      HoptimatorDdlUtils.SpecifyResult created = create(store,
          nullable("KEY_id", Schema.Type.INT),
          nullable("i", Schema.Type.INT),
          nullable("s", Schema.Type.STRING));
      assertThat(created.sinkRowType.getFieldNames()).containsExactly("KEY_id", "i", "s");

      // Verify it registered in Venice.
      RelDataType resolved = resolveVenice(store);
      assertThat(resolved.getFieldNames()).contains("i", "s");

      // Backward-compatible evolution: add a nullable value field.
      HoptimatorDdlUtils.SpecifyResult evolved = create(store,
          nullable("KEY_id", Schema.Type.INT),
          nullable("i", Schema.Type.INT),
          nullable("s", Schema.Type.STRING),
          nullable("new_field", Schema.Type.DOUBLE));
      assertThat(evolved.sinkRowType.getFieldNames()).contains("new_field");
      assertThat(resolveVenice(store).getFieldNames()).contains("new_field");

      // Invalid updates on the same store must fail.
      assertThatThrownBy(() -> create(store,
          nullable("KEY_user_id", Schema.Type.INT),
          nullable("i", Schema.Type.INT),
          nullable("s", Schema.Type.STRING),
          nullable("new_field", Schema.Type.DOUBLE)))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Key schema evolution is not supported in Venice");

      assertThatThrownBy(() -> create(store,
          nullable("KEY_id", Schema.Type.STRING),
          nullable("i", Schema.Type.INT),
          nullable("s", Schema.Type.STRING),
          nullable("new_field", Schema.Type.DOUBLE)))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Key schema evolution is not supported in Venice");

      assertThatThrownBy(() -> create(store,
          nullable("KEY_id", Schema.Type.INT),
          nullable("i", Schema.Type.INT),
          nullable("s", Schema.Type.STRING),
          required("new_field", Schema.Type.DOUBLE)))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Value schema is not backward compatible");
    } finally {
      drop(store);
    }
  }

  @Test
  void compositeKeyStoreLifecycle() throws SQLException {
    String store = "directapi-store-composite";
    try {
      HoptimatorDdlUtils.SpecifyResult created = create(store,
          nullable("KEY_user_id", Schema.Type.INT),
          nullable("KEY_order_id", Schema.Type.INT),
          nullable("total", Schema.Type.DOUBLE),
          nullable("status", Schema.Type.STRING));
      assertThat(created.sinkRowType.getFieldNames())
          .containsExactly("KEY_user_id", "KEY_order_id", "total", "status");
      assertThat(resolveVenice(store).getFieldNames())
          .contains("total", "status");
    } finally {
      drop(store);
    }
  }

  @Test
  void primitiveKeyStoreLifecycle() throws SQLException {
    String store = "directapi-store-primitive";
    try {
      HoptimatorDdlUtils.SpecifyResult created = create(store,
          nullable("KEY", Schema.Type.INT),
          nullable("i", Schema.Type.INT),
          nullable("s", Schema.Type.STRING));
      assertThat(created.sinkRowType.getFieldNames()).containsExactly("KEY", "i", "s");
      assertThat(resolveVenice(store).getFieldNames()).contains("i", "s");
    } finally {
      drop(store);
    }
  }

  @Test
  void createWithoutKeyFieldsFails() {
    assertThatThrownBy(() -> create("directapi-nokey",
        nullable("i", Schema.Type.INT), nullable("s", Schema.Type.STRING)))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Failed to generate key schema for Venice store directapi-nokey");
  }

  @Test
  void createWithoutValueFieldsFails() {
    assertThatThrownBy(() -> create("directapi-novalue", nullable("KEY", Schema.Type.INT)))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Failed to generate value schema for Venice store directapi-novalue");
  }

  @Test
  void createInUnknownDatabaseFails() {
    assertThatThrownBy(() -> TableService.create(new Properties(), Collections.emptyList(),
        List.of("NOSUCHDB", "t"), recordOf("t", nullable("KEY", Schema.Type.INT), nullable("i", Schema.Type.INT)),
        Map.of(), true, false))
        .isInstanceOf(SQLException.class);
  }

  @Test
  void providedValueSchemaNamespaceIsPreservedInVenice() throws Exception {
    // End-to-end proof that the direct API deploys the caller's Avro verbatim (no lossy
    // Avro->RelDataType->Avro round-trip): a value field whose type is a nested record in its own
    // namespace must come back from Venice with that namespace intact.
    String store = "directapi-nsvalue";
    try {
      Schema.Field widget = new Schema.Field("widget",
          Schema.createRecord("Widget", null, "com.example.custom", false,
              List.of(new Schema.Field("w", Schema.create(Schema.Type.STRING), null, null))),
          null, null);
      TableService.create(new Properties(), Collections.emptyList(), List.of(SCHEMA, store),
          recordOf(sanitize(store), nullable("KEY_id", Schema.Type.INT), widget),
          Map.of(), true, false);

      // Read the value schema back from real Venice and assert the nested namespace survived.
      Properties veniceProps = DatabaseConfigResolvers.forProperties(new Properties())
          .databaseProperties(null, SCHEMA, "jdbc:venice://");
      Source source = new Source(store, List.of(SCHEMA, store), Map.of());
      VeniceDeployer reader = new VeniceDeployer(source, veniceProps,
          new DirectDeploymentContext(veniceProps, null, null));
      try (StoreSchemaFetcher fetcher = reader.createStoreSchemaFetcher(store)) {
        Schema registeredValue = fetcher.getLatestValueSchema();
        Schema nested = registeredValue.getField("widget").schema();
        assertThat(nested.getName()).isEqualTo("Widget");
        assertThat(nested.getNamespace()).isEqualTo("com.example.custom");
      }
    } finally {
      drop(store);
    }
  }

  @Test
  void createWithoutUpdateIfExistsFailsWhenStoreExists() throws SQLException {
    String store = "directapi-exists";
    try {
      create(store, nullable("KEY", Schema.Type.INT), nullable("i", Schema.Type.INT));
      // Re-creating the same store with updateIfExists=false must fail rather than silently skip.
      assertThatThrownBy(() -> TableService.create(new Properties(), Collections.emptyList(),
          List.of(SCHEMA, store),
          recordOf(sanitize(store), nullable("KEY", Schema.Type.INT), nullable("i", Schema.Type.INT)),
          Map.of(), false, false))
          .isInstanceOf(SQLNonTransientException.class)
          .hasMessageContaining("already exists");
    } finally {
      drop(store);
    }
  }

  private HoptimatorDdlUtils.SpecifyResult create(String store, Schema.Field... fields) throws SQLException {
    return TableService.create(new Properties(), Collections.emptyList(),
        List.of(SCHEMA, store), recordOf(sanitize(store), fields), Map.of(), true, false);
  }

  private void drop(String store) throws SQLException {
    TableService.delete(new Properties(), List.of(SCHEMA, store));
  }

  /**
   * Resolves a Venice store's row type (key fields, {@code KEY_}-prefixed or {@code KEY} for a
   * primitive key, followed by the value fields), failing if the store is not present.
   *
   * <p>Uses a direct {@code jdbc:venice://} connection rather than the shared {@code catalogs=k8s}
   * catalog on purpose. The catalog resolves a table by enumerating the whole schema, which for
   * Venice means the router's bulk {@code /stores} list — a periodically-refreshed cache that lags
   * the controller, so a just-created store can be momentarily absent from it (and, because that
   * refresh window can outlast a whole test run, absent for every store created that run — the
   * "all three or none" flake). A direct connection instead resolves the single store via the
   * router's targeted {@code discover_cluster} endpoint, which reflects the create immediately, so
   * no polling is needed. The store deployers run synchronously, so the store exists the instant
   * {@code create} returns.
   */
  private RelDataType resolveVenice(String store) throws SQLException {
    Properties veniceProps = DatabaseConfigResolvers.forProperties(new Properties())
        .databaseProperties(null, SCHEMA, "jdbc:venice://");
    try (Connection conn = DriverManager.getConnection("jdbc:venice://", veniceProps)) {
      CalciteConnection calciteConnection = conn.unwrap(CalciteConnection.class);
      SchemaPlus veniceSchema = calciteConnection.getRootSchema().subSchemas().get(SCHEMA);
      Table table = veniceSchema == null ? null : veniceSchema.tables().get(store);
      if (table == null) {
        throw new SQLException("Venice store not resolvable after create: " + store);
      }
      return table.getRowType(calciteConnection.getTypeFactory());
    }
  }

  private static Schema recordOf(String name, Schema.Field... fields) {
    return Schema.createRecord(name, null, "com.linkedin.hoptimator.test", false, Arrays.asList(fields));
  }

  private static Schema.Field nullable(String name, Schema.Type type) {
    Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type));
    return new Schema.Field(name, union, null, Schema.Field.NULL_DEFAULT_VALUE);
  }

  private static Schema.Field required(String name, Schema.Type type) {
    return new Schema.Field(name, Schema.create(type), null, null);
  }

  private static String sanitize(String name) {
    return name.replaceAll("\\W", "_");
  }
}
