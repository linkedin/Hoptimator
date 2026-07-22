package com.linkedin.hoptimator.venice;

import com.linkedin.hoptimator.jdbc.CatalogResolver;
import com.linkedin.hoptimator.jdbc.HoptimatorDdlUtils;
import com.linkedin.hoptimator.jdbc.TableService;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
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
 *
 * <p>{@code TableService} is connection-free (a {@link Properties} bag + log hooks, no JDBC connection), so
 * these tests pass an empty {@code Properties} and no hooks.
 */
@Tag("integration")
public class VeniceTableServiceIntegrationTest {

  private static final String SCHEMA = "VENICE";

  private final Properties properties = new Properties();
  private final List<java.util.function.Consumer<String>> noHooks = Collections.emptyList();

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
      RelDataType resolved = CatalogResolver.resolve(List.of(SCHEMA, store));
      assertThat(resolved.getFieldNames()).contains("i", "s");

      // Backward-compatible evolution: add a nullable value field.
      HoptimatorDdlUtils.SpecifyResult evolved = create(store,
          nullable("KEY_id", Schema.Type.INT),
          nullable("i", Schema.Type.INT),
          nullable("s", Schema.Type.STRING),
          nullable("new_field", Schema.Type.DOUBLE));
      assertThat(evolved.sinkRowType.getFieldNames()).contains("new_field");
      assertThat(CatalogResolver.resolve(List.of(SCHEMA, store)).getFieldNames()).contains("new_field");

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
      assertThat(CatalogResolver.resolve(List.of(SCHEMA, store)).getFieldNames())
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
      assertThat(CatalogResolver.resolve(List.of(SCHEMA, store)).getFieldNames()).contains("i", "s");
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
    assertThatThrownBy(() -> TableService.create(properties, noHooks,
        List.of("NOSUCHDB", "t"), recordOf("t", nullable("KEY", Schema.Type.INT), nullable("i", Schema.Type.INT)),
        Map.of(), true, false))
        .isInstanceOf(SQLException.class);
  }

  @Test
  void createWithoutUpdateIfExistsFailsWhenStoreExists() throws SQLException {
    String store = "directapi-exists";
    try {
      create(store, nullable("KEY", Schema.Type.INT), nullable("i", Schema.Type.INT));
      // Re-creating the same store with updateIfExists=false must fail rather than silently skip.
      assertThatThrownBy(() -> TableService.create(properties, noHooks,
          List.of(SCHEMA, store),
          recordOf(sanitize(store), nullable("KEY", Schema.Type.INT), nullable("i", Schema.Type.INT)),
          Map.of(), false, false))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("already exists");
    } finally {
      drop(store);
    }
  }

  private HoptimatorDdlUtils.SpecifyResult create(String store, Schema.Field... fields) throws SQLException {
    return TableService.create(properties, noHooks,
        List.of(SCHEMA, store), recordOf(sanitize(store), fields), Map.of(), true, false);
  }

  private void drop(String store) throws SQLException {
    TableService.delete(properties, List.of(SCHEMA, store));
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
