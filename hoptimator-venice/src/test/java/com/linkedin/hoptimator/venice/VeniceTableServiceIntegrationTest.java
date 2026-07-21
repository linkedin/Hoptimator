package com.linkedin.hoptimator.venice;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Java port of {@code venice-ddl-create-table.id}: exercises the same Venice CREATE TABLE
 * behaviors (key/value schema derivation, backward-compatible evolution, and the validation
 * failures) via the SQL-free {@link TableService} direct API instead of quidem SQL. Requires the
 * integration environment (the {@code VENICE} database and its stores), hence {@code @Tag}.
 */
@Tag("integration")
public class VeniceTableServiceIntegrationTest {

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
  void veniceCreateTableLifecycle() throws SQLException {
    try {
      // Create a store with a record key (KEY_ prefixed) plus value fields.
      HoptimatorDdlUtils.SpecifyResult created = create("directapi-store",
          nullable("KEY_id", Schema.Type.INT),
          nullable("i", Schema.Type.INT),
          nullable("s", Schema.Type.STRING));
      assertThat(created.sinkRowType.getFieldNames()).containsExactly("KEY_id", "i", "s");
      assertColumnType(created, "KEY_id", SqlTypeName.INTEGER);
      assertColumnType(created, "s", SqlTypeName.VARCHAR);

      // Backward-compatible evolution: add a new value field.
      HoptimatorDdlUtils.SpecifyResult evolved = create("directapi-store",
          nullable("KEY_id", Schema.Type.INT),
          nullable("i", Schema.Type.INT),
          nullable("s", Schema.Type.STRING),
          nullable("new_field", Schema.Type.DOUBLE));
      assertThat(evolved.sinkRowType.getFieldNames()).containsExactly("KEY_id", "i", "s", "new_field");
      assertColumnType(evolved, "new_field", SqlTypeName.DOUBLE);

      // Composite record key.
      HoptimatorDdlUtils.SpecifyResult composite = create("directapi-store-composite",
          nullable("KEY_user_id", Schema.Type.INT),
          nullable("KEY_order_id", Schema.Type.INT),
          nullable("total", Schema.Type.DOUBLE),
          nullable("status", Schema.Type.STRING));
      assertThat(composite.sinkRowType.getFieldNames())
          .containsExactly("KEY_user_id", "KEY_order_id", "total", "status");

      // Primitive key.
      HoptimatorDdlUtils.SpecifyResult primitive = create("directapi-store-primitive",
          nullable("KEY", Schema.Type.INT),
          nullable("i", Schema.Type.INT),
          nullable("s", Schema.Type.STRING));
      assertThat(primitive.sinkRowType.getFieldNames()).containsExactly("KEY", "i", "s");

      // A store with no KEY field fails validation.
      assertThatThrownBy(() -> create("directapi-nokey",
          nullable("i", Schema.Type.INT), nullable("s", Schema.Type.STRING)))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Failed to generate key schema for Venice store directapi-nokey");

      // A store with only a KEY (no value fields) fails validation.
      assertThatThrownBy(() -> create("directapi-novalue", nullable("KEY", Schema.Type.INT)))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Failed to generate value schema for Venice store directapi-novalue");

      // Changing the key fields is rejected (key schema evolution unsupported).
      assertThatThrownBy(() -> create("directapi-store",
          nullable("KEY_user_id", Schema.Type.INT),
          nullable("i", Schema.Type.INT),
          nullable("s", Schema.Type.STRING),
          nullable("new_field", Schema.Type.DOUBLE)))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Key schema evolution is not supported in Venice");

      // Changing a key field's type is rejected.
      assertThatThrownBy(() -> create("directapi-store",
          nullable("KEY_id", Schema.Type.STRING),
          nullable("i", Schema.Type.INT),
          nullable("s", Schema.Type.STRING),
          nullable("new_field", Schema.Type.DOUBLE)))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Key schema evolution is not supported in Venice");

      // Making an existing nullable value field non-nullable is backward-incompatible.
      assertThatThrownBy(() -> create("directapi-store",
          nullable("KEY_id", Schema.Type.INT),
          nullable("i", Schema.Type.INT),
          nullable("s", Schema.Type.STRING),
          required("new_field", Schema.Type.DOUBLE)))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Value schema is not backward compatible");
    } finally {
      drop("directapi-store");
      drop("directapi-store-composite");
      drop("directapi-store-primitive");
    }
  }

  /** Creates (CREATE OR REPLACE) a Venice store from an Avro record of the given fields. */
  private HoptimatorDdlUtils.SpecifyResult create(String store, Schema.Field... fields) throws SQLException {
    Schema schema = Schema.createRecord(sanitize(store), null, "com.linkedin.hoptimator.test", false,
        Arrays.asList(fields));
    return TableService.create(connection.connectionProperties(), connection.logHooks(), List.of("VENICE", store), schema, Map.of(), true, false);
  }

  private void assertColumnType(HoptimatorDdlUtils.SpecifyResult result, String field, SqlTypeName expected) {
    assertThat(result.sinkRowType.getField(field, false, false).getType().getSqlTypeName()).isEqualTo(expected);
  }

  private void drop(String store) {
    try {
      TableService.delete(connection.connectionProperties(), List.of("VENICE", store));
    } catch (SQLException ignored) {
      // best-effort cleanup
    }
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
