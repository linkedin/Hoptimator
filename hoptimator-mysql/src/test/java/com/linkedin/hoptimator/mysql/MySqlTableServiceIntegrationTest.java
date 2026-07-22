package com.linkedin.hoptimator.mysql;

import com.linkedin.hoptimator.jdbc.CatalogResolver;
import com.linkedin.hoptimator.jdbc.HoptimatorDdlUtils;
import com.linkedin.hoptimator.jdbc.TableService;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
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
 * Integration tests using the SQL-free {@link TableService} direct API. A single lifecycle test groups the
 * create/update/drop of one table (verifying registration with {@link CatalogResolver}, mirroring
 * {@code !describe}); independent validation/error checks are separate tests. MySQL is a
 * catalog-style database ({@code MYSQL.test_database.<name>}).
 *
 * <p>{@code TableService} is connection-free (a {@link Properties} bag + log hooks, no JDBC connection), so
 * these tests pass an empty {@code Properties} and no hooks.
 */
@Tag("integration")
public class MySqlTableServiceIntegrationTest {

  private static final String CATALOG = "MYSQL";
  private static final String DB = "test_database";

  private final Properties properties = new Properties();
  private final List<java.util.function.Consumer<String>> noHooks = Collections.emptyList();

  @Test
  void usersTableLifecycle() throws SQLException {
    String table = "ts_users";
    try {
      // Create with a KEY_ prefixed primary key.
      HoptimatorDdlUtils.SpecifyResult created = create(table,
          nullable("KEY_id", Schema.Type.INT),
          nullable("name", Schema.Type.STRING),
          nullable("email", Schema.Type.STRING));
      assertThat(created.sinkRowType.getFieldNames()).containsExactly("KEY_id", "name", "email");

      // Verify it registered in MySQL. MySQL maps KEY_ fields to the primary key with the prefix
      // stripped, so the physical columns are id/name/email (not KEY_id).
      RelDataType resolved = CatalogResolver.resolve(List.of(CATALOG, DB, table));
      assertThat(resolved.getFieldNames()).contains("id", "name", "email");
      assertThat(resolved.getField("id", false, false).getType().getSqlTypeName())
          .isEqualTo(SqlTypeName.INTEGER);

      // Backward-compatible evolution: add a value column (MySQL adds columns, never drops).
      HoptimatorDdlUtils.SpecifyResult evolved = create(table,
          nullable("KEY_id", Schema.Type.INT),
          nullable("name", Schema.Type.STRING),
          nullable("email", Schema.Type.STRING),
          nullable("age", Schema.Type.INT));
      assertThat(evolved.sinkRowType.getFieldNames()).contains("age");
      assertThat(CatalogResolver.resolve(List.of(CATALOG, DB, table)).getFieldNames()).contains("age");

      // Drop (cleanup + exercises the delete path).
      drop(table);
    } catch (SQLException | RuntimeException e) {
      drop(table);
      throw e;
    }
  }

  @Test
  void ordersCompositeKeyLifecycle() throws SQLException {
    String table = "ts_orders";
    try {
      HoptimatorDdlUtils.SpecifyResult created = create(table,
          nullable("KEY_user_id", Schema.Type.INT),
          nullable("KEY_order_id", Schema.Type.INT),
          nullable("total", Schema.Type.DOUBLE),
          nullable("status", Schema.Type.STRING));
      assertThat(created.sinkRowType.getFieldNames())
          .containsExactly("KEY_user_id", "KEY_order_id", "total", "status");

      assertThat(CatalogResolver.resolve(List.of(CATALOG, DB, table)).getFieldNames())
          .contains("user_id", "order_id", "total", "status");

      drop(table);
    } catch (SQLException | RuntimeException e) {
      drop(table);
      throw e;
    }
  }

  @Test
  void createWithoutKeyFieldsFails() {
    assertThatThrownBy(() -> create("ts_nokey",
        nullable("id", Schema.Type.INT), nullable("data", Schema.Type.STRING)))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("No KEY_ fields found in table ts_nokey");
  }

  @Test
  void changingKeyFieldsFails() throws SQLException {
    String table = "ts_keychange";
    try {
      create(table, nullable("KEY_id", Schema.Type.INT), nullable("name", Schema.Type.STRING));
      assertThatThrownBy(() -> create(table,
          nullable("KEY_user_id", Schema.Type.INT), nullable("name", Schema.Type.STRING)))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Cannot modify KEY fields for table " + table);
    } finally {
      drop(table);
    }
  }

  @Test
  void changingKeyFieldTypeFails() throws SQLException {
    String table = "ts_keytype";
    try {
      create(table, nullable("KEY_id", Schema.Type.INT), nullable("name", Schema.Type.STRING));
      assertThatThrownBy(() -> create(table,
          nullable("KEY_id", Schema.Type.STRING), nullable("name", Schema.Type.STRING)))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Cannot modify KEY field type for table " + table);
    } finally {
      drop(table);
    }
  }

  @Test
  void createInUnknownCatalogFails() {
    assertThatThrownBy(() -> TableService.create(properties, noHooks,
        List.of("NOSUCHCATALOG", DB, "t"), recordOf("t", nullable("KEY_id", Schema.Type.INT)),
        Map.of(), true, false))
        .isInstanceOf(SQLException.class);
  }

  @Test
  void createWithoutUpdateIfExistsFailsWhenTableExists() throws SQLException {
    String table = "ts_exists";
    try {
      create(table, nullable("KEY_id", Schema.Type.INT), nullable("name", Schema.Type.STRING));
      // Re-creating the same table with updateIfExists=false must fail rather than silently skip,
      // mirroring the SQL path's CREATE (without OR REPLACE) on an existing table.
      assertThatThrownBy(() -> TableService.create(properties, noHooks,
          List.of(CATALOG, DB, table),
          recordOf(table, nullable("KEY_id", Schema.Type.INT), nullable("name", Schema.Type.STRING)),
          Map.of(), false, false))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("already exists");
    } finally {
      drop(table);
    }
  }

  /** Creates (updateIfExists) a MySQL table at {@code MYSQL.test_database.<name>}. */
  private HoptimatorDdlUtils.SpecifyResult create(String table, Schema.Field... fields) throws SQLException {
    return TableService.create(properties, noHooks,
        List.of(CATALOG, DB, table), recordOf(table, fields), Map.of(), true, false);
  }

  private void drop(String table) throws SQLException {
    TableService.delete(properties, List.of(CATALOG, DB, table));
  }

  private static Schema recordOf(String table, Schema.Field... fields) {
    return Schema.createRecord(table, null, "com.linkedin.hoptimator.test", false, Arrays.asList(fields));
  }

  private static Schema.Field nullable(String name, Schema.Type type) {
    Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type));
    return new Schema.Field(name, union, null, Schema.Field.NULL_DEFAULT_VALUE);
  }
}
