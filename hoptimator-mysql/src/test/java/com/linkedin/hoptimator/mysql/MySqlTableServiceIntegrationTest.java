package com.linkedin.hoptimator.mysql;

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
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Java port of {@code mysql-ddl-create-table.id}: exercises MySQL CREATE TABLE key handling and
 * evolution validation via the SQL-free {@link TableService} direct API instead of quidem SQL.
 * Requires the integration environment, hence {@code @Tag}.
 */
@Tag("integration")
public class MySqlTableServiceIntegrationTest {

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
  void mysqlCreateTableLifecycle() throws SQLException {
    try {
      // Create a table with a KEY_ prefixed primary key.
      HoptimatorDdlUtils.SpecifyResult users = create("ts_users",
          nullable("KEY_id", Schema.Type.INT),
          nullable("name", Schema.Type.STRING),
          nullable("email", Schema.Type.STRING));
      assertThat(users.sinkRowType.getFieldNames()).containsExactly("KEY_id", "name", "email");
      assertThat(users.sinkRowType.getField("KEY_id", false, false).getType().getSqlTypeName())
          .isEqualTo(SqlTypeName.INTEGER);

      // Backward-compatible value-column change (add "age", widen/replace "name").
      HoptimatorDdlUtils.SpecifyResult evolved = create("ts_users",
          nullable("KEY_id", Schema.Type.INT),
          nullable("name", Schema.Type.STRING),
          nullable("age", Schema.Type.INT));
      assertThat(evolved.sinkRowType.getFieldNames()).containsExactly("KEY_id", "name", "age");

      // Composite primary key.
      HoptimatorDdlUtils.SpecifyResult orders = create("ts_orders",
          nullable("KEY_user_id", Schema.Type.INT),
          nullable("KEY_order_id", Schema.Type.INT),
          nullable("total", Schema.Type.DOUBLE),
          nullable("status", Schema.Type.STRING));
      assertThat(orders.sinkRowType.getFieldNames())
          .containsExactly("KEY_user_id", "KEY_order_id", "total", "status");

      // A table with no KEY_ field fails validation.
      assertThatThrownBy(() -> create("ts_invalid",
          nullable("id", Schema.Type.INT), nullable("data", Schema.Type.STRING)))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("No KEY_ fields found in table ts_invalid");

      // Changing the KEY fields is rejected.
      assertThatThrownBy(() -> create("ts_users",
          nullable("KEY_user_id", Schema.Type.INT),
          nullable("name", Schema.Type.STRING),
          nullable("email", Schema.Type.STRING)))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Cannot modify KEY fields for table ts_users");

      // Changing a KEY field's type is rejected.
      assertThatThrownBy(() -> create("ts_users",
          nullable("KEY_id", Schema.Type.STRING),
          nullable("name", Schema.Type.STRING),
          nullable("email", Schema.Type.STRING)))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Cannot modify KEY field type for table ts_users");
    } finally {
      drop("ts_users");
      drop("ts_orders");
    }
  }

  /** Creates (CREATE OR REPLACE) a MySQL table at {@code MYSQL.test_database.<name>}. */
  private HoptimatorDdlUtils.SpecifyResult create(String table, Schema.Field... fields) throws SQLException {
    Schema schema = Schema.createRecord(table, null, "com.linkedin.hoptimator.test", false,
        Arrays.asList(fields));
    return TableService.create(connection, List.of("MYSQL", "test_database", table), schema, Map.of(), true, false);
  }

  private void drop(String table) {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeUpdate("DROP TABLE MYSQL.\"test_database\".\"" + table + "\"");
    } catch (SQLException ignored) {
      // best-effort cleanup
    }
  }

  private static Schema.Field nullable(String name, Schema.Type type) {
    Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type));
    return new Schema.Field(name, union, null, Schema.Field.NULL_DEFAULT_VALUE);
  }
}
