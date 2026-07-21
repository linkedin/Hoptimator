package com.linkedin.hoptimator.jdbc;

import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class TableServiceTest {

  private static final String RECORD_SCHEMA = "{"
      + "\"type\":\"record\",\"name\":\"MyTable\",\"namespace\":\"com.linkedin.test\","
      + "\"fields\":["
      + "{\"name\":\"id\",\"type\":\"long\"},"
      + "{\"name\":\"name\",\"type\":\"string\"}"
      + "]}";

  private HoptimatorConnection connection;

  @BeforeEach
  void setUp() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    connection = (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties());
  }

  @AfterEach
  void tearDown() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  private static Schema recordSchema() {
    return new Schema.Parser().parse(RECORD_SCHEMA);
  }

  @Test
  void dryRunDerivesRowTypeFromAvroSchema() throws SQLException {
    HoptimatorDdlUtils.SpecifyResult result =
        TableService.create(connection, List.of("UTIL", "myTable"), recordSchema(),
            Collections.emptyMap(), false, true);

    assertThat(result).isNotNull();
    assertThat(result.viewPath).endsWith("myTable");

    RelDataType rowType = result.sinkRowType;
    assertThat(rowType.isStruct()).isTrue();
    assertThat(rowType.getFieldNames()).containsExactly("id", "name");
    assertThat(rowType.getField("id", false, false).getType().getSqlTypeName()).isEqualTo(SqlTypeName.BIGINT);
    assertThat(rowType.getField("name", false, false).getType().getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
  }

  @Test
  void dryRunDoesNotMutateTheSchema() throws SQLException {
    TableService.create(connection, List.of("UTIL", "ghostTable"), recordSchema(),
        Collections.emptyMap(), false, true);

    // The temporary table registered during specify() must be rolled back.
    assertThat(connection.calciteConnection().getRootSchema()
        .subSchemas().get("UTIL").tables().get("ghostTable")).isNull();
  }

  @Test
  void rejectsPathWithoutDatabaseAndTable() {
    assertThatThrownBy(() ->
        TableService.create(connection, List.of("onlyOne"), recordSchema(),
            Collections.emptyMap(), false, true))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("database and a table name");
  }

  @Test
  void rejectsNullSchema() {
    assertThatThrownBy(() ->
        TableService.create(connection, List.of("UTIL", "myTable"), null,
            Collections.emptyMap(), false, true))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Avro schema is required");
  }

  @Test
  void rejectsNonRecordSchema() {
    Schema primitive = Schema.create(Schema.Type.STRING);
    assertThatThrownBy(() ->
        TableService.create(connection, List.of("UTIL", "myTable"), primitive,
            Collections.emptyMap(), false, true))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("must be a record");
  }
}
