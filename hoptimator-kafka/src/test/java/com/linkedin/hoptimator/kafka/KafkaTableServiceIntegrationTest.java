package com.linkedin.hoptimator.kafka;

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

/**
 * Java port of {@code kafka-ddl-create-table.id}: previews (dry-run) and creates a Kafka topic via
 * the SQL-free {@link TableService} direct API instead of quidem SQL, asserting the rendered
 * {@code KafkaTopic} spec and the resolved row type. Requires the integration environment, hence
 * {@code @Tag}.
 */
@Tag("integration")
public class KafkaTableServiceIntegrationTest {

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
  void kafkaCreateTableDryRunAndCreate() throws SQLException {
    Schema schema = Schema.createRecord("createtabletest", null, "com.linkedin.hoptimator.test", false,
        Arrays.asList(
            nullable("KEY", Schema.Type.STRING),
            nullable("VALUE", Schema.Type.BYTES)));
    try {
      // Dry-run: previews the KafkaTopic YAML with the requested partition count, no mutation.
      HoptimatorDdlUtils.SpecifyResult preview = TableService.create(connection,
          List.of("KAFKA", "create-table-test"), schema, Map.of("kafka.partitions", "5"), false, true);
      assertThat(preview.specs).anyMatch(s -> s.contains("kind: KafkaTopic"));
      assertThat(preview.specs).anyMatch(s -> s.contains("partitions: 5"));

      // Create for real with a different partition count.
      HoptimatorDdlUtils.SpecifyResult created = TableService.create(connection,
          List.of("KAFKA", "create-table-test"), schema, Map.of("kafka.partitions", "10"), true, false);
      assertThat(created.sinkRowType.getFieldNames()).containsExactly("KEY", "VALUE");
      assertThat(created.sinkRowType.getField("KEY", false, false).getType().getSqlTypeName())
          .isEqualTo(SqlTypeName.VARCHAR);
      assertThat(created.sinkRowType.getField("VALUE", false, false).getType().getSqlTypeName())
          .isEqualTo(SqlTypeName.VARBINARY);
    } finally {
      try (Statement stmt = connection.createStatement()) {
        stmt.executeUpdate("DROP TABLE \"KAFKA\".\"create-table-test\"");
      } catch (SQLException ignored) {
        // best-effort cleanup
      }
    }
  }

  private static Schema.Field nullable(String name, Schema.Type type) {
    Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type));
    return new Schema.Field(name, union, null, Schema.Field.NULL_DEFAULT_VALUE);
  }
}
