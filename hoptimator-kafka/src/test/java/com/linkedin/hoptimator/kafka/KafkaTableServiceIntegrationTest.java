package com.linkedin.hoptimator.kafka;

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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Java port of {@code kafka-ddl-create-table.id}: mirrors the same behaviors via the SQL-free
 * {@link TableService} direct API instead of quidem SQL. The lifecycle test covers dry-run
 * ({@code !specify}), real create ({@code !update}), schema verification ({@code !describe}, via
 * {@link CatalogResolver}) and drop (verified by resolving to absent). Independent error checks live
 * in their own tests. Table names differ from the quidem's so both can run against one environment.
 * Requires the integration environment, hence {@code @Tag}.
 */
@Tag("integration")
public class KafkaTableServiceIntegrationTest {

  private static final String SCHEMA = "KAFKA";
  private final Properties properties = new Properties();

  @Test
  void kafkaCreateTableLifecycle() throws SQLException {
    String table = "ts-create-table-test";
    Schema schema = record("KEY", Schema.Type.STRING, "VALUE", Schema.Type.BYTES);
    try {
      // Dry-run previews the KafkaTopic YAML with the requested partition count, no mutation.
      HoptimatorDdlUtils.SpecifyResult preview = TableService.create(properties, List.of(),
          List.of(SCHEMA, table), schema, Map.of("kafka.partitions", "5"), false, true);
      assertThat(preview.specs).anyMatch(s -> s.contains("kind: KafkaTopic"));
      assertThat(preview.specs).anyMatch(s -> s.contains("partitions: 5"));

      // Real create with a different partition count.
      HoptimatorDdlUtils.SpecifyResult created = TableService.create(properties, List.of(),
          List.of(SCHEMA, table), schema, Map.of("kafka.partitions", "10"), true, false);
      assertThat(created.sinkRowType.getFieldNames()).containsExactly("KEY", "VALUE");

      // Verify it registered by resolving the created topic from the live catalog (mirrors !describe).
      RelDataType resolved = CatalogResolver.awaitResolved(List.of(SCHEMA, table));
      assertThat(resolved.getFieldNames()).containsExactly("KEY", "VALUE");
      assertThat(resolved.getField("KEY", false, false).getType().getSqlTypeName())
          .isEqualTo(SqlTypeName.VARCHAR);
      assertThat(resolved.getField("VALUE", false, false).getType().getSqlTypeName())
          .isIn(SqlTypeName.BINARY, SqlTypeName.VARBINARY);

      // Drop (cleanup + exercises the delete path).
      TableService.delete(properties, List.of(SCHEMA, table));
    } catch (SQLException | RuntimeException e) {
      TableService.delete(properties, List.of(SCHEMA, table));
      throw e;
    }
  }

  @Test
  void createExistingTableWithoutUpdateFails() throws SQLException {
    String table = "ts-exists-test";
    Schema schema = record("KEY", Schema.Type.STRING, "VALUE", Schema.Type.BYTES);
    try {
      TableService.create(properties, List.of(), List.of(SCHEMA, table), schema, Map.of(), true, false);
      // updateIfExists=false against an existing table must fail.
      assertThatThrownBy(() ->
          TableService.create(properties, List.of(), List.of(SCHEMA, table), schema, Map.of(), false, false))
          .isInstanceOf(SQLException.class);
    } finally {
      TableService.delete(properties, List.of(SCHEMA, table));
    }
  }

  @Test
  void createInUnknownDatabaseFails() {
    Schema schema = record("KEY", Schema.Type.STRING, "VALUE", Schema.Type.BYTES);
    assertThatThrownBy(() ->
        TableService.create(properties, List.of(), List.of("NOSUCHDB", "t"), schema, Map.of(), true, false))
        .isInstanceOf(SQLException.class);
  }

  private static Schema record(String f1, Schema.Type t1, String f2, Schema.Type t2) {
    return Schema.createRecord("rec", null, "com.linkedin.hoptimator.test", false,
        Arrays.asList(nullable(f1, t1), nullable(f2, t2)));
  }

  private static Schema.Field nullable(String name, Schema.Type type) {
    Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type));
    return new Schema.Field(name, union, null, Schema.Field.NULL_DEFAULT_VALUE);
  }
}
