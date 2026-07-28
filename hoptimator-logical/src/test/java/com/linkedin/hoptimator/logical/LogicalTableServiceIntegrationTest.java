package com.linkedin.hoptimator.logical;

import com.linkedin.hoptimator.jdbc.CatalogResolver;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDdlUtils;
import com.linkedin.hoptimator.jdbc.TableService;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests using the SQL-free {@link TableService} direct API.
 * Asserts the rendered inter-tier pipeline specs; the real-create lifecycle deploys and verifies the
 * pipeline custom resource/elements (querying the {@code k8s.pipelines} / {@code k8s.pipeline_element_map}
 * metadata tables) and the online (Venice) tier's resolved row type.
 */
@Tag("integration")
public class LogicalTableServiceIntegrationTest {

  private static final String DB = "LOGICAL";

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
  void dryRunPlansInterTierPipeline() throws SQLException {
    String table = "tslogicaldryrun";
    HoptimatorDdlUtils.SpecifyResult result = TableService.create(connection.connectionProperties(),
        connection.logHooks(), List.of(DB, table),
        record(table, nullable("KEY", Schema.Type.STRING), nullable("memberId", Schema.Type.LONG),
            nullable("pageKey", Schema.Type.STRING)), Map.of(), true, true);

    assertThat(result.sinkRowType.getFieldNames()).containsExactly("KEY", "memberId", "pageKey");
    String specs = String.join("\n", result.specs);
    // The nearline (Kafka) and online (Venice) physical tiers plus the identity inter-tier job.
    assertThat(specs).contains("kind: FlinkSessionJob");
    assertThat(specs).contains("kind: KafkaTopic");
    assertThat(specs).containsIgnoringCase("insert into");
  }

  @Test
  void dryRunPropagatesTierOptions() throws SQLException {
    // Options provided to a logical table must reach its physical tiers: kafka.partitions=5 should
    // render on the Kafka tier's topic, mirroring KafkaTableServiceIntegrationTest's assertion.
    String table = "tslogicalopts";
    HoptimatorDdlUtils.SpecifyResult result = TableService.create(connection.connectionProperties(),
        connection.logHooks(), List.of(DB, table),
        record(table, nullable("KEY", Schema.Type.STRING), nullable("memberId", Schema.Type.LONG),
            nullable("pageKey", Schema.Type.STRING)),
        Map.of("kafka.partitions", "5"), true, true);

    String specs = String.join("\n", result.specs);
    assertThat(specs).contains("kind: KafkaTopic");
    assertThat(specs).contains("partitions: 5");
  }

  @Test
  void createAgainstNonExistentSchemaFails() {
    assertThatThrownBy(() -> TableService.create(connection.connectionProperties(), connection.logHooks(),
        List.of("LOGICAL-NONEXISTENT", "t"),
        record("t", nullable("KEY", Schema.Type.STRING), nullable("id", Schema.Type.LONG)), Map.of(), true, false))
        .isInstanceOf(SQLException.class);
  }

  @Test
  void onlineLogicalTableLifecycle() throws SQLException {
    String table = "tslogicalonline";
    String pipeline = "logical-" + table + "-nearline-to-online";
    try {
      // Real create: deploys the Kafka (nearline) + Venice (online) physical tiers and the implicit
      // nearline->online identity pipeline.
      create(table, nullable("KEY", Schema.Type.STRING), nullable("memberId", Schema.Type.LONG),
          nullable("pageKey", Schema.Type.STRING));

      // The online (Venice) physical tier has a real value schema, so unlike the raw Kafka nearline
      // tier it resolves to the full row type. Verify the online store's columns directly.
      RelDataType onlineRowType = CatalogResolver.resolve(List.of("VENICE", table));
      assertThat(onlineRowType.getFieldNames()).contains("memberId", "pageKey");

      // The deployment is also verified structurally via the pipeline custom resource and its elements: the
      // nearline KafkaTopic physical table plus the identity FlinkSessionJob.
      assertThat(pipelineNames()).contains(pipeline);
      List<String> elements = pipelineElements(pipeline);
      assertThat(elements).anyMatch(e -> e.startsWith("FlinkSessionJob/"));
      assertThat(elements).anyMatch(e -> e.startsWith("KafkaTopic/"));

      // CREATE OR REPLACE — add a column; each tier validates backward compatibility. The pipeline
      // remains in place and the online tier's schema evolves.
      create(table, nullable("KEY", Schema.Type.STRING), nullable("memberId", Schema.Type.LONG),
          nullable("pageKey", Schema.Type.STRING), nullable("sessionId", Schema.Type.STRING));
      assertThat(pipelineNames()).contains(pipeline);
      assertThat(CatalogResolver.resolve(List.of("VENICE", table)).getFieldNames()).contains("sessionId");

      // Drop cascades: the implicit pipeline is removed.
      TableService.delete(connection.connectionProperties(), List.of(DB, table));
      assertThat(pipelineNames()).doesNotContain(pipeline);
    } catch (SQLException | RuntimeException e) {
      TableService.delete(connection.connectionProperties(), List.of(DB, table));
      throw e;
    }
  }

  private void create(String table, Schema.Field... fields) throws SQLException {
    TableService.create(connection.connectionProperties(), connection.logHooks(),
        List.of(DB, table), record(table, fields), Map.of(), true, false);
  }

  private List<String> pipelineNames() throws SQLException {
    List<String> names = new ArrayList<>();
    try (Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("select name from \"k8s\".pipelines")) {
      while (rs.next()) {
        names.add(rs.getString("NAME"));
      }
    }
    return names;
  }

  private List<String> pipelineElements(String pipeline) throws SQLException {
    List<String> elements = new ArrayList<>();
    try (Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery(
            "select pipeline_name, element_name from \"k8s\".pipeline_element_map")) {
      while (rs.next()) {
        if (pipeline.equals(rs.getString("PIPELINE_NAME"))) {
          elements.add(rs.getString("ELEMENT_NAME"));
        }
      }
    }
    return elements;
  }

  private static Schema record(String name, Schema.Field... fields) {
    return Schema.createRecord(name.replaceAll("\\W", "_"), null, "com.linkedin.hoptimator.test", false,
        Arrays.asList(fields));
  }

  private static Schema.Field nullable(String name, Schema.Type type) {
    Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type));
    return new Schema.Field(name, union, null, Schema.Field.NULL_DEFAULT_VALUE);
  }
}
