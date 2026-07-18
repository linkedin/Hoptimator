package com.linkedin.hoptimator.logical;

import com.linkedin.hoptimator.jdbc.HoptimatorDdlUtils;
import com.linkedin.hoptimator.jdbc.TableService;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that creating a logical table through the SQL-free, connection-free {@link TableService}
 * plans the implicit inter-tier pipeline without a Calcite connection. A logical table's tiers
 * share the row type it is created from, so the nearline→online copy is an identity pipeline that
 * {@code LogicalTableDeployer} can plan via {@link com.linkedin.hoptimator.util.planner.IdentityQuery}
 * instead of the Calcite SQL planner. Uses dry-run so nothing is deployed (in particular, no Venice
 * store is created/deleted, which would trigger the controller's recreation cooldown).
 */
@Tag("integration")
public class LogicalTableServiceIntegrationTest {

  private final Properties properties = new Properties();

  @Test
  void logicalCreateTableDryRunPlansInterTierPipelineWithoutConnection() throws SQLException {
    Schema schema = Schema.createRecord("testevent", null, "com.linkedin.hoptimator.test", false,
        Arrays.asList(
            nullable("KEY", Schema.Type.STRING),
            nullable("memberId", Schema.Type.LONG),
            nullable("pageKey", Schema.Type.STRING)));

    HoptimatorDdlUtils.SpecifyResult result = TableService.create(properties, List.of(),
        List.of("LOGICAL", "ts-logical-test"), schema, Map.of(), true, true);

    // The resolved row type mirrors the requested Avro schema.
    assertThat(result.sinkRowType.getFieldNames()).containsExactly("KEY", "memberId", "pageKey");

    // The dry-run rendered the inter-tier pipeline job — the connection-free identity plan produced
    // an INSERT INTO the online (Venice) tier selecting from the nearline (Kafka) tier.
    String allSpecs = String.join("\n", result.specs);
    assertThat(result.specs).isNotEmpty();
    assertThat(allSpecs).containsIgnoringCase("insert into");
    assertThat(allSpecs).contains("ts-logical-test");
  }



  private static Schema.Field nullable(String name, Schema.Type type) {
    Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type));
    return new Schema.Field(name, union, null, Schema.Field.NULL_DEFAULT_VALUE);
  }
}
