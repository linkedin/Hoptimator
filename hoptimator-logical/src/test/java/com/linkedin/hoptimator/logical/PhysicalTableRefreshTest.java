package com.linkedin.hoptimator.logical;

import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobList;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


/**
 * Grounds {@code REFRESH <physical table> FROM ... TO ...} against a live cluster.
 *
 * <p>Hoptimator doesn't distinguish logical from physical tables, and a consumer always reads a
 * specific physical table (tier). REFRESH targets that physical table and fires the trigger that
 * <em>produces</em> it as a one-off backfill over the requested window.
 *
 * <p>The fixture is a reverse-ETL logical table ({@code logical-retl}: offline -> online) whose
 * implicit offline-tier trigger writes to the online tier — the real physical table
 * {@code ADS.CAMPAIGNS}. Refreshing that physical table must fire the trigger. A backfill is capped
 * at the watermark, so the test seeds one first.
 */
@Tag("integration")
public class PhysicalTableRefreshTest {

  private static final String TRIGGER = "logical-campaigns-offline-trigger";

  @Test
  @SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
      justification = "Test DDL with computed backfill-window bounds; values are controlled, not user input.")
  public void refreshBackfillsProducingTriggerOverWindow() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:hoptimator://catalogs=k8s")) {
      K8sContext ctx = K8sContext.create(conn);
      K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi =
          new K8sApi<>(ctx, K8sApiEndpoints.TABLE_TRIGGERS);
      K8sApi<V1Job, V1JobList> jobApi = new K8sApi<>(ctx, K8sApiEndpoints.JOBS);

      // logical-retl.CAMPAIGNS: online tier is the physical table ADS.CAMPAIGNS; the implicit
      // offline trigger produces it.
      execute(conn, "create or replace table \"LOGICAL-RETL\".\"CAMPAIGNS\" (\"ID\" bigint, \"NAME\" varchar)");
      try {
        // A logical table itself is not refreshable — refresh a physical tier instead.
        assertThatThrownBy(() -> execute(conn, "refresh \"LOGICAL-RETL\".\"CAMPAIGNS\""))
            .hasMessageContaining("logical table");
        // An unknown table errors.
        assertThatThrownBy(() -> execute(conn, "refresh \"ADS\".\"NOPE\""))
            .hasMessageContaining("does not exist");

        // Simulate a trigger that has processed history (watermark == timestamp == now).
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        V1alpha1TableTrigger trigger = triggerApi.get(TRIGGER);
        V1alpha1TableTriggerStatus status = new V1alpha1TableTriggerStatus();
        status.setTimestamp(now);
        status.setWatermark(now);
        triggerApi.updateStatus(trigger, status);

        OffsetDateTime from = now.minusDays(2);
        OffsetDateTime to = now.minusDays(1);
        // Refresh the PHYSICAL table the consumer reads; fires the producing trigger over the window.
        execute(conn, "refresh \"ADS\".\"CAMPAIGNS\" from '" + from + "' to '" + to + "'");

        V1Job backfill = findBackfillJob(jobApi);
        assertThat(backfill).as("backfill Job for %s", TRIGGER).isNotNull();
        assertThat(instant(env(backfill, "WINDOW_START"))).isEqualTo(from.toInstant());
        assertThat(instant(env(backfill, "WINDOW_END"))).isEqualTo(to.toInstant());
        jobApi.delete(backfill);
      } finally {
        execute(conn, "drop table \"LOGICAL-RETL\".\"CAMPAIGNS\"");
      }
    }
  }

  private static void execute(Connection conn, String sql) throws SQLException {
    try (Statement s = conn.createStatement()) {
      s.executeUpdate(sql);
    }
  }

  private V1Job findBackfillJob(K8sApi<V1Job, V1JobList> jobApi) throws Exception {
    Collection<V1Job> jobs = jobApi.select("backfill=true");
    return jobs.stream()
        .filter(j -> j.getMetadata() != null && j.getMetadata().getName() != null
            && j.getMetadata().getName().startsWith("logical-campaigns-offline-trigger"))
        .findFirst().orElse(null);
  }

  private static String env(V1Job job, String name) {
    List<V1Container> containers = job.getSpec().getTemplate().getSpec().getContainers();
    Optional<V1EnvVar> var = containers.get(0).getEnv().stream()
        .filter(e -> name.equals(e.getName())).findFirst();
    return var.map(V1EnvVar::getValue).orElse(null);
  }

  private static Instant instant(String iso) {
    return OffsetDateTime.parse(iso).toInstant();
  }
}
