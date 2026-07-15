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
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Grounds {@code REFRESH <logical table> FROM ... TO ...} — a windowed backfill of a logical table —
 * against a live cluster: it must fire the table's offline-tier trigger as a one-off backfill Job
 * over exactly the requested data-time window.
 *
 * <p>A backfill can only cover already-processed history (it is capped at the watermark), so the test
 * first seeds the offline trigger with a watermark to simulate a trigger that has run, then refreshes
 * a window behind it and asserts the resulting backfill Job carries that window.
 */
@Tag("integration")
public class LogicalTableBackfillTest {

  // Short table name keeps the derived backfill Job name within the 63-char Kubernetes limit.
  private static final String TABLE = "BF";
  private static final String TRIGGER = "logical-bf-offline-trigger";

  @Test
  @SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
      justification = "Test DDL with computed backfill-window bounds; values are controlled, not user input.")
  public void refreshBackfillsLogicalTableOverWindow() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:hoptimator://catalogs=k8s")) {
      K8sContext ctx = K8sContext.create(conn);
      K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi =
          new K8sApi<>(ctx, K8sApiEndpoints.TABLE_TRIGGERS);
      K8sApi<V1Job, V1JobList> jobApi = new K8sApi<>(ctx, K8sApiEndpoints.JOBS);

      execute(conn, "create or replace table \"LOGICAL-OFFLINE\".\"" + TABLE
          + "\" (\"ID\" bigint, \"NAME\" varchar)");
      try {
        // Simulate a trigger that has processed history up to `now` (watermark == timestamp).
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        V1alpha1TableTrigger trigger = triggerApi.get(TRIGGER);
        V1alpha1TableTriggerStatus status = new V1alpha1TableTriggerStatus();
        status.setTimestamp(now);
        status.setWatermark(now);
        triggerApi.updateStatus(trigger, status);

        OffsetDateTime from = now.minusDays(2);
        OffsetDateTime to = now.minusDays(1);
        execute(conn, "refresh table \"LOGICAL-OFFLINE\".\"" + TABLE + "\" from '" + from + "' to '" + to + "'");

        // A one-off backfill Job for this trigger must now exist, over the requested window.
        V1Job backfill = findBackfillJob(jobApi);
        assertThat(backfill).as("backfill Job for %s", TRIGGER).isNotNull();
        assertThat(backfill.getSpec().getSuspend())
            .as("a standalone (non-recursive) backfill runs immediately, not suspended")
            .isNotEqualTo(Boolean.TRUE);
        assertThat(instant(env(backfill, "WINDOW_START"))).isEqualTo(from.toInstant());
        assertThat(instant(env(backfill, "WINDOW_END"))).isEqualTo(to.toInstant());

        jobApi.delete(backfill);
      } finally {
        execute(conn, "drop table \"LOGICAL-OFFLINE\".\"" + TABLE + "\"");
      }
    }
  }

  private static void execute(Connection conn, String sql) throws Exception {
    try (Statement s = conn.createStatement()) {
      s.executeUpdate(sql);
    }
  }

  private V1Job findBackfillJob(K8sApi<V1Job, V1JobList> jobApi) throws Exception {
    Collection<V1Job> jobs = jobApi.select("backfill=true");
    return jobs.stream()
        .filter(j -> j.getMetadata() != null && j.getMetadata().getName() != null
            && j.getMetadata().getName().startsWith(TRIGGER))
        .findFirst().orElse(null);
  }

  private static String env(V1Job job, String name) {
    List<V1Container> containers = job.getSpec().getTemplate().getSpec().getContainers();
    Optional<V1EnvVar> var = containers.get(0).getEnv().stream()
        .filter(e -> name.equals(e.getName())).findFirst();
    return var.map(V1EnvVar::getValue).orElse(null);
  }

  private static java.time.Instant instant(String iso) {
    return OffsetDateTime.parse(iso).toInstant();
  }
}
