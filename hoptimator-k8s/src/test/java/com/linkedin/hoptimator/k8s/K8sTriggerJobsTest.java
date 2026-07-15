package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sTriggerJobsTest {

  private static final OffsetDateTime A = OffsetDateTime.parse("2026-05-01T00:00Z");
  private static final OffsetDateTime B = OffsetDateTime.parse("2026-05-08T00:00Z");
  private static final OffsetDateTime C = OffsetDateTime.parse("2026-05-09T00:00Z");

  @Test
  void backfillJobNameIsDistinctPerWindowAndStable() {
    assertNotEquals(K8sTriggerJobs.backfillJobName("job", A, B, null),
        K8sTriggerJobs.backfillJobName("job", A, C, null));
    // Same window + discriminator => same name (idempotent).
    assertEquals(K8sTriggerJobs.backfillJobName("job", A, B, null),
        K8sTriggerJobs.backfillJobName("job", A, B, null));
    assertTrue(K8sTriggerJobs.backfillJobName("job", A, B, null).startsWith("job-bf-"),
        "name must carry the -bf- infix");
  }

  @Test
  void discriminatorDistinguishesSameWindow() {
    // Auto-repair passes the change arrival as discriminator, so genuinely new late data on the same
    // window gets a fresh Job name.
    assertNotEquals(K8sTriggerJobs.backfillJobName("job", A, B, null),
        K8sTriggerJobs.backfillJobName("job", A, B, C));
  }

  @Test
  void renderExposesWindowVars() throws Exception {
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("t"))
        .spec(new V1alpha1TableTriggerSpec().schema("S").table("T")
            .yaml("from={{watermark}} to={{timestamp}} table={{table}}"));

    assertEquals("from=2026-05-01T00:00Z to=2026-05-08T00:00Z table=T",
        K8sTriggerJobs.render(trigger, A, B));
  }
}
