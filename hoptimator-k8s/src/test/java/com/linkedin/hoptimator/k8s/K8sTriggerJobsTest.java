package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;
import com.linkedin.hoptimator.util.Template;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
  void backfillJobNameStaysWithinKubernetesLimit() {
    // Real trigger names overflow: logical-<table>-offline-trigger-<template>-job is already ~50+.
    String longBase = "logical-adclicks-offline-trigger-retl-job-template-job";
    String name = K8sTriggerJobs.backfillJobName(longBase, A, B, null);
    assertTrue(name.length() <= 63, "name must fit K8s 63-char limit; was " + name.length());
    assertTrue(name.contains("-bf-"), "name must keep the -bf- infix");
    assertFalse(name.contains("--"), "name must not contain empty segments");
    // Still deterministic and window-distinct even when shortened.
    assertEquals(name, K8sTriggerJobs.backfillJobName(longBase, A, B, null));
    assertNotEquals(name, K8sTriggerJobs.backfillJobName(longBase, A, C, null));
    // A different long base with the same window gets a different name (base hash preserves it).
    assertNotEquals(name,
        K8sTriggerJobs.backfillJobName("logical-widgets-offline-trigger-retl-job-template-job", A, B, null));
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

  @Test
  void renderWithNullWatermarkExposesAnEmptyWindowStart() throws Exception {
    // The first incremental fire has no prior watermark. The window start must render as empty (not
    // null the whole template), so the Job still launches. Regression for the reconcile NPE where
    // render() returned null and objFromYaml(null) threw.
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("t"))
        .spec(new V1alpha1TableTriggerSpec().schema("S").table("T")
            .yaml("start=[{{watermark}}] day=[{{watermarkDate}}] end=[{{timestamp}}]"));

    assertEquals("start=[] day=[] end=[2026-05-08T00:00Z]",
        K8sTriggerJobs.render(trigger, null, B));
  }

  @Test
  void renderThrowsAClearErrorWhenTemplateReferencesAnUnprovidedVariable() {
    // A template that references a variable the per-fire environment does not provide (here {{name}},
    // which only CREATE-time rendering binds) must fail with a named error, not a downstream NPE.
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("t"))
        .spec(new V1alpha1TableTriggerSpec().schema("S").table("T").yaml("name={{name}}"));

    SQLException e = assertThrows(SQLException.class, () -> K8sTriggerJobs.render(trigger, A, B));
    assertTrue(e.getMessage().contains("rendered to nothing"),
        "error must explain the template rendered to nothing — got: " + e.getMessage());
  }

  @Test
  void isWindowVarCoversTheFireWindowFamily() {
    for (String key : K8sTriggerJobs.WINDOW_VARS) {
      assertTrue(K8sTriggerJobs.isWindowVar(key), key + " must be recognized as a window var");
    }
    assertFalse(K8sTriggerJobs.isWindowVar("table"), "static vars must not be treated as window vars");
    assertFalse(K8sTriggerJobs.isWindowVar("watermarkk"), "a misspelled window var must not be deferred");
  }

  @Test
  void windowVarsSurviveAFirstRenderThenResolveOnTheSecond() throws Exception {
    // Model the two-pass CREATE-TRIGGER flow: render once deferring the window vars (static vars
    // only), store the result, then render again with the fire window.
    String template = "start={{watermark}} end={{timestamp}} day={{watermarkDate}} table={{table}}";
    Template.Environment createEnv = new Template.SimpleEnvironment().with("table", "T");
    String stored = new Template.SimpleTemplate(template).render(createEnv, K8sTriggerJobs::isWindowVar);
    assertEquals("start={{watermark}} end={{timestamp}} day={{watermarkDate}} table=T", stored,
        "window vars must pass through the first render untouched");

    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("t"))
        .spec(new V1alpha1TableTriggerSpec().schema("S").table("T").yaml(stored));
    assertEquals("start=2026-05-01T00:00Z end=2026-05-08T00:00Z day=2026-05-01 table=T",
        K8sTriggerJobs.render(trigger, A, B));
  }

  @Test
  void deferredWindowVarKeepsItsTransformForTheSecondRender() throws Exception {
    // A transform on a deferred window var must ride along to the second render, not be applied to
    // the placeholder text at CREATE (which would mangle {{watermarkDate}} into {{WATERMARKDATE}}
    // and break the second render).
    String template = "day={{watermarkDate toUpperCase}} table={{table toUpperCase}}";
    Template.Environment createEnv = new Template.SimpleEnvironment().with("table", "t");
    String stored = new Template.SimpleTemplate(template).render(createEnv, K8sTriggerJobs::isWindowVar);
    assertEquals("day={{watermarkDate toUpperCase}} table=T", stored,
        "the window var's transform must survive verbatim; the static var's transform applies now");

    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("t"))
        .spec(new V1alpha1TableTriggerSpec().schema("S").table("T").yaml(stored));
    // watermarkDate for 2026-05-01 is "2026-05-01"; toUpperCase is a no-op on digits but proves the
    // transform is applied on the second pass to the resolved value, not to the placeholder.
    assertEquals("day=2026-05-01 table=T", K8sTriggerJobs.render(trigger, A, B));
  }

  @Test
  void nonWindowVarStillSkipsTheTemplateOnTheFirstRender() throws Exception {
    // A genuinely missing (non-deferred) variable must still null the whole template, so CREATE
    // TRIGGER's empty-yaml guard fires rather than deferring the failure to fire time.
    String template = "x={{watermark}} y={{job.properties.typo}}";
    Template.Environment createEnv = new Template.SimpleEnvironment();
    assertNull(new Template.SimpleTemplate(template).render(createEnv, K8sTriggerJobs::isWindowVar),
        "a non-window unresolved variable must skip the template");
  }
}
