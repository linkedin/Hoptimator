package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class DependencyCheckerTest {

  @Mock
  private K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi;
  @Mock
  private K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi;

  private static final String DB = "kafka1";
  private static final List<String> PATH = Collections.singletonList("my-topic");
  private static final String IDENTIFIER = "kafka1_my-topic";

  private static V1alpha1Pipeline pipeline(String name, String ownerKind, String ownerName,
      String annotationValue) {
    return new V1alpha1Pipeline().metadata(metadata(name, ownerKind, ownerName, annotationValue));
  }

  private static V1alpha1TableTrigger trigger(String name, String ownerKind, String ownerName,
      String sourcesAnnotation) {
    return new V1alpha1TableTrigger().metadata(metadata(name, ownerKind, ownerName, sourcesAnnotation));
  }

  private static V1ObjectMeta metadata(String name, String ownerKind, String ownerName,
      String annotationValue) {
    V1ObjectMeta meta = new V1ObjectMeta().name(name);
    if (ownerKind != null && ownerName != null) {
      meta.addOwnerReferencesItem(new V1OwnerReference().kind(ownerKind).name(ownerName));
    }
    if (annotationValue != null) {
      Map<String, String> annotations = new HashMap<>();
      annotations.put(DependencyLabels.ANNOTATION_KEY_SOURCES, annotationValue);
      meta.setAnnotations(annotations);
    }
    return meta;
  }

  /** Default both APIs to empty so each test only stubs the side it cares about. */
  private void emptyByDefault() throws SQLException {
    String labelKey = DependencyLabels.labelKey(DB, PATH);
    lenient().when(pipelineApi.select(labelKey)).thenReturn(Collections.emptyList());
    lenient().when(triggerApi.select(labelKey)).thenReturn(Collections.emptyList());
  }

  @Test
  void passesWhenNoMatches() throws SQLException {
    emptyByDefault();

    assertDoesNotThrow(() -> DependencyChecker.assertNoExternalDependents(
        pipelineApi, triggerApi, DB, PATH, null, null));
  }

  @Test
  void blocksOnExternalPipeline() throws SQLException {
    emptyByDefault();
    when(pipelineApi.select(DependencyLabels.labelKey(DB, PATH)))
        .thenReturn(Collections.singletonList(pipeline("ext-pipe", "View", "owner", IDENTIFIER)));

    SQLException ex = assertThrows(SQLException.class,
        () -> DependencyChecker.assertNoExternalDependents(
            pipelineApi, triggerApi, DB, PATH, null, null));
    assertTrue(ex.getMessage().contains("ext-pipe"));
    assertTrue(ex.getMessage().contains(IDENTIFIER));
    assertTrue(ex.getMessage().contains("pipeline/ext-pipe"),
        "blocker should be tagged with its CRD kind: " + ex.getMessage());
  }

  @Test
  void blocksOnExternalTrigger() throws SQLException {
    emptyByDefault();
    when(triggerApi.select(DependencyLabels.labelKey(DB, PATH)))
        .thenReturn(Collections.singletonList(trigger("backfill", "LogicalTable", "lt-name", IDENTIFIER)));

    SQLException ex = assertThrows(SQLException.class,
        () -> DependencyChecker.assertNoExternalDependents(
            pipelineApi, triggerApi, DB, PATH, null, null));
    assertTrue(ex.getMessage().contains("trigger/backfill"),
        "blocker should be tagged with its CRD kind so the user knows what to look at: "
            + ex.getMessage());
    assertTrue(ex.getMessage().contains("LogicalTable/lt-name"),
        "owner reference should appear in the blocker description: " + ex.getMessage());
  }

  @Test
  void skipsSelfOwnedPipeline() throws SQLException {
    emptyByDefault();
    when(pipelineApi.select(DependencyLabels.labelKey(DB, PATH)))
        .thenReturn(Collections.singletonList(pipeline("owned-pipe", "LogicalTable", "self-name", IDENTIFIER)));

    assertDoesNotThrow(() -> DependencyChecker.assertNoExternalDependents(
        pipelineApi, triggerApi, DB, PATH, "LogicalTable", "self-name"));
  }

  @Test
  void skipsSelfOwnedTrigger() throws SQLException {
    emptyByDefault();
    when(triggerApi.select(DependencyLabels.labelKey(DB, PATH)))
        .thenReturn(Collections.singletonList(trigger("owned-trigger", "LogicalTable", "self-name", IDENTIFIER)));

    // Triggers spawned by the same LogicalTable cascade-delete with the parent — they must not
    // count as external dependents or you can't drop the LT at all.
    assertDoesNotThrow(() -> DependencyChecker.assertNoExternalDependents(
        pipelineApi, triggerApi, DB, PATH, "LogicalTable", "self-name"));
  }

  @Test
  void blocksOnExternalWhenSomeAreSelfOwned() throws SQLException {
    emptyByDefault();
    when(pipelineApi.select(DependencyLabels.labelKey(DB, PATH))).thenReturn(Arrays.asList(
        pipeline("owned-pipe", "LogicalTable", "self-name", IDENTIFIER),
        pipeline("external-pipe", "View", "other-owner", IDENTIFIER)));

    SQLException ex = assertThrows(SQLException.class,
        () -> DependencyChecker.assertNoExternalDependents(
            pipelineApi, triggerApi, DB, PATH, "LogicalTable", "self-name"));
    assertTrue(ex.getMessage().contains("external-pipe"));
    assertFalse(ex.getMessage().contains("owned-pipe"), "self-owned pipeline must not be listed");
  }

  @Test
  void rejectsSlugCollisionViaAnnotation() throws SQLException {
    emptyByDefault();
    // Pipeline labels collide on the slug (which is what api.select matched on) but the
    // annotation reveals the actual identifier is different — so this should NOT block.
    when(pipelineApi.select(DependencyLabels.labelKey(DB, PATH)))
        .thenReturn(Collections.singletonList(pipeline("colliding-pipe", "View", "owner",
            "some-other-database/some-other-path")));

    assertDoesNotThrow(() -> DependencyChecker.assertNoExternalDependents(
        pipelineApi, triggerApi, DB, PATH, null, null));
  }

  @Test
  void treatsMissingAnnotationAsTrusted() throws SQLException {
    emptyByDefault();
    // A pipeline with the matching label but no depends-on annotation (pre-labeling migration
    // case, or future code path that didn't write the annotation) is still treated as a blocker.
    when(pipelineApi.select(DependencyLabels.labelKey(DB, PATH)))
        .thenReturn(Collections.singletonList(pipeline("legacy-pipe", "View", "owner", null)));

    SQLException ex = assertThrows(SQLException.class,
        () -> DependencyChecker.assertNoExternalDependents(
            pipelineApi, triggerApi, DB, PATH, null, null));
    assertTrue(ex.getMessage().contains("legacy-pipe"));
  }

  @Test
  void errorMessageIncludesOwnerKindAndName() throws SQLException {
    emptyByDefault();
    when(pipelineApi.select(DependencyLabels.labelKey(DB, PATH)))
        .thenReturn(Collections.singletonList(pipeline("ext-pipe", "View", "owner", IDENTIFIER)));

    SQLException ex = assertThrows(SQLException.class,
        () -> DependencyChecker.assertNoExternalDependents(
            pipelineApi, triggerApi, DB, PATH, null, null));
    assertTrue(ex.getMessage().contains("View/owner"),
        "error should name the owning View so the user knows what to unhook: " + ex.getMessage());
  }

  @Test
  void errorMessageListsAllBlockersAcrossKinds() throws SQLException {
    emptyByDefault();
    when(pipelineApi.select(DependencyLabels.labelKey(DB, PATH))).thenReturn(Arrays.asList(
        pipeline("p1", "View", "owner1", IDENTIFIER),
        pipeline("p2", "View", "owner2", IDENTIFIER)));
    when(triggerApi.select(DependencyLabels.labelKey(DB, PATH))).thenReturn(
        Collections.singletonList(trigger("t1", "LogicalTable", "lt-name", IDENTIFIER)));

    SQLException ex = assertThrows(SQLException.class,
        () -> DependencyChecker.assertNoExternalDependents(
            pipelineApi, triggerApi, DB, PATH, null, null));
    assertTrue(ex.getMessage().contains("pipeline/p1"));
    assertTrue(ex.getMessage().contains("pipeline/p2"));
    assertTrue(ex.getMessage().contains("trigger/t1"));
    assertTrue(ex.getMessage().contains("3 active dependent"));
  }
}
