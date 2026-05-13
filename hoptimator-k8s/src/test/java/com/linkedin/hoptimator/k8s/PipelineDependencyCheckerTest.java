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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class PipelineDependencyCheckerTest {

  @Mock
  private K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> api;

  private static final String DB = "kafka1";
  private static final List<String> PATH = Collections.singletonList("my-topic");
  private static final String IDENTIFIER = "kafka1_my-topic";

  private static V1alpha1Pipeline pipeline(String name, String ownerKind, String ownerName,
      String annotationValue) {
    V1ObjectMeta meta = new V1ObjectMeta().name(name);
    if (ownerKind != null && ownerName != null) {
      meta.addOwnerReferencesItem(new V1OwnerReference().kind(ownerKind).name(ownerName));
    }
    if (annotationValue != null) {
      Map<String, String> annotations = new HashMap<>();
      annotations.put(PipelineDependencyLabels.ANNOTATION_KEY, annotationValue);
      meta.setAnnotations(annotations);
    }
    return new V1alpha1Pipeline().metadata(meta);
  }

  @Test
  void passesWhenNoPipelinesMatch() throws SQLException {
    when(api.select(PipelineDependencyLabels.labelKey(DB, PATH))).thenReturn(Collections.emptyList());

    assertDoesNotThrow(() -> PipelineDependencyChecker.assertNoExternalDependents(api, DB, PATH, null, null));
  }

  @Test
  void blocksOnExternalPipeline() throws SQLException {
    when(api.select(PipelineDependencyLabels.labelKey(DB, PATH)))
        .thenReturn(Collections.singletonList(pipeline("ext-pipe", "View", "owner", IDENTIFIER)));

    SQLException ex = assertThrows(SQLException.class,
        () -> PipelineDependencyChecker.assertNoExternalDependents(api, DB, PATH, null, null));
    assertTrue(ex.getMessage().contains("ext-pipe"));
    assertTrue(ex.getMessage().contains(IDENTIFIER));
  }

  @Test
  void skipsSelfOwnedPipeline() throws SQLException {
    when(api.select(PipelineDependencyLabels.labelKey(DB, PATH)))
        .thenReturn(Collections.singletonList(pipeline("owned-pipe", "LogicalTable", "self-name", IDENTIFIER)));

    assertDoesNotThrow(() -> PipelineDependencyChecker.assertNoExternalDependents(
        api, DB, PATH, "LogicalTable", "self-name"));
  }

  @Test
  void blocksOnExternalWhenSomeAreSelfOwned() throws SQLException {
    when(api.select(PipelineDependencyLabels.labelKey(DB, PATH))).thenReturn(Arrays.asList(
        pipeline("owned-pipe", "LogicalTable", "self-name", IDENTIFIER),
        pipeline("external-pipe", "View", "other-owner", IDENTIFIER)));

    SQLException ex = assertThrows(SQLException.class,
        () -> PipelineDependencyChecker.assertNoExternalDependents(
            api, DB, PATH, "LogicalTable", "self-name"));
    assertTrue(ex.getMessage().contains("external-pipe"));
    assertFalse(ex.getMessage().contains("owned-pipe"), "self-owned pipeline must not be listed");
  }

  @Test
  void rejectsSlugCollisionViaAnnotation() throws SQLException {
    // Pipeline labels collide on the slug (which is what api.select matched on) but the
    // annotation reveals the actual identifier is different — so this should NOT block.
    when(api.select(PipelineDependencyLabels.labelKey(DB, PATH)))
        .thenReturn(Collections.singletonList(pipeline("colliding-pipe", "View", "owner",
            "some-other-database/some-other-path")));

    assertDoesNotThrow(() -> PipelineDependencyChecker.assertNoExternalDependents(api, DB, PATH, null, null));
  }

  @Test
  void treatsMissingAnnotationAsTrusted() throws SQLException {
    // A pipeline with the matching label but no depends-on annotation (pre-labeling migration
    // case, or future code path that didn't write the annotation) is still treated as a blocker.
    when(api.select(PipelineDependencyLabels.labelKey(DB, PATH)))
        .thenReturn(Collections.singletonList(pipeline("legacy-pipe", "View", "owner", null)));

    SQLException ex = assertThrows(SQLException.class,
        () -> PipelineDependencyChecker.assertNoExternalDependents(api, DB, PATH, null, null));
    assertTrue(ex.getMessage().contains("legacy-pipe"));
  }

  @Test
  void errorMessageIncludesOwnerKindAndName() throws SQLException {
    when(api.select(PipelineDependencyLabels.labelKey(DB, PATH)))
        .thenReturn(Collections.singletonList(pipeline("ext-pipe", "View", "owner", IDENTIFIER)));

    SQLException ex = assertThrows(SQLException.class,
        () -> PipelineDependencyChecker.assertNoExternalDependents(api, DB, PATH, null, null));
    assertTrue(ex.getMessage().contains("View/owner"),
        "error should name the owning View so the user knows what to unhook: " + ex.getMessage());
  }

  @Test
  void errorMessageListsAllBlockers() throws SQLException {
    when(api.select(PipelineDependencyLabels.labelKey(DB, PATH))).thenReturn(Arrays.asList(
        pipeline("p1", "View", "owner1", IDENTIFIER),
        pipeline("p2", "View", "owner2", IDENTIFIER),
        pipeline("p3", "View", "owner3", IDENTIFIER)));

    SQLException ex = assertThrows(SQLException.class,
        () -> PipelineDependencyChecker.assertNoExternalDependents(api, DB, PATH, null, null));
    assertTrue(ex.getMessage().contains("p1"));
    assertTrue(ex.getMessage().contains("p2"));
    assertTrue(ex.getMessage().contains("p3"));
    assertTrue(ex.getMessage().contains("3 active pipeline"));
  }
}
