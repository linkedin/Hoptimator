package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;


/**
 * Pre-delete dependency check for a {@link Source}. Lists Pipeline CRDs in the namespace and
 * looks for a backtick-quoted reference to the source's {@code schema.table} in each pipeline's
 * {@code spec.sql}. Any match becomes a validation error, blocking the DROP.
 *
 * <p>Why scan {@code spec.sql} rather than maintain a separate index?
 * <ul>
 *   <li>The dependency information is already there — every Pipeline's SQL fully-qualifies
 *       its source/sink references with backticks ({@code `KAFKA`.`existing-topic-2`}).</li>
 *   <li>Cost is bounded: O(pipelines-in-namespace) per DROP TABLE, which is rare.</li>
 *   <li>No new state to keep consistent with the source-of-truth (the SQL itself).</li>
 * </ul>
 *
 * <p>The match pattern is the literal substring {@code `<schema>`.`<table>`}. Backtick boundaries
 * make this resistant to substring false positives ({@code `audience`} won't match
 * {@code `audience-derived`} because the pattern requires a closing backtick after the name).
 */
class K8sPipelineDependencyValidator implements Validator {

  private final Source source;

  K8sPipelineDependencyValidator(Source source) {
    this.source = source;
  }

  @Override
  public void validate(Issues issues, Connection connection) {
    String pattern = referencePattern(source);
    if (pattern == null) {
      // Nothing to match against (e.g. single-element path with no schema).
      return;
    }
    if (connection == null) {
      issues.error("Cannot run pre-delete dependency check without a connection");
      return;
    }
    Collection<V1alpha1Pipeline> pipelines;
    try {
      pipelines = pipelineApi(connection).list();
    } catch (SQLException e) {
      issues.error("Failed to list pipelines for dependency check: " + e.getMessage());
      return;
    }
    List<String> blockers = new ArrayList<>();
    for (V1alpha1Pipeline p : pipelines) {
      String sql = p.getSpec() == null ? null : p.getSpec().getSql();
      if (sql == null) {
        continue;
      }
      if (sql.contains(pattern)) {
        blockers.add(p.getMetadata() == null ? "<unknown>" : p.getMetadata().getName());
      }
    }
    if (!blockers.isEmpty()) {
      issues.error(String.format(
          "Cannot drop %s — active pipeline(s) depend on it: %s",
          pattern, String.join(", ", blockers)));
    }
  }

  /** Package-visible factory hook so tests can inject a {@link FakeK8sApi}. */
  K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi(Connection connection) {
    return new K8sApi<>(K8sContext.create(connection), K8sApiEndpoints.PIPELINES);
  }

  /**
   * Builds the backtick-quoted reference string the pipeline's SQL would contain if it referenced
   * this source. Returns {@code null} for sources without a schema component (those don't show
   * up in pipeline SQL with this shape).
   */
  static String referencePattern(Source source) {
    String schema = source.schema();
    String table = source.table();
    if (schema == null || table == null) {
      return null;
    }
    return "`" + schema + "`.`" + table + "`";
  }
}
