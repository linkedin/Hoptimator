package com.linkedin.hoptimator.logical;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;


/**
 * Pre-delete dependency check for a logical-table {@link Source}. Resolves each tier's Source
 * (via the tier's Database CRD), then scans Pipeline CRDs for references to <em>any</em> tier
 * shape ({@code `KAFKA`.`testevent`}, {@code `VENICE`.`testevent`}, ...). Any match blocks the
 * DROP — pipelines reading the underlying tier would otherwise be orphaned.
 *
 * <p>The logical-table reference itself ({@code `LOGICAL`.`testevent`}) is also checked, even
 * though pipelines don't typically use that shape — included for completeness in case a future
 * caller does.
 */
class LogicalTableDependencyValidator implements Validator {

  private final Source source;
  private final Properties tierProps;
  private final K8sContext context;

  LogicalTableDependencyValidator(Source source, Properties tierProps, K8sContext context) {
    this.source = source;
    this.tierProps = tierProps;
    this.context = context;
  }

  @Override
  public void validate(Issues issues, Connection connection) {
    Map<String, Source> tierSources;
    try {
      tierSources = resolveTierSources();
    } catch (SQLException e) {
      issues.error("Could not resolve tier sources for "
          + source.pathString() + ": " + e.getMessage());
      return;
    }

    List<String> patterns = new ArrayList<>();
    addPatternIfPresent(patterns, source);
    for (Source tierSource : tierSources.values()) {
      addPatternIfPresent(patterns, tierSource);
    }
    if (patterns.isEmpty()) {
      return;
    }

    Collection<V1alpha1Pipeline> pipelines;
    try {
      pipelines = pipelineApi().list();
    } catch (SQLException e) {
      issues.error("Failed to list pipelines for logical-table dependency check: " + e.getMessage());
      return;
    }

    List<String> blockers = new ArrayList<>();
    for (V1alpha1Pipeline p : pipelines) {
      String sql = p.getSpec() == null ? null : p.getSpec().getSql();
      if (sql == null) {
        continue;
      }
      for (String pattern : patterns) {
        if (sql.contains(pattern)) {
          String name = p.getMetadata() == null ? "<unknown>" : p.getMetadata().getName();
          blockers.add(name + " (refs " + pattern + ")");
          break;
        }
      }
    }
    if (!blockers.isEmpty()) {
      issues.error(String.format(
          "Cannot drop logical table %s — active pipeline(s) depend on it or its tiers: %s",
          source.pathString(), String.join(", ", blockers)));
    }
  }

  private static void addPatternIfPresent(List<String> patterns, Source source) {
    String schema = source.schema();
    String table = source.table();
    if (schema != null && table != null) {
      patterns.add("`" + schema + "`.`" + table + "`");
    }
  }

  /**
   * Resolves each tier's Source by looking up the tier Database CRD and computing the
   * {schema, table} the tier exposes. Mirrors {@code LogicalTableDeployer.buildTierSources}
   * so the patterns match what real pipelines reference.
   */
  private Map<String, Source> resolveTierSources() throws SQLException {
    K8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new K8sApi<>(context, K8sApiEndpoints.DATABASES);
    Map<String, Source> tierSources = new LinkedHashMap<>();
    for (LogicalTier tier : LogicalTier.values()) {
      String tierName = tier.tierName();
      String tierDatabaseName = tierProps.getProperty(tierName);
      if (tierDatabaseName == null) {
        continue;
      }
      V1alpha1Database db = dbApi.get(tierDatabaseName);
      tierSources.put(tierName, new Source(tierDatabaseName, pathFromDatabase(db, source.table()),
          source.options()));
    }
    return tierSources;
  }

  private static List<String> pathFromDatabase(V1alpha1Database db, String table) {
    V1alpha1DatabaseSpec spec = db.getSpec();
    List<String> path = new ArrayList<>();
    if (spec.getCatalog() != null) {
      path.add(spec.getCatalog());
    }
    path.add(spec.getSchema());
    path.add(table);
    return path;
  }

  /** Package-visible factory hook so tests can inject a {@link com.linkedin.hoptimator.k8s.FakeK8sApi}. */
  K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi() {
    return new K8sApi<>(context, K8sApiEndpoints.PIPELINES);
  }
}
