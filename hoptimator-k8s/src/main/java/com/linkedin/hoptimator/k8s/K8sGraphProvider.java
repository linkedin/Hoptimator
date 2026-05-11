package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.hoptimator.graph.GraphProvider;
import com.linkedin.hoptimator.graph.GraphTarget;
import com.linkedin.hoptimator.graph.PipelineGraph;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;


/**
 * {@link GraphProvider} backed by Hoptimator's K8s state. Wraps {@link PipelineGraphBuilder} —
 * the existing entry-point methods become routes for each {@link GraphTarget} subtype:
 *
 * <ul>
 *   <li>{@link GraphTarget.View} → {@link PipelineGraphBuilder#forView(String, String, int)}
 *   <li>{@link GraphTarget.LogicalTable} → {@link PipelineGraphBuilder#forLogicalTable(String, String, int)}
 *   <li>{@link GraphTarget.Resource} → {@link PipelineGraphBuilder#forResource(String, List, int)},
 *       with SQL-side → K8s-side identifier resolution (see {@link #resolveResource}).
 * </ul>
 *
 * <p>Registered via {@code META-INF/services/com.linkedin.hoptimator.graph.GraphProvider} so callers
 * reach it through {@link com.linkedin.hoptimator.graph.GraphService}.
 */
public final class K8sGraphProvider implements GraphProvider {

  @Override
  public PipelineGraph forTarget(GraphTarget target, int depth, Connection connection)
      throws SQLException {
    K8sContext context = K8sContext.create(connection);
    PipelineGraphBuilder builder = new PipelineGraphBuilder(context);
    if (target instanceof GraphTarget.View) {
      GraphTarget.View v = (GraphTarget.View) target;
      return builder.forView(defaultNamespace(v.namespace(), context), v.name(), depth);
    }
    if (target instanceof GraphTarget.LogicalTable) {
      GraphTarget.LogicalTable lt = (GraphTarget.LogicalTable) target;
      return builder.forLogicalTable(defaultNamespace(lt.namespace(), context), lt.name(), depth);
    }
    if (target instanceof GraphTarget.Resource) {
      GraphTarget.Resource r = (GraphTarget.Resource) target;
      Resolved resolved = resolveResource(context, r.database(), r.path());
      if (resolved == null) {
        // No Database CRD matches the user-typed schema / catalog. Pass through verbatim so the
        // builder produces a degenerate graph and the renderer surfaces the "no pipelines
        // reference this resource" warning.
        return builder.forResource(r.database(), r.path(), depth);
      }
      return builder.forResource(resolved.database, resolved.path, depth);
    }
    throw new SQLException("K8sGraphProvider does not support target: " + target);
  }

  /** When the caller didn't supply a namespace, fall back to the K8s context's default. */
  private static String defaultNamespace(String requested, K8sContext context) {
    return requested != null ? requested : context.namespace();
  }

  @Override
  public boolean supports(GraphTarget target) {
    return target instanceof GraphTarget.View
        || target instanceof GraphTarget.LogicalTable
        || target instanceof GraphTarget.Resource;
  }

  /**
   * Resolve the user's SQL identifier into the form Pipelines and Triggers used when stamping
   * their {@code depends-on-<slug>} labels.
   *
   * <p>The {@code !graph table} CLI accepts SQL identifiers — {@code SCHEMA.TABLE} for 2-level
   * databases or {@code CATALOG.SCHEMA.TABLE} for 3-level (JDBC-style) ones. The planner
   * stamps labels using the K8s-side form: the {@code Database} CRD's {@code metadata.name}
   * plus the qualified path. Without bridging, lookups silently return empty — the slugs are
   * derived from different strings.
   *
   * <p>Resolution scans the {@code Database} CRD list and tries:
   * <ol>
   *   <li>Catalog match (case-insensitive). User typed {@code CATALOG.<rest>}. Canonical path
   *       is {@code [catalog, schema, ...rest]}. If the user already included the schema as
   *       the first segment of {@code <rest>}, don't duplicate it.</li>
   *   <li>Schema match (case-insensitive). User typed {@code SCHEMA.<rest>}. Canonical path
   *       is {@code [catalog, schema, ...rest]} if the CRD has a catalog, otherwise
   *       {@code [schema, ...rest]}.</li>
   * </ol>
   * Catalog match wins when both are present so the canonical 3-level form
   * {@code CATALOG.SCHEMA.TABLE} resolves correctly.
   *
   * <p>Returns {@code null} when nothing matches; callers then pass the input through
   * unchanged so the builder produces the degenerate graph + "no pipelines reference" warning.
   *
   * <p>The path tail is preserved verbatim. The bridge can't know whether the actual stamped
   * label was upper- or mixed-case: Calcite-normalized MV sources are upper case
   * ({@code [ADS, AD_CLICKS]}); LogicalTable-spawned inter-tier pipelines are mixed
   * ({@code [KAFKA, testevent]}) because the LT carries the table name as the user wrote it.
   * Auto-canonicalization would help one case and break the other. Users copy the exact
   * identifier from {@code !graph view} / {@code !graph logical} output, which renders the
   * canonical stamped form, and paste it into {@code !graph table}.
   */
  static Resolved resolveResource(K8sContext context, String database, List<String> path)
      throws SQLException {
    return resolveResource(new K8sApi<>(context, K8sApiEndpoints.DATABASES), database, path);
  }

  /** Variant that takes a pre-built {@link K8sApi} — used by tests to inject mocks. */
  static Resolved resolveResource(K8sApi<V1alpha1Database, V1alpha1DatabaseList> databaseApi,
      String database, List<String> path) throws SQLException {

    // Scan Database CRDs once; prefer catalog match over schema match when both are present
    // (so canonical 3-level CATALOG.SCHEMA.TABLE input resolves correctly).
    V1alpha1Database catalogMatch = null;
    V1alpha1Database schemaMatch = null;
    for (V1alpha1Database d : databaseApi.list()) {
      if (d.getSpec() == null || d.getMetadata() == null || d.getMetadata().getName() == null) {
        continue;
      }
      String catalog = d.getSpec().getCatalog();
      String schema = d.getSpec().getSchema();
      if (catalogMatch == null && catalog != null && catalog.equalsIgnoreCase(database)) {
        catalogMatch = d;
      }
      if (schemaMatch == null && schema != null && schema.equalsIgnoreCase(database)) {
        schemaMatch = d;
      }
    }
    if (catalogMatch != null) {
      String catalog = catalogMatch.getSpec().getCatalog();
      String schema = catalogMatch.getSpec().getSchema();
      List<String> canonical = new ArrayList<>(path.size() + 2);
      canonical.add(catalog);
      if (schema != null && (path.isEmpty() || !path.get(0).equalsIgnoreCase(schema))) {
        canonical.add(schema);
      }
      canonical.addAll(path);
      return new Resolved(catalogMatch.getMetadata().getName(), canonical);
    }
    if (schemaMatch != null) {
      String catalog = schemaMatch.getSpec().getCatalog();
      String schema = schemaMatch.getSpec().getSchema();
      List<String> canonical = new ArrayList<>(path.size() + 2);
      if (catalog != null) {
        canonical.add(catalog);
      }
      canonical.add(schema);
      canonical.addAll(path);
      return new Resolved(schemaMatch.getMetadata().getName(), canonical);
    }
    return null;
  }

  /** Carries the result of {@link #resolveResource} — final {@code (database, path)} for the lookup. */
  static final class Resolved {
    final String database;
    final List<String> path;

    Resolved(String database, List<String> path) {
      this.database = database;
      this.path = path;
    }
  }
}
