package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.linkedin.hoptimator.GraphProvider;
import com.linkedin.hoptimator.GraphTarget;
import com.linkedin.hoptimator.PipelineGraph;
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
 * <p>Registered via {@code META-INF/services/com.linkedin.hoptimator.GraphProvider} so callers
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
   * Resolve the user-supplied {@code (database, path)} into the form Pipelines and Triggers
   * used when stamping their {@code depends-on-<slug>} labels.
   *
   * <p>The {@code !graph table} CLI accepts SQL-side identifiers like {@code ADS.AD_CLICKS}
   * (2-level: schema + table) or {@code MYSQL.testdb.orders} (3-level: catalog + schema +
   * table), but the planner stamps labels using the K8s-side form: the {@code Database} CRD's
   * {@code metadata.name} plus the qualified path. Without bridging, lookups silently return
   * empty — the slugs are derived from different strings.
   *
   * <p>Resolution order:
   * <ol>
   *   <li>If a {@code Database} CRD exists with {@code metadata.name == database}, treat the
   *       input as already-canonical and pass through.</li>
   *   <li>Scan {@code Database} CRDs. Catalog match (case-insensitive) wins over schema match:
   *       a user typing {@code MYSQL.testdb.orders} means the catalog is {@code MYSQL}, and
   *       {@code mysql} is the CRD name we want.</li>
   *   <li>Catalog match: substitute the CRD name as database; canonical path is
   *       {@code [catalog, schema, ...rest]}. If the user already included the schema as the
   *       first segment of {@code <rest>}, don't duplicate it.</li>
   *   <li>Schema match: substitute the CRD name; canonical path is
   *       {@code [catalog, schema, ...rest]} if the CRD has a catalog, otherwise
   *       {@code [schema, ...rest]}.</li>
   *   <li>If nothing matches, pass through — the lookup returns empty and the renderer surfaces
   *       the "no pipelines reference this resource" warning.</li>
   * </ol>
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
    return resolveResource(new K8sApi<>(context, K8sApiEndpoints.DATABASES),
        context.namespace(), database, path);
  }

  /** Variant that takes a pre-built {@link K8sApi} — used by tests to inject mocks. */
  static Resolved resolveResource(K8sApi<V1alpha1Database, V1alpha1DatabaseList> databaseApi,
      String namespace, String database, List<String> path) throws SQLException {

    // Step 1: exact name match — input is already canonical.
    if (databaseApi.getIfExists(namespace, database) != null) {
      return new Resolved(database, path);
    }

    // Step 2 + 3: try catalog and schema match on the Database CRD list. Catalog match wins
    // when both are present because users typing the canonical 3-level form `CATALOG.SCHEMA.TABLE`
    // expect the catalog to be the first segment. Pipelines don't collide on (CRD name, catalog,
    // schema), so the first match in each category wins.
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
      // User typed `CATALOG.<rest>`. Canonical path is `[catalog, schema, ...rest]`. If the
      // user already included the schema as the first segment of <rest>, don't duplicate it.
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
      // User typed `SCHEMA.<rest>`. If the Database has a catalog (3-level), the stamped path
      // includes it: `[catalog, schema, ...rest]`. Otherwise it's `[schema, ...rest]`.
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

    // Step 4: pass through. forResource() will return a degenerate graph and the CLI / Quidem
    // wrapper surfaces the "no pipelines reference" warning.
    return new Resolved(database, path);
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
