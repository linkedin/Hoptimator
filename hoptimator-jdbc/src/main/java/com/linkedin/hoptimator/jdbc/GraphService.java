package com.linkedin.hoptimator.jdbc;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.graph.GraphProvider;
import com.linkedin.hoptimator.graph.GraphRenderer;
import com.linkedin.hoptimator.graph.GraphTarget;
import com.linkedin.hoptimator.graph.PipelineGraph;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcSchema;
import org.apache.calcite.util.Util;


/**
 * SPI dispatcher for {@link GraphProvider} (graph construction) and {@link GraphRenderer}
 * (graph serialization). Mirrors the {@code DeploymentService} pattern: providers and
 * renderers register via {@code META-INF/services}, callers go through static methods on
 * this class to discover and dispatch.
 */
public final class GraphService {

  private GraphService() {
  }

  /**
   * Build a {@link PipelineGraph} for the given target by dispatching to the first registered
   * {@link GraphProvider} that {@code supports(target)}.
   *
   * @throws SQLException if no provider supports the target, or if the underlying provider
   *                      throws.
   */
  public static PipelineGraph buildGraph(String identifier, int depth, HoptimatorConnection connection)
      throws SQLException {
    if (depth < 0) {
      throw new SQLException("depth must be non-negative; got: " + depth);
    }
    GraphTarget target = resolve(identifier, connection);
    for (GraphProvider provider : providers()) {
      if (provider.supports(target)) {
        return provider.forTarget(target, depth, connection);
      }
    }
    throw new SQLException("No GraphProvider supports target: " + target
        + ". Registered providers: " + providerSummary());
  }

  /**
   * Walks the user-typed SQL identifier through Calcite's schema tree and produces the
   * {@link GraphTarget} subtype that matches what was found:
   *
   * <ul>
   *   <li>{@link MaterializedViewTable} at the leaf → {@link GraphTarget.View}.</li>
   *   <li>Leaf inside a {@link HoptimatorJdbcSchema} whose downstream surfaces a
   *       {@code LogicalSchemaMarker}-tagged schema → {@link GraphTarget.LogicalTable}.</li>
   *   <li>Otherwise — leaf inside a physical {@link Database}-backed schema →
   *       {@link GraphTarget.Resource}. The graph downstream may still collapse to just the
   *       root if no pipeline references the resource — that's a legitimate "exists but
   *       unused" state and surfaces as a no-pipelines warning at render time.</li>
   * </ul>
   *
   * <p>Throws {@link SQLException} for any path that can't be resolved: a schema segment
   * that doesn't exist, a leaf schema that isn't backed by a Hoptimator {@link Database}, or
   * a leaf name that no Table in the catalog matches. All three are real "you typed something
   * this connection can't see" conditions, distinct from "table exists but has no pipelines."
   *
   * <p>LogicalTable detection is schema-level because the outer connection sees LogicalTables
   * wrapped as generic JDBC adapter tables — the class identity is erased on the JDBC hop.
   * {@code HoptimatorJdbcSchema} lazily walks its downstream connection to find the marker.
   */
  static GraphTarget resolve(String identifier, HoptimatorConnection connection) throws SQLException {
    List<String> fullPath = Arrays.asList(identifier.split("\\."));
    SchemaPlus schema = connection.calciteConnection().getRootSchema();
    List<String> schemaPath = Util.skipLast(fullPath);
    for (String segment : schemaPath) {
      SchemaPlus next = schema == null ? null : schema.subSchemas().get(segment);
      if (next == null) {
        throw new SQLException("Identifier " + identifier
            + " does not exist: schema segment '" + segment + "' not found in this connection.");
      }
      schema = next;
    }

    String leafName = Util.last(fullPath);
    Table table = schema == null ? null : schema.tables().get(leafName);
    if (table == null) {
      throw new SQLException("Identifier " + identifier
          + " does not exist: table '" + leafName + "' not found in schema "
          + String.join(".", schemaPath) + ".");
    }
    if (table instanceof MaterializedViewTable) {
      return new GraphTarget.View(identifier);
    }

    HoptimatorJdbcSchema hjs = schema.unwrap(HoptimatorJdbcSchema.class);
    if (hjs == null) {
      throw new SQLException("Identifier " + identifier
          + " does not resolve to a database in this connection.");
    }
    if (hjs.isLogical()) {
      return new GraphTarget.LogicalTable(identifier);
    }
    return new GraphTarget.Resource(hjs.databaseName(), fullPath);
  }

  /**
   * Render a graph using the registered {@link GraphRenderer} whose {@link GraphRenderer#format()}
   * matches {@code format} (case-insensitive).
   *
   * @throws IllegalArgumentException if no renderer supports {@code format}.
   */
  public static String render(PipelineGraph graph, String format) {
    for (GraphRenderer renderer : renderers()) {
      if (renderer.format().equalsIgnoreCase(format)) {
        return renderer.render(graph);
      }
    }
    throw new IllegalArgumentException("No GraphRenderer registered for format: " + format
        + ". Available: " + availableFormats());
  }

  /** Format identifiers (e.g. {@code mermaid}) registered by visible {@link GraphRenderer}s. */
  public static List<String> availableFormats() {
    return renderers().stream()
        .map(GraphRenderer::format)
        .distinct()
        .collect(Collectors.toList());
  }

  // ─── SPI loading ─────────────────────────────────────────────────────────

  private static List<GraphProvider> providers() {
    List<GraphProvider> providers = new ArrayList<>();
    ServiceLoader.load(GraphProvider.class).iterator().forEachRemaining(providers::add);
    return providers;
  }

  private static List<GraphRenderer> renderers() {
    List<GraphRenderer> renderers = new ArrayList<>();
    ServiceLoader.load(GraphRenderer.class).iterator().forEachRemaining(renderers::add);
    return renderers;
  }

  private static String providerSummary() {
    List<String> names = new ArrayList<>();
    for (GraphProvider p : providers()) {
      names.add(p.getClass().getSimpleName());
    }
    return names.isEmpty() ? "<none>" : String.join(", ", names);
  }
}
