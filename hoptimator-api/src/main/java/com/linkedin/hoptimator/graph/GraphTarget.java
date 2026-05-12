package com.linkedin.hoptimator.graph;

import java.util.List;
import java.util.Objects;


/**
 * The thing a {@link GraphProvider} is asked to materialize a {@link PipelineGraph} around.
 *
 * <p>Abstract base + nested final subclasses, one per CLI mode:
 * <ul>
 *   <li>{@link View} — graph rooted at a view (regular or materialized).</li>
 *   <li>{@link LogicalTable} — graph rooted at a logical table.</li>
 *   <li>{@link Resource} — reverse lookup against the dependency index, rooted at an external
 *       resource identified by {@code (database, path)}.</li>
 * </ul>
 *
 * <p>Providers dispatch on the concrete subclass via {@code instanceof} to route to the right
 * internal implementation. Same pattern {@link GraphNode} uses for its kinds, kept here for
 * Java-11 compatibility (no sealed types).
 */
public abstract class GraphTarget {

  private GraphTarget() {
  }

  /** A regular or materialized view, identified by namespace + canonical name. */
  public static final class View extends GraphTarget {
    private final String namespace;
    private final String name;

    public View(String namespace, String name) {
      this.namespace = namespace;
      this.name = Objects.requireNonNull(name, "name");
    }

    public String namespace() {
      return namespace;
    }

    public String name() {
      return name;
    }

    @Override
    public String toString() {
      return "View[" + namespace + "/" + name + "]";
    }
  }

  /** A logical table identified by namespace + canonical name. */
  public static final class LogicalTable extends GraphTarget {
    private final String namespace;
    private final String name;

    public LogicalTable(String namespace, String name) {
      this.namespace = namespace;
      this.name = Objects.requireNonNull(name, "name");
    }

    public String namespace() {
      return namespace;
    }

    public String name() {
      return name;
    }

    @Override
    public String toString() {
      return "LogicalTable[" + namespace + "/" + name + "]";
    }
  }

  /**
   * An external resource (Kafka topic, Venice store, MySQL table, etc.) identified by its
   * database name plus path components. Used for reverse lookups.
   */
  public static final class Resource extends GraphTarget {
    private final String database;
    private final List<String> path;

    public Resource(String database, List<String> path) {
      this.database = Objects.requireNonNull(database, "database");
      this.path = Objects.requireNonNull(path, "path");
    }

    public String database() {
      return database;
    }

    public List<String> path() {
      return path;
    }

    @Override
    public String toString() {
      return "Resource[" + database + "/" + String.join(".", path) + "]";
    }
  }
}
