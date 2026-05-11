package com.linkedin.hoptimator.graph;

import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * A node in a pipeline visualization graph.
 *
 * <p>Concrete node types are nested final subclasses keyed off a {@link Kind} enum so renderers
 * can dispatch on kind without {@code instanceof} chains. Node identity for set/edge purposes is
 * determined by {@link #id()} — typically a stable string built from the resource's name, or
 * for {@link External} nodes from the database + path identifier.
 */
public abstract class GraphNode {

  public enum Kind {
    PIPELINE,
    VIEW,
    LOGICAL_TABLE,
    TRIGGER,
    EXTERNAL
  }

  private final Kind kind;
  private final String id;

  private GraphNode(Kind kind, String id) {
    this.kind = Objects.requireNonNull(kind, "kind");
    this.id = Objects.requireNonNull(id, "id");
  }

  public final Kind kind() {
    return kind;
  }

  public final String id() {
    return id;
  }

  /** Human-readable label used by renderers as the primary node text. */
  public abstract String displayName();

  @Override
  public final boolean equals(Object o) {
    return o instanceof GraphNode && id.equals(((GraphNode) o).id);
  }

  @Override
  public final int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return kind + "[" + id + "]";
  }

  /**
   * A deployed pipeline — the job that materializes a query from sources to a sink. Carries
   * optional {@code jobKind} and {@code engine} descriptors extracted from the underlying job
   * artifact and surfaced inline in the rendered label so users can tell at a glance whether a
   * pipeline runs e.g. on Flink vs a Beam runner.
   */
  public static final class Pipeline extends GraphNode {
    private final String name;
    private final String jobKind;
    private final String engine;

    public Pipeline(String name, String jobKind, String engine) {
      super(Kind.PIPELINE, "pipeline:" + name);
      this.name = Objects.requireNonNull(name, "name");
      this.jobKind = jobKind;
      this.engine = engine;
    }

    public String name() {
      return name;
    }

    /** Type of the underlying job artifact (e.g. {@code FlinkDeployment}); nullable. */
    public String jobKind() {
      return jobKind;
    }

    /** Execution engine inferred from the job artifact (e.g. {@code Flink}, {@code Flink Beam}); nullable. */
    public String engine() {
      return engine;
    }

    @Override
    public String displayName() {
      return name;
    }
  }

  /** A view — regular or materialized. */
  public static final class View extends GraphNode {
    private final String name;
    private final boolean materialized;

    public View(String name, boolean materialized) {
      super(Kind.VIEW, "view:" + name);
      this.name = Objects.requireNonNull(name, "name");
      this.materialized = materialized;
    }

    public String name() {
      return name;
    }

    public boolean materialized() {
      return materialized;
    }

    @Override
    public String displayName() {
      return name;
    }
  }

  /** A logical table. Tier names preserve insertion order. */
  public static final class LogicalTable extends GraphNode {
    private final String name;
    private final Map<String, String> tiers;

    public LogicalTable(String name, Map<String, String> tiers) {
      super(Kind.LOGICAL_TABLE, "logicaltable:" + name);
      this.name = Objects.requireNonNull(name, "name");
      this.tiers = Objects.requireNonNull(tiers, "tiers");
    }

    public String name() {
      return name;
    }

    /** Tier name → backing database identifier (e.g. {@code nearline → kafka-db}). */
    public Map<String, String> tiers() {
      return tiers;
    }

    @Override
    public String displayName() {
      return name;
    }
  }

  /**
   * A trigger — a scheduled or event-driven job that fires against its target. Carries optional
   * rendering hints (container name, job-template binding) pulled from the trigger's underlying
   * job declaration. Renderers display whichever hints are present so the user can tell at a
   * glance what the underlying job is doing.
   */
  public static final class Trigger extends GraphNode {
    private final String name;
    private final String schedule;
    private final boolean paused;
    private final String jobTemplateName;
    private final String containerName;

    public Trigger(String name, String schedule, boolean paused,
        String jobTemplateName, String containerName) {
      super(Kind.TRIGGER, "trigger:" + name);
      this.name = Objects.requireNonNull(name, "name");
      this.schedule = schedule;
      this.paused = paused;
      this.jobTemplateName = jobTemplateName;
      this.containerName = containerName;
    }

    public String name() {
      return name;
    }

    /** Cron expression; null if the trigger isn't schedule-driven. */
    public String schedule() {
      return schedule;
    }

    public boolean paused() {
      return paused;
    }

    /** Job-template name this trigger was rendered from (e.g. {@code retl-job-template}); null when unknown. */
    public String jobTemplateName() {
      return jobTemplateName;
    }

    /** First container's name in the rendered job spec; nullable. */
    public String containerName() {
      return containerName;
    }

    @Override
    public String displayName() {
      return name;
    }
  }

  /** A resource managed outside Hoptimator (Kafka topic, Venice store, MySQL table, etc.). */
  public static final class External extends GraphNode {
    private final String database;
    private final List<String> path;
    private final String driver;

    public External(String database, List<String> path, String driver) {
      super(Kind.EXTERNAL, "external:" + database + "/" + String.join(".", path));
      this.database = Objects.requireNonNull(database, "database");
      this.path = Objects.requireNonNull(path, "path");
      this.driver = driver;
    }

    public String database() {
      return database;
    }

    public List<String> path() {
      return path;
    }

    /** Driver name (e.g. {@code kafka}, {@code venice}); null when the database couldn't be resolved. */
    public String driver() {
      return driver;
    }

    @Override
    public String displayName() {
      return String.join(".", path);
    }
  }
}
