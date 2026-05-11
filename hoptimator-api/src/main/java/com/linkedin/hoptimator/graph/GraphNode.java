package com.linkedin.hoptimator.graph;

import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * A node in a pipeline visualization graph.
 *
 * <p>Concrete node types are nested final subclasses keyed off a {@link Kind} enum so renderers
 * can dispatch on kind without {@code instanceof} chains. Node identity for set/edge purposes is
 * determined by {@link #id()} — typically a stable string built from the underlying K8s
 * resource's namespace+name, or for {@link External} nodes from the database+path identifier.
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
   * A {@code V1alpha1Pipeline} CRD. Carries optional {@code jobKind} and {@code engine}
   * descriptors extracted from the rendered job YAML inside {@code V1alpha1PipelineSpec.yaml},
   * surfaced inline in the rendered Mermaid label so users can tell at a glance whether a
   * pipeline is e.g. {@code FlinkDeployment} on Flink vs a Beam runner.
   */
  public static final class Pipeline extends GraphNode {
    private final String namespace;
    private final String name;
    private final String sql;
    private final String jobKind;
    private final String engine;

    public Pipeline(String namespace, String name, String sql) {
      this(namespace, name, sql, null, null);
    }

    public Pipeline(String namespace, String name, String sql, String jobKind, String engine) {
      super(Kind.PIPELINE, "pipeline:" + namespace + "/" + name);
      this.namespace = namespace;
      this.name = Objects.requireNonNull(name, "name");
      this.sql = sql;
      this.jobKind = jobKind;
      this.engine = engine;
    }

    public String namespace() {
      return namespace;
    }

    public String name() {
      return name;
    }

    /** Pipeline SQL — useful for tooltips/hover; not used to determine graph topology. */
    public String sql() {
      return sql;
    }

    /** K8s {@code kind:} of the underlying job artifact (e.g. {@code FlinkDeployment}); nullable. */
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

  /** A {@code V1alpha1View} CRD (regular or materialized). */
  public static final class View extends GraphNode {
    private final String namespace;
    private final String name;
    private final boolean materialized;

    public View(String namespace, String name, boolean materialized) {
      super(Kind.VIEW, "view:" + namespace + "/" + name);
      this.namespace = namespace;
      this.name = Objects.requireNonNull(name, "name");
      this.materialized = materialized;
    }

    public String namespace() {
      return namespace;
    }

    public String name() {
      return name;
    }

    public boolean materialized() {
      return materialized;
    }

    @Override
    public String displayName() {
      // Bare name only — the renderer conveys "view"-ness via shape (rectangle for leaves)
      // and via the kind-only subgraph wrapper title when a View owns its realizing Pipeline.
      // Repeating "Materialized View " in the displayName duplicated info that's already
      // present visually.
      return name;
    }
  }

  /** A {@code V1alpha1LogicalTable} CRD. Tier names preserve insertion order. */
  public static final class LogicalTable extends GraphNode {
    private final String namespace;
    private final String name;
    private final Map<String, String> tiers;

    public LogicalTable(String namespace, String name, Map<String, String> tiers) {
      super(Kind.LOGICAL_TABLE, "logicaltable:" + namespace + "/" + name);
      this.namespace = namespace;
      this.name = Objects.requireNonNull(name, "name");
      this.tiers = Objects.requireNonNull(tiers, "tiers");
    }

    public String namespace() {
      return namespace;
    }

    public String name() {
      return name;
    }

    /** Tier name → backing database CRD name (e.g. {@code nearline → kafka-db}). */
    public Map<String, String> tiers() {
      return tiers;
    }

    @Override
    public String displayName() {
      // Bare name — the renderer prepends "LogicalTable " on the subgraph wrapper title
      // because nothing inside the wrapper carries the LT name otherwise. Leaving the prefix
      // off the displayName keeps the contract symmetric with View and Trigger.
      return name;
    }
  }

  /**
   * A {@code V1alpha1TableTrigger} CRD. Carries optional rendering hints pulled from the trigger's
   * rendered job YAML (kind, container name, generated job name) and from the
   * {@code hoptimator.linkedin.com/job-template} annotation. Renderers display whichever hints
   * are present so the user can tell at a glance what the underlying job is doing.
   */
  public static final class Trigger extends GraphNode {
    private final String namespace;
    private final String name;
    private final String schedule;
    private final boolean paused;
    private final String jobTemplateName;
    private final String jobKind;
    private final String jobName;
    private final String containerName;

    public Trigger(String namespace, String name, String schedule, boolean paused) {
      this(namespace, name, schedule, paused, null, null, null, null);
    }

    public Trigger(String namespace, String name, String schedule, boolean paused,
        String jobTemplateName, String jobKind, String jobName, String containerName) {
      super(Kind.TRIGGER, "trigger:" + namespace + "/" + name);
      this.namespace = namespace;
      this.name = Objects.requireNonNull(name, "name");
      this.schedule = schedule;
      this.paused = paused;
      this.jobTemplateName = jobTemplateName;
      this.jobKind = jobKind;
      this.jobName = jobName;
      this.containerName = containerName;
    }

    public String namespace() {
      return namespace;
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

    /** JobTemplate name (e.g. {@code retl-job-template}); null when unknown. */
    public String jobTemplateName() {
      return jobTemplateName;
    }

    /** K8s {@code kind:} of the rendered job (e.g. {@code Job}, {@code CronJob}); nullable. */
    public String jobKind() {
      return jobKind;
    }

    /** Rendered Job's {@code metadata.name}; nullable. */
    public String jobName() {
      return jobName;
    }

    /** First container's name in the rendered Job spec; nullable. */
    public String containerName() {
      return containerName;
    }

    @Override
    public String displayName() {
      // Bare name only — the rhombus shape the renderer uses for triggers already conveys
      // the kind. Job metadata (cron, kind, etc.) gets appended by the renderer separately.
      return name;
    }
  }

  /** A resource managed outside Hoptimator's K8s state (Kafka topic, Venice store, etc.). */
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

    /** {@code V1alpha1DatabaseSpec.driver} — null when the database CRD couldn't be resolved. */
    public String driver() {
      return driver;
    }

    @Override
    public String displayName() {
      // Drop the database prefix — the database (CRD name) is internal plumbing, not something
      // SQL clients see. The path is the user-visible identifier.
      return String.join(".", path);
    }
  }
}
