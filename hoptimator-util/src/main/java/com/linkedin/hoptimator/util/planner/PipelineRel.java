package com.linkedin.hoptimator.util.planner;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Litmus;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.util.ConnectionService;
import com.linkedin.hoptimator.util.DeploymentService;


/**
 * Calling convention which implements a data pipeline.
 * <p>
 * "Convention" here just means a target set of "traits" the planner should
 * aim for. We can ask the planner to convert a query into the PIPELINE
 * convention, and the result will be a PipelineRel. This in turn can be
 * implemented as a Pipeline.
 */
public interface PipelineRel extends RelNode {

  Convention CONVENTION = new Convention.Impl("PIPELINE", PipelineRel.class);

  void implement(Implementor implementor);

  /** Implements a deployable Pipeline. */
  class Implementor {
    private final Map<Source, RelDataType> sources = new HashMap<>();
    private RelNode query;
    private String sinkDatabase = "pipeline";
    private List<String> sinkPath = Arrays.asList("PIPELINE", "SINK");
    private RelDataType sinkRowType = null;
    private Map<String, String> sinkOptions = Collections.emptyMap();

    public void visit(RelNode node) {
      if (query == null) {
        query = node;
      }
      for (RelNode input : node.getInputs()) {
        visit(input);
      }
      ((PipelineRel) node).implement(this);
    }

    /**
     * Adds a source to the pipeline.
     * <p>
     * This involves deploying any relevant objects, and configuring
     * a connector. The connector is configured via `CREATE TABLE...WITH(...)`.
     */
    public void addSource(String database, List<String> path, RelDataType rowType, Map<String, String> options) {
      sources.put(new Source(database, path, options), rowType);
    }

    /**
     * Sets the sink to use for the pipeline.
     * <p>
     * By default, the sink is `PIPELINE.SINK`. An expected row type is required
     * for validation purposes.
     */
    public void setSink(String database, List<String> path, RelDataType rowType, Map<String, String> options) {
      this.sinkDatabase = database;
      this.sinkPath = path;
      this.sinkRowType = rowType;
      this.sinkOptions = options;
    }

    public void setQuery(RelNode query) {
      this.query = query;
    }

    /** Combine Deployables into a Pipeline */
    public Pipeline pipeline() throws SQLException {
      List<Deployable> deployables = new ArrayList<>();
      for (Source source : sources.keySet()) {
        DeploymentService.deployables(source, Source.class).forEach(x -> deployables.add(x));
      }
      RelDataType targetRowType = sinkRowType;
      if (targetRowType == null) {
        targetRowType = query.getRowType();
      }
      Sink sink = new Sink(sinkDatabase, sinkPath, sinkOptions);
      Job job = new Job(sink, sql());
      RelOptUtil.eq(sink.table(), targetRowType, "pipeline", query.getRowType(), Litmus.THROW);
      DeploymentService.deployables(sink, Sink.class).forEach(x -> deployables.add(x));
      DeploymentService.deployables(job, Job.class).forEach(x -> deployables.add(x));
      return new Pipeline(deployables);
    }

    public Function<SqlDialect, String> sql() throws SQLException {
      ScriptImplementor script = ScriptImplementor.empty();
      for (Map.Entry<Source, RelDataType> source : sources.entrySet()) {
        Map<String, String> configs = ConnectionService.configure(source.getKey(), Source.class);
        script = script.connector(source.getKey().table(), source.getValue(), configs);
      }
      RelDataType targetRowType = sinkRowType;
      if (targetRowType == null) {
        targetRowType = query.getRowType();
      }
      Sink sink = new Sink(sinkDatabase, sinkPath, sinkOptions);
      Map<String, String> sinkConfigs = ConnectionService.configure(sink, Sink.class);
      script = script.connector(sink.table(), targetRowType, sinkConfigs);
      script = script.insert(sink.table(), query);
      RelOptUtil.eq(sink.table(), targetRowType, "pipeline", query.getRowType(), Litmus.THROW);
      return script.seal();
    }
  }
}
