package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.util.ConnectionService;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.Source;
import com.linkedin.hoptimator.util.Sink;
import com.linkedin.hoptimator.util.Job;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.util.Litmus;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.sql.SQLException;

/**
 * Calling convention which implements a data pipeline.
 *
 * "Convention" here just means a target set of "traits" the planner should
 * aim for. We can ask the planner to convert a query into the PIPELINE
 * convention, and the result will be a PipelineRel. This in turn can be
 * implemented as a Pipeline.
 */
public interface PipelineRel extends RelNode {

  Convention CONVENTION = new Convention.Impl("PIPELINE", PipelineRel.class);

  void implement(Implementor implementor) throws SQLException;

  /** Implements a deployable Pipeline. */
  class Implementor {
    private final List<Source> sources = new ArrayList<>();
    private RelNode query;
    private String sinkDatabase = "pipeline";
    private List<String> sinkPath = Arrays.asList(new String[]{"PIPELINE", "SINK"});
    private RelDataType sinkRowType = null;
    private Map<String, String> sinkOptions = Collections.emptyMap();

    public void visit(RelNode node) throws SQLException {
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
     * 
     * This involves deploying any relevant objects, and configuring a
     * a connector. The connector is configured via `CREATE TABLE...WITH(...)`.
     */
    public void addSource(String database, List<String> path, RelDataType rowType,
        Map<String, String> options) {
      sources.add(new Source(database, path, rowType, options));
    }

    /**
     * Sets the sink to use for the pipeline.
     *
     * By default, the sink is `PIPELINE.SINK`. An expected row type is required
     * for validation purposes.
     */
    public void setSink(String database, List<String> path, RelDataType rowType,
        Map<String, String> options) {
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
      for (Source source : sources) {
        DeploymentService.deployables(source, Source.class).forEach(x -> deployables.add(x));
        Map<String, String> configs = ConnectionService.configure(source, Source.class);
      }
      RelDataType targetRowType = sinkRowType;
      if (targetRowType == null) {
        targetRowType = query.getRowType();
      }
      Sink sink = new Sink(sinkDatabase, sinkPath, targetRowType, sinkOptions);
      Map<String, String> sinkConfigs = ConnectionService.configure(sink, Sink.class);
      Job job = new Job(sink, sql());
      RelOptUtil.eq(sink.table(), sink.rowType(), "pipeline", query.getRowType(), Litmus.THROW);
      DeploymentService.deployables(sink, Sink.class).forEach(x -> deployables.add(x));
      DeploymentService.deployables(job, Job.class).forEach(x -> deployables.add(x));
      return new Pipeline(deployables);
    }

    public Function<SqlDialect, String> sql() throws SQLException {
      ScriptImplementor script = ScriptImplementor.empty();
      List<Deployable> deployables = new ArrayList<>();
      for (Source source : sources) {
        Map<String, String> configs = ConnectionService.configure(source, Source.class);
        script = script.connector(source.table(), source.rowType(), configs);
      }
      RelDataType targetRowType = sinkRowType;
      if (targetRowType == null) {
        targetRowType = query.getRowType();
      }
      Sink sink = new Sink(sinkDatabase, sinkPath, targetRowType, sinkOptions);
      Map<String, String> sinkConfigs = ConnectionService.configure(sink, Sink.class);
      script = script.connector(sink.table(), sink.rowType(), sinkConfigs);
      script = script.insert(sink.table(), query);
      RelOptUtil.eq(sink.table(), sink.rowType(), "pipeline", query.getRowType(), Litmus.THROW);
      return script.seal();
    }
  }
}