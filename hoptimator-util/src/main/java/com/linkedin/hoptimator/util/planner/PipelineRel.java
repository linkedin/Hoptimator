package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.util.ConnectionService;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.Source;
import com.linkedin.hoptimator.util.Sink;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Litmus;

import java.util.Map;
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
    private final RelNode relNode;
    private ScriptImplementor script = ScriptImplementor.empty();
    private final Pipeline pipeline = new Pipeline();

    public Implementor(RelNode relNode) throws SQLException {
      this.relNode = relNode;
      visit(relNode);
    }

    public void visit(RelNode node) throws SQLException {
      for (RelNode input : node.getInputs()) {
        visit(input);
      }
      ((PipelineRel) node).implement(this);
    }

    public RelDataType rowType() {
      return relNode.getRowType(); 
    }

    /**
     * Adds a Source/Sink to the pipeline.
     * 
     * This involves deploying any relevant objects, and configuring a
     * a connector. The connector is configured via `CREATE TABLE...WITH(...)`.
     */
    public <T extends Source> void implement(T source, Class<T> clazz) throws SQLException {
      DeploymentService.deployables(source, clazz).forEach(x -> pipeline.add(x));
      Map<String, String> configs = ConnectionService.configure(source, clazz);
      script = script.connector(source.table(), source.rowType(), configs);
    }

    /** Script ending in INSERT INTO ... */
    public ScriptImplementor insertInto(Sink sink) throws SQLException {
      RelOptUtil.eq(sink.table(), sink.rowType(), "pipeline", rowType(), Litmus.THROW);
      Map<String, String> configs = ConnectionService.configure(sink, Sink.class);
      return script.connector(sink.table(), sink.rowType(), configs)
          .insert(sink.table(), relNode);
    }

    /** Combine SQL and any Deployables into a Pipeline */
    public Pipeline pipeline() {
      return pipeline;
    }
  }
}
