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
 */
public interface PipelineRel extends RelNode {

  Convention CONVENTION = new Convention.Impl("PIPELINE", PipelineRel.class);

  void implement(Implementor implementor) throws SQLException;

  /** Implements a deployable Pipeline.
   */
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

    public void implement(Source source) throws SQLException {
      DeploymentService.deployables(source, Source.class).forEach(x -> pipeline.add(x));
      Map<String, String> configs = ConnectionService.configure(source, Source.class);
      script = script.connector(source.table(), source.rowType(), configs);
    }

    /** Script ending in INSERT INTO ... */
    public ScriptImplementor insertInto(Sink sink) throws SQLException {
      RelOptUtil.eq(sink.table(), sink.rowType(), "pipeline", rowType(), Litmus.THROW);
      Map<String, String> configs = ConnectionService.configure(sink, Sink.class);
      return script.connector(sink.table(), sink.rowType(), configs)
          .insert(sink.table(), relNode);
    }

    /** Combine SQL and any Resources into a Pipeline */
    public Pipeline pipeline() {
      return pipeline;
    }
  }
}
