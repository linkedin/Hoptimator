package com.linkedin.hoptimator.planner;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.util.Litmus;

import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.catalog.ResourceProvider;
import com.linkedin.hoptimator.catalog.HopTable;
import com.linkedin.hoptimator.catalog.ScriptImplementor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Calling convention which implements an SQL-based streaming data pipeline.
 *
 * Pipelines tend to have the following general shape:
 * <pre>
 *                                   _________
 *   topic1 ----------------------> |         |
 *   table2 --> CDC ---> topic2 --> | SQL job | --> topic4
 *   table3 --> rETL --> topic3 --> |_________|
 *
 * </pre>
 * The SQL job may consume multiple sources but writes to a single sink. The
 * CDC and rETL "hops" are made possible by Resources, which describe any
 * additional infra required by the Pipeline. As Resources are essentially
 * YAML, anything can be represented there, including additional SQL jobs.
 *
 */
public interface PipelineRel extends RelNode {

  Convention CONVENTION = new Convention.Impl("PIPELINE", PipelineRel.class);

  void implement(Implementor implementor);

  /** Implements a Pipeline using SQL.
   */
  class Implementor {
    private final RelNode relNode;
    private final List<Resource> resources = new ArrayList<>();
    private ScriptImplementor script = ScriptImplementor.empty().database("PIPELINE");

    public Implementor(RelNode relNode) {
      this.relNode = relNode;
      visit(relNode);
    }

    public RelDataType rowType() {
      return relNode.getRowType();
    }

    public RelProtoDataType rowProtoType() {
      return RelDataTypeImpl.proto(rowType());
    }

    /** Depend on an arbitrary Resource, e.g. a Kafka topic */
    public void resource(Resource resource) {
      this.resources.add(resource);
    }

    private void visit(RelNode input) {
      input.getInputs().forEach(x -> visit(x));
      ((PipelineRel) input).implement(this);
    }

    /** Script ending in SELECT... */
    public ScriptImplementor query() {
      return script.query(relNode);
    }

    /** Script ending in INSERT INTO ... */
    public ScriptImplementor insertInto(HopTable sink) {
      RelOptUtil.eq(sink.name(), sink.rowType(), "subscription", rowType(), Litmus.THROW);
      RelNode castRel = RelOptUtil.createCastRel(relNode, sink.rowType(), true);
      return script.database(sink.database()).with(sink)
        .insert(sink.database(), sink.name(), castRel);
    }

    /** Add any resources, SQL, DDL etc required to access the table. */
    public void implement(HopTable table) {
      script = script.database(table.database()).with(table);
      table.readResources().forEach(x -> resource(x));
    }

    /** Combine SQL and any Resources into a Pipeline, using ANSI dialect */
    public Pipeline pipeline(HopTable sink) {
      return pipeline(sink, AnsiSqlDialect.DEFAULT);
    }

    /** Combine SQL and any Resources into a Pipeline */
    public Pipeline pipeline(HopTable sink, SqlDialect sqlDialect) {
      SqlJob sqlJob = new SqlJob(insertInto(sink).sql(sqlDialect));
      return new Pipeline(resources, sqlJob, sink.writeResources(), rowType());
    }
  }
}
