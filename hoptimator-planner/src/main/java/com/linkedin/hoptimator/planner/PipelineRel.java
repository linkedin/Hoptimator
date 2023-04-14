package com.linkedin.hoptimator.planner;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.catalog.Resources;
import com.linkedin.hoptimator.catalog.HopTable;
import com.linkedin.hoptimator.catalog.ScriptImplementor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Calling convention which implements an SQL-based streaming data pipeline.
 *
 * Pipelines tend to have the following general shape:
 *                                   _________
 *   topic1 ----------------------> |         |
 *   table2 --> CDC ---> topic2 --> | SQL job | --> topic4
 *   table3 --> rETL --> topic3 --> |_________|
 *
 * The SQL job may consume multiple sources but writes to a single sink. The
 * CDC and rETL "hops" are made possible by Resources, which describe any
 * additional infra required by the Pipeline. As Resources are essentially
 * YAML, anything can be represented there, including additional SQL jobs.
 *
 */
public interface PipelineRel extends RelNode {

  Convention CONVENTION = new Convention.Impl("PIPELINE", PipelineRel.class);
  SqlDialect OUTPUT_DIALECT = MysqlSqlDialect.DEFAULT; // closely resembles Flink SQL
  // TODO support alternative output dialects

  void implement(Implementor implementor);

  /** Implements a Pipeline using SQL.
   */
  class Implementor {
    private final RelNode relNode;
    private final Set<Resource> resources = new HashSet<>();
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
    public String query() {
      return script.query(relNode).sql(OUTPUT_DIALECT);
    }

    /** Script ending in INSERT INTO ... */
    public String sink(Map<String, String> connectorConfig) {
      return script.connector("PIPELINE", "SINK", rowType(), connectorConfig)
        .insert("PIPELINE", "SINK", relNode).sql(OUTPUT_DIALECT);
    }

    /** Add any resources, SQL, DDL etc required to access the table. */
    void implement(HopTable table) {
      script = script.database(table.database()).with(table);
      resources.addAll(resources);
    }

    /** Combine SQL and any Resources into a Pipeline */
    public Pipeline pipeline(Map<String, String> connectorConfig) {
      Set<Resource> resourcesAndJob = new HashSet<>();
      resourcesAndJob.addAll(resources);
      resourcesAndJob.add(new Resources.SqlJob(sink(connectorConfig)));
      return new Pipeline(resourcesAndJob, rowType());
    }
  }
}
