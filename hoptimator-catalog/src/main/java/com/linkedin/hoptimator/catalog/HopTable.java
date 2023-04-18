package com.linkedin.hoptimator.catalog;

import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptCluster;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * HopTables can have "baggage", including Resources and arbitrary DDL/SQL.
 *
 * This mechanism is extremely powerful. In addition to enabling views, we can
 * bring along arbitrary infra required to materialize a view. For example, a
 * table can bring along a CDC stream or a cache. Generally, such Resources
 * won't physically exist until they are needed by a pipeline, at which point
 * Hoptimator will orchestrate their deployment.
 */ 
public class HopTable extends AbstractTable implements ScriptImplementor, TranslatableTable {
  private final String database;
  private final String name;
  private final RelDataType rowType;
  private final Collection<Resource> resources;
  private final ScriptImplementor implementor;

  public HopTable(String database, String name, RelDataType rowType, Collection<Resource> resources,
      ScriptImplementor implementor) {
    this.database = database;
    this.name = name;
    this.rowType = rowType;
    this.resources = resources;
    this.implementor = implementor;
  }

  /** Convenience constructor for HopTables that only need a connector config. */
  public HopTable(String database, String name, RelDataType rowType, Collection<Resource> resources,
      Map<String, String> connectorConfig) {
    this(database, name, rowType, resources,
      new ScriptImplementor.ConnectorImplementor(database, name, rowType, connectorConfig));
  }

  /** Convenience constructor for HopTables that only need a connector config. */
  public HopTable(String database, String name, RelDataType rowType,
      Map<String, String> connectorConfig) {
    this(database, name, rowType, Collections.emptyList(), connectorConfig);
  }

  public String name() {
    return name;
  }

  public String database() {
    return database;
  }

  public RelDataType rowType() {
    return rowType;
  }

  public Collection<Resource> resources() {
    return resources;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.copyType(rowType());
  }

  /** Writes DDL/SQL that implements the table, e.g. a view or connector.  */
  @Override
  public void implement(SqlWriter writer) {
    implementor.implement(writer);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    RelOptCluster cluster = context.getCluster();
    return new HopTableScan(cluster, cluster.traitSetOf(HopRel.CONVENTION), relOptTable);
  }

  /** Expresses the table as SQL/DDL in the default dialect. */
  @Override
  public String toString() {
    SqlWriter w = new SqlPrettyWriter();
    implement(w);
    return w.toSqlString().getSql();
  }
}
