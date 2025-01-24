package com.linkedin.hoptimator.util.planner;

import java.util.Collection;
import java.util.List;

import com.linkedin.hoptimator.util.DataTypeUtils;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;


public class HoptimatorJdbcTable extends AbstractQueryableTable implements TranslatableTable,
    ModifiableTable {

  private final JdbcTable jdbcTable;
  private final HoptimatorJdbcConvention convention;

  public HoptimatorJdbcTable(JdbcTable jdbcTable, HoptimatorJdbcConvention convention) {
    super(Object[].class);
    this.jdbcTable = jdbcTable;
    this.convention = convention;
  }

  public JdbcTable jdbcTable() {
    return jdbcTable;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory factory) {
    return DataTypeUtils.unflatten(jdbcTable.getRowType(factory), factory);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return new HoptimatorJdbcTableScan(context.getCluster(), context.getTableHints(), relOptTable, this,
        convention);
  }

  @Override
  public TableModify toModificationRel(RelOptCluster cluster,
      RelOptTable table, CatalogReader catalogReader, RelNode input,
      TableModify.Operation operation, List<String> updateColumnList,
      List<RexNode> sourceExpressionList, boolean flattened) {
    return jdbcTable.toModificationRel(cluster, table, catalogReader, input, operation,
        updateColumnList, sourceExpressionList, flattened);
  }

  @Override
  public Collection getModifiableCollection() {
    return null;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return jdbcTable.asQueryable(queryProvider, schema, tableName);
  }
}
