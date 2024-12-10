package com.linkedin.hoptimator.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;


/** An in-memory table. */
public abstract class ArrayTable<T> extends AbstractTable
    implements TranslatableTable, ModifiableTable, ScannableTable {

  private final Class<T> elementType;
  private final RelDataType javaType;
  private final List<T> rows = new ArrayList<>();

  public ArrayTable(Class<T> elementType, RelDataType javaType) {
    this.elementType = elementType;
    this.javaType = javaType;
  }

  public ArrayTable(Class<T> elementType, JavaTypeFactory javaTypeFactory) {
    this(elementType, javaTypeFactory.createType(elementType));
  }

  public ArrayTable(Class<T> elementType) {
    this(elementType, new JavaTypeFactoryImpl());
  }

  public Collection<T> rows() {
    return rows;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.copyType(javaType);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return EnumerableTableScan.create(context.getCluster(), relOptTable);
  }

  @Override
  public Collection<T> getModifiableCollection() {
    return rows;
  }

  @Override
  public Expression getExpression(SchemaPlus parentSchema, String name, Class clazz) {
    return Schemas.tableExpression(parentSchema, getElementType(), name, clazz);
  }

  @Override
  public Class<T> getElementType() {
    return elementType;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> Queryable<V> asQueryable(QueryProvider provider, SchemaPlus schema, String tableName) {
    return (Queryable<V>) Linq4j.asEnumerable(rows).asQueryable();
  }

  @Override
  public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader schema,
      RelNode input, TableModify.Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList,
      boolean flattened) {
    RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalTableModify(cluster, traitSet, table, schema, input, operation, updateColumnList,
        sourceExpressionList, flattened);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    return (Enumerable<Object[]>) Linq4j.asEnumerable(rows);
  }
}
