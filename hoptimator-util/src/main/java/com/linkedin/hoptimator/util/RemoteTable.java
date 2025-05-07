package com.linkedin.hoptimator.util;

import java.util.Collection;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;


/** A table behind some CRUD API */
public abstract class RemoteTable<OBJECT_TYPE, ROW_TYPE> extends AbstractTable
    implements TranslatableTable, ModifiableTable, RowMapper<OBJECT_TYPE, ROW_TYPE> {

  private final Api<OBJECT_TYPE> api;
  private final Class<ROW_TYPE> elementType;
  private final RelDataType javaType;
  private final RemoteRowList<OBJECT_TYPE, ROW_TYPE> rows;

  RemoteTable(Api<OBJECT_TYPE> api, Class<ROW_TYPE> elementType, RelDataType javaType) {
    this.api = api;
    this.elementType = elementType;
    this.javaType = javaType;
    this.rows = new RemoteRowList<>(api, this);
  }

  RemoteTable(Api<OBJECT_TYPE> api, Class<ROW_TYPE> elementType, JavaTypeFactory javaTypeFactory) {
    this(api, elementType, javaTypeFactory.createType(elementType));
  }

  public RemoteTable(Api<OBJECT_TYPE> api, Class<ROW_TYPE> elementType) {
    this(api, elementType, new JavaTypeFactoryImpl());
  }

  public Collection<ROW_TYPE> rows() {
    return rows;
  }

  public Api<OBJECT_TYPE> api() {
    return api;
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
  public Collection<ROW_TYPE> getModifiableCollection() {
    return rows;
  }

  @Override
  public Expression getExpression(SchemaPlus parentSchema, String name, Class clazz) {
    return Schemas.tableExpression(parentSchema, getElementType(), name, clazz);
  }

  @Override
  public Class getElementType() {
    return elementType;
  }

  @Override
  public OBJECT_TYPE fromRow(ROW_TYPE u) {
    throw new UnsupportedOperationException("This object is not writable.");
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
}
