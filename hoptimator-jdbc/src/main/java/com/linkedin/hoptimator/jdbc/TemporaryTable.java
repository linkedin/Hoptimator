package com.linkedin.hoptimator.jdbc;

import javax.annotation.Nullable;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;

import com.linkedin.hoptimator.util.ArrayTable;


/**
 * A temporary, in-memory table that provides row type information to deployers before
 * the actual table exists in the underlying database.
 *
 * <p>During {@code CREATE TABLE} DDL execution, {@link HoptimatorDdlExecutor} registers a
 * {@code TemporaryTable} in the target schema so that deployers (e.g. VeniceDeployer) can
 * call {@link HoptimatorDriver#rowType} and find the table before it is physically created.
 *
 * <p>Extends {@link ArrayTable} (which implements {@code ModifiableTable} and
 * {@code ScannableTable}) so that the table can be queried and written to in-memory
 * during DDL processing (e.g. {@code INSERT INTO T VALUES ...} after {@code CREATE TABLE T}).
 *
 * <p>Supports {@link InitializerExpressionFactory} via {@link #unwrap} so that Calcite's
 * DDL machinery can process column default value expressions.
 */
public class TemporaryTable extends ArrayTable<Object[]> {

  private final String databaseName;
  @Nullable
  private final InitializerExpressionFactory initializerExpressionFactory;

  public TemporaryTable(RelDataType rowType, String databaseName) {
    this(rowType, databaseName, NullInitializerExpressionFactory.INSTANCE);
  }

  public TemporaryTable(RelDataType rowType, String databaseName,
      InitializerExpressionFactory initializerExpressionFactory) {
    super(Object[].class, rowType);
    this.databaseName = databaseName;
    this.initializerExpressionFactory = initializerExpressionFactory;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return super.getRowType(typeFactory);
  }

  /** Returns the database name this table belongs to, or {@code null} if not set. */
  @Nullable
  public String databaseName() {
    return databaseName;
  }

  @Override
  public <C> C unwrap(Class<C> aClass) {
    if (initializerExpressionFactory != null && aClass.isInstance(initializerExpressionFactory)) {
      return aClass.cast(initializerExpressionFactory);
    }
    return super.unwrap(aClass);
  }
}
