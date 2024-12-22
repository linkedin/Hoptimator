package com.linkedin.hoptimator.util;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.SqlDialectFactoryImpl;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcConvention;


public class HoptimatorJdbcSchema extends JdbcSchema implements Database {

  private final String database;

  public static HoptimatorJdbcSchema create(String database, String schema, DataSource dataSource,
      SchemaPlus parentSchema, SqlDialect dialect) {
    if (dialect == null) {
      return new HoptimatorJdbcSchema(database, schema, dataSource, parentSchema);
    } else {
      return new HoptimatorJdbcSchema(database, schema, dataSource, parentSchema, dialect);
    }
  }

  public HoptimatorJdbcSchema(String database, String schema, DataSource dataSource, SqlDialect dialect,
      Expression expression) {
    super(dataSource, dialect, new HoptimatorJdbcConvention(dialect, expression, database),
        null, schema);
    this.database = database;
  }

  public HoptimatorJdbcSchema(String database, String schema, DataSource dataSource,
      SchemaPlus parentSchema, SqlDialect dialect) {
    this(database, schema, dataSource, dialect,
        Schemas.subSchemaExpression(parentSchema, schema, HoptimatorJdbcSchema.class));
  }

  public HoptimatorJdbcSchema(String database, String schema, DataSource dataSource,
      SchemaPlus parentSchema, SqlDialectFactory dialectFactory) {
    this(database, schema, dataSource, parentSchema, createDialect(dialectFactory, dataSource));
  }

  public HoptimatorJdbcSchema(String database, String schema, DataSource dataSource,
      SchemaPlus parentSchema) {
    this(database, schema, dataSource, parentSchema, SqlDialectFactoryImpl.INSTANCE);
  }

  @Override
  public String databaseName() {
    return database;
  }
}
