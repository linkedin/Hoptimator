package com.linkedin.hoptimator.util;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.SqlDialectFactoryImpl;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcConvention;


public class HoptimatorJdbcSchema extends JdbcSchema implements Database {

  private final String database;

  public static HoptimatorJdbcSchema create(String database, String catalog, String schema, DataSource dataSource,
      SchemaPlus parentSchema, SqlDialect dialect) {
    if (dialect == null) {
      return new HoptimatorJdbcSchema(database, catalog, schema, dataSource, parentSchema);
    } else {
      return new HoptimatorJdbcSchema(database, catalog, schema, dataSource, parentSchema, dialect);
    }
  }

  public HoptimatorJdbcSchema(String database, String catalog, String schema, DataSource dataSource, SqlDialect dialect,
      Expression expression) {
    super(dataSource, dialect, new HoptimatorJdbcConvention(dialect, expression, database), catalog, schema);
    this.database = database;
  }

  public HoptimatorJdbcSchema(String database, String catalog, String schema, DataSource dataSource,
      SchemaPlus parentSchema, SqlDialect dialect) {
    this(database, catalog, schema, dataSource, dialect,
        Schemas.subSchemaExpression(parentSchema, schema, HoptimatorJdbcSchema.class));
  }

  public HoptimatorJdbcSchema(String database, String catalog, String schema, DataSource dataSource,
      SchemaPlus parentSchema, SqlDialectFactory dialectFactory) {
    this(database, catalog, schema, dataSource, parentSchema, createDialect(dialectFactory, dataSource));
  }

  public HoptimatorJdbcSchema(String database, String catalog, String schema, DataSource dataSource,
      SchemaPlus parentSchema) {
    this(database, catalog, schema, dataSource, parentSchema, SqlDialectFactoryImpl.INSTANCE);
  }

  @Override
  public String databaseName() {
    return database;
  }
}
