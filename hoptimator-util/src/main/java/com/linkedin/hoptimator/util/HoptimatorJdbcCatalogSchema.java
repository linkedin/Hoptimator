package com.linkedin.hoptimator.util;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcCatalogSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.SqlDialectFactoryImpl;

import com.linkedin.hoptimator.util.planner.HoptimatorJdbcConvention;


public class HoptimatorJdbcCatalogSchema extends JdbcCatalogSchema {

  public HoptimatorJdbcCatalogSchema(String name, String database, DataSource dataSource, SqlDialect dialect,
      Expression expression) {
    super(dataSource, dialect, new HoptimatorJdbcConvention(dialect, expression, name), name);
  }

  public HoptimatorJdbcCatalogSchema(String name, String database, DataSource dataSource, SchemaPlus parentSchema,
      SqlDialect dialect) {
    this(name, database, dataSource, dialect,
        Schemas.subSchemaExpression(parentSchema, name, HoptimatorJdbcCatalogSchema.class));
  }

  public HoptimatorJdbcCatalogSchema(String name, String database, DataSource dataSource, SchemaPlus parentSchema,
      SqlDialectFactory dialectFactory) {
    this(name, database, dataSource, parentSchema, JdbcSchema.createDialect(dialectFactory, dataSource));
  }

  public HoptimatorJdbcCatalogSchema(String name, String database, DataSource dataSource, SchemaPlus parentSchema) {
    this(name, database, dataSource, parentSchema, SqlDialectFactoryImpl.INSTANCE);
  }
}
