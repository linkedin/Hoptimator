package com.linkedin.hoptimator.util;

import java.util.List;
import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.SqlDialectFactoryImpl;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.Engine;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcConvention;


public class HoptimatorJdbcSchema extends JdbcSchema implements Database {

  private final String database;
  private final List<Engine> engines;

  public static HoptimatorJdbcSchema create(String database, String schema, DataSource dataSource,
      SchemaPlus parentSchema, SqlDialect dialect, List<Engine> engines) {
    if (dialect == null) {
      return new HoptimatorJdbcSchema(database, schema, dataSource, parentSchema, engines);
    } else {
      return new HoptimatorJdbcSchema(database, schema, dataSource, parentSchema, dialect, engines);
    }
  }

  public HoptimatorJdbcSchema(String database, String schema, DataSource dataSource, SqlDialect dialect,
      Expression expression, List<Engine> engines) {
    super(dataSource, dialect, new HoptimatorJdbcConvention(dialect, expression, database, engines),
        null, schema);
    this.database = database;
    this.engines = engines;
  }

  public HoptimatorJdbcSchema(String database, String schema, DataSource dataSource,
      SchemaPlus parentSchema, SqlDialect dialect, List<Engine> engines) {
    this(database, schema, dataSource, dialect,
        Schemas.subSchemaExpression(parentSchema, schema, HoptimatorJdbcSchema.class), engines);
  }

  public HoptimatorJdbcSchema(String database, String schema, DataSource dataSource,
      SchemaPlus parentSchema, SqlDialectFactory dialectFactory, List<Engine> engines) {
    this(database, schema, dataSource, parentSchema, createDialect(dialectFactory, dataSource), engines);
  }

  public HoptimatorJdbcSchema(String database, String schema, DataSource dataSource,
      SchemaPlus parentSchema, List<Engine> engines) {
    this(database, schema, dataSource, parentSchema, SqlDialectFactoryImpl.INSTANCE, engines);
  }

  @Override
  public String databaseName() {
    return database;
  }

  public List<Engine> engines() {
    return engines;
  }
}
