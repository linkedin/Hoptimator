package com.linkedin.hoptimator.util.planner;

import java.util.List;
import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.SqlDialectFactoryImpl;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.Engine;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcConvention;


public class HoptimatorJdbcSchema extends JdbcSchema implements Database {

  private final String database;
  private final List<Engine> engines;
  private final HoptimatorJdbcConvention convention;

  public static HoptimatorJdbcSchema create(String database, String schema, DataSource dataSource,
      SchemaPlus parentSchema, SqlDialect dialect, List<Engine> engines) {
    Expression expression = Schemas.subSchemaExpression(parentSchema, schema, HoptimatorJdbcSchema.class);
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(dialect, expression, database, engines);
    return new HoptimatorJdbcSchema(database, schema, dataSource, dialect, convention, engines);
  }

  public HoptimatorJdbcSchema(String database, String schema, DataSource dataSource, SqlDialect dialect,
      HoptimatorJdbcConvention convention, List<Engine> engines) {
    super(dataSource, dialect, convention, null, schema);
    this.database = database;
    this.engines = engines;
    this.convention = convention;
  }

  @Override
  public String databaseName() {
    return database;
  }

  public List<Engine> engines() {
    return engines;
  }

  @Override
  public Table getTable(String name) {
    return new HoptimatorJdbcTable((JdbcTable) super.getTable(name), convention);
  }

  @Override
  public Schema snapshot(SchemaVersion version) {
    return this;
  }
}
