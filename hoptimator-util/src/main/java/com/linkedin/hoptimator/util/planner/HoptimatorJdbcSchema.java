package com.linkedin.hoptimator.util.planner;

import java.sql.Connection;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.IgnoreCaseLookup;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.sql.SqlDialect;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.Engine;
import org.apache.calcite.util.LazyReference;


public class HoptimatorJdbcSchema extends JdbcSchema implements Database {

  private final String database;
  private final List<Engine> engines;
  private final HoptimatorJdbcConvention convention;
  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();

  public static HoptimatorJdbcSchema create(String database, String catalog, String schema, DataSource dataSource,
      SchemaPlus parentSchema, SqlDialect dialect, List<Engine> engines, Connection connection) {
    Expression expression = Schemas.subSchemaExpression(parentSchema, schema, HoptimatorJdbcSchema.class);
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(dialect, expression, database, engines, connection);
    return new HoptimatorJdbcSchema(database, catalog, schema, dataSource, dialect, convention, engines);
  }

  public HoptimatorJdbcSchema(String database, String catalog, String schema, DataSource dataSource, SqlDialect dialect,
      HoptimatorJdbcConvention convention, List<Engine> engines) {
    super(dataSource, dialect, convention, catalog, schema);
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

  @Override public Lookup<Table> tables() {
    Lookup<Table> jdbcTableLookup = super.tables();
    return tables.getOrCompute(() -> new IgnoreCaseLookup<>() {
      @Override
      public @Nullable Table get(String name) {
        Table jdbcTable = jdbcTableLookup.get(name);
        if (jdbcTable == null) {
          return null;
        }
        return new HoptimatorJdbcTable((JdbcTable) jdbcTable, convention);
      }

      @Override
      public Set<String> getNames(LikePattern pattern) {
        return jdbcTableLookup.getNames(pattern);
      }
    });
  }

  @Override
  public Schema snapshot(SchemaVersion version) {
    return this;
  }
}
