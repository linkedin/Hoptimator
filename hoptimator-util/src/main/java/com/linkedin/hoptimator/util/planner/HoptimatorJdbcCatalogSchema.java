package com.linkedin.hoptimator.util.planner;

import com.google.common.collect.ImmutableSet;
import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.Engine;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.calcite.adapter.jdbc.JdbcCatalogSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.lookup.IgnoreCaseLookup;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.LoadingCacheLookup;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.sql.SqlDialect;

import static java.util.Objects.requireNonNull;


public class HoptimatorJdbcCatalogSchema extends JdbcCatalogSchema implements Database {

  private final String catalog;
  private final String database;
  private final List<Engine> engines;
  private final Lookup<JdbcSchema> subSchemas;

  public static HoptimatorJdbcCatalogSchema create(String database, String catalog, String schema, DataSource dataSource,
      SchemaPlus parentSchema, SqlDialect dialect, List<Engine> engines, Connection connection) {
    Expression expression = Schemas.subSchemaExpression(parentSchema, schema, HoptimatorJdbcCatalogSchema.class);
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(dialect, expression, database, engines, connection);
    return new HoptimatorJdbcCatalogSchema(database, catalog, dataSource, dialect, convention, engines);
  }

  public HoptimatorJdbcCatalogSchema(String database, String catalog, DataSource dataSource, SqlDialect dialect,
      HoptimatorJdbcConvention convention, List<Engine> engines) {
    super(dataSource, dialect, convention, catalog);
    this.database = database;
    this.catalog = catalog;
    this.engines = engines;
    this.subSchemas = new LoadingCacheLookup<>(new IgnoreCaseLookup<>() {
      @Override public @Nullable JdbcSchema get(String name) {
        try (Connection connection = dataSource.getConnection();
            ResultSet resultSet =
                connection.getMetaData().getSchemas(catalog, name)) {
          // There can only be one schema with a given name
          while (resultSet.next()) {
            final String schemaName =
                requireNonNull(resultSet.getString(1),
                    "got null schemaName from the database");
            return new HoptimatorJdbcSchema(database, catalog, schemaName, dataSource, dialect, convention, engines);
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return null;
      }

      @Override public Set<String> getNames(LikePattern pattern) {
        final ImmutableSet.Builder<String> builder =
            ImmutableSet.builder();
        try (Connection connection = dataSource.getConnection();
            ResultSet resultSet =
                connection.getMetaData().getSchemas(catalog, pattern.pattern)) {
          while (resultSet.next()) {
            builder.add(
                requireNonNull(resultSet.getString(1),
                    "got null schemaName from the database"));
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return builder.build();
      }
    });
  }

  @Override
  public String databaseName() {
    return database;
  }

  public String catalog() {
    return catalog;
  }

  public List<Engine> engines() {
    return engines;
  }

  @Override
  public Schema snapshot(SchemaVersion version) {
    return this;
  }

  @Override public Lookup<? extends Schema> subSchemas() {
    return subSchemas;
  }
}
