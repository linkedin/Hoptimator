package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.Engine;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.IgnoreCaseLookup;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.LoadingCacheLookup;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.util.LazyReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;


public class HoptimatorJdbcSchema extends JdbcSchema implements Database {

  private static final Logger LOG = LoggerFactory.getLogger(HoptimatorJdbcSchema.class);

  private final String database;
  private final String catalog;
  private final String schema;
  private final List<Engine> engines;
  private final HoptimatorJdbcConvention convention;
  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();
  private volatile Boolean cachedLogical;

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
    this.catalog = catalog;
    this.schema = schema;
    this.engines = engines;
    this.convention = convention;
  }

  /**
   * Returns true when the downstream JDBC adapter surfaces a {@link LogicalSchemaMarker}-tagged
   * schema at the configured catalog/schema path. Walks the downstream on first call and caches
   * the result; the cost is one JDBC connection open per {@code HoptimatorJdbcSchema} lifetime.
   * Drivers that surface logical tables participate by having their inner Calcite schema implement
   * the marker.
   *
   * <p>A transient JDBC failure (e.g. downstream temporarily unreachable) is <i>not</i> cached —
   * subsequent calls retry — so a flaky moment can't poison the resolver's view of the database
   * for the schema's lifetime. Only a definitive determination ("the marker is/isn't present")
   * is memoized.
   */
  public boolean isLogical() {
    Boolean cached = cachedLogical;
    if (cached != null) {
      return cached;
    }
    try {
      boolean determined = detectLogical();
      cachedLogical = determined;
      return determined;
    } catch (SQLException e) {
      // Cache deliberately left unset — the next caller retries.
      LOG.warn("Transient failure determining isLogical for database {}; treating as non-logical "
          + "for this call and will retry on next access", database, e);
      return false;
    }
  }

  /**
   * @throws SQLException when the downstream connection can't be acquired; the caller
   *     translates that into a non-cached "not logical" outcome so a transient blip doesn't
   *     poison the resolver. A driver that simply doesn't implement {@link CalciteConnection} is
   *     a permanent characteristic and is cached as non-logical instead of being treated as a
   *     failure.
   */
  private Boolean detectLogical() throws SQLException {
    try (Connection downstream = getDataSource().getConnection()) {
      CalciteConnection cc;
      try {
        cc = downstream.unwrap(CalciteConnection.class);
      } catch (SQLException e) {
        // Permanent: this driver doesn't surface a CalciteConnection at all. Cache as non-logical
        // so we don't keep re-asking.
        return false;
      }
      SchemaPlus root = cc.getRootSchema();
      if (root == null) {
        return false;
      }
      SchemaPlus sub = root;
      if (catalog != null) {
        sub = sub.subSchemas().get(catalog);
      }
      if (sub != null && schema != null) {
        sub = sub.subSchemas().get(schema);
      }
      if (sub == null) {
        return false;
      }
      // SchemaPlus.unwrap throws ClassCastException for non-matching types — that's the dominant
      // "looked and isn't logical" outcome, not a crash.
      try {
        return sub.unwrap(LogicalSchemaMarker.class) != null;
      } catch (ClassCastException e) {
        return false;
      }
    }
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
    return tables.getOrCompute(() -> new LoadingCacheLookup<>(new IgnoreCaseLookup<>() {
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
    }));
  }

  @Override
  public Schema snapshot(SchemaVersion version) {
    return this;
  }
}
