package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcCatalogSchema;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcSchema;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

import javax.sql.DataSource;
import java.util.Objects;
import java.sql.Connection;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.StringJoiner;


public class K8sDatabaseTable extends K8sTable<V1alpha1Database, V1alpha1DatabaseList, K8sDatabaseTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public String NAME;
    public String URL;
    public String CATALOG;
    public String SCHEMA;
    public String DIALECT;
    public String DRIVER;

    public Row(String name, String url, String catalog, String schema, String dialect, String driver) {
      this.NAME = name;
      this.URL = url;
      this.CATALOG = catalog;
      this.SCHEMA = schema;
      this.DIALECT = dialect;
      this.DRIVER = driver;
    }
  }
  // CHECKSTYLE:ON

  private final K8sEngineTable engines;

  public K8sDatabaseTable(K8sContext context, K8sEngineTable engines) {
    super(context, K8sApiEndpoints.DATABASES, Row.class);
    this.engines = engines;
  }

  public void addDatabases(SchemaPlus parentSchema, Connection connection) {
    DeploymentContext context = ((HoptimatorConnection) connection).deploymentContext();
    for (Row row : rows()) {
      if (row.CATALOG != null) {
        Schema catalogSchema = HoptimatorJdbcCatalogSchema.create(row.NAME, row.CATALOG, row.SCHEMA, dataSource(row,
                ((HoptimatorConnection) connection).connectionProperties()), parentSchema,
            dialect(row), engines.forDatabase(row.NAME), context);
        parentSchema.add(row.CATALOG.toUpperCase(Locale.ROOT), catalogSchema);
      } else {
        Schema schema = HoptimatorJdbcSchema.create(row.NAME, row.CATALOG, row.SCHEMA, dataSource(row,
                ((HoptimatorConnection) connection).connectionProperties()), parentSchema,
            dialect(row), engines.forDatabase(row.NAME), context);
        parentSchema.add(schemaName(row), schema);
      }
    }
  }

  @Override
  public Row toRow(V1alpha1Database obj) {
    return rowOf(obj);
  }

  /** Builds a {@link Row} from a Database custom resource, usable without a {@link K8sDatabaseTable} instance. */
  static Row rowOf(V1alpha1Database obj) {
    return new Row(Objects.requireNonNull(obj.getMetadata()).getName(), Objects.requireNonNull(obj.getSpec()).getUrl(),
        obj.getSpec().getCatalog(), obj.getSpec().getSchema(),
        Optional.ofNullable(obj.getSpec().getDialect()).map(V1alpha1DatabaseSpec.DialectEnum::toString).orElse(null),
        obj.getSpec().getDriver());
  }

  @Override
  public V1alpha1Database fromRow(Row row) {
    K8sUtils.checkK8sName(row.NAME);
    return new V1alpha1Database().kind(K8sApiEndpoints.DATABASES.kind())
        .apiVersion(K8sApiEndpoints.DATABASES.apiVersion())
        .metadata(new V1ObjectMeta().name(row.NAME))
        .spec(new V1alpha1DatabaseSpec().url(row.URL)
            .catalog(row.CATALOG)
            .schema(row.SCHEMA)
            .driver(row.DRIVER)
            .dialect(V1alpha1DatabaseSpec.DialectEnum.fromValue(row.DIALECT)));
  }

  static String schemaName(Row row) {
    if (row.SCHEMA != null && !row.SCHEMA.isEmpty()) {
      return row.SCHEMA;
    } else {
      return row.NAME.toUpperCase(Locale.ROOT);
    }
  }

  static DataSource dataSource(Row row, Properties connectionProperties) {
    String user = "nouser";
    String pass = "nopass";
    for (String key : connectionProperties.stringPropertyNames()) {
      if ("user".equals(key)) {
        user = connectionProperties.getProperty(key);
      } else if ("password".equals(key)) {
        pass = connectionProperties.getProperty(key);
      }
    }
    return JdbcSchema.dataSource(joinedUrl(row, connectionProperties), row.DRIVER, user, pass);
  }

  /**
   * Builds the effective JDBC URL for a Database: its custom resource {@code url} with the connection-level
   * properties (except {@code user}/{@code password}) and the custom resource name appended as
   * {@code database=<name>}. This is the URL a {@code DatabaseConfigResolver} parses to recover a
   * database's connection properties, kept here so it stays in lockstep with {@link #dataSource}.
   */
  static String joinedUrl(Row row, Properties connectionProperties) {
    StringJoiner joiner = new StringJoiner(";");
    for (String key : connectionProperties.stringPropertyNames()) {
      if (!"user".equals(key) && !"password".equals(key)) {
        joiner.add(key + "=" + connectionProperties.getProperty(key));
      }
    }
    // Inject the Database custom resource name so drivers can identify which custom resource they are backing.
    // This is the value returned by source.database() in deployer/provider contexts.
    if (row.NAME != null && !row.NAME.isEmpty()) {
      joiner.add("database=" + row.NAME);
    }
    // Handles case where there are no properties already in the URL
    if (row.URL.endsWith("//")) {
      return row.URL + joiner;
    }
    return row.URL + ";" + joiner;
  }

  static SqlDialect dialect(Row row) {
    if (row.DIALECT == null) {
      return null;
    }
    switch (row.DIALECT) {
      case "ANSI":
        return AnsiSqlDialect.DEFAULT;
      case "MySQL":
        return MysqlSqlDialect.DEFAULT;
      default:
        return CalciteSqlDialect.DEFAULT;
    }
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.SYSTEM_TABLE;
  }
}
