package com.linkedin.hoptimator.k8s;

import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.StringJoiner;
import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcSchema;


public class K8sDatabaseTable extends K8sTable<V1alpha1Database, V1alpha1DatabaseList, K8sDatabaseTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public String NAME;
    public String URL;
    public String SCHEMA;
    public String DIALECT;
    public String DRIVER;

    public Row(String name, String url, String schema, String dialect, String driver) {
      this.NAME = name;
      this.URL = url;
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

  public void addDatabases(SchemaPlus parentSchema, Properties connectionProperties) {
    for (Row row : rows()) {
      parentSchema.add(schemaName(row),
          HoptimatorJdbcSchema.create(row.NAME, row.SCHEMA, dataSource(row, connectionProperties), parentSchema,
              dialect(row), engines.forDatabase(row.NAME), connectionProperties));
    }
  }

  @Override
  public Row toRow(V1alpha1Database obj) {
    return new Row(obj.getMetadata().getName(), obj.getSpec().getUrl(), obj.getSpec().getSchema(),
        Optional.ofNullable(obj.getSpec().getDialect()).map(x -> x.toString()).orElseGet(() -> null),
        obj.getSpec().getDriver());
  }

  @Override
  public V1alpha1Database fromRow(Row row) {
    K8sUtils.checkK8sName(row.NAME);
    return new V1alpha1Database().kind(K8sApiEndpoints.DATABASES.kind())
        .apiVersion(K8sApiEndpoints.DATABASES.apiVersion())
        .metadata(new V1ObjectMeta().name(row.NAME))
        .spec(new V1alpha1DatabaseSpec().url(row.URL)
            .schema(row.SCHEMA)
            .driver(row.DRIVER)
            .dialect(V1alpha1DatabaseSpec.DialectEnum.fromValue(row.DIALECT)));
  }

  private static String schemaName(Row row) {
    if (row.SCHEMA != null && !row.SCHEMA.isEmpty()) {
      return row.SCHEMA;
    } else {
      return row.NAME.toUpperCase(Locale.ROOT);
    }
  }

  private static DataSource dataSource(Row row, Properties connectionProperties) {
    String user = "nouser";
    String pass = "nopass";
    StringJoiner joiner = new StringJoiner(";");
    for (String key : connectionProperties.stringPropertyNames()) {
      if ("user".equals(key)) {
        user = connectionProperties.getProperty(key);
      } else if ("password".equals(key)) {
        pass = connectionProperties.getProperty(key);
      } else {
        String value = connectionProperties.getProperty(key);
        joiner.add(key + "=" + value);
      }
    }
    String joinedUrl = row.URL;
    // Handles case where there are no properties already in the URL
    if (row.URL.endsWith("//")) {
      joinedUrl = joinedUrl + joiner;
    } else {
      joinedUrl = joinedUrl + ";" + joiner;
    }
    return JdbcSchema.dataSource(joinedUrl, row.DRIVER, user, pass);
  }

  private static SqlDialect dialect(Row row) {
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
