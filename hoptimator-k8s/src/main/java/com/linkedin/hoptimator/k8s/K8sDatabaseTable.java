package com.linkedin.hoptimator.k8s;

import java.util.Locale;
import java.util.Optional;
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
import com.linkedin.hoptimator.util.HoptimatorJdbcSchema;


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

  public K8sDatabaseTable(K8sContext context) {
    super(context, K8sApiEndpoints.DATABASES, Row.class);
  }

  public void addDatabases(SchemaPlus parentSchema) {
    for (Row row : rows()) {
      parentSchema.add(schemaName(row),
          HoptimatorJdbcSchema.create(row.NAME, row.SCHEMA, dataSource(row), parentSchema, dialect(row)));
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

  private static DataSource dataSource(Row row) {
    // TODO fetch username/password from Secret
    return JdbcSchema.dataSource(row.URL, row.DRIVER, "nouser", "nopass");
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
