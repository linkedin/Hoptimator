package com.linkedin.hoptimator.k8s;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.calcite.schema.Schema;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.Engine;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.k8s.models.V1alpha1Engine;
import com.linkedin.hoptimator.k8s.models.V1alpha1EngineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1EngineSpec;


public class K8sEngineTable extends K8sTable<V1alpha1Engine, V1alpha1EngineList, K8sEngineTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public String NAME;
    public String URL;
    public String DIALECT;
    public String DRIVER;
    public String[] DATABASES;

    public Row(String name, String url, String dialect, String driver, String[] databases) {
      this.NAME = name;
      this.URL = url;
      this.DIALECT = dialect;
      this.DRIVER = driver;
      this.DATABASES = databases;
    }
  }
  // CHECKSTYLE:ON

  public K8sEngineTable(K8sContext context) {
    super(context, K8sApiEndpoints.ENGINES, Row.class);
  }

  /** Engines supporting a given database. */
  public List<Engine> forDatabase(String database) {
    return rows().stream().filter(x -> x.DATABASES == null
        || x.DATABASES.length == 0
        || Arrays.asList(x.DATABASES).contains(database))
        .map(x -> new K8sEngine(x.NAME, x.URL, dialect(x), x.DRIVER))
        .collect(Collectors.toList());
  }

  @Override
  public Row toRow(V1alpha1Engine obj) {
    return new Row(obj.getMetadata().getName(), obj.getSpec().getUrl(),
        Optional.ofNullable(obj.getSpec().getDialect()).map(x -> x.toString()).orElseGet(() -> null),
        obj.getSpec().getDriver(), obj.getSpec().getDatabases() != null
        ? obj.getSpec().getDatabases().toArray(new String[0]) : null);
  }

  @Override
  public V1alpha1Engine fromRow(Row row) {
    K8sUtils.checkK8sName(row.NAME);
    return new V1alpha1Engine().kind(K8sApiEndpoints.ENGINES.kind())
        .apiVersion(K8sApiEndpoints.ENGINES.apiVersion())
        .metadata(new V1ObjectMeta().name(row.NAME))
        .spec(new V1alpha1EngineSpec().url(row.URL)
            .dialect(V1alpha1EngineSpec.DialectEnum.fromValue(row.DIALECT))
            .driver(row.DRIVER).databases(Arrays.asList(row.DATABASES)));
  }

  private static SqlDialect dialect(Row row) {
    if (row.DIALECT == null) {
      return SqlDialect.ANSI;
    }
    switch (row.DIALECT) {
    case "Flink":
      return SqlDialect.FLINK;
    default:
      return SqlDialect.valueOf(row.DIALECT);
    }
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.SYSTEM_TABLE;
  }
}
