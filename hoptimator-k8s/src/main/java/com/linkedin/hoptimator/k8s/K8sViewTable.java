package com.linkedin.hoptimator.k8s;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.Validated;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.jdbc.MaterializedViewTable;
import com.linkedin.hoptimator.jdbc.schema.HoptimatorViewTableMacro;
import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewList;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewSpec;


public class K8sViewTable extends K8sTable<V1alpha1View, V1alpha1ViewList, K8sViewTable.Row> implements Validated {

  // CHECKSTYLE:OFF
  public static class Row {
    public String NAME;
    public String SCHEMA;
    public String VIEW;
    public String SQL;
    public boolean MATERIALIZED;

    public Row(String name, String schema, String view, String sql, boolean materialized) {
      this.NAME = name;
      this.SCHEMA = schema;
      this.VIEW = view;
      this.SQL = sql;
      this.MATERIALIZED = materialized;
    }

    public List<String> viewPath() {
      List<String> path = new ArrayList<>();
      path.addAll(schemaPath());
      path.add(viewName());
      return path;
    }

    public List<String> schemaPath() {
      List<String> path = new ArrayList<>();
      if (SCHEMA != null) {
        path.add(SCHEMA);
      } else {
        path.add("DEFAULT");
      }
      return path;
    }

    public String viewName() {
      if (VIEW != null) {
        return VIEW;
      } else {
        return NAME;
      }
    }

    @Override
    public String toString() {
      return String.join("\t", NAME, SCHEMA, VIEW, SQL, Boolean.toString(MATERIALIZED));
    }
  }
  // CHECKSTYLE:ON

  public K8sViewTable(K8sContext context) {
    super(context, K8sApiEndpoints.VIEWS, Row.class);
  }

  public void addViews(SchemaPlus parentSchema, Properties connectionProperties) {
    for (Row row : rows()) {

      // build schema path, filling in any missing schemas
      SchemaPlus schema = parentSchema;
      for (String pos : row.schemaPath()) {
        SchemaPlus next = schema.getSubSchema(pos);
        if (next == null) {
          schema.add(pos, new AbstractSchema());
          next = schema.getSubSchema(pos);
        }
        schema = next;
      }
      schema.add(row.viewName(), makeView(schema, row, connectionProperties));
    }
  }

  public void add(String name, String schema, String view, String sql, boolean materialized) {
    rows().add(new Row(name, schema, view, sql, materialized));
  }

  public Row find(String name) {
    return rows().stream()
        .filter(x -> x.NAME.equals(name))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Table " + name + " not found."));
  }

  public void remove(String name) {
    rows().remove(find(name));
  }

  private Table makeView(SchemaPlus parentSchema, Row row, Properties connectionProperties) {
    HoptimatorViewTableMacro viewTableMacro = new HoptimatorViewTableMacro(CalciteSchema.from(parentSchema), row.SQL,
        row.schemaPath(), row.viewPath(), false);
    if (row.MATERIALIZED) {
      return new MaterializedViewTable(viewTableMacro, connectionProperties);
    } else {
      return viewTableMacro.apply(Collections.singletonList(connectionProperties));
    }
  }

  @Override
  public Row toRow(V1alpha1View obj) {
    return new Row(obj.getMetadata().getName(), obj.getSpec().getSchema(), obj.getSpec().getView(),
        obj.getSpec().getSql(), Boolean.TRUE.equals(obj.getSpec().getMaterialized()));
  }

  @Override
  public V1alpha1View fromRow(Row row) {
    K8sUtils.checkK8sName(row.NAME);
    return new V1alpha1View().kind(K8sApiEndpoints.VIEWS.kind())
        .apiVersion(K8sApiEndpoints.VIEWS.apiVersion())
        .metadata(new V1ObjectMeta().name(row.NAME))
        .spec(new V1alpha1ViewSpec().sql(row.SQL).materialized(row.MATERIALIZED));
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.SYSTEM_TABLE;
  }

  @Override
  public void validate(Validator.Issues issues) {
    for (Row row : rows()) {
      Validator.Issues issues2 = issues.child(row.toString());
      Validator.validateSubdomainName(row.NAME, issues2.child("NAME"));
    }
    Validator.validateUnique(rows(), x -> x.NAME, issues);
  }
}
