package com.linkedin.hoptimator.k8s.models;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.gson.annotations.SerializedName;


/**
 * Table spec.
 */
public class V1alpha1TableSpec {
  public static final String SERIALIZED_NAME_CATALOG = "catalog";
  @SerializedName(SERIALIZED_NAME_CATALOG)
  private String catalog;

  public static final String SERIALIZED_NAME_SCHEMA = "schema";
  @SerializedName(SERIALIZED_NAME_SCHEMA)
  private String schema;

  public static final String SERIALIZED_NAME_TABLE = "table";
  @SerializedName(SERIALIZED_NAME_TABLE)
  private String table;

  public static final String SERIALIZED_NAME_DATABASE = "database";
  @SerializedName(SERIALIZED_NAME_DATABASE)
  private String database;

  public static final String SERIALIZED_NAME_COLUMNS = "columns";
  @SerializedName(SERIALIZED_NAME_COLUMNS)
  private List<V1alpha1TableSpecColumn> columns;

  public static final String SERIALIZED_NAME_OPTIONS = "options";
  @SerializedName(SERIALIZED_NAME_OPTIONS)
  private Map<String, String> options;


  public V1alpha1TableSpec catalog(String catalog) {
    this.catalog = catalog;
    return this;
  }

  @javax.annotation.Nullable
  public String getCatalog() {
    return catalog;
  }

  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  public V1alpha1TableSpec schema(String schema) {
    this.schema = schema;
    return this;
  }

  @javax.annotation.Nullable
  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public V1alpha1TableSpec table(String table) {
    this.table = table;
    return this;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public V1alpha1TableSpec database(String database) {
    this.database = database;
    return this;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public V1alpha1TableSpec columns(List<V1alpha1TableSpecColumn> columns) {
    this.columns = columns;
    return this;
  }

  @javax.annotation.Nullable
  public List<V1alpha1TableSpecColumn> getColumns() {
    return columns;
  }

  public void setColumns(List<V1alpha1TableSpecColumn> columns) {
    this.columns = columns;
  }

  public V1alpha1TableSpec options(Map<String, String> options) {
    this.options = options;
    return this;
  }

  @javax.annotation.Nullable
  public Map<String, String> getOptions() {
    return options;
  }

  public void setOptions(Map<String, String> options) {
    this.options = options;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1TableSpec v1alpha1TableSpec = (V1alpha1TableSpec) o;
    return Objects.equals(this.catalog, v1alpha1TableSpec.catalog) &&
        Objects.equals(this.schema, v1alpha1TableSpec.schema) &&
        Objects.equals(this.table, v1alpha1TableSpec.table) &&
        Objects.equals(this.database, v1alpha1TableSpec.database) &&
        Objects.equals(this.columns, v1alpha1TableSpec.columns) &&
        Objects.equals(this.options, v1alpha1TableSpec.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalog, schema, table, database, columns, options);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1TableSpec {\n");
    sb.append("    catalog: ").append(toIndentedString(catalog)).append("\n");
    sb.append("    schema: ").append(toIndentedString(schema)).append("\n");
    sb.append("    table: ").append(toIndentedString(table)).append("\n");
    sb.append("    database: ").append(toIndentedString(database)).append("\n");
    sb.append("    columns: ").append(toIndentedString(columns)).append("\n");
    sb.append("    options: ").append(toIndentedString(options)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
