/*
 * Kubernetes
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: v1.21.1
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */


package com.linkedin.hoptimator.k8s.models;

import java.util.Objects;
import java.util.Arrays;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;

/**
 * View spec.
 */
@ApiModel(description = "View spec.")
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2024-12-04T03:00:43.571Z[Etc/UTC]")
public class V1alpha1ViewSpec {
  public static final String SERIALIZED_NAME_MATERIALIZED = "materialized";
  @SerializedName(SERIALIZED_NAME_MATERIALIZED)
  private Boolean materialized;

  public static final String SERIALIZED_NAME_SCHEMA = "schema";
  @SerializedName(SERIALIZED_NAME_SCHEMA)
  private String schema;

  public static final String SERIALIZED_NAME_SQL = "sql";
  @SerializedName(SERIALIZED_NAME_SQL)
  private String sql;

  public static final String SERIALIZED_NAME_VIEW = "view";
  @SerializedName(SERIALIZED_NAME_VIEW)
  private String view;


  public V1alpha1ViewSpec materialized(Boolean materialized) {
    
    this.materialized = materialized;
    return this;
  }

   /**
   * Whether the view should be materialized.
   * @return materialized
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Whether the view should be materialized.")

  public Boolean getMaterialized() {
    return materialized;
  }


  public void setMaterialized(Boolean materialized) {
    this.materialized = materialized;
  }


  public V1alpha1ViewSpec schema(String schema) {
    
    this.schema = schema;
    return this;
  }

   /**
   * Schema name.
   * @return schema
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Schema name.")

  public String getSchema() {
    return schema;
  }


  public void setSchema(String schema) {
    this.schema = schema;
  }


  public V1alpha1ViewSpec sql(String sql) {
    
    this.sql = sql;
    return this;
  }

   /**
   * View SQL.
   * @return sql
  **/
  @ApiModelProperty(required = true, value = "View SQL.")

  public String getSql() {
    return sql;
  }


  public void setSql(String sql) {
    this.sql = sql;
  }


  public V1alpha1ViewSpec view(String view) {
    
    this.view = view;
    return this;
  }

   /**
   * View name.
   * @return view
  **/
  @ApiModelProperty(required = true, value = "View name.")

  public String getView() {
    return view;
  }


  public void setView(String view) {
    this.view = view;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1ViewSpec v1alpha1ViewSpec = (V1alpha1ViewSpec) o;
    return Objects.equals(this.materialized, v1alpha1ViewSpec.materialized) &&
        Objects.equals(this.schema, v1alpha1ViewSpec.schema) &&
        Objects.equals(this.sql, v1alpha1ViewSpec.sql) &&
        Objects.equals(this.view, v1alpha1ViewSpec.view);
  }

  @Override
  public int hashCode() {
    return Objects.hash(materialized, schema, sql, view);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1ViewSpec {\n");
    sb.append("    materialized: ").append(toIndentedString(materialized)).append("\n");
    sb.append("    schema: ").append(toIndentedString(schema)).append("\n");
    sb.append("    sql: ").append(toIndentedString(sql)).append("\n");
    sb.append("    view: ").append(toIndentedString(view)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}
