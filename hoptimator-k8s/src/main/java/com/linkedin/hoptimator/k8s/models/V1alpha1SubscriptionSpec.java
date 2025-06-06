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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Subscription spec
 */
@ApiModel(description = "Subscription spec")
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2025-05-21T02:31:00.123Z[Etc/UTC]")
public class V1alpha1SubscriptionSpec {
  public static final String SERIALIZED_NAME_DATABASE = "database";
  @SerializedName(SERIALIZED_NAME_DATABASE)
  private String database;

  public static final String SERIALIZED_NAME_HINTS = "hints";
  @SerializedName(SERIALIZED_NAME_HINTS)
  private Map<String, String> hints = null;

  public static final String SERIALIZED_NAME_SQL = "sql";
  @SerializedName(SERIALIZED_NAME_SQL)
  private String sql;


  public V1alpha1SubscriptionSpec database(String database) {
    
    this.database = database;
    return this;
  }

   /**
   * The database in which to create the output/sink table.
   * @return database
  **/
  @ApiModelProperty(required = true, value = "The database in which to create the output/sink table.")

  public String getDatabase() {
    return database;
  }


  public void setDatabase(String database) {
    this.database = database;
  }


  public V1alpha1SubscriptionSpec hints(Map<String, String> hints) {
    
    this.hints = hints;
    return this;
  }

  public V1alpha1SubscriptionSpec putHintsItem(String key, String hintsItem) {
    if (this.hints == null) {
      this.hints = new HashMap<>();
    }
    this.hints.put(key, hintsItem);
    return this;
  }

   /**
   * Hints to adapters, which may disregard them.
   * @return hints
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Hints to adapters, which may disregard them.")

  public Map<String, String> getHints() {
    return hints;
  }


  public void setHints(Map<String, String> hints) {
    this.hints = hints;
  }


  public V1alpha1SubscriptionSpec sql(String sql) {
    
    this.sql = sql;
    return this;
  }

   /**
   * A single SQL query.
   * @return sql
  **/
  @ApiModelProperty(required = true, value = "A single SQL query.")

  public String getSql() {
    return sql;
  }


  public void setSql(String sql) {
    this.sql = sql;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1SubscriptionSpec v1alpha1SubscriptionSpec = (V1alpha1SubscriptionSpec) o;
    return Objects.equals(this.database, v1alpha1SubscriptionSpec.database) &&
        Objects.equals(this.hints, v1alpha1SubscriptionSpec.hints) &&
        Objects.equals(this.sql, v1alpha1SubscriptionSpec.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, hints, sql);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1SubscriptionSpec {\n");
    sb.append("    database: ").append(toIndentedString(database)).append("\n");
    sb.append("    hints: ").append(toIndentedString(hints)).append("\n");
    sb.append("    sql: ").append(toIndentedString(sql)).append("\n");
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

