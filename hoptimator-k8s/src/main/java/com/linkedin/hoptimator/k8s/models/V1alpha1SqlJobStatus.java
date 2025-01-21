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
 * Filled in by the operator.
 */
@ApiModel(description = "Filled in by the operator.")
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2025-01-21T21:11:41.897Z[Etc/UTC]")
public class V1alpha1SqlJobStatus {
  public static final String SERIALIZED_NAME_CONFIGS = "configs";
  @SerializedName(SERIALIZED_NAME_CONFIGS)
  private Map<String, String> configs = null;

  public static final String SERIALIZED_NAME_FAILED = "failed";
  @SerializedName(SERIALIZED_NAME_FAILED)
  private Boolean failed;

  public static final String SERIALIZED_NAME_MESSAGE = "message";
  @SerializedName(SERIALIZED_NAME_MESSAGE)
  private String message;

  public static final String SERIALIZED_NAME_READY = "ready";
  @SerializedName(SERIALIZED_NAME_READY)
  private Boolean ready;

  public static final String SERIALIZED_NAME_SQL = "sql";
  @SerializedName(SERIALIZED_NAME_SQL)
  private String sql;


  public V1alpha1SqlJobStatus configs(Map<String, String> configs) {
    
    this.configs = configs;
    return this;
  }

  public V1alpha1SqlJobStatus putConfigsItem(String key, String configsItem) {
    if (this.configs == null) {
      this.configs = new HashMap<>();
    }
    this.configs.put(key, configsItem);
    return this;
  }

   /**
   * The SQL configurations used by this SqlJob.
   * @return configs
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "The SQL configurations used by this SqlJob.")

  public Map<String, String> getConfigs() {
    return configs;
  }


  public void setConfigs(Map<String, String> configs) {
    this.configs = configs;
  }


  public V1alpha1SqlJobStatus failed(Boolean failed) {
    
    this.failed = failed;
    return this;
  }

   /**
   * Whether the SqlJob has failed.
   * @return failed
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Whether the SqlJob has failed.")

  public Boolean getFailed() {
    return failed;
  }


  public void setFailed(Boolean failed) {
    this.failed = failed;
  }


  public V1alpha1SqlJobStatus message(String message) {
    
    this.message = message;
    return this;
  }

   /**
   * Error or success message, for information only.
   * @return message
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Error or success message, for information only.")

  public String getMessage() {
    return message;
  }


  public void setMessage(String message) {
    this.message = message;
  }


  public V1alpha1SqlJobStatus ready(Boolean ready) {
    
    this.ready = ready;
    return this;
  }

   /**
   * Whether the SqlJob is running or completed.
   * @return ready
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Whether the SqlJob is running or completed.")

  public Boolean getReady() {
    return ready;
  }


  public void setReady(Boolean ready) {
    this.ready = ready;
  }


  public V1alpha1SqlJobStatus sql(String sql) {
    
    this.sql = sql;
    return this;
  }

   /**
   * The SQL being implemented by this SqlJob.
   * @return sql
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "The SQL being implemented by this SqlJob.")

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
    V1alpha1SqlJobStatus v1alpha1SqlJobStatus = (V1alpha1SqlJobStatus) o;
    return Objects.equals(this.configs, v1alpha1SqlJobStatus.configs) &&
        Objects.equals(this.failed, v1alpha1SqlJobStatus.failed) &&
        Objects.equals(this.message, v1alpha1SqlJobStatus.message) &&
        Objects.equals(this.ready, v1alpha1SqlJobStatus.ready) &&
        Objects.equals(this.sql, v1alpha1SqlJobStatus.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configs, failed, message, ready, sql);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1SqlJobStatus {\n");
    sb.append("    configs: ").append(toIndentedString(configs)).append("\n");
    sb.append("    failed: ").append(toIndentedString(failed)).append("\n");
    sb.append("    message: ").append(toIndentedString(message)).append("\n");
    sb.append("    ready: ").append(toIndentedString(ready)).append("\n");
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

