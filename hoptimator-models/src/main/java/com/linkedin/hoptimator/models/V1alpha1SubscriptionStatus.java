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


package com.linkedin.hoptimator.models;

import java.util.Objects;
import java.util.Arrays;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionStatusResources;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Filled in by the operator.
 */
@ApiModel(description = "Filled in by the operator.")
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2023-06-06T00:58:39.977Z[Etc/UTC]")
public class V1alpha1SubscriptionStatus {
  public static final String SERIALIZED_NAME_MESSAGE = "message";
  @SerializedName(SERIALIZED_NAME_MESSAGE)
  private String message;

  public static final String SERIALIZED_NAME_READY = "ready";
  @SerializedName(SERIALIZED_NAME_READY)
  private Boolean ready;

  public static final String SERIALIZED_NAME_RESOURCES = "resources";
  @SerializedName(SERIALIZED_NAME_RESOURCES)
  private List<V1alpha1SubscriptionStatusResources> resources = null;

  public static final String SERIALIZED_NAME_SQL = "sql";
  @SerializedName(SERIALIZED_NAME_SQL)
  private String sql;


  public V1alpha1SubscriptionStatus message(String message) {
    
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


  public V1alpha1SubscriptionStatus ready(Boolean ready) {
    
    this.ready = ready;
    return this;
  }

   /**
   * Whether the subscription is ready, which implies all downstream resources are ready.
   * @return ready
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Whether the subscription is ready, which implies all downstream resources are ready.")

  public Boolean getReady() {
    return ready;
  }


  public void setReady(Boolean ready) {
    this.ready = ready;
  }


  public V1alpha1SubscriptionStatus resources(List<V1alpha1SubscriptionStatusResources> resources) {
    
    this.resources = resources;
    return this;
  }

  public V1alpha1SubscriptionStatus addResourcesItem(V1alpha1SubscriptionStatusResources resourcesItem) {
    if (this.resources == null) {
      this.resources = new ArrayList<>();
    }
    this.resources.add(resourcesItem);
    return this;
  }

   /**
   * Resources owned by this Subscription.
   * @return resources
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Resources owned by this Subscription.")

  public List<V1alpha1SubscriptionStatusResources> getResources() {
    return resources;
  }


  public void setResources(List<V1alpha1SubscriptionStatusResources> resources) {
    this.resources = resources;
  }


  public V1alpha1SubscriptionStatus sql(String sql) {
    
    this.sql = sql;
    return this;
  }

   /**
   * The SQL deployed by the operator.
   * @return sql
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "The SQL deployed by the operator.")

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
    V1alpha1SubscriptionStatus v1alpha1SubscriptionStatus = (V1alpha1SubscriptionStatus) o;
    return Objects.equals(this.message, v1alpha1SubscriptionStatus.message) &&
        Objects.equals(this.ready, v1alpha1SubscriptionStatus.ready) &&
        Objects.equals(this.resources, v1alpha1SubscriptionStatus.resources) &&
        Objects.equals(this.sql, v1alpha1SubscriptionStatus.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(message, ready, resources, sql);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1SubscriptionStatus {\n");
    sb.append("    message: ").append(toIndentedString(message)).append("\n");
    sb.append("    ready: ").append(toIndentedString(ready)).append("\n");
    sb.append("    resources: ").append(toIndentedString(resources)).append("\n");
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

