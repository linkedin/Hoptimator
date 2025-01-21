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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Filled in by the operator.
 */
@ApiModel(description = "Filled in by the operator.")
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2025-01-21T21:11:41.897Z[Etc/UTC]")
public class V1alpha1SubscriptionStatus {
  public static final String SERIALIZED_NAME_ATTRIBUTES = "attributes";
  @SerializedName(SERIALIZED_NAME_ATTRIBUTES)
  private Map<String, String> attributes = null;

  public static final String SERIALIZED_NAME_DOWNSTREAM_RESOURCES = "downstreamResources";
  @SerializedName(SERIALIZED_NAME_DOWNSTREAM_RESOURCES)
  private List<String> downstreamResources = null;

  public static final String SERIALIZED_NAME_FAILED = "failed";
  @SerializedName(SERIALIZED_NAME_FAILED)
  private Boolean failed;

  public static final String SERIALIZED_NAME_HINTS = "hints";
  @SerializedName(SERIALIZED_NAME_HINTS)
  private Map<String, String> hints = null;

  public static final String SERIALIZED_NAME_JOB_RESOURCES = "jobResources";
  @SerializedName(SERIALIZED_NAME_JOB_RESOURCES)
  private List<String> jobResources = null;

  public static final String SERIALIZED_NAME_MESSAGE = "message";
  @SerializedName(SERIALIZED_NAME_MESSAGE)
  private String message;

  public static final String SERIALIZED_NAME_READY = "ready";
  @SerializedName(SERIALIZED_NAME_READY)
  private Boolean ready;

  public static final String SERIALIZED_NAME_RESOURCES = "resources";
  @SerializedName(SERIALIZED_NAME_RESOURCES)
  private List<String> resources = null;

  public static final String SERIALIZED_NAME_SQL = "sql";
  @SerializedName(SERIALIZED_NAME_SQL)
  private String sql;


  public V1alpha1SubscriptionStatus attributes(Map<String, String> attributes) {
    
    this.attributes = attributes;
    return this;
  }

  public V1alpha1SubscriptionStatus putAttributesItem(String key, String attributesItem) {
    if (this.attributes == null) {
      this.attributes = new HashMap<>();
    }
    this.attributes.put(key, attributesItem);
    return this;
  }

   /**
   * Physical attributes of the job and sink/output table.
   * @return attributes
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Physical attributes of the job and sink/output table.")

  public Map<String, String> getAttributes() {
    return attributes;
  }


  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }


  public V1alpha1SubscriptionStatus downstreamResources(List<String> downstreamResources) {
    
    this.downstreamResources = downstreamResources;
    return this;
  }

  public V1alpha1SubscriptionStatus addDownstreamResourcesItem(String downstreamResourcesItem) {
    if (this.downstreamResources == null) {
      this.downstreamResources = new ArrayList<>();
    }
    this.downstreamResources.add(downstreamResourcesItem);
    return this;
  }

   /**
   * The yaml generated to implement the sink/output table.
   * @return downstreamResources
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "The yaml generated to implement the sink/output table.")

  public List<String> getDownstreamResources() {
    return downstreamResources;
  }


  public void setDownstreamResources(List<String> downstreamResources) {
    this.downstreamResources = downstreamResources;
  }


  public V1alpha1SubscriptionStatus failed(Boolean failed) {
    
    this.failed = failed;
    return this;
  }

   /**
   * Indicates that the operator was unable to deploy a pipeline for this subscription.
   * @return failed
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Indicates that the operator was unable to deploy a pipeline for this subscription.")

  public Boolean getFailed() {
    return failed;
  }


  public void setFailed(Boolean failed) {
    this.failed = failed;
  }


  public V1alpha1SubscriptionStatus hints(Map<String, String> hints) {
    
    this.hints = hints;
    return this;
  }

  public V1alpha1SubscriptionStatus putHintsItem(String key, String hintsItem) {
    if (this.hints == null) {
      this.hints = new HashMap<>();
    }
    this.hints.put(key, hintsItem);
    return this;
  }

   /**
   * The hints being used by this pipeline.
   * @return hints
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "The hints being used by this pipeline.")

  public Map<String, String> getHints() {
    return hints;
  }


  public void setHints(Map<String, String> hints) {
    this.hints = hints;
  }


  public V1alpha1SubscriptionStatus jobResources(List<String> jobResources) {
    
    this.jobResources = jobResources;
    return this;
  }

  public V1alpha1SubscriptionStatus addJobResourcesItem(String jobResourcesItem) {
    if (this.jobResources == null) {
      this.jobResources = new ArrayList<>();
    }
    this.jobResources.add(jobResourcesItem);
    return this;
  }

   /**
   * The yaml generated to implement the job.
   * @return jobResources
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "The yaml generated to implement the job.")

  public List<String> getJobResources() {
    return jobResources;
  }


  public void setJobResources(List<String> jobResources) {
    this.jobResources = jobResources;
  }


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
   * Whether the subscription is ready to be consumed.
   * @return ready
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Whether the subscription is ready to be consumed.")

  public Boolean getReady() {
    return ready;
  }


  public void setReady(Boolean ready) {
    this.ready = ready;
  }


  public V1alpha1SubscriptionStatus resources(List<String> resources) {
    
    this.resources = resources;
    return this;
  }

  public V1alpha1SubscriptionStatus addResourcesItem(String resourcesItem) {
    if (this.resources == null) {
      this.resources = new ArrayList<>();
    }
    this.resources.add(resourcesItem);
    return this;
  }

   /**
   * The yaml generated to implement this pipeline.
   * @return resources
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "The yaml generated to implement this pipeline.")

  public List<String> getResources() {
    return resources;
  }


  public void setResources(List<String> resources) {
    this.resources = resources;
  }


  public V1alpha1SubscriptionStatus sql(String sql) {
    
    this.sql = sql;
    return this;
  }

   /**
   * The SQL being implemented by this pipeline.
   * @return sql
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "The SQL being implemented by this pipeline.")

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
    return Objects.equals(this.attributes, v1alpha1SubscriptionStatus.attributes) &&
        Objects.equals(this.downstreamResources, v1alpha1SubscriptionStatus.downstreamResources) &&
        Objects.equals(this.failed, v1alpha1SubscriptionStatus.failed) &&
        Objects.equals(this.hints, v1alpha1SubscriptionStatus.hints) &&
        Objects.equals(this.jobResources, v1alpha1SubscriptionStatus.jobResources) &&
        Objects.equals(this.message, v1alpha1SubscriptionStatus.message) &&
        Objects.equals(this.ready, v1alpha1SubscriptionStatus.ready) &&
        Objects.equals(this.resources, v1alpha1SubscriptionStatus.resources) &&
        Objects.equals(this.sql, v1alpha1SubscriptionStatus.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(attributes, downstreamResources, failed, hints, jobResources, message, ready, resources, sql);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1SubscriptionStatus {\n");
    sb.append("    attributes: ").append(toIndentedString(attributes)).append("\n");
    sb.append("    downstreamResources: ").append(toIndentedString(downstreamResources)).append("\n");
    sb.append("    failed: ").append(toIndentedString(failed)).append("\n");
    sb.append("    hints: ").append(toIndentedString(hints)).append("\n");
    sb.append("    jobResources: ").append(toIndentedString(jobResources)).append("\n");
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

