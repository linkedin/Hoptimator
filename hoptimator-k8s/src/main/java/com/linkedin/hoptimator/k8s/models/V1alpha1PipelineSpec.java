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
 * Pipeline spec.
 */
@ApiModel(description = "Pipeline spec.")
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2025-01-20T21:16:25.561Z[Etc/UTC]")
public class V1alpha1PipelineSpec {
  public static final String SERIALIZED_NAME_SQL = "sql";
  @SerializedName(SERIALIZED_NAME_SQL)
  private String sql;

  public static final String SERIALIZED_NAME_YAML = "yaml";
  @SerializedName(SERIALIZED_NAME_YAML)
  private String yaml;


  public V1alpha1PipelineSpec sql(String sql) {
    
    this.sql = sql;
    return this;
  }

   /**
   * The INSERT INTO statement this pipeline implements.
   * @return sql
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "The INSERT INTO statement this pipeline implements.")

  public String getSql() {
    return sql;
  }


  public void setSql(String sql) {
    this.sql = sql;
  }


  public V1alpha1PipelineSpec yaml(String yaml) {
    
    this.yaml = yaml;
    return this;
  }

   /**
   * The objects that make up the pipeline.
   * @return yaml
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "The objects that make up the pipeline.")

  public String getYaml() {
    return yaml;
  }


  public void setYaml(String yaml) {
    this.yaml = yaml;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1PipelineSpec v1alpha1PipelineSpec = (V1alpha1PipelineSpec) o;
    return Objects.equals(this.sql, v1alpha1PipelineSpec.sql) &&
        Objects.equals(this.yaml, v1alpha1PipelineSpec.yaml);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sql, yaml);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1PipelineSpec {\n");
    sb.append("    sql: ").append(toIndentedString(sql)).append("\n");
    sb.append("    yaml: ").append(toIndentedString(yaml)).append("\n");
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

