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
import java.util.List;

/**
 * TableTemplate spec.
 */
@ApiModel(description = "TableTemplate spec.")
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2025-01-20T21:16:25.561Z[Etc/UTC]")
public class V1alpha1JobTemplateSpec {
  public static final String SERIALIZED_NAME_DATABASES = "databases";
  @SerializedName(SERIALIZED_NAME_DATABASES)
  private List<String> databases = null;

  public static final String SERIALIZED_NAME_YAML = "yaml";
  @SerializedName(SERIALIZED_NAME_YAML)
  private String yaml;


  public V1alpha1JobTemplateSpec databases(List<String> databases) {
    
    this.databases = databases;
    return this;
  }

  public V1alpha1JobTemplateSpec addDatabasesItem(String databasesItem) {
    if (this.databases == null) {
      this.databases = new ArrayList<>();
    }
    this.databases.add(databasesItem);
    return this;
  }

   /**
   * Databases this template matches. If null, matches everything.
   * @return databases
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Databases this template matches. If null, matches everything.")

  public List<String> getDatabases() {
    return databases;
  }


  public void setDatabases(List<String> databases) {
    this.databases = databases;
  }


  public V1alpha1JobTemplateSpec yaml(String yaml) {
    
    this.yaml = yaml;
    return this;
  }

   /**
   * YAML template used to generate K8s specs.
   * @return yaml
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "YAML template used to generate K8s specs.")

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
    V1alpha1JobTemplateSpec v1alpha1JobTemplateSpec = (V1alpha1JobTemplateSpec) o;
    return Objects.equals(this.databases, v1alpha1JobTemplateSpec.databases) &&
        Objects.equals(this.yaml, v1alpha1JobTemplateSpec.yaml);
  }

  @Override
  public int hashCode() {
    return Objects.hash(databases, yaml);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1JobTemplateSpec {\n");
    sb.append("    databases: ").append(toIndentedString(databases)).append("\n");
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

