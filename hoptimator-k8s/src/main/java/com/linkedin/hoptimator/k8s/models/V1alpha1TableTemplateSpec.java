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
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2025-01-14T23:39:16.570Z[Etc/UTC]")
public class V1alpha1TableTemplateSpec {
  public static final String SERIALIZED_NAME_CONNECTOR = "connector";
  @SerializedName(SERIALIZED_NAME_CONNECTOR)
  private String connector;

  public static final String SERIALIZED_NAME_DATABASES = "databases";
  @SerializedName(SERIALIZED_NAME_DATABASES)
  private List<String> databases = null;

  /**
   * Gets or Sets methods
   */
  @JsonAdapter(MethodsEnum.Adapter.class)
  public enum MethodsEnum {
    SCAN("Scan"),
    
    MODIFY("Modify");

    private String value;

    MethodsEnum(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    public static MethodsEnum fromValue(String value) {
      for (MethodsEnum b : MethodsEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }

    public static class Adapter extends TypeAdapter<MethodsEnum> {
      @Override
      public void write(final JsonWriter jsonWriter, final MethodsEnum enumeration) throws IOException {
        jsonWriter.value(enumeration.getValue());
      }

      @Override
      public MethodsEnum read(final JsonReader jsonReader) throws IOException {
        String value =  jsonReader.nextString();
        return MethodsEnum.fromValue(value);
      }
    }
  }

  public static final String SERIALIZED_NAME_METHODS = "methods";
  @SerializedName(SERIALIZED_NAME_METHODS)
  private List<MethodsEnum> methods = null;

  public static final String SERIALIZED_NAME_YAML = "yaml";
  @SerializedName(SERIALIZED_NAME_YAML)
  private String yaml;


  public V1alpha1TableTemplateSpec connector(String connector) {
    
    this.connector = connector;
    return this;
  }

   /**
   * Config template used to generate connector configs.
   * @return connector
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Config template used to generate connector configs.")

  public String getConnector() {
    return connector;
  }


  public void setConnector(String connector) {
    this.connector = connector;
  }


  public V1alpha1TableTemplateSpec databases(List<String> databases) {
    
    this.databases = databases;
    return this;
  }

  public V1alpha1TableTemplateSpec addDatabasesItem(String databasesItem) {
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


  public V1alpha1TableTemplateSpec methods(List<MethodsEnum> methods) {
    
    this.methods = methods;
    return this;
  }

  public V1alpha1TableTemplateSpec addMethodsItem(MethodsEnum methodsItem) {
    if (this.methods == null) {
      this.methods = new ArrayList<>();
    }
    this.methods.add(methodsItem);
    return this;
  }

   /**
   * Table access methods this template matches. If null, matches any access method.
   * @return methods
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Table access methods this template matches. If null, matches any access method.")

  public List<MethodsEnum> getMethods() {
    return methods;
  }


  public void setMethods(List<MethodsEnum> methods) {
    this.methods = methods;
  }


  public V1alpha1TableTemplateSpec yaml(String yaml) {
    
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
    V1alpha1TableTemplateSpec v1alpha1TableTemplateSpec = (V1alpha1TableTemplateSpec) o;
    return Objects.equals(this.connector, v1alpha1TableTemplateSpec.connector) &&
        Objects.equals(this.databases, v1alpha1TableTemplateSpec.databases) &&
        Objects.equals(this.methods, v1alpha1TableTemplateSpec.methods) &&
        Objects.equals(this.yaml, v1alpha1TableTemplateSpec.yaml);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connector, databases, methods, yaml);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1TableTemplateSpec {\n");
    sb.append("    connector: ").append(toIndentedString(connector)).append("\n");
    sb.append("    databases: ").append(toIndentedString(databases)).append("\n");
    sb.append("    methods: ").append(toIndentedString(methods)).append("\n");
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

