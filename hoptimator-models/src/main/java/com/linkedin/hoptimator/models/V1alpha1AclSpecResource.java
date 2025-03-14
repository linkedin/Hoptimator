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
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;

/**
 * The resource being controlled.
 */
@ApiModel(description = "The resource being controlled.")
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2025-01-21T21:15:02.597Z[Etc/UTC]")
public class V1alpha1AclSpecResource {
  public static final String SERIALIZED_NAME_KIND = "kind";
  @SerializedName(SERIALIZED_NAME_KIND)
  private String kind;

  public static final String SERIALIZED_NAME_NAME = "name";
  @SerializedName(SERIALIZED_NAME_NAME)
  private String name;


  public V1alpha1AclSpecResource kind(String kind) {
    
    this.kind = kind;
    return this;
  }

   /**
   * The kind of resource being controlled.
   * @return kind
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "The kind of resource being controlled.")

  public String getKind() {
    return kind;
  }


  public void setKind(String kind) {
    this.kind = kind;
  }


  public V1alpha1AclSpecResource name(String name) {
    
    this.name = name;
    return this;
  }

   /**
   * The name of the resource being controlled.
   * @return name
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "The name of the resource being controlled.")

  public String getName() {
    return name;
  }


  public void setName(String name) {
    this.name = name;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1AclSpecResource v1alpha1AclSpecResource = (V1alpha1AclSpecResource) o;
    return Objects.equals(this.kind, v1alpha1AclSpecResource.kind) &&
        Objects.equals(this.name, v1alpha1AclSpecResource.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kind, name);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1AclSpecResource {\n");
    sb.append("    kind: ").append(toIndentedString(kind)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
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

