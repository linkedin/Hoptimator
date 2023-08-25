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
import com.linkedin.hoptimator.models.V1alpha1AclSpecResource;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;

/**
 * A set of related ACL rules.
 */
@ApiModel(description = "A set of related ACL rules.")
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2023-08-25T02:17:32.460Z[Etc/UTC]")
public class V1alpha1AclSpec {
  /**
   * The resource access method.
   */
  @JsonAdapter(MethodEnum.Adapter.class)
  public enum MethodEnum {
    ALTER("Alter"),
    
    CREATE("Create"),
    
    DELETE("Delete"),
    
    DESCRIBE("Describe"),
    
    READ("Read"),
    
    WRITE("Write"),
    
    POST("Post"),
    
    PUT("Put"),
    
    GET("Get"),
    
    HEAD("Head"),
    
    PATCH("Patch"),
    
    TRACE("Trace"),
    
    OPTIONS("Options"),
    
    GETALL("GetAll"),
    
    BATCHGET("BatchGet"),
    
    BATCHCREATE("BatchCreate"),
    
    BATCHUPDATE("BatchUpdate"),
    
    PARTIALUPDATE("PartialUpdate"),
    
    BATCHDELETE("BatchDelete"),
    
    BATCHPARTIALDELETE("BatchPartialDelete");

    private String value;

    MethodEnum(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    public static MethodEnum fromValue(String value) {
      for (MethodEnum b : MethodEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }

    public static class Adapter extends TypeAdapter<MethodEnum> {
      @Override
      public void write(final JsonWriter jsonWriter, final MethodEnum enumeration) throws IOException {
        jsonWriter.value(enumeration.getValue());
      }

      @Override
      public MethodEnum read(final JsonReader jsonReader) throws IOException {
        String value =  jsonReader.nextString();
        return MethodEnum.fromValue(value);
      }
    }
  }

  public static final String SERIALIZED_NAME_METHOD = "method";
  @SerializedName(SERIALIZED_NAME_METHOD)
  private MethodEnum method;

  public static final String SERIALIZED_NAME_PRINCIPAL = "principal";
  @SerializedName(SERIALIZED_NAME_PRINCIPAL)
  private String principal;

  public static final String SERIALIZED_NAME_RESOURCE = "resource";
  @SerializedName(SERIALIZED_NAME_RESOURCE)
  private V1alpha1AclSpecResource resource;


  public V1alpha1AclSpec method(MethodEnum method) {
    
    this.method = method;
    return this;
  }

   /**
   * The resource access method.
   * @return method
  **/
  @ApiModelProperty(required = true, value = "The resource access method.")

  public MethodEnum getMethod() {
    return method;
  }


  public void setMethod(MethodEnum method) {
    this.method = method;
  }


  public V1alpha1AclSpec principal(String principal) {
    
    this.principal = principal;
    return this;
  }

   /**
   * The principal being allowed access. Format depends on principal type.
   * @return principal
  **/
  @ApiModelProperty(required = true, value = "The principal being allowed access. Format depends on principal type.")

  public String getPrincipal() {
    return principal;
  }


  public void setPrincipal(String principal) {
    this.principal = principal;
  }


  public V1alpha1AclSpec resource(V1alpha1AclSpecResource resource) {
    
    this.resource = resource;
    return this;
  }

   /**
   * Get resource
   * @return resource
  **/
  @ApiModelProperty(required = true, value = "")

  public V1alpha1AclSpecResource getResource() {
    return resource;
  }


  public void setResource(V1alpha1AclSpecResource resource) {
    this.resource = resource;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1AclSpec v1alpha1AclSpec = (V1alpha1AclSpec) o;
    return Objects.equals(this.method, v1alpha1AclSpec.method) &&
        Objects.equals(this.principal, v1alpha1AclSpec.principal) &&
        Objects.equals(this.resource, v1alpha1AclSpec.resource);
  }

  @Override
  public int hashCode() {
    return Objects.hash(method, principal, resource);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1AclSpec {\n");
    sb.append("    method: ").append(toIndentedString(method)).append("\n");
    sb.append("    principal: ").append(toIndentedString(principal)).append("\n");
    sb.append("    resource: ").append(toIndentedString(resource)).append("\n");
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

