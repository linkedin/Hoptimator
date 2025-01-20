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
 * Database spec.
 */
@ApiModel(description = "Database spec.")
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2025-01-14T23:39:16.570Z[Etc/UTC]")
public class V1alpha1DatabaseSpec {
  /**
   * SQL dialect the driver expects.
   */
  @JsonAdapter(DialectEnum.Adapter.class)
  public enum DialectEnum {
    ANSI("ANSI"),
    
    MYSQL("MySQL"),
    
    CALCITE("Calcite");

    private String value;

    DialectEnum(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    public static DialectEnum fromValue(String value) {
      for (DialectEnum b : DialectEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }

    public static class Adapter extends TypeAdapter<DialectEnum> {
      @Override
      public void write(final JsonWriter jsonWriter, final DialectEnum enumeration) throws IOException {
        jsonWriter.value(enumeration.getValue());
      }

      @Override
      public DialectEnum read(final JsonReader jsonReader) throws IOException {
        String value =  jsonReader.nextString();
        return DialectEnum.fromValue(value);
      }
    }
  }

  public static final String SERIALIZED_NAME_DIALECT = "dialect";
  @SerializedName(SERIALIZED_NAME_DIALECT)
  private DialectEnum dialect;

  public static final String SERIALIZED_NAME_DRIVER = "driver";
  @SerializedName(SERIALIZED_NAME_DRIVER)
  private String driver;

  public static final String SERIALIZED_NAME_SCHEMA = "schema";
  @SerializedName(SERIALIZED_NAME_SCHEMA)
  private String schema;

  public static final String SERIALIZED_NAME_URL = "url";
  @SerializedName(SERIALIZED_NAME_URL)
  private String url;


  public V1alpha1DatabaseSpec dialect(DialectEnum dialect) {
    
    this.dialect = dialect;
    return this;
  }

   /**
   * SQL dialect the driver expects.
   * @return dialect
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "SQL dialect the driver expects.")

  public DialectEnum getDialect() {
    return dialect;
  }


  public void setDialect(DialectEnum dialect) {
    this.dialect = dialect;
  }


  public V1alpha1DatabaseSpec driver(String driver) {
    
    this.driver = driver;
    return this;
  }

   /**
   * Fully qualified class name of JDBD driver.
   * @return driver
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Fully qualified class name of JDBD driver.")

  public String getDriver() {
    return driver;
  }


  public void setDriver(String driver) {
    this.driver = driver;
  }


  public V1alpha1DatabaseSpec schema(String schema) {
    
    this.schema = schema;
    return this;
  }

   /**
   * Schema name, as rendered in the catalog.
   * @return schema
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Schema name, as rendered in the catalog.")

  public String getSchema() {
    return schema;
  }


  public void setSchema(String schema) {
    this.schema = schema;
  }


  public V1alpha1DatabaseSpec url(String url) {
    
    this.url = url;
    return this;
  }

   /**
   * JDBC connection URL
   * @return url
  **/
  @ApiModelProperty(required = true, value = "JDBC connection URL")

  public String getUrl() {
    return url;
  }


  public void setUrl(String url) {
    this.url = url;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1DatabaseSpec v1alpha1DatabaseSpec = (V1alpha1DatabaseSpec) o;
    return Objects.equals(this.dialect, v1alpha1DatabaseSpec.dialect) &&
        Objects.equals(this.driver, v1alpha1DatabaseSpec.driver) &&
        Objects.equals(this.schema, v1alpha1DatabaseSpec.schema) &&
        Objects.equals(this.url, v1alpha1DatabaseSpec.url);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dialect, driver, schema, url);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1DatabaseSpec {\n");
    sb.append("    dialect: ").append(toIndentedString(dialect)).append("\n");
    sb.append("    driver: ").append(toIndentedString(driver)).append("\n");
    sb.append("    schema: ").append(toIndentedString(schema)).append("\n");
    sb.append("    url: ").append(toIndentedString(url)).append("\n");
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

