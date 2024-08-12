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
import java.util.ArrayList;
import java.util.List;

/**
 * SQL job spec
 */
@ApiModel(description = "SQL job spec")
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2024-07-26T20:21:01.735Z[Etc/UTC]")
public class V1alpha1SqlJobSpec {
  /**
   * Flink, etc.
   */
  @JsonAdapter(DialectEnum.Adapter.class)
  public enum DialectEnum {
    FLINK("Flink");

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

  /**
   * Streaming or Batch.
   */
  @JsonAdapter(ExecutionModeEnum.Adapter.class)
  public enum ExecutionModeEnum {
    STREAMING("Streaming"),
    
    BATCH("Batch");

    private String value;

    ExecutionModeEnum(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    public static ExecutionModeEnum fromValue(String value) {
      for (ExecutionModeEnum b : ExecutionModeEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }

    public static class Adapter extends TypeAdapter<ExecutionModeEnum> {
      @Override
      public void write(final JsonWriter jsonWriter, final ExecutionModeEnum enumeration) throws IOException {
        jsonWriter.value(enumeration.getValue());
      }

      @Override
      public ExecutionModeEnum read(final JsonReader jsonReader) throws IOException {
        String value =  jsonReader.nextString();
        return ExecutionModeEnum.fromValue(value);
      }
    }
  }

  public static final String SERIALIZED_NAME_EXECUTION_MODE = "executionMode";
  @SerializedName(SERIALIZED_NAME_EXECUTION_MODE)
  private ExecutionModeEnum executionMode;

  public static final String SERIALIZED_NAME_SQL = "sql";
  @SerializedName(SERIALIZED_NAME_SQL)
  private List<String> sql = new ArrayList<>();


  public V1alpha1SqlJobSpec dialect(DialectEnum dialect) {
    
    this.dialect = dialect;
    return this;
  }

   /**
   * Flink, etc.
   * @return dialect
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Flink, etc.")

  public DialectEnum getDialect() {
    return dialect;
  }


  public void setDialect(DialectEnum dialect) {
    this.dialect = dialect;
  }


  public V1alpha1SqlJobSpec executionMode(ExecutionModeEnum executionMode) {
    
    this.executionMode = executionMode;
    return this;
  }

   /**
   * Streaming or Batch.
   * @return executionMode
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Streaming or Batch.")

  public ExecutionModeEnum getExecutionMode() {
    return executionMode;
  }


  public void setExecutionMode(ExecutionModeEnum executionMode) {
    this.executionMode = executionMode;
  }


  public V1alpha1SqlJobSpec sql(List<String> sql) {
    
    this.sql = sql;
    return this;
  }

  public V1alpha1SqlJobSpec addSqlItem(String sqlItem) {
    this.sql.add(sqlItem);
    return this;
  }

   /**
   * SQL script the job should run.
   * @return sql
  **/
  @ApiModelProperty(required = true, value = "SQL script the job should run.")

  public List<String> getSql() {
    return sql;
  }


  public void setSql(List<String> sql) {
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
    V1alpha1SqlJobSpec v1alpha1SqlJobSpec = (V1alpha1SqlJobSpec) o;
    return Objects.equals(this.dialect, v1alpha1SqlJobSpec.dialect) &&
        Objects.equals(this.executionMode, v1alpha1SqlJobSpec.executionMode) &&
        Objects.equals(this.sql, v1alpha1SqlJobSpec.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dialect, executionMode, sql);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1SqlJobSpec {\n");
    sb.append("    dialect: ").append(toIndentedString(dialect)).append("\n");
    sb.append("    executionMode: ").append(toIndentedString(executionMode)).append("\n");
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

