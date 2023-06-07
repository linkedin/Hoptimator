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
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicSpecConfigMapRef;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;

/**
 * V1alpha1KafkaTopicSpecClientConfigs
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2023-06-07T04:20:03.484Z[Etc/UTC]")
public class V1alpha1KafkaTopicSpecClientConfigs {
  public static final String SERIALIZED_NAME_CONFIG_MAP_REF = "configMapRef";
  @SerializedName(SERIALIZED_NAME_CONFIG_MAP_REF)
  private V1alpha1KafkaTopicSpecConfigMapRef configMapRef;


  public V1alpha1KafkaTopicSpecClientConfigs configMapRef(V1alpha1KafkaTopicSpecConfigMapRef configMapRef) {
    
    this.configMapRef = configMapRef;
    return this;
  }

   /**
   * Get configMapRef
   * @return configMapRef
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public V1alpha1KafkaTopicSpecConfigMapRef getConfigMapRef() {
    return configMapRef;
  }


  public void setConfigMapRef(V1alpha1KafkaTopicSpecConfigMapRef configMapRef) {
    this.configMapRef = configMapRef;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1KafkaTopicSpecClientConfigs v1alpha1KafkaTopicSpecClientConfigs = (V1alpha1KafkaTopicSpecClientConfigs) o;
    return Objects.equals(this.configMapRef, v1alpha1KafkaTopicSpecClientConfigs.configMapRef);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configMapRef);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1KafkaTopicSpecClientConfigs {\n");
    sb.append("    configMapRef: ").append(toIndentedString(configMapRef)).append("\n");
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

