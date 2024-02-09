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
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicSpecClientConfigs;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Desired Kafka topic configuration.
 */
@ApiModel(description = "Desired Kafka topic configuration.")
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2024-02-09T06:53:38.470Z[Etc/UTC]")
public class V1alpha1KafkaTopicSpec {
  public static final String SERIALIZED_NAME_CLIENT_CONFIGS = "clientConfigs";
  @SerializedName(SERIALIZED_NAME_CLIENT_CONFIGS)
  private List<V1alpha1KafkaTopicSpecClientConfigs> clientConfigs = null;

  public static final String SERIALIZED_NAME_CLIENT_OVERRIDES = "clientOverrides";
  @SerializedName(SERIALIZED_NAME_CLIENT_OVERRIDES)
  private Map<String, String> clientOverrides = null;

  public static final String SERIALIZED_NAME_CONFIGS = "configs";
  @SerializedName(SERIALIZED_NAME_CONFIGS)
  private Map<String, String> configs = null;

  public static final String SERIALIZED_NAME_NUM_PARTITIONS = "numPartitions";
  @SerializedName(SERIALIZED_NAME_NUM_PARTITIONS)
  private Integer numPartitions;

  public static final String SERIALIZED_NAME_REPLICATION_FACTOR = "replicationFactor";
  @SerializedName(SERIALIZED_NAME_REPLICATION_FACTOR)
  private Integer replicationFactor;

  public static final String SERIALIZED_NAME_TOPIC_NAME = "topicName";
  @SerializedName(SERIALIZED_NAME_TOPIC_NAME)
  private String topicName;


  public V1alpha1KafkaTopicSpec clientConfigs(List<V1alpha1KafkaTopicSpecClientConfigs> clientConfigs) {
    
    this.clientConfigs = clientConfigs;
    return this;
  }

  public V1alpha1KafkaTopicSpec addClientConfigsItem(V1alpha1KafkaTopicSpecClientConfigs clientConfigsItem) {
    if (this.clientConfigs == null) {
      this.clientConfigs = new ArrayList<>();
    }
    this.clientConfigs.add(clientConfigsItem);
    return this;
  }

   /**
   * ConfigMaps for AdminClient configuration.
   * @return clientConfigs
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "ConfigMaps for AdminClient configuration.")

  public List<V1alpha1KafkaTopicSpecClientConfigs> getClientConfigs() {
    return clientConfigs;
  }


  public void setClientConfigs(List<V1alpha1KafkaTopicSpecClientConfigs> clientConfigs) {
    this.clientConfigs = clientConfigs;
  }


  public V1alpha1KafkaTopicSpec clientOverrides(Map<String, String> clientOverrides) {
    
    this.clientOverrides = clientOverrides;
    return this;
  }

  public V1alpha1KafkaTopicSpec putClientOverridesItem(String key, String clientOverridesItem) {
    if (this.clientOverrides == null) {
      this.clientOverrides = new HashMap<>();
    }
    this.clientOverrides.put(key, clientOverridesItem);
    return this;
  }

   /**
   * AdminClient overrides.
   * @return clientOverrides
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "AdminClient overrides.")

  public Map<String, String> getClientOverrides() {
    return clientOverrides;
  }


  public void setClientOverrides(Map<String, String> clientOverrides) {
    this.clientOverrides = clientOverrides;
  }


  public V1alpha1KafkaTopicSpec configs(Map<String, String> configs) {
    
    this.configs = configs;
    return this;
  }

  public V1alpha1KafkaTopicSpec putConfigsItem(String key, String configsItem) {
    if (this.configs == null) {
      this.configs = new HashMap<>();
    }
    this.configs.put(key, configsItem);
    return this;
  }

   /**
   * Topic configurations.
   * @return configs
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Topic configurations.")

  public Map<String, String> getConfigs() {
    return configs;
  }


  public void setConfigs(Map<String, String> configs) {
    this.configs = configs;
  }


  public V1alpha1KafkaTopicSpec numPartitions(Integer numPartitions) {
    
    this.numPartitions = numPartitions;
    return this;
  }

   /**
   * Number of partitions the topic should have. By default, the cluster decides.
   * @return numPartitions
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Number of partitions the topic should have. By default, the cluster decides.")

  public Integer getNumPartitions() {
    return numPartitions;
  }


  public void setNumPartitions(Integer numPartitions) {
    this.numPartitions = numPartitions;
  }


  public V1alpha1KafkaTopicSpec replicationFactor(Integer replicationFactor) {
    
    this.replicationFactor = replicationFactor;
    return this;
  }

   /**
   * The replication factor the topic should have. By default, the cluster decides.
   * @return replicationFactor
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "The replication factor the topic should have. By default, the cluster decides.")

  public Integer getReplicationFactor() {
    return replicationFactor;
  }


  public void setReplicationFactor(Integer replicationFactor) {
    this.replicationFactor = replicationFactor;
  }


  public V1alpha1KafkaTopicSpec topicName(String topicName) {
    
    this.topicName = topicName;
    return this;
  }

   /**
   * The topic name.
   * @return topicName
  **/
  @ApiModelProperty(required = true, value = "The topic name.")

  public String getTopicName() {
    return topicName;
  }


  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1KafkaTopicSpec v1alpha1KafkaTopicSpec = (V1alpha1KafkaTopicSpec) o;
    return Objects.equals(this.clientConfigs, v1alpha1KafkaTopicSpec.clientConfigs) &&
        Objects.equals(this.clientOverrides, v1alpha1KafkaTopicSpec.clientOverrides) &&
        Objects.equals(this.configs, v1alpha1KafkaTopicSpec.configs) &&
        Objects.equals(this.numPartitions, v1alpha1KafkaTopicSpec.numPartitions) &&
        Objects.equals(this.replicationFactor, v1alpha1KafkaTopicSpec.replicationFactor) &&
        Objects.equals(this.topicName, v1alpha1KafkaTopicSpec.topicName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clientConfigs, clientOverrides, configs, numPartitions, replicationFactor, topicName);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1KafkaTopicSpec {\n");
    sb.append("    clientConfigs: ").append(toIndentedString(clientConfigs)).append("\n");
    sb.append("    clientOverrides: ").append(toIndentedString(clientOverrides)).append("\n");
    sb.append("    configs: ").append(toIndentedString(configs)).append("\n");
    sb.append("    numPartitions: ").append(toIndentedString(numPartitions)).append("\n");
    sb.append("    replicationFactor: ").append(toIndentedString(replicationFactor)).append("\n");
    sb.append("    topicName: ").append(toIndentedString(topicName)).append("\n");
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

