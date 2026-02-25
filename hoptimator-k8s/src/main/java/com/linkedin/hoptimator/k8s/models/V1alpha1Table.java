package com.linkedin.hoptimator.k8s.models;

import java.util.Objects;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.openapi.models.V1ObjectMeta;


/**
 * A SQL table with connector configuration.
 */
public class V1alpha1Table implements io.kubernetes.client.common.KubernetesObject {
  public static final String SERIALIZED_NAME_API_VERSION = "apiVersion";
  @SerializedName(SERIALIZED_NAME_API_VERSION)
  private String apiVersion;

  public static final String SERIALIZED_NAME_KIND = "kind";
  @SerializedName(SERIALIZED_NAME_KIND)
  private String kind;

  public static final String SERIALIZED_NAME_METADATA = "metadata";
  @SerializedName(SERIALIZED_NAME_METADATA)
  private V1ObjectMeta metadata = null;

  public static final String SERIALIZED_NAME_SPEC = "spec";
  @SerializedName(SERIALIZED_NAME_SPEC)
  private V1alpha1TableSpec spec;

  public static final String SERIALIZED_NAME_STATUS = "status";
  @SerializedName(SERIALIZED_NAME_STATUS)
  private V1alpha1TableStatus status;


  public V1alpha1Table apiVersion(String apiVersion) {
    this.apiVersion = apiVersion;
    return this;
  }

  @javax.annotation.Nullable
  public String getApiVersion() {
    return apiVersion;
  }

  public void setApiVersion(String apiVersion) {
    this.apiVersion = apiVersion;
  }

  public V1alpha1Table kind(String kind) {
    this.kind = kind;
    return this;
  }

  @javax.annotation.Nullable
  public String getKind() {
    return kind;
  }

  public void setKind(String kind) {
    this.kind = kind;
  }

  public V1alpha1Table metadata(V1ObjectMeta metadata) {
    this.metadata = metadata;
    return this;
  }

  @javax.annotation.Nullable
  public V1ObjectMeta getMetadata() {
    return metadata;
  }

  public void setMetadata(V1ObjectMeta metadata) {
    this.metadata = metadata;
  }

  public V1alpha1Table spec(V1alpha1TableSpec spec) {
    this.spec = spec;
    return this;
  }

  @javax.annotation.Nullable
  public V1alpha1TableSpec getSpec() {
    return spec;
  }

  public void setSpec(V1alpha1TableSpec spec) {
    this.spec = spec;
  }

  public V1alpha1Table status(V1alpha1TableStatus status) {
    this.status = status;
    return this;
  }

  @javax.annotation.Nullable
  public V1alpha1TableStatus getStatus() {
    return status;
  }

  public void setStatus(V1alpha1TableStatus status) {
    this.status = status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1Table v1alpha1Table = (V1alpha1Table) o;
    return Objects.equals(this.apiVersion, v1alpha1Table.apiVersion) &&
        Objects.equals(this.kind, v1alpha1Table.kind) &&
        Objects.equals(this.metadata, v1alpha1Table.metadata) &&
        Objects.equals(this.spec, v1alpha1Table.spec) &&
        Objects.equals(this.status, v1alpha1Table.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(apiVersion, kind, metadata, spec, status);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1Table {\n");
    sb.append("    apiVersion: ").append(toIndentedString(apiVersion)).append("\n");
    sb.append("    kind: ").append(toIndentedString(kind)).append("\n");
    sb.append("    metadata: ").append(toIndentedString(metadata)).append("\n");
    sb.append("    spec: ").append(toIndentedString(spec)).append("\n");
    sb.append("    status: ").append(toIndentedString(status)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
