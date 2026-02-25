package com.linkedin.hoptimator.k8s.models;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.openapi.models.V1ListMeta;


/**
 * TableList is a list of Table
 */
public class V1alpha1TableList implements io.kubernetes.client.common.KubernetesListObject {
  public static final String SERIALIZED_NAME_API_VERSION = "apiVersion";
  @SerializedName(SERIALIZED_NAME_API_VERSION)
  private String apiVersion;

  public static final String SERIALIZED_NAME_ITEMS = "items";
  @SerializedName(SERIALIZED_NAME_ITEMS)
  private List<V1alpha1Table> items = new ArrayList<>();

  public static final String SERIALIZED_NAME_KIND = "kind";
  @SerializedName(SERIALIZED_NAME_KIND)
  private String kind;

  public static final String SERIALIZED_NAME_METADATA = "metadata";
  @SerializedName(SERIALIZED_NAME_METADATA)
  private V1ListMeta metadata = null;


  public V1alpha1TableList apiVersion(String apiVersion) {
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

  public V1alpha1TableList items(List<V1alpha1Table> items) {
    this.items = items;
    return this;
  }

  public V1alpha1TableList addItemsItem(V1alpha1Table itemsItem) {
    this.items.add(itemsItem);
    return this;
  }

  public List<V1alpha1Table> getItems() {
    return items;
  }

  public void setItems(List<V1alpha1Table> items) {
    this.items = items;
  }

  public V1alpha1TableList kind(String kind) {
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

  public V1alpha1TableList metadata(V1ListMeta metadata) {
    this.metadata = metadata;
    return this;
  }

  @javax.annotation.Nullable
  public V1ListMeta getMetadata() {
    return metadata;
  }

  public void setMetadata(V1ListMeta metadata) {
    this.metadata = metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1TableList v1alpha1TableList = (V1alpha1TableList) o;
    return Objects.equals(this.apiVersion, v1alpha1TableList.apiVersion) &&
        Objects.equals(this.items, v1alpha1TableList.items) &&
        Objects.equals(this.kind, v1alpha1TableList.kind) &&
        Objects.equals(this.metadata, v1alpha1TableList.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(apiVersion, items, kind, metadata);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1TableList {\n");
    sb.append("    apiVersion: ").append(toIndentedString(apiVersion)).append("\n");
    sb.append("    items: ").append(toIndentedString(items)).append("\n");
    sb.append("    kind: ").append(toIndentedString(kind)).append("\n");
    sb.append("    metadata: ").append(toIndentedString(metadata)).append("\n");
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
