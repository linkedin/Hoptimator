package com.linkedin.hoptimator.k8s.models;

import java.util.Objects;

import com.google.gson.annotations.SerializedName;


/**
 * V1alpha1TableStatus
 */
public class V1alpha1TableStatus {
  public static final String SERIALIZED_NAME_READY = "ready";
  @SerializedName(SERIALIZED_NAME_READY)
  private Boolean ready;

  public static final String SERIALIZED_NAME_MESSAGE = "message";
  @SerializedName(SERIALIZED_NAME_MESSAGE)
  private String message;


  public V1alpha1TableStatus ready(Boolean ready) {
    this.ready = ready;
    return this;
  }

  @javax.annotation.Nullable
  public Boolean getReady() {
    return ready;
  }

  public void setReady(Boolean ready) {
    this.ready = ready;
  }

  public V1alpha1TableStatus message(String message) {
    this.message = message;
    return this;
  }

  @javax.annotation.Nullable
  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1TableStatus that = (V1alpha1TableStatus) o;
    return Objects.equals(this.ready, that.ready) &&
        Objects.equals(this.message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ready, message);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1TableStatus {\n");
    sb.append("    ready: ").append(toIndentedString(ready)).append("\n");
    sb.append("    message: ").append(toIndentedString(message)).append("\n");
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
