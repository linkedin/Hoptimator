package com.linkedin.hoptimator.k8s.models;

import java.util.Objects;

import com.google.gson.annotations.SerializedName;


/**
 * A column definition within a Table spec.
 */
public class V1alpha1TableSpecColumn {
  public static final String SERIALIZED_NAME_NAME = "name";
  @SerializedName(SERIALIZED_NAME_NAME)
  private String name;

  public static final String SERIALIZED_NAME_TYPE = "type";
  @SerializedName(SERIALIZED_NAME_TYPE)
  private String type;

  public static final String SERIALIZED_NAME_NULLABLE = "nullable";
  @SerializedName(SERIALIZED_NAME_NULLABLE)
  private Boolean nullable;


  public V1alpha1TableSpecColumn name(String name) {
    this.name = name;
    return this;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public V1alpha1TableSpecColumn type(String type) {
    this.type = type;
    return this;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public V1alpha1TableSpecColumn nullable(Boolean nullable) {
    this.nullable = nullable;
    return this;
  }

  @javax.annotation.Nullable
  public Boolean getNullable() {
    return nullable;
  }

  public void setNullable(Boolean nullable) {
    this.nullable = nullable;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1TableSpecColumn that = (V1alpha1TableSpecColumn) o;
    return Objects.equals(this.name, that.name) &&
        Objects.equals(this.type, that.type) &&
        Objects.equals(this.nullable, that.nullable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, nullable);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1TableSpecColumn {\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    nullable: ").append(toIndentedString(nullable)).append("\n");
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
