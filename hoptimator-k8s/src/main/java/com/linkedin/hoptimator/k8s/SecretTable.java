package com.linkedin.hoptimator.k8s;

import java.util.Objects;

import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1SecretList;

import com.linkedin.hoptimator.util.RemoteTable;


public class SecretTable extends RemoteTable<V1Secret, SecretTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public final String NAME;
    public final String TYPE;

    public Row(String name, String type) {
      this.NAME = name;
      this.TYPE = type;
    }
  }
  // CHECKSTYLE:ON

  public SecretTable(K8sApi<V1Secret, V1SecretList> api) {
    super(api, Row.class);
  }

  public SecretTable(K8sContext context) {
    this(new K8sApi<>(context, K8sApiEndpoints.SECRETS));
  }

  @Override
  public Row toRow(V1Secret obj) {
    return new Row(Objects.requireNonNull(obj.getMetadata()).getName(), obj.getType());
  }
}
