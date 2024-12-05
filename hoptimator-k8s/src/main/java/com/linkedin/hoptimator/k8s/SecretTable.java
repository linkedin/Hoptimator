package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.RemoteTable;

import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1SecretList;

public class SecretTable extends RemoteTable<V1Secret, SecretTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public String NAME;
    public String TYPE;

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
    this(new K8sApi<V1Secret, V1SecretList>(context, K8sApiEndpoints.SECRETS));
  }

  @Override
  public Row toRow(V1Secret obj) {
    return new Row(obj.getMetadata().getName(), obj.getType());
  }
}
