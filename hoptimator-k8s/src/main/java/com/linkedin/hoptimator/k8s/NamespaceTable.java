package com.linkedin.hoptimator.k8s;

import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;

import com.linkedin.hoptimator.util.RemoteTable;


public class NamespaceTable extends RemoteTable<V1Namespace, NamespaceTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public String NAME;
    public String STATUS;

    public Row(String name, String status) {
      this.NAME = name;
      this.STATUS = status;
    }
  }
  // CHECKSTYLE:ON

  public NamespaceTable(K8sApi<V1Namespace, V1NamespaceList> api) {
    super(api, Row.class);
  }

  public NamespaceTable(K8sContext context) {
    this(new K8sApi<V1Namespace, V1NamespaceList>(context, K8sApiEndpoints.NAMESPACES));
  }

  @Override
  public Row toRow(V1Namespace obj) {
    return new Row(obj.getMetadata().getName(), obj.getStatus().getPhase());
  }
}
