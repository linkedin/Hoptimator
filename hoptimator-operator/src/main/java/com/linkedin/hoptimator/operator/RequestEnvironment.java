package com.linkedin.hoptimator.operator;

import io.kubernetes.client.extended.controller.reconciler.Request;

import com.linkedin.hoptimator.catalog.Resource;

/** Exposes variables to resource templates */
public class RequestEnvironment extends Resource.SimpleEnvironment {

  public RequestEnvironment(Request request) {
    export("namespace", request.getNamespace());
    export("name", request.getName());
  }
}
