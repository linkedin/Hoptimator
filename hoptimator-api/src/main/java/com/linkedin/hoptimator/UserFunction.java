package com.linkedin.hoptimator;

import java.util.Collections;
import java.util.Map;


public class UserFunction implements Deployable {

  private final String name;
  private final String as;
  private final String namespace;
  private final Map<String, String> options;

  public UserFunction(String name, String as, String namespace, Map<String, String> options) {
    this.name = name;
    this.as = as;
    this.namespace = namespace;
    this.options = options != null ? options : Collections.emptyMap();
  }

  public String name() {
    return name;
  }

  public String as() {
    return as;
  }

  public String namespace() {
    return namespace;
  }

  public Map<String, String> options() {
    return options;
  }

  @Override
  public String toString() {
    return "UserFunction[" + name + "]";
  }
}
