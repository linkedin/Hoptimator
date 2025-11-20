package com.linkedin.hoptimator;


/** An opaque user-defined job, analogous to a UDF. */
public class UserJob implements Deployable {

  private final String namespace;
  private final String name;

  public UserJob(String namespace, String name) {
    this.namespace = namespace;
    this.name = name;
  }

  public String namespace() {
    return namespace;
  }

  public String name() {
    return name;
  }
}
