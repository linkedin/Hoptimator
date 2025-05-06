package com.linkedin.hoptimator.k8s.status;

/** Represents status of an element which belongs to a {@link com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline}. */
public class K8sPipelineElementStatus {
  private final String name;
  private final boolean ready;
  private final boolean failed;
  private final String message;

  public K8sPipelineElementStatus(String name, boolean ready, boolean failed, String message) {
    this.name = name;
    this.ready = ready;
    this.failed = failed;
    this.message = message;
  }

  /** Returns the name of this element. */
  public String getName() {
    return name;
  }

  /** Returns true if this element is ready. */
  public boolean isReady() {
    return ready;
  }

  /** Returns true if this element has failed . */
  public boolean isFailed() {
    return failed;
  }

  /** Returns the detail message string of this element . */
  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class K8sPipelineElementStatus {\n");
    sb.append("    name: ").append(ready).append("\n");
    sb.append("    ready: ").append(ready).append("\n");
    sb.append("    failed: ").append(failed).append("\n");
    sb.append("    message: ").append(message).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
