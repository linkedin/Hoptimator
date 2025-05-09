package com.linkedin.hoptimator.k8s;

/** Maps an element name to the name of one of its pipelines. */
class K8sPipelineElementMapEntry {
  private String elementName;
  private String pipelineName;

  public K8sPipelineElementMapEntry(String elementName, String pipelineName) {
    this.elementName = elementName;
    this.pipelineName = pipelineName;
  }

  public String elementName() {
    return elementName;
  }

  public String pipelineName() {
    return pipelineName;
  }
}
