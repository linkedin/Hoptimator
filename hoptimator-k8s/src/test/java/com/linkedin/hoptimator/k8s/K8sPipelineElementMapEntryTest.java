package com.linkedin.hoptimator.k8s;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


class K8sPipelineElementMapEntryTest {

  @Test
  void elementNameReturnsName() {
    K8sPipelineElementMapEntry entry = new K8sPipelineElementMapEntry("my-element", "my-pipeline");
    assertEquals("my-element", entry.elementName());
  }

  @Test
  void pipelineNameReturnsName() {
    K8sPipelineElementMapEntry entry = new K8sPipelineElementMapEntry("my-element", "my-pipeline");
    assertEquals("my-pipeline", entry.pipelineName());
  }
}
