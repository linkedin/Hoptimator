package com.linkedin.hoptimator.k8s.status;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sPipelineElementStatusTest {

  @Test
  void getNameReturnsName() {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("my-element", true, false, "Ready");
    assertEquals("my-element", status.getName());
  }

  @Test
  void isReadyReturnsTrue() {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("el", true, false, "OK");
    assertTrue(status.isReady());
  }

  @Test
  void isReadyReturnsFalse() {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("el", false, false, "Pending");
    assertFalse(status.isReady());
  }

  @Test
  void isFailedReturnsTrue() {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("el", false, true, "Error");
    assertTrue(status.isFailed());
  }

  @Test
  void isFailedReturnsFalse() {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("el", true, false, "OK");
    assertFalse(status.isFailed());
  }

  @Test
  void getMessageReturnsMessage() {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("el", true, false, "All good");
    assertEquals("All good", status.getMessage());
  }

  @Test
  void toStringContainsFields() {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("my-element", true, false, "Ready");
    String str = status.toString();
    assertTrue(str.contains("ready: true"));
    assertTrue(str.contains("failed: false"));
    assertTrue(str.contains("message: Ready"));
  }
}
