package com.linkedin.hoptimator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


class UserJobTest {

  @Test
  void testAccessors() {
    UserJob userJob = new UserJob("myNamespace", "myJob");
    assertEquals("myNamespace", userJob.namespace());
    assertEquals("myJob", userJob.name());
  }
}
