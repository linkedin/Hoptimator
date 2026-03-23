package com.linkedin.hoptimator.util.planner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linkedin.hoptimator.Engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@ExtendWith(MockitoExtension.class)
class RemoteConventionTest {

  @Mock
  private Engine mockEngine;

  @Test
  void testRemoteConventionStoresEngine() {
    RemoteConvention convention = new RemoteConvention("test-remote", mockEngine);

    assertEquals(mockEngine, convention.engine());
    assertNotNull(convention.getName());
  }
}
