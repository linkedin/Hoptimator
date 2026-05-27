package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.Engine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertNotNull;


@ExtendWith(MockitoExtension.class)
class RemoteToEnumerableConverterRuleTest {

  @Mock
  private Engine mockEngine;

  @Mock
  private Connection mockConnection;

  // If create() returns null (NullReturnVals), assertNotNull fails.
  @Test
  void testCreateReturnsNonNull() {
    RemoteConvention convention = new RemoteConvention("test-remote", mockEngine);

    RemoteToEnumerableConverterRule rule = RemoteToEnumerableConverterRule.create(convention, mockConnection);

    assertNotNull(rule, "create() must return a non-null RemoteToEnumerableConverterRule");
  }

  @Test
  void testCreatedRuleHasCorrectDescription() {
    RemoteConvention convention = new RemoteConvention("test-remote", mockEngine);

    RemoteToEnumerableConverterRule rule = RemoteToEnumerableConverterRule.create(convention, mockConnection);

    assertNotNull(rule.toString(), "created rule must have a non-null description");
  }
}
