package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.PendingDelete;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;


class K8sValidatorProviderTest {

  private final K8sValidatorProvider provider = new K8sValidatorProvider();

  private static Source source() {
    return new Source("kafka-database", Arrays.asList("KAFKA", "some-topic"), Collections.emptyMap());
  }

  @Test
  void rawSourceWithoutDeleteIntentReturnsNoValidators() {
    // The dep-guard validator is opt-in: it only fires when the caller wraps in PendingDelete.
    // Validating a raw Source (e.g. from a future CREATE-time hook) must NOT trigger pre-delete
    // checks.
    Connection connection = mock(Connection.class);
    Collection<Validator> validators = provider.validators(source(), connection);
    assertTrue(validators.isEmpty(), "raw Source must not get a pre-delete validator");
  }

  @Test
  void pendingDeleteOfSourceReturnsDependencyValidator() {
    Connection connection = mock(Connection.class);
    Collection<Validator> validators = provider.validators(new PendingDelete<>(source()), connection);
    assertEquals(1, validators.size());
    assertTrue(validators.iterator().next() instanceof K8sPipelineDependencyValidator);
  }

  @Test
  void pendingDeleteOfNonSourceReturnsNoValidators() {
    // PendingDelete<NotASource> is not our concern — return nothing rather than over-fire.
    Connection connection = mock(Connection.class);
    Collection<Validator> validators = provider.validators(new PendingDelete<>("not-a-source"), connection);
    assertTrue(validators.isEmpty());
  }

  @Test
  void nullConnectionStillReturnsTheValidator() {
    // The provider is connection-agnostic: it just decides which validators apply based on the
    // object type. The validator itself fails fast at validate-time if the connection is missing.
    Collection<Validator> validators = provider.validators(new PendingDelete<>(source()), null);
    assertEquals(1, validators.size());
  }
}
