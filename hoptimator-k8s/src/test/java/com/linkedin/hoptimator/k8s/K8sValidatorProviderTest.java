package com.linkedin.hoptimator.k8s;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.PendingDelete;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sValidatorProviderTest {

  private static Source testSource() {
    return new Source("kafka1", List.of("KAFKA", "my-topic"), Map.of());
  }

  @Test
  void returnsDependencyValidatorForPendingDeleteOfSource() {
    K8sValidatorProvider provider = new K8sValidatorProvider();
    PendingDelete<Source> pd = new PendingDelete<>(testSource());

    Collection<Validator> validators = provider.validators(pd, null);

    assertEquals(1, validators.size());
    assertInstanceOf(K8sDependencyValidator.class, validators.iterator().next());
  }

  @Test
  void returnsDependencyValidatorWhenSelfOwnerIsSet() {
    // Self-owner fields are stored on the validator; this exercises the (kind, name) plumbing
    // through the provider boundary.
    K8sValidatorProvider provider = new K8sValidatorProvider();
    PendingDelete<Source> pd = new PendingDelete<>(testSource(), "LogicalTable", "my-table");

    Collection<Validator> validators = provider.validators(pd, null);

    assertEquals(1, validators.size());
    assertInstanceOf(K8sDependencyValidator.class, validators.iterator().next());
  }

  @Test
  void returnsEmptyForRawSourceWithoutPendingDeleteWrapper() {
    // The validator only fires for a delete-intent signal — not for a plain Source. Other callers
    // of ValidationService.validate(source, ...) must NOT trigger the K8s pipeline lookup.
    K8sValidatorProvider provider = new K8sValidatorProvider();

    Collection<Validator> validators = provider.validators(testSource(), null);

    assertTrue(validators.isEmpty());
  }

  @Test
  void returnsEmptyForPendingDeleteOfNonSourceTarget() {
    K8sValidatorProvider provider = new K8sValidatorProvider();
    PendingDelete<String> pd = new PendingDelete<>("not-a-source");

    Collection<Validator> validators = provider.validators(pd, null);

    assertTrue(validators.isEmpty());
  }

  @Test
  void returnsEmptyForUnrelatedTypes() {
    K8sValidatorProvider provider = new K8sValidatorProvider();

    assertTrue(provider.validators("just-a-string", null).isEmpty());
    assertTrue(provider.validators(42, null).isEmpty());
    assertTrue(provider.validators(Collections.emptyList(), null).isEmpty());
  }
}
