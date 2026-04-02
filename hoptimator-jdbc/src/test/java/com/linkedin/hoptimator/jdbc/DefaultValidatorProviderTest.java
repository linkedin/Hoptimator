package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Validated;
import com.linkedin.hoptimator.Validator;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class DefaultValidatorProviderTest {

  @Test
  void testValidatorsReturnsValidatorForValidatedObject() {
    DefaultValidatorProvider provider = new DefaultValidatorProvider();
    Validated validated = issues -> issues.error("test error");

    Collection<Validator> validators = provider.validators(validated);

    assertEquals(1, validators.size());
  }

  @Test
  void testValidatorsReturnsEmptyForNonValidatedObject() {
    DefaultValidatorProvider provider = new DefaultValidatorProvider();

    Collection<Validator> validators = provider.validators("not-validated");

    assertTrue(validators.isEmpty());
  }

  @Test
  void testValidatorsReturnsEmptyForNull() {
    DefaultValidatorProvider provider = new DefaultValidatorProvider();

    Collection<Validator> validators = provider.validators(null);

    assertTrue(validators.isEmpty());
  }

  @Test
  void testReturnedValidatorDelegates() {
    DefaultValidatorProvider provider = new DefaultValidatorProvider();
    Validated validated = issues -> issues.error("validation failed");

    Collection<Validator> validators = provider.validators(validated);
    Validator.Issues issues = new Validator.Issues("test");
    validators.iterator().next().validate(issues);

    assertTrue(issues.toString().contains("validation failed"));
  }
}
