package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;

import java.util.Collection;
import java.util.Collections;

/** Provides DefaultValidator. */
public class DefaultValidatorProvider implements ValidatorProvider {

  @Override
  public <T> Collection<Validator<T>> validators(Class<T> clazz) {
    return Collections.singletonList(new Validator.DefaultValidator<T>());
  }
}
