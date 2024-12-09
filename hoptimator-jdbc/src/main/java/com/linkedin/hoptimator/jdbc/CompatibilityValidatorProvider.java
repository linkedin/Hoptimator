package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;

import org.apache.calcite.schema.SchemaPlus;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/** Provides BackwardCompatibilityValidator and ForwardCompatibilityValidator. */
public class CompatibilityValidatorProvider implements ValidatorProvider {

  @SuppressWarnings("unchecked")
  @Override
  public <T> Collection<Validator<T>> validators(Class<T> clazz) {
    if (SchemaPlus.class.isAssignableFrom(clazz)) {
      return Arrays.asList(new Validator[]{
          (Validator<T>) new BackwardCompatibilityValidator(),
          (Validator<T>) new ForwardCompatibilityValidator()});
    } else {
      return Collections.emptyList();
    }
  }
}