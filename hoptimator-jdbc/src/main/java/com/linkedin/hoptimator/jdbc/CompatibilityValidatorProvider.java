package com.linkedin.hoptimator.jdbc;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.calcite.schema.SchemaPlus;

import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;


/** Provides BackwardCompatibilityValidator and ForwardCompatibilityValidator. */
public class CompatibilityValidatorProvider implements ValidatorProvider {

  @Override
  public <T> Collection<Validator> validators(T obj) {
    if (obj instanceof SchemaPlus) {
      return Arrays.asList(new Validator[]{new BackwardCompatibilityValidator((SchemaPlus) obj),
          new ForwardCompatibilityValidator((SchemaPlus) obj)});
    } else {
      return Collections.emptyList();
    }
  }
}
