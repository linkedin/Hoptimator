package com.linkedin.hoptimator.avro;

import java.util.Collection;
import java.util.Collections;

import org.apache.calcite.schema.SchemaPlus;

import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;


/** Provides AvroValidator. */
public class AvroValidatorProvider implements ValidatorProvider {

  @SuppressWarnings("unchecked")
  @Override
  public <T> Collection<Validator<T>> validators(Class<T> clazz) {
    if (SchemaPlus.class.isAssignableFrom(clazz)) {
      return Collections.singletonList((Validator<T>) new AvroTableValidator());
    } else {
      return Collections.emptyList();
    }
  }
}
