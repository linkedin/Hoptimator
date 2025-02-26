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
  public <T> Collection<Validator> validators(T obj) {
    if (obj instanceof SchemaPlus) {
      return Collections.singletonList(new AvroTableValidator((SchemaPlus) obj));
    } else {
      return Collections.emptyList();
    }
  }
}
