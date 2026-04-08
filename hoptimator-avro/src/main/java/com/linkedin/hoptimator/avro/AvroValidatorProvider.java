package com.linkedin.hoptimator.avro;

import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Collection;
import java.util.Collections;


/** Provides AvroValidator. */
public class AvroValidatorProvider implements ValidatorProvider {

  @Override
  public <T> Collection<Validator> validators(T obj) {
    if (obj instanceof SchemaPlus) {
      return Collections.singletonList(new AvroTableValidator((SchemaPlus) obj));
    } else {
      return Collections.emptyList();
    }
  }
}
