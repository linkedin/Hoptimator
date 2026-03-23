package com.linkedin.hoptimator.jdbc;

import java.util.Collection;

import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linkedin.hoptimator.Validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class CompatibilityValidatorProviderTest {

  @Mock
  private SchemaPlus mockSchema;

  @Test
  void testValidatorsReturnsTwoValidatorsForSchemaPlus() {
    CompatibilityValidatorProvider provider = new CompatibilityValidatorProvider();

    Collection<Validator> validators = provider.validators(mockSchema);

    assertEquals(2, validators.size());
  }

  @Test
  void testValidatorsReturnsEmptyForNonSchemaPlus() {
    CompatibilityValidatorProvider provider = new CompatibilityValidatorProvider();

    Collection<Validator> validators = provider.validators("not-a-schema");

    assertTrue(validators.isEmpty());
  }

  @Test
  void testValidatorsReturnsEmptyForNull() {
    CompatibilityValidatorProvider provider = new CompatibilityValidatorProvider();

    Collection<Validator> validators = provider.validators(null);

    assertTrue(validators.isEmpty());
  }
}
