package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Validator;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class CompatibilityValidatorProviderTest {

  @Mock
  private SchemaPlus mockSchema;

  @Test
  void testValidatorsReturnsTwoValidatorsForSchemaPlus() {
    CompatibilityValidatorProvider provider = new CompatibilityValidatorProvider();

    Collection<Validator> validators = provider.validators(mockSchema, null);

    assertEquals(2, validators.size());
  }

  @Test
  void testValidatorsReturnsEmptyForNonSchemaPlus() {
    CompatibilityValidatorProvider provider = new CompatibilityValidatorProvider();

    Collection<Validator> validators = provider.validators("not-a-schema", null);

    assertTrue(validators.isEmpty());
  }

  @Test
  void testValidatorsReturnsEmptyForNull() {
    CompatibilityValidatorProvider provider = new CompatibilityValidatorProvider();

    Collection<Validator> validators = provider.validators(null, null);

    assertTrue(validators.isEmpty());
  }
}
