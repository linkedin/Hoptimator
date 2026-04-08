package com.linkedin.hoptimator.avro;

import com.linkedin.hoptimator.Validator;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class AvroValidatorProviderTest {

  @Mock
  private SchemaPlus schemaPlus;

  @Test
  void testValidatorsReturnsAvroTableValidatorForSchemaPlus() {
    AvroValidatorProvider provider = new AvroValidatorProvider();

    Collection<Validator> validators = provider.validators(schemaPlus);

    assertEquals(1, validators.size());
    assertInstanceOf(AvroTableValidator.class, validators.iterator().next());
  }

  @Test
  void testValidatorsReturnsEmptyForNonSchemaPlus() {
    AvroValidatorProvider provider = new AvroValidatorProvider();

    Collection<Validator> validators = provider.validators("not-a-schema");

    assertTrue(validators.isEmpty());
  }

  @Test
  void testValidatorsReturnsEmptyForNull() {
    AvroValidatorProvider provider = new AvroValidatorProvider();

    Collection<Validator> validators = provider.validators(null);

    assertTrue(validators.isEmpty());
  }
}
