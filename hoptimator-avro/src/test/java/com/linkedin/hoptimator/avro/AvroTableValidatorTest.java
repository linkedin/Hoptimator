package com.linkedin.hoptimator.avro;

import com.linkedin.hoptimator.Validator;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class AvroTableValidatorTest {

  @Mock
  private SchemaPlus schema;

  @Test
  void testValidateCatchesClassCastExceptionSilently() {
    when(schema.unwrap(CalciteSchema.class)).thenThrow(new ClassCastException("test"));

    AvroTableValidator validator = new AvroTableValidator(schema);
    Validator.Issues issues = new Validator.Issues("test");
    validator.validate(issues);

    assertTrue(issues.valid(), "ClassCastException should be silently caught");
  }

  @Test
  void testValidateThrowsForNullOriginalSchema() {
    when(schema.unwrap(CalciteSchema.class)).thenReturn(null);

    AvroTableValidator validator = new AvroTableValidator(schema);
    Validator.Issues issues = new Validator.Issues("test");

    assertThrows(IllegalArgumentException.class, () -> validator.validate(issues));
  }
}
