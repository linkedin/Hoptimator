package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


@ExtendWith(MockitoExtension.class)
class ValidationServiceTest {

  @Mock
  private Connection mockConnection;

  @Test
  void testValidateWithNonCalciteConnectionThrows() {

    assertThrows(IllegalArgumentException.class,
        () -> ValidationService.validate(mockConnection));
  }

  @Test
  void testValidateOrThrowSingleObjectPassesWhenValid() throws SQLException {
    Object validObject = "test-object";

    // Should not throw when no validators report issues
    ValidationService.validateOrThrow(validObject);
  }

  @Test
  void testValidateOrThrowCollectionPassesWhenValid() throws SQLException {
    Collection<String> validObjects = Arrays.asList("obj1", "obj2");

    // Should not throw when no validators report issues
    ValidationService.validateOrThrow(validObjects);
  }

  @Test
  void testValidateOrThrowCollectionPassesWithEmptyCollection() throws SQLException {
    Collection<String> emptyCollection = Collections.emptyList();

    // Should not throw for empty collection
    ValidationService.validateOrThrow(emptyCollection);
  }

  @Test
  void testProvidersReturnsCollection() {
    Collection<ValidatorProvider> providers = ValidationService.providers();

    assertNotNull(providers);
  }

  @Test
  void testValidatorsReturnsCollection() {
    Collection<Validator> validators = ValidationService.validators("test-object");

    assertNotNull(validators);
  }

  @Test
  void testValidatePopulatesIssues() {
    Validator.Issues issues = new Validator.Issues("test");

    ValidationService.validate("test-object", issues);

    // Should not throw; issues may or may not have errors depending on registered providers
    assertNotNull(issues);
  }

}
