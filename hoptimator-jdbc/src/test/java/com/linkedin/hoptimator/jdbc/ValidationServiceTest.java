package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class ValidationServiceTest {

  @BeforeEach
  void setUp() {
    ValidatorProviderTest.reset();
  }

  @AfterEach
  void tearDown() {
    ValidatorProviderTest.reset();
  }

  /**
   * Removes the forEach in validate(obj, issues).
   * When ValidatorProviderTest is in error mode, calling validate(obj, issues) must
   * record an error into issues. If the forEach call is removed, issues stays valid.
   */
  @Test
  void testValidateObjIssuesInvokesValidators() throws SQLException {
    ValidatorProviderTest.enableErrors();
    Validator.Issues issues = new Validator.Issues("test");
    ValidationService.validate("any-object", issues, null);
    assertFalse(issues.valid(),
        "validate(obj, issues) must invoke validators; if forEach is removed no error fires");
    assertTrue(issues.toString().contains("injected error"),
        "Error message from ValidatorProviderTest must appear in issues");
  }

  // When ValidatorProviderTest is in error mode, validateOrThrow must throw SQLException.
  @Test
  void testValidateOrThrowSingleObjectThrowsWhenErrorRecorded() {
    ValidatorProviderTest.enableErrors();
    assertThrows(SQLException.class,
        () -> ValidationService.validateOrThrow("any-object", null),
        "validateOrThrow must throw when a provider records an error");
  }

  /**
   * Sanity check: no providers = no errors = no throw.
   */
  @Test
  void testValidateOrThrowSingleObjectPassesWhenValid() throws SQLException {
    // ValidatorProviderTest is in no-error mode (reset in setUp)
    ValidationService.validateOrThrow("test-object", null);
  }

  @Test
  void testValidateOrThrowCollectionThrowsWhenErrorRecorded() {
    ValidatorProviderTest.enableErrors();
    assertThrows(SQLException.class,
        () -> ValidationService.validateOrThrow(Arrays.asList("obj1", "obj2"), null),
        "validateOrThrow(Collection) must throw when provider records an error");
  }

  @Test
  void testValidateOrThrowCollectionPassesWhenValid() throws SQLException {
    Collection<String> validObjects = Arrays.asList("obj1", "obj2");
    ValidationService.validateOrThrow(validObjects, null);
  }

  @Test
  void testValidateOrThrowCollectionPassesWithEmptyCollection() throws SQLException {
    Collection<String> emptyCollection = Collections.emptyList();
    ValidationService.validateOrThrow(emptyCollection, null);
  }

  // ValidatorProviderTest is registered via META-INF/services so ServiceLoader must find it.
  @Test
  void testProvidersReturnsAtLeastOneRegisteredProvider() {
    Collection<ValidatorProvider> providers = ValidationService.providers();
    assertNotNull(providers);
    assertFalse(providers.isEmpty(),
        "ServiceLoader must find ValidatorProviderTest registered via META-INF/services");
  }

  // ValidatorProviderTest is registered and, when in error mode, returns a non-empty list.
  @Test
  void testValidatorsReturnsValidatorsFromRegisteredProvider() throws SQLException {
    ValidatorProviderTest.enableErrors();
    Collection<Validator> validators = ValidationService.validators("any-object", null);
    assertNotNull(validators);
    assertFalse(validators.isEmpty(),
        "validators() must return the non-empty list provided by ValidatorProviderTest");
  }

  @Test
  void testProvidersReturnsCollection() {
    Collection<ValidatorProvider> providers = ValidationService.providers();
    assertNotNull(providers);
  }

  @Test
  void testValidatorsReturnsCollection() throws SQLException {
    Collection<Validator> validators = ValidationService.validators("test-object", null);
    assertNotNull(validators);
  }

  @Test
  void testValidatePopulatesIssues() throws SQLException {
    Validator.Issues issues = new Validator.Issues("test");
    ValidationService.validate("test-object", issues, null);
    assertNotNull(issues);
  }

  // -------------------------------------------------------------------------
  // Table helper
  // -------------------------------------------------------------------------

  private static AbstractTable tableWithVarcharColumn(String columnName) {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder().add(columnName, SqlTypeName.VARCHAR).build();
      }
    };
  }
}
