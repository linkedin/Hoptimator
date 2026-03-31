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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class ValidationServiceTest {

  @Mock
  private Connection mockConnection;

  @BeforeEach
  void setUp() {
    TestValidatorProvider.reset();
  }

  @AfterEach
  void tearDown() {
    TestValidatorProvider.reset();
  }

  @Test
  void testValidateWithNonCalciteConnectionThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> ValidationService.validate(mockConnection));
  }

  /**
   * <p>We create the table while errors are off so DDL succeeds, then enable errors
   * and call validate(). If walk() or the walk-subschema calls are removed, the provider
   * is never called and issues stays valid — the assertion fails.
   */
  @Test
  void testWalkVisitsTablesInSchema() throws SQLException {
    try (HoptimatorConnection conn =
        (HoptimatorConnection) DriverManager.getConnection("jdbc:hoptimator://")) {
      // Create the table while errors are disabled so the DDL itself does not fail
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("CREATE TABLE WALK_VST (X VARCHAR)");
      }
      TestValidatorProvider.enableErrors();
      // Use the underlying CalciteConnection — ValidationService requires CalciteConnection
      Validator.Issues issues = ValidationService.validate(conn.calciteConnection());
      assertFalse(issues.valid(),
          "walk() must visit tables so that TestValidatorProvider can record errors");
    }
  }

  // util catalog has sub-schemas; if walk() skips recursion, no errors fire.
  @Test
  void testWalkVisitsSubSchemas() throws SQLException {
    try (HoptimatorConnection conn =
        (HoptimatorConnection) DriverManager.getConnection("jdbc:hoptimator://catalogs=util")) {
      TestValidatorProvider.enableErrors();
      Validator.Issues issues = ValidationService.validate(conn.calciteConnection());
      assertFalse(issues.valid(),
          "walk() must recurse into sub-schemas so that errors in children are propagated");
    }
  }

  // Uses the util catalog (always has schemas) to ensure traversal fires.
  @Test
  void testValidateConnectionCallsWalk() throws SQLException {
    try (HoptimatorConnection conn =
        (HoptimatorConnection) DriverManager.getConnection("jdbc:hoptimator://catalogs=util")) {
      TestValidatorProvider.enableErrors();
      Validator.Issues issues = ValidationService.validate(conn.calciteConnection());
      assertFalse(issues.valid(),
          "validate(connection) must call walk() so that provider errors are propagated");
    }
  }

  /**
   * Removes the forEach in validate(obj, issues).
   * When TestValidatorProvider is in error mode, calling validate(obj, issues) must
   * record an error into issues. If the forEach call is removed, issues stays valid.
   */
  @Test
  void testValidateObjIssuesInvokesValidators() {
    TestValidatorProvider.enableErrors();
    Validator.Issues issues = new Validator.Issues("test");
    ValidationService.validate("any-object", issues);
    assertFalse(issues.valid(),
        "validate(obj, issues) must invoke validators; if forEach is removed no error fires");
    assertTrue(issues.toString().contains("injected error"),
        "Error message from TestValidatorProvider must appear in issues");
  }

  // When TestValidatorProvider is in error mode, validateOrThrow must throw SQLException.
  @Test
  void testValidateOrThrowSingleObjectThrowsWhenErrorRecorded() {
    TestValidatorProvider.enableErrors();
    assertThrows(SQLException.class,
        () -> ValidationService.validateOrThrow("any-object"),
        "validateOrThrow must throw when a provider records an error");
  }

  /**
   * Sanity check: no providers = no errors = no throw.
   */
  @Test
  void testValidateOrThrowSingleObjectPassesWhenValid() throws SQLException {
    // TestValidatorProvider is in no-error mode (reset in setUp)
    ValidationService.validateOrThrow("test-object");
  }

  @Test
  void testValidateOrThrowCollectionThrowsWhenErrorRecorded() {
    TestValidatorProvider.enableErrors();
    assertThrows(SQLException.class,
        () -> ValidationService.validateOrThrow(Arrays.asList("obj1", "obj2")),
        "validateOrThrow(Collection) must throw when provider records an error");
  }

  @Test
  void testValidateOrThrowCollectionPassesWhenValid() throws SQLException {
    Collection<String> validObjects = Arrays.asList("obj1", "obj2");
    ValidationService.validateOrThrow(validObjects);
  }

  @Test
  void testValidateOrThrowCollectionPassesWithEmptyCollection() throws SQLException {
    Collection<String> emptyCollection = Collections.emptyList();
    ValidationService.validateOrThrow(emptyCollection);
  }

  // TestValidatorProvider is registered via META-INF/services so ServiceLoader must find it.
  @Test
  void testProvidersReturnsAtLeastOneRegisteredProvider() {
    Collection<ValidatorProvider> providers = ValidationService.providers();
    assertNotNull(providers);
    assertFalse(providers.isEmpty(),
        "ServiceLoader must find TestValidatorProvider registered via META-INF/services");
  }

  // TestValidatorProvider is registered and, when in error mode, returns a non-empty list.
  @Test
  void testValidatorsReturnsValidatorsFromRegisteredProvider() {
    TestValidatorProvider.enableErrors();
    Collection<Validator> validators = ValidationService.validators("any-object");
    assertNotNull(validators);
    assertFalse(validators.isEmpty(),
        "validators() must return the non-empty list provided by TestValidatorProvider");
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
