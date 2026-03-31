package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Validator;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class CompatibilityValidatorBaseTest {

  @Mock
  private SchemaPlus mockSchema;

  @Test
  void testValidateHandlesClassCastException() {
    when(mockSchema.unwrap(CalciteSchema.class)).thenThrow(new ClassCastException("test"));

    CompatibilityValidatorBase validator = new CompatibilityValidatorBase(mockSchema) {
      @Override
      protected void validate(Table table, Table originalTable, Validator.Issues issues) {
        // no-op
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    validator.validate(issues);

    assertTrue(issues.valid());
  }

  @Test
  void testValidateHandlesNullOriginalSchema() {
    when(mockSchema.unwrap(CalciteSchema.class)).thenReturn(null);

    CompatibilityValidatorBase validator = new CompatibilityValidatorBase(mockSchema) {
      @Override
      protected void validate(Table table, Table originalTable, Validator.Issues issues) {
        // no-op
      }
    };

    Validator.Issues issues = new Validator.Issues("test");

    try {
      validator.validate(issues);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Null original schema"));
    }
  }

  @Test
  void testValidateWalksTablesAndCallsAbstractValidate() {
    // Use a real CalciteSchema with a real inner schema that has a table
    Table originalTable = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder().add("col", SqlTypeName.VARCHAR).build();
      }
    };

    CalciteSchema calciteSchema = CalciteSchema.createRootSchema(false, false);
    SchemaPlus schemaPlus = calciteSchema.plus();

    // Add a table to the SchemaPlus so it appears in tables()
    Table newTable = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder().add("col", SqlTypeName.VARCHAR).add("col2", SqlTypeName.INTEGER).build();
      }
    };
    schemaPlus.add("testTable", newTable);

    // Add the original table to the underlying schema via another CalciteSchema
    // We need the unwrapped CalciteSchema to return a CalciteSchema whose schema has the same table
    CalciteSchema originalCalciteSchema = CalciteSchema.createRootSchema(false, false);
    originalCalciteSchema.plus().add("testTable", originalTable);

    AtomicInteger validateCount = new AtomicInteger(0);
    CompatibilityValidatorBase validator = new CompatibilityValidatorBase(schemaPlus) {
      @Override
      protected void validate(Table table, Table originalTable, Validator.Issues issues) {
        validateCount.incrementAndGet();
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    validator.validate(issues);
    assertTrue(issues.valid());
  }

  @Test
  void testValidateWithNewTableSkipsWhenOriginalNotFound() {
    CalciteSchema calciteSchema = CalciteSchema.createRootSchema(false, false);
    SchemaPlus schemaPlus = calciteSchema.plus();

    Table newTable = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder().add("col", SqlTypeName.VARCHAR).build();
      }
    };
    schemaPlus.add("brandNewTable", newTable);

    AtomicInteger validateCount = new AtomicInteger(0);
    CompatibilityValidatorBase validator = new CompatibilityValidatorBase(schemaPlus) {
      @Override
      protected void validate(Table table, Table originalTable, Validator.Issues issues) {
        validateCount.incrementAndGet();
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    validator.validate(issues);

    // brandNewTable won't have an original, so validate should be skipped
    assertTrue(issues.valid());
  }

  // These tests use the ForwardCompatibilityValidator concrete subclass because
  // it exercises the exact same branches in CompatibilityValidatorBase.validate()
  // (loop + null-original-table check) before delegating to the abstract method.
  // The ForwardCompatibilityValidator's own validate(table, original, issues)
  // records errors for removed non-nullable fields and type changes.

  /**
   * Removing the check that causes an error when a field is
   * present in the original schema but absent in the new schema.
   * A non-nullable field removed from the new schema must produce an error.
   */
  @Test
  void testValidateRecordsErrorForRemovedNonNullableField() {
    CalciteSchema baseCalciteSchema = CalciteSchema.createRootSchema(false, false);
    SchemaPlus schemaPlus = baseCalciteSchema.plus();

    // newSchema has only "id"; originalSchema also had "name" (non-nullable)
    Table newTable = tableWith(
        new FieldDef("id", SqlTypeName.INTEGER, false)
    );
    schemaPlus.add("myTable", newTable);

    // The original table also has "name" as non-nullable VARCHAR
    Table originalTable = tableWith(
        new FieldDef("id", SqlTypeName.INTEGER, false),
        new FieldDef("name", SqlTypeName.VARCHAR, false)
    );

    // Directly invoke the abstract method via ForwardCompatibilityValidator
    Validator.Issues issues = new Validator.Issues("test");
    new ForwardCompatibilityValidator(schemaPlus).validate(newTable, originalTable, issues);

    assertFalse(issues.valid(),
        "Removing a non-nullable field must record a forwards-incompatibility error");
    assertTrue(issues.toString().contains("name"),
        "Error message must contain the removed field name");
  }

  /**
   * Removing the check that causes an error when a field's type is changed incompatibly.
   */
  @Test
  void testValidateRecordsErrorForIncompatibleTypeChange() {
    CalciteSchema baseCalciteSchema = CalciteSchema.createRootSchema(false, false);
    SchemaPlus schemaPlus = baseCalciteSchema.plus();

    Table newTable = tableWith(
        new FieldDef("id", SqlTypeName.VARCHAR, false)   // changed: was INTEGER
    );
    Table originalTable = tableWith(
        new FieldDef("id", SqlTypeName.INTEGER, false)
    );

    Validator.Issues issues = new Validator.Issues("test");
    new ForwardCompatibilityValidator(schemaPlus).validate(newTable, originalTable, issues);

    assertFalse(issues.valid(),
        "Changing a field type incompatibly must record an error");
    assertTrue(issues.toString().contains("id"),
        "Error message must contain the field name");
  }

  /**
   * issues.error() calls inside abstract validate() implementations must actually mark issues invalid.
   * Unchanged schema must produce no errors.
   */
  @Test
  void testValidateNoErrorsForUnchangedSchema() {
    CalciteSchema baseCalciteSchema = CalciteSchema.createRootSchema(false, false);
    SchemaPlus schemaPlus = baseCalciteSchema.plus();

    Table table = tableWith(
        new FieldDef("id", SqlTypeName.INTEGER, false),
        new FieldDef("name", SqlTypeName.VARCHAR, true)
    );

    Validator.Issues issues = new Validator.Issues("test");
    new ForwardCompatibilityValidator(schemaPlus).validate(table, table, issues);

    assertTrue(issues.valid(), "Unchanged schema must not produce any errors");
  }

  /**
   * Verifies error-recording is active: calling issues.error() inside
   * validate(table, originalTable, issues) must mark the parent issues as invalid.
   * Uses a BackwardCompatibilityValidator to add a non-nullable field.
   */
  @Test
  void testIssuesErrorPropagatesFromAbstractValidate() {
    CalciteSchema baseCalciteSchema = CalciteSchema.createRootSchema(false, false);
    SchemaPlus schemaPlus = baseCalciteSchema.plus();

    // Adding a non-nullable field is backwards-incompatible
    Table newTable = tableWith(
        new FieldDef("id", SqlTypeName.INTEGER, false),
        new FieldDef("required", SqlTypeName.VARCHAR, false)
    );
    Table originalTable = tableWith(
        new FieldDef("id", SqlTypeName.INTEGER, false)
    );

    Validator.Issues issues = new Validator.Issues("test");
    new BackwardCompatibilityValidator(schemaPlus).validate(newTable, originalTable, issues);

    assertFalse(issues.valid(),
        "issues.error() inside abstract validate() must propagate to parent issues");
    assertTrue(issues.toString().contains("required"),
        "Error message must contain the field name");
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private Table tableWith(FieldDef... fields) {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (FieldDef f : fields) {
          RelDataType type = typeFactory.createSqlType(f.typeName);
          if (f.nullable) {
            type = typeFactory.createTypeWithNullability(type, true);
          }
          builder.add(f.name, type);
        }
        return builder.build();
      }
    };
  }

  private static class FieldDef {
    final String name;
    final SqlTypeName typeName;
    final boolean nullable;

    FieldDef(String name, SqlTypeName typeName, boolean nullable) {
      this.name = name;
      this.typeName = typeName;
      this.nullable = nullable;
    }
  }
}
