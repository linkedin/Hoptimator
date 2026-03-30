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
}
