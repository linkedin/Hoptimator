package com.linkedin.hoptimator.avro;

import com.linkedin.hoptimator.Validator;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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

  @Test
  void testValidateRecordsErrorForIncompatibleSchemas() {
    // Build two tables with incompatible types: original has INTEGER, new has VARCHAR
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType originalRowType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.INTEGER)),
        List.of("value"));
    RelDataType newRowType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
        List.of("value"));

    Table originalTable = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory factory) {
        return originalRowType;
      }
    };
    Table newTable = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory factory) {
        return newRowType;
      }
    };

    // Build a real CalciteSchema whose inner schema has the originalTable
    AbstractSchema innerSchema = new AbstractSchema() {
      @Override
      protected Map<String, Table> getTableMap() {
        return Map.of("MY_TABLE", originalTable);
      }
    };
    CalciteSchema calciteSchema = CalciteSchema.createRootSchema(false, false, "root", innerSchema);

    // Mock the SchemaPlus: tables() returns newTable; unwrap returns the CalciteSchema with originalTable
    @SuppressWarnings("unchecked")
    Lookup<Table> tableLookup = mock(Lookup.class);
    when(tableLookup.getNames(any())).thenReturn(Set.of("MY_TABLE"));
    when(tableLookup.get("MY_TABLE")).thenReturn(newTable);
    when(schema.tables()).thenReturn(tableLookup);
    when(schema.unwrap(CalciteSchema.class)).thenReturn(calciteSchema);

    AvroTableValidator validator = new AvroTableValidator(schema);
    Validator.Issues issues = new Validator.Issues("root");
    validator.validate(issues);

    assertFalse(issues.valid(),
        "Incompatible schema (INT→VARCHAR) should produce validation errors");
  }

  @Test
  void testValidatePassesForCompatibleSchemas() {
    // Both tables have the same VARCHAR field — evolution should succeed
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
        List.of("value"));

    Table sameTable = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory factory) {
        return rowType;
      }
    };

    AbstractSchema innerSchema = new AbstractSchema() {
      @Override
      protected Map<String, Table> getTableMap() {
        return Map.of("MY_TABLE", sameTable);
      }
    };
    CalciteSchema calciteSchema = CalciteSchema.createRootSchema(false, false, "root", innerSchema);

    @SuppressWarnings("unchecked")
    Lookup<Table> tableLookup = mock(Lookup.class);
    when(tableLookup.getNames(any())).thenReturn(Set.of("MY_TABLE"));
    when(tableLookup.get("MY_TABLE")).thenReturn(sameTable);
    when(schema.tables()).thenReturn(tableLookup);
    when(schema.unwrap(CalciteSchema.class)).thenReturn(calciteSchema);

    AvroTableValidator validator = new AvroTableValidator(schema);
    Validator.Issues issues = new Validator.Issues("root");
    validator.validate(issues);

    assertTrue(issues.valid(), "Compatible schemas should pass validation without errors");
  }
}
