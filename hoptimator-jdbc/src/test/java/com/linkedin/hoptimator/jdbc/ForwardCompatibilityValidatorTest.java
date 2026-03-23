package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.Validator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class ForwardCompatibilityValidatorTest {

  @Test
  void testRemoveNullableFieldIsCompatible() {
    Table original = tableWithFields(
        new FieldDef("id", SqlTypeName.INTEGER, false),
        new FieldDef("name", SqlTypeName.VARCHAR, true)
    );
    Table updated = tableWithFields(
        new FieldDef("id", SqlTypeName.INTEGER, false)
    );

    Validator.Issues issues = new Validator.Issues("test");
    new ForwardCompatibilityValidator(null).validate(updated, original, issues);

    assertTrue(issues.valid());
  }

  @Test
  void testRemoveNonNullableFieldIsIncompatible() {
    Table original = tableWithFields(
        new FieldDef("id", SqlTypeName.INTEGER, false),
        new FieldDef("name", SqlTypeName.VARCHAR, false)
    );
    Table updated = tableWithFields(
        new FieldDef("id", SqlTypeName.INTEGER, false)
    );

    Validator.Issues issues = new Validator.Issues("test");
    new ForwardCompatibilityValidator(null).validate(updated, original, issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Forwards-incompatible"));
  }

  @Test
  void testChangeFieldTypeIncompatibly() {
    Table original = tableWithFields(
        new FieldDef("id", SqlTypeName.INTEGER, false)
    );
    Table updated = tableWithFields(
        new FieldDef("id", SqlTypeName.VARCHAR, false)
    );

    Validator.Issues issues = new Validator.Issues("test");
    new ForwardCompatibilityValidator(null).validate(updated, original, issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("cannot assign"));
  }

  @Test
  void testSameSchemaIsCompatible() {
    Table table = tableWithFields(
        new FieldDef("id", SqlTypeName.INTEGER, false),
        new FieldDef("name", SqlTypeName.VARCHAR, true)
    );

    Validator.Issues issues = new Validator.Issues("test");
    new ForwardCompatibilityValidator(null).validate(table, table, issues);

    assertTrue(issues.valid());
  }

  private Table tableWithFields(FieldDef... fields) {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory factory) {
        RelDataTypeFactory.Builder builder = factory.builder();
        for (FieldDef field : fields) {
          RelDataType type = factory.createSqlType(field.typeName);
          if (field.nullable) {
            type = factory.createTypeWithNullability(type, true);
          }
          builder.add(field.name, type);
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
