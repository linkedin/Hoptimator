package com.linkedin.hoptimator.catalog.builtin;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class DatagenSchemaFactoryTest {

  @Test
  void createReturnsSchemaContainingPersonAndCompany() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(false).plus();
    Schema schema = new DatagenSchemaFactory().create(parentSchema, "DATAGEN", Collections.emptyMap());

    assertNotNull(schema);
    Set<String> tableNames = schema.tables().getNames(LikePattern.any());
    assertTrue(tableNames.contains("PERSON"), "Expected PERSON table in schema");
    assertTrue(tableNames.contains("COMPANY"), "Expected COMPANY table in schema");
  }

  @Test
  void personTableHasNonNullRowType() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(false).plus();
    Schema schema = new DatagenSchemaFactory().create(parentSchema, "DATAGEN", Collections.emptyMap());

    Table person = schema.tables().get("PERSON");
    assertNotNull(person, "PERSON table should not be null");
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = person.getRowType(typeFactory);
    assertNotNull(rowType, "PERSON row type should not be null");
  }

  @Test
  void companyTableHasNonNullRowType() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(false).plus();
    Schema schema = new DatagenSchemaFactory().create(parentSchema, "DATAGEN", Collections.emptyMap());

    Table company = schema.tables().get("COMPANY");
    assertNotNull(company, "COMPANY table should not be null");
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = company.getRowType(typeFactory);
    assertNotNull(rowType, "COMPANY row type should not be null");
  }
}
