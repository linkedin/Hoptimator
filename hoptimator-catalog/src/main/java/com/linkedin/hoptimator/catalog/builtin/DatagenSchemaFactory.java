package com.linkedin.hoptimator.catalog.builtin;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaFactory;

import com.linkedin.hoptimator.catalog.ConfigProvider;
import com.linkedin.hoptimator.catalog.Database;
import com.linkedin.hoptimator.catalog.DatabaseSchema;
import com.linkedin.hoptimator.catalog.HopTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Provides built-in DATAGEN databases */
public class DatagenSchemaFactory implements SchemaFactory {

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    Map<String, HopTable> datagenTables = new HashMap<>();
    datagenTables.put("PERSON", new HopTable("DATAGEN", "PERSON", (new RelDataTypeFactory.Builder(typeFactory))
      .add("NAME", SqlTypeName.VARCHAR).add("AGE", SqlTypeName.INTEGER)
      .add("EMPID", SqlTypeName.BIGINT).build(), ConfigProvider.empty()
      .with("connector", "datagen").with("number-of-rows", "10").with("fields.AGE.min", "0")
      .with("fields.AGE.max", "100").with("fields.NAME.length", "5").config("PERSON")));
    datagenTables.put("COMPANY", new HopTable("DATAGEN", "COMPANY", (new RelDataTypeFactory.Builder(typeFactory))
      .add("NAME", SqlTypeName.VARCHAR).add("CEO", SqlTypeName.VARCHAR).build(), ConfigProvider.empty()
      .with("connector", "datagen").with("number-of-rows", "10").with("fields.NAME.length", "5")
      .with("fields.CEO.length", "5").config("COMPANY")));
    return new DatabaseSchema(new Database(name, datagenTables));
  }
}
