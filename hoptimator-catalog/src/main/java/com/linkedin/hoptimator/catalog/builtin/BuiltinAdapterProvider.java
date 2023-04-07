package com.linkedin.hoptimator.catalog.builtin;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import com.linkedin.hoptimator.catalog.Adapter;
import com.linkedin.hoptimator.catalog.AdapterImpl;
import com.linkedin.hoptimator.catalog.AdapterProvider;
import com.linkedin.hoptimator.catalog.ConfigProvider;
import com.linkedin.hoptimator.catalog.AdapterTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Provides built-in Adapters like DATAGEN. */
public class BuiltinAdapterProvider implements AdapterProvider {

  @Override
  public Collection<Adapter> adapters() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    Map<String, AdapterTable> datagenTables = new HashMap<>();
    datagenTables.put("PERSON", new AdapterTable("DATAGEN", "PERSON", (new RelDataTypeFactory.Builder(typeFactory))
      .add("NAME", SqlTypeName.VARCHAR).add("AGE", SqlTypeName.INTEGER).build(), ConfigProvider.empty()
      .with("connector", "datagen").with("number-of-rows", "10").with("fields.AGE.min", "0")
      .with("fields.AGE.max", "100").with("fields.NAME.length", "5").config("PERSON")));
    datagenTables.put("COMPANY", new AdapterTable("DATAGEN", "COMPANY", (new RelDataTypeFactory.Builder(typeFactory))
      .add("NAME", SqlTypeName.VARCHAR).add("CEO", SqlTypeName.VARCHAR).build(), ConfigProvider.empty()
      .with("connector", "datagen").with("number-of-rows", "10").with("fields.NAME.length", "5")
      .with("fields.CEO.length", "5").config("COMPANY")));
    List<Adapter> adapters = new ArrayList<>();
    adapters.add(new AdapterImpl("DATAGEN", datagenTables));
    return adapters;
  }
}
