package com.linkedin.hoptimator.venice;

import java.util.Properties;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;


/** A batch of records from a Venice store. */
public class VeniceStore extends AbstractTable {

  private final String storeName;
  private final Properties properties;

  public VeniceStore(String storeName, Properties properties) {
    this.storeName = storeName;
    this.properties = properties;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    return builder.add("KEY", SqlTypeName.VARCHAR)
        .nullable(true)
        .add("VALUE", SqlTypeName.BINARY)
        .nullable(true)
        .build();
  }
}
