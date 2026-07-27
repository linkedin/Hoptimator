package com.linkedin.hoptimator.util.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.ImmutablePairList;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


class IdentityQueryTest {

  private static RelDataType rowType() {
    RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    return factory.builder()
        .add("ID", factory.createSqlType(SqlTypeName.INTEGER))
        .add("NAME", factory.createSqlType(SqlTypeName.VARCHAR))
        .build();
  }

  @Test
  void scanBuildsIdentityScanExposingRowType() {
    List<String> path = Arrays.asList("CATALOG", "SCHEMA", "TABLE");

    RelNode scan = IdentityQuery.scan(path, rowType());

    assertThat(scan).isNotNull();
    // The scan re-homes the carried row type; field names/count must round-trip.
    assertThat(scan.getRowType().getFieldNames()).containsExactly("ID", "NAME");
    assertThat(scan.getTable().getQualifiedName()).containsExactly("CATALOG", "SCHEMA", "TABLE");
  }

  @Test
  void scanWorksForSingleSchemaPath() {
    RelNode scan = IdentityQuery.scan(Arrays.asList("SCHEMA", "TABLE"), rowType());

    assertThat(scan.getTable().getQualifiedName()).containsExactly("SCHEMA", "TABLE");
    assertThat(scan.getRowType().getFieldNames()).containsExactly("ID", "NAME");
  }

  @Test
  void fieldsReturnsIndexedTargetList() {
    ImmutablePairList<Integer, String> fields = IdentityQuery.fields(rowType());

    assertThat(fields).hasSize(2);
    assertThat(fields.leftList()).containsExactly(0, 1);
    assertThat(fields.rightList()).containsExactly("ID", "NAME");
  }

  @Test
  void fieldsOfEmptyRowTypeIsEmpty() {
    RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType empty = factory.builder().build();

    assertThat(IdentityQuery.fields(empty)).isEmpty();
  }
}
