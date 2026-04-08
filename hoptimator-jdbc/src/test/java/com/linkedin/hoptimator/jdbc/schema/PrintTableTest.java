package com.linkedin.hoptimator.jdbc.schema;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class PrintTableTest {

  private PrintTable printTable;

  @BeforeEach
  void setUp() {
    printTable = new PrintTable();
  }

  @Test
  void testGetRowTypeHasOutputColumn() {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    RelDataType rowType = printTable.getRowType(typeFactory);

    assertNotNull(rowType);
    assertEquals(1, rowType.getFieldCount());
    assertEquals("OUTPUT", rowType.getFieldNames().get(0));
  }

  @Test
  void testToRowReturnsInput() {
    Object result = printTable.toRow("hello");

    assertEquals("hello", result);
  }

  @Test
  void testFromRowReturnsToString() {
    String result = printTable.fromRow("test-value");

    assertEquals("test-value", result);
  }

  @Test
  void testFromRowConvertsObjectToString() {
    String result = printTable.fromRow(42);

    assertEquals("42", result);
  }

  @Test
  void testRowsReturnsEmptyCollection() {
    Collection<?> rows = printTable.rows();

    assertNotNull(rows);
    assertTrue(rows.isEmpty());
  }

  @Test
  void testGetModifiableCollectionReturnsNonNull() {
    Collection<?> modifiable = printTable.getModifiableCollection();

    assertNotNull(modifiable);
  }

  @Test
  void testCreatePrintsToStdout() throws SQLException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(baos, true, StandardCharsets.UTF_8));
    try {
      printTable.api().create("hello world");

      String output = baos.toString(StandardCharsets.UTF_8);
      assertTrue(output.contains("hello world"));
    } finally {
      System.setOut(originalOut);
    }
  }
}
