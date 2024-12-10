package com.linkedin.hoptimator.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.RandomData;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import com.linkedin.hoptimator.Validator;


/** Validates that tables follow Avro schema evolution rules.  */
class AvroTableValidator implements Validator<SchemaPlus> {

  @Override
  public void validate(SchemaPlus schema, Issues issues) {
    try {
      CalciteSchema originalSchema = schema.unwrap(CalciteSchema.class);
      if (originalSchema == null || originalSchema.schema == null) {
        throw new IllegalArgumentException("Null original schema (BUG)");
      }
      for (String x : schema.getTableNames()) {
        Table table = schema.getTable(x);
        Table originalTable = originalSchema.schema.getTable(x);
        if (table == null) {
          throw new IllegalArgumentException("Null table (BUG)");
        }
        if (originalTable == null) {
          continue;
        }
        validate(schema, table, originalTable, issues.child(x));
      }
    } catch (ClassCastException e) {
      // nop
    }
  }

  private void validate(SchemaPlus schema, Table table, Table originalTable, Issues issues) {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(typeFactory);
    RelDataType originalRowType = originalTable.getRowType(typeFactory);
    Schema avroSchema = AvroConverter.avro("ns", "n", rowType);
    Schema originalAvroSchema = AvroConverter.avro("ns", "n", originalRowType);
    DatumWriter<Object> datumWriter = new GenericDatumWriter<>(originalAvroSchema);
    try (OutputStream out = new ByteArrayOutputStream();
        DataFileWriter<Object> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(originalAvroSchema, out);
      for (Object obj : new RandomData(avroSchema, 1)) {
        dataFileWriter.append(obj);
      }
    } catch (IOException | RuntimeException e) {
      issues.error("Avro schema evolution error: cannot serialize new records using the existing schema");
    }
  }
}
