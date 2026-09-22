package com.linkedin.hoptimator.pinot;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;

/**
 * Converts between a Pinot {@link Schema} and a Calcite {@link RelDataType} row type, in both
 * directions.
 *
 * <p>A Pinot schema is a flat set of columns: dimension, metric, date-time, and complex field specs.
 * Columns are surfaced in dimension → metric → date-time → complex order so the row type is stable.
 *
 * <p>Type mapping:
 * <ul>
 *   <li>Primitive single-valued columns map to the corresponding scalar SQL type.</li>
 *   <li>Multi-valued primitive columns map to {@code ARRAY&lt;element&gt;}.</li>
 *   <li>Complex {@code STRUCT} maps to a Calcite {@code ROW} (members sorted by name for
 *       determinism, since Pinot stores them unordered), {@code MAP} to {@code MAP&lt;k,v&gt;}, and
 *       {@code LIST} to {@code ARRAY&lt;element&gt;} — recursively.</li>
 * </ul>
 *
 * <p>Every column is marked nullable: Pinot substitutes a per-column default null value rather than
 * enforcing NOT NULL, so a permissive catalog surface avoids spurious planner constraints.
 */
public final class PinotTypeConverter {

  private PinotTypeConverter() {
  }

  // ---------------------------------------------------------------------------------------------
  // Pinot -> Calcite (read/catalog path)
  // ---------------------------------------------------------------------------------------------

  /** Builds a Calcite row type from a Pinot schema. */
  public static RelDataType rel(Schema schema, RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    List<FieldSpec> fieldSpecs = new ArrayList<>();
    fieldSpecs.addAll(schema.getDimensionFieldSpecs());
    fieldSpecs.addAll(schema.getMetricFieldSpecs());
    fieldSpecs.addAll(schema.getDateTimeFieldSpecs());
    fieldSpecs.addAll(schema.getComplexFieldSpecs());
    for (FieldSpec fieldSpec : fieldSpecs) {
      builder.add(fieldSpec.getName(), typeFactory.createTypeWithNullability(rel(fieldSpec, typeFactory), true));
    }
    return builder.build();
  }

  static RelDataType rel(FieldSpec fieldSpec, RelDataTypeFactory typeFactory) {
    RelDataType baseType = baseType(fieldSpec, typeFactory);
    // Multi-valued primitives (and multi-valued complex) surface as arrays. A LIST is already an
    // array via its element type, so it is not double-wrapped.
    boolean alreadyArray = fieldSpec instanceof ComplexFieldSpec
        && fieldSpec.getDataType() == FieldSpec.DataType.LIST;
    if (!fieldSpec.isSingleValueField() && !alreadyArray) {
      return typeFactory.createArrayType(baseType, -1);
    }
    return baseType;
  }

  private static RelDataType baseType(FieldSpec fieldSpec, RelDataTypeFactory typeFactory) {
    if (fieldSpec instanceof ComplexFieldSpec) {
      ComplexFieldSpec complex = (ComplexFieldSpec) fieldSpec;
      switch (complex.getDataType()) {
        case MAP:
          return mapType(complex, typeFactory);
        case LIST:
          return typeFactory.createArrayType(listElementType(complex, typeFactory), -1);
        // STRUCT has no default-null value in pinot-spi and cannot be constructed, so it never
        // reaches here; fall through to the VARCHAR fallback defensively.
        default:
          return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      }
    }
    return scalarType(fieldSpec.getDataType(), typeFactory);
  }

  private static RelDataType mapType(ComplexFieldSpec complex, RelDataTypeFactory typeFactory) {
    RelDataType keyType = childType(complex, ComplexFieldSpec.KEY_FIELD, typeFactory);
    RelDataType valueType = childType(complex, ComplexFieldSpec.VALUE_FIELD, typeFactory);
    return typeFactory.createMapType(keyType, valueType);
  }

  private static RelDataType listElementType(ComplexFieldSpec complex, RelDataTypeFactory typeFactory) {
    FieldSpec element = complex.getChildFieldSpec(ComplexFieldSpec.VALUE_FIELD);
    if (element == null && !complex.getChildFieldSpecs().isEmpty()) {
      element = complex.getChildFieldSpecs().values().iterator().next();
    }
    return element == null ? typeFactory.createSqlType(SqlTypeName.VARCHAR) : rel(element, typeFactory);
  }

  private static RelDataType childType(ComplexFieldSpec complex, String childName, RelDataTypeFactory typeFactory) {
    FieldSpec child = complex.getChildFieldSpec(childName);
    return child == null ? typeFactory.createSqlType(SqlTypeName.VARCHAR) : rel(child, typeFactory);
  }

  private static RelDataType scalarType(FieldSpec.DataType dataType, RelDataTypeFactory typeFactory) {
    switch (dataType) {
      case INT:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case LONG:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case FLOAT:
        return typeFactory.createSqlType(SqlTypeName.REAL);
      case DOUBLE:
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
      case BIG_DECIMAL:
        return typeFactory.createSqlType(SqlTypeName.DECIMAL);
      case BOOLEAN:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case TIMESTAMP:
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      case BYTES:
        return typeFactory.createSqlType(SqlTypeName.VARBINARY);
      case STRING:
      case JSON:
      case UNKNOWN:
      default:
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Calcite -> Pinot (provisioning/write path)
  // ---------------------------------------------------------------------------------------------

  /**
   * Builds a Pinot {@link FieldSpec} from a Calcite column type. Structs/maps/arrays-of-complex map
   * to a {@link ComplexFieldSpec} ({@code STRUCT}/{@code MAP}/{@code LIST}); arrays of primitives map
   * to a multi-valued {@link DimensionFieldSpec}; scalars map to a single-valued dimension.
  /**
   * Builds a Pinot {@link FieldSpec} from a Calcite column type, for provisioning. Mirrors the
   * sanctioned {@code AvroUtils.getPinotSchemaFromAvroSchema} (strict mode): scalars become
   * single-valued dimensions and arrays of primitives become multi-valued dimensions.
   *
   * <p>Complex columns — struct/ROW, MAP, and arrays of complex — are rejected. Pinot's canonical
   * handling for semi-structured data is a {@code JSON} column (see
   * {@code AvroSchemaUtil.valueOf(RECORD|MAP|ARRAY) -> JSON}) or upstream flattening, neither of
   * which is inferable from the Calcite type alone, so we fail fast with a clear message rather than
   * emit a schema that won't ingest.
   */
  public static FieldSpec toFieldSpec(String name, RelDataType type) {
    if (type.isStruct()) {
      throw new UnsupportedOperationException("Pinot cannot provision STRUCT/ROW column '" + name
          + "'. Flatten it upstream or model it as a JSON column.");
    }
    if (type.getSqlTypeName() == SqlTypeName.MAP) {
      throw new UnsupportedOperationException("Pinot cannot provision MAP column '" + name
          + "'. Model it as a JSON column instead.");
    }
    if (type.getSqlTypeName() == SqlTypeName.ARRAY) {
      RelDataType element = type.getComponentType();
      if (element != null && isComplex(element)) {
        throw new UnsupportedOperationException("Pinot cannot provision an array of complex values for column '"
            + name + "'. Model it as a JSON column instead.");
      }
      FieldSpec.DataType elementType = element == null
          ? FieldSpec.DataType.STRING : pinotType(element.getSqlTypeName());
      return new DimensionFieldSpec(name, elementType, false);
    }
    return new DimensionFieldSpec(name, pinotType(type.getSqlTypeName()), true);
  }

  /** Whether a Calcite type maps to a Pinot complex field (struct, map, or array). */
  public static boolean isComplex(RelDataType type) {
    return type.isStruct()
        || type.getSqlTypeName() == SqlTypeName.MAP
        || type.getSqlTypeName() == SqlTypeName.ARRAY;
  }

  /** Maps a Calcite SQL type to the Pinot {@link FieldSpec.DataType} used when provisioning. */
  public static FieldSpec.DataType pinotType(SqlTypeName sqlTypeName) {
    switch (sqlTypeName) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return FieldSpec.DataType.INT;
      case BIGINT:
        return FieldSpec.DataType.LONG;
      case REAL:
      case FLOAT:
        return FieldSpec.DataType.FLOAT;
      case DOUBLE:
        return FieldSpec.DataType.DOUBLE;
      case DECIMAL:
        return FieldSpec.DataType.BIG_DECIMAL;
      case BOOLEAN:
        return FieldSpec.DataType.BOOLEAN;
      case TIMESTAMP:
        return FieldSpec.DataType.TIMESTAMP;
      case VARBINARY:
      case BINARY:
        return FieldSpec.DataType.BYTES;
      default:
        return FieldSpec.DataType.STRING;
    }
  }
}
