package com.linkedin.hoptimator.pinot;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;

/**
 * Builds and validates a Pinot {@link Schema} from a Calcite row type plus connector hints, shared
 * by the controller-REST deployer and any downstream deployers so both enforce the exact same rules
 * and generate the same schema.
 *
 * <p>Column roles are driven by hints:
 * <ul>
 *   <li>{@code metrics} — comma-separated metric column names (Pinot requires numeric/BYTES types)</li>
 *   <li>{@code timeColumns} — comma-separated date-time column names (or the legacy singular
 *       {@code timeColumn}); each needs a {@code format} and {@code granularity}</li>
 *   <li>{@code primaryTimeColumn} — the primary time column for the table's {@code timeColumnName}
 *       (defaults to the sole time column)</li>
 *   <li>{@code format.<col>} / {@code granularity.<col>} — per-column date-time format and
 *       granularity (e.g. {@code 1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd} and {@code 3:DAYS}); fall back to
 *       the global {@code timeFormat} / {@code timeGranularity}, then to epoch-millis defaults</li>
 * </ul>
 * Everything else becomes a dimension (single- or multi-valued).
 *
 * <p>{@link #validationErrors} pre-flights everything Pinot's controller enforces — complex-column
 * rejection, metric/time-column type and arity rules, date-time format/granularity parseability, and
 * the per-field-type data-type rules ({@link Schema#validate()}) — so a bad spec fails at our layer
 * rather than creating something (or being rejected with an opaque controller 4xx).
 */
public final class PinotSchemas {

  public static final String METRICS = "metrics";
  public static final String TIME_COLUMNS = "timeColumns";
  public static final String TIME_COLUMN = "timeColumn";
  public static final String PRIMARY_TIME_COLUMN = "primaryTimeColumn";
  public static final String TIME_FORMAT = "timeFormat";
  public static final String TIME_GRANULARITY = "timeGranularity";
  public static final String FORMAT_PREFIX = "format.";
  public static final String GRANULARITY_PREFIX = "granularity.";
  public static final String DEFAULT_TIME_FORMAT = "1:MILLISECONDS:EPOCH";
  public static final String DEFAULT_TIME_GRANULARITY = "1:MILLISECONDS";

  private PinotSchemas() {
  }

  /** Builds a validated Pinot schema; throws on bad format/granularity or unsupported column types. */
  public static Schema build(String schemaName, RelDataType rowType, Map<String, String> hints) {
    Set<String> metrics = commaSeparated(hints.get(METRICS));
    Set<String> timeColumns = timeColumns(hints);

    Schema.SchemaBuilder builder = new Schema.SchemaBuilder().setSchemaName(schemaName);
    for (RelDataTypeField field : rowType.getFieldList()) {
      RelDataType fieldType = field.getType();
      String name = field.getName();
      if (timeColumns.contains(name) && !PinotTypeConverter.isComplex(fieldType)) {
        // Validating DateTimeFieldSpec constructor: parses (and thus validates) format + granularity.
        builder.addField(new DateTimeFieldSpec(name, PinotTypeConverter.pinotType(fieldType.getSqlTypeName()),
            format(hints, name), granularity(hints, name)));
      } else if (metrics.contains(name) && !PinotTypeConverter.isComplex(fieldType)) {
        builder.addMetricField(name, PinotTypeConverter.pinotType(fieldType.getSqlTypeName()));
      } else {
        builder.addField(PinotTypeConverter.toFieldSpec(name, fieldType));
      }
    }
    return builder.build();
  }

  /** Returns all Pinot-rule violations for the given row type + hints; empty when the spec is valid. */
  public static List<String> validationErrors(String schemaName, RelDataType rowType, Map<String, String> hints) {
    List<String> errors = new ArrayList<>();
    if (rowType.getFieldList().isEmpty()) {
      errors.add("Failed to derive a non-empty schema for Pinot table " + schemaName);
      return errors;
    }
    Set<String> metrics = commaSeparated(hints.get(METRICS));
    Set<String> timeColumns = timeColumns(hints);
    for (RelDataTypeField field : rowType.getFieldList()) {
      String name = field.getName();
      RelDataType type = field.getType();
      RelDataType componentType = type.getComponentType();
      boolean multiValued = type.getSqlTypeName() == SqlTypeName.ARRAY;
      boolean complex = type.isStruct()
          || type.getSqlTypeName() == SqlTypeName.MAP
          || (multiValued && componentType != null && PinotTypeConverter.isComplex(componentType));
      if (complex) {
        errors.add("Pinot cannot provision complex column '" + name + "' in table " + schemaName
            + " (struct/map/array-of-complex). Flatten it upstream or model it as a JSON column.");
      }
      if (multiValued && metrics.contains(name)) {
        errors.add("Pinot metric column '" + name + "' cannot be multi-valued (table " + schemaName + ").");
      }
      if (multiValued && timeColumns.contains(name)) {
        errors.add("Pinot time column '" + name + "' cannot be multi-valued (table " + schemaName + ").");
      }
      if (metrics.contains(name) && timeColumns.contains(name)) {
        errors.add("Column '" + name + "' cannot be both a metric and a time column (table " + schemaName + ").");
      }
    }
    String primaryTimeColumn = primaryTimeColumn(hints, timeColumns);
    if (primaryTimeColumn != null && !timeColumns.contains(primaryTimeColumn)) {
      errors.add("primaryTimeColumn '" + primaryTimeColumn + "' is not one of the declared time columns "
          + timeColumns + " (table " + schemaName + ").");
    }
    // Build + validate the pinot-spi schema, surfacing any remaining Pinot rule violation (bad
    // format/granularity, illegal metric data type, ...).
    try {
      build(schemaName, rowType, hints).validate();
    } catch (RuntimeException e) {
      errors.add("Invalid Pinot schema for table " + schemaName + ": " + e.getMessage());
    }
    return errors;
  }

  /** The set of date-time column names, from {@code timeColumns} (CSV) or the legacy {@code timeColumn}. */
  public static Set<String> timeColumns(Map<String, String> hints) {
    Set<String> columns = commaSeparated(hints.get(TIME_COLUMNS));
    String legacy = hints.get(TIME_COLUMN);
    if (legacy != null && !legacy.isEmpty()) {
      columns.add(legacy);
    }
    return columns;
  }

  /** The primary time column (for {@code segmentsConfig.timeColumnName}), defaulting to the sole one. */
  public static String primaryTimeColumn(Map<String, String> hints, Set<String> timeColumns) {
    String explicit = hints.get(PRIMARY_TIME_COLUMN);
    if (explicit == null) {
      explicit = hints.get(TIME_COLUMN);
    }
    if (explicit != null && !explicit.isEmpty()) {
      return explicit;
    }
    return timeColumns.size() == 1 ? timeColumns.iterator().next() : null;
  }

  /** Per-column date-time format ({@code format.<col>}), then global {@code timeFormat}, then default. */
  public static String format(Map<String, String> hints, String column) {
    String perColumn = hints.get(FORMAT_PREFIX + column);
    if (perColumn != null && !perColumn.isEmpty()) {
      return perColumn;
    }
    return hints.getOrDefault(TIME_FORMAT, DEFAULT_TIME_FORMAT);
  }

  /** Per-column granularity ({@code granularity.<col>}), then global {@code timeGranularity}, then default. */
  public static String granularity(Map<String, String> hints, String column) {
    String perColumn = hints.get(GRANULARITY_PREFIX + column);
    if (perColumn != null && !perColumn.isEmpty()) {
      return perColumn;
    }
    return hints.getOrDefault(TIME_GRANULARITY, DEFAULT_TIME_GRANULARITY);
  }

  public static Set<String> commaSeparated(String value) {
    Set<String> result = new LinkedHashSet<>();
    if (value != null && !value.isEmpty()) {
      for (String part : value.split(",")) {
        String trimmed = part.trim();
        if (!trimmed.isEmpty()) {
          result.add(trimmed);
        }
      }
    }
    return result;
  }
}
