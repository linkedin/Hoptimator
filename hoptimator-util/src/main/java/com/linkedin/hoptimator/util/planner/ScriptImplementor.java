package com.linkedin.hoptimator.util.planner;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.ImmutablePairList;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMapTypeNameSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlRowTypeNameSpec;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;


/**
 * An abstract way to write SQL scripts.
 * <p>
 * To generate a specific statement, implement this interface, or use one
 * of the provided implementations, e.g. `QueryImplementor`.
 * <p>
 * To generate a script (more than one statement), start with `empty()`
 * and append subsequent ScriptImplementors with `with(...)` etc.
 * <p>
 * e.g.
 * <p>
 *   ScriptImplementor.empty()
 *     .database(db)
 *     .connector(name, rowType, configs)
 * <p>
 * ... would produce something like
 * <p>
 *   CREATE DATABASE IF NOT EXIST `FOO`;
 *   CREATE TABLE `BAR` (NAME VARCHAR) WITH ('key1' = 'value1');
 */
public interface ScriptImplementor {

  /** Writes arbitrary DDL/SQL */
  void implement(SqlWriter writer);

  /** Construct an empty ScriptImplementor */
  static ScriptImplementor empty() {
    return w -> {
    };
  }

  /** Append a subsequent ScriptImplementor */
  default ScriptImplementor with(ScriptImplementor next) {
    return w -> {
      implement(w);
      next.implement(w);
    };
  }

  /** Append a query */
  default ScriptImplementor query(RelNode relNode) {
    return query(relNode, Collections.emptyMap());
  }

  /** Append a query with table name replacements */
  default ScriptImplementor query(RelNode relNode, Map<String, String> tableNameReplacements) {
    return with(new QueryImplementor(relNode, tableNameReplacements));
  }

  /** Append a connector definition, e.g. `CREATE TABLE ... WITH (...)` */
  default ScriptImplementor connector(@Nullable String catalog, String schema, String table, RelDataType rowType, Map<String, String> connectorConfig) {
    return connector(catalog, schema, table, null, rowType, connectorConfig);
  }

  /** Append a connector definition with an optional suffix, e.g. `CREATE TABLE ... WITH (...)` */
  default ScriptImplementor connector(@Nullable String catalog, String schema, String table, @Nullable String suffix, RelDataType rowType,
      Map<String, String> connectorConfig) {
    return with(new ConnectorImplementor(catalog, schema, table, suffix, rowType, connectorConfig));
  }

  /** Append a database definition, e.g. `CREATE CATALOG ...` */
  default ScriptImplementor catalog(@Nullable String catalog) {
    return with(new CatalogImplementor(catalog));
  }

  /** Append a database definition, e.g. `CREATE DATABASE ...` */
  default ScriptImplementor database(@Nullable String catalog, String database) {
    return with(new DatabaseImplementor(catalog, database));
  }

  /** Append an insert statement, e.g. `INSERT INTO ... SELECT ...` */
  default ScriptImplementor insert(@Nullable String catalog, String schema, String table, RelNode relNode, ImmutablePairList<Integer, String> targetFields) {
    return insert(catalog, schema, table, null, relNode, targetFields, Collections.emptyMap());
  }

  /** Append an insert statement with an optional suffix for the target table, e.g. `INSERT INTO ... SELECT ...` */
  default ScriptImplementor insert(@Nullable String catalog, String schema, String table, @Nullable String suffix, RelNode relNode,
      ImmutablePairList<Integer, String> targetFields) {
    return insert(catalog, schema, table, suffix, relNode, targetFields, Collections.emptyMap());
  }

  /** Append an insert statement with an optional suffix and table name replacements for the query, e.g. `INSERT INTO ... SELECT ...` */
  default ScriptImplementor insert(@Nullable String catalog, String schema, String table, @Nullable String suffix, RelNode relNode,
      ImmutablePairList<Integer, String> targetFields, Map<String, String> tableNameReplacements) {
    return with(new InsertImplementor(catalog, schema, table, suffix, relNode, targetFields, tableNameReplacements));
  }

  /** Render the script as DDL/SQL in the default dialect */
  default String sql() {
    return sql(AnsiSqlDialect.DEFAULT);
  }

  /** Render the script as DDL/SQL in the given dialect */
  default String sql(SqlDialect dialect) {
    SqlWriter w = new SqlPrettyWriter(SqlWriterConfig.of().withDialect(dialect));
    implement(w);
    return w.toSqlString().getSql().replaceAll("\\n", " ").replaceAll("\\s*;\\s*", ";\n").trim();
  }

  /** Generate SQL for a given dialect */
  default Function<com.linkedin.hoptimator.SqlDialect, String> seal() {
    return x -> {
      final String sql;
      switch (x) {
        case ANSI:
        case FLINK:
          sql = sql(AnsiSqlDialect.DEFAULT);
          break;
        default:
          throw new IllegalStateException("unreachable");
      }
      return sql;
    };
  }

  /** Implements an arbitrary RelNode as a statement */
  class StatementImplementor implements ScriptImplementor {
    private final RelNode relNode;

    public StatementImplementor(RelNode relNode) {
      this.relNode = relNode;
    }

    @Override
    public void implement(SqlWriter w) {
      RelToSqlConverter converter = new RelToSqlConverter(w.getDialect());
      w.literal(converter.visitRoot(relNode).asStatement().toSqlString(w.getDialect()).getSql());
      w.literal(";");
    }
  }

  /** Implements an arbitrary RelNode as a query */
  class QueryImplementor implements ScriptImplementor {
    private final RelNode relNode;
    private final Map<String, String> tableNameReplacements;

    public QueryImplementor(RelNode relNode) {
      this(relNode, Collections.emptyMap());
    }

    public QueryImplementor(RelNode relNode, Map<String, String> tableNameReplacements) {
      this.relNode = relNode;
      this.tableNameReplacements = tableNameReplacements;
    }

    @Override
    public void implement(SqlWriter w) {
      RelToSqlConverter converter = new RelToSqlConverter(w.getDialect());
      SqlImplementor.Result result = converter.visitRoot(relNode);
      SqlNode node = result.asStatement();
      if (node instanceof SqlSelect && ((SqlSelect) node).getSelectList() != null) {
        SqlSelect select = (SqlSelect) node;
        select.setSelectList((SqlNodeList) Objects.requireNonNull(select.getSelectList().accept(REMOVE_ROW_CONSTRUCTOR)));
      }
      // Apply table name replacements if any
      if (!tableNameReplacements.isEmpty()) {
        node = node.accept(new TableNameReplacer(tableNameReplacements));
      }
      node.unparse(w, 0, 0);
    }

    // A `ROW(...)` operator which will unparse as just `(...)`.
    private static final SqlRowOperator IMPLIED_ROW_OPERATOR = new SqlRowOperator(""); // empty string name

    // a shuttle that replaces `Row(...)` with just `(...)`
    private static final SqlShuttle REMOVE_ROW_CONSTRUCTOR = new SqlShuttle() {
      @Override
      public SqlNode visit(SqlCall call) {
        List<SqlNode> operands = call.getOperandList().stream().map(x -> x == null ? null : x.accept(this)).collect(Collectors.toList());
        if ((call.getKind() == SqlKind.ROW || call.getKind() == SqlKind.COLUMN_LIST
            || call.getOperator() instanceof SqlRowOperator) && operands.size() > 1) {
          return IMPLIED_ROW_OPERATOR.createCall(call.getParserPosition(), operands);
        } else {
          return call.getOperator().createCall(call.getParserPosition(), operands);
        }
      }
    };
  }

  /**
   * A SqlShuttle that replaces table names in SQL nodes.
   * Used to add suffixes to table names when there are source/sink collisions.
   */
  class TableNameReplacer extends SqlShuttle {
    private final Map<String, String> replacements;

    public TableNameReplacer(Map<String, String> replacements) {
      this.replacements = replacements;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      if (id.names.size() >= 2) {
        // Build the qualified name to check for replacement
        String qualifiedName = String.join(".", id.names);
        if (replacements.containsKey(qualifiedName)) {
          // Replace the last component (table name) with the suffixed version
          List<String> newNames = new ArrayList<>(id.names);
          newNames.set(newNames.size() - 1, replacements.get(qualifiedName));
          return new SqlIdentifier(newNames, id.getParserPosition());
        }
      }
      return id;
    }
  }

  /**
   * Implements a CREATE TABLE...WITH... DDL statement.
   * <p>
   * N.B. the following magic:
   *  - field 'PRIMARY_KEY' is treated as a PRIMARY KEY
   *  - NULL fields are promoted to BYTES
   */
  class ConnectorImplementor implements ScriptImplementor {
    private final String catalog;
    private final String schema;
    private final String table;
    private final String suffix;
    private final RelDataType rowType;
    private final Map<String, String> connectorConfig;

    public ConnectorImplementor(String catalog, String schema, String table, String suffix, RelDataType rowType, Map<String, String> connectorConfig) {
      this.catalog = catalog;
      this.schema = schema;
      this.table = table;
      this.suffix = suffix;
      this.rowType = rowType;
      this.connectorConfig = connectorConfig;
    }

    @Override
    public void implement(SqlWriter w) {
      w.keyword("CREATE TABLE IF NOT EXISTS");
      String effectiveTable = suffix != null ? table + suffix : table;
      (new CompoundIdentifierImplementor(catalog, schema, effectiveTable)).implement(w);
      SqlWriter.Frame frame1 = w.startList("(", ")");
      (new RowTypeSpecImplementor(rowType)).implement(w);
      if (rowType.getField("PRIMARY_KEY", true, false) != null) {
        w.sep(",");
        w.literal("PRIMARY KEY (PRIMARY_KEY) NOT ENFORCED");
      }
      w.endList(frame1);
      // TODO support PARTITIONED BY for Tables that support it
      w.keyword("WITH");
      SqlWriter.Frame frame2 = w.startList("(", ")");
      (new ConfigSpecImplementor(connectorConfig)).implement(w);
      w.endList(frame2);
      w.literal(";");
    }
  }

  /** Implements a CREATE TEMPORARY VIEW DDL statement */
  class ViewImplementor implements ScriptImplementor {
    private final String name;
    private final RelNode relNode;

    public ViewImplementor(String name, RelNode relNode) {
      this.name = name;
      this.relNode = relNode;
    }

    @Override
    public void implement(SqlWriter w) {
      w.keyword("CREATE TEMPORARY VIEW");
      (new IdentifierImplementor(name)).implement(w);
      w.keyword("AS");
      (new QueryImplementor(relNode)).implement(w);
      w.literal(";");
    }
  }

  /** Implements an INSERT INTO statement.
   * <p>
   * N.B. the following magic:
   *  - NULL columns (e.g. `NULL AS KEY`) are elided from the pipeline
   * <p>
   * */
  class InsertImplementor implements ScriptImplementor {
    private final String catalog;
    private final String schema;
    private final String table;
    private final String suffix;
    private final RelNode relNode;
    private final ImmutablePairList<Integer, String> targetFields;
    private final Map<String, String> tableNameReplacements;

    public InsertImplementor(@Nullable String catalog, String schema, String table, @Nullable String suffix, RelNode relNode,
        ImmutablePairList<Integer, String> targetFields, Map<String, String> tableNameReplacements) {
      this.catalog = catalog;
      this.schema = schema;
      this.table = table;
      this.suffix = suffix;
      this.relNode = relNode;
      this.targetFields = targetFields;
      this.tableNameReplacements = tableNameReplacements;
    }

    @Override
    public void implement(SqlWriter w) {
      w.keyword("INSERT INTO");
      String effectiveTable = suffix != null ? table + suffix : table;
      (new CompoundIdentifierImplementor(catalog, schema, effectiveTable)).implement(w);
      RelNode project = dropFields(relNode, targetFields);
      (new ColumnListImplementor(project.getRowType())).implement(w);
      (new QueryImplementor(project, tableNameReplacements)).implement(w);
      w.literal(";");
    }

    // Drops NULL fields
    // Drops non-target columns, for use case: INSERT INTO (col1, col2) SELECT * FROM ...
    private static RelNode dropFields(RelNode relNode, ImmutablePairList<Integer, String> targetFields) {
      List<Integer> cols = new ArrayList<>();
      int i = 0;
      List<String> targetFieldNames = targetFields.rightList();
      for (RelDataTypeField field : relNode.getRowType().getFieldList()) {
        if (targetFieldNames.contains(field.getName())
            && !field.getType().getSqlTypeName().equals(SqlTypeName.NULL)) {
          cols.add(i);
        }
        i++;
      }
      return createForceProject(relNode, cols);
    }
  }

  static RelNode createForceProject(final RelNode child, final List<Integer> posList) {
    return createForceProject(RelFactories.DEFAULT_PROJECT_FACTORY, child, posList);
  }

  // By default, "projectNamed" will try to provide an optimization by not creating a new project if the
  // field types are the same. This is not desirable in the insert case as the field names need to match the sink.
  //
  // Example:
  // INSERT INTO `my-store` (`KEY_id`, `stringField`) SELECT * FROM `KAFKA`.`existing-topic-1`;
  // Without forced projection this will get optimized to:
  // INSERT INTO `my-store` (`KEYFIELD`, `VARCHARFIELD`) SELECT * FROM `KAFKA`.`existing-topic-1`;
  // With forced project this will resolve as:
  // INSERT INTO `my-store` (`KEY_id`, `stringField`) SELECT `KEYFIELD` AS `KEY_id`, \
  // `VARCHARFIELD` AS `stringField` FROM `KAFKA`.`existing-topic-1`;
  //
  // This implementation is largely a duplicate of RelOptUtil.createProject(relNode, cols); which does not allow
  // overriding the "force" argument of "projectNamed".
  static RelNode createForceProject(final RelFactories.ProjectFactory factory,
      final RelNode child, final List<Integer> posList) {
    RelDataType rowType = child.getRowType();
    final List<String> fieldNames = rowType.getFieldNames();
    final RelBuilder relBuilder =
        RelBuilder.proto(factory).create(child.getCluster(), null);
    final List<RexNode> exprs = new AbstractList<>() {
      @Override
      public int size() {
        return posList.size();
      }

      @Override
      public RexNode get(int index) {
        final int pos = posList.get(index);
        return relBuilder.getRexBuilder().makeInputRef(child, pos);
      }
    };
    final List<String> names = Util.select(fieldNames, posList);
    return relBuilder
        .push(child)
        .projectNamed(exprs, names, true)
        .build();
  }

  /** Implements a CREATE CATALOG IF NOT EXISTS statement */
  class CatalogImplementor implements ScriptImplementor {
    private final String catalog;

    public CatalogImplementor(@Nullable String catalog) {
      this.catalog = catalog;
    }

    @Override
    public void implement(SqlWriter w) {
      if (catalog == null) {
        return;
      }

      w.keyword("CREATE CATALOG IF NOT EXISTS");
      w.identifier(catalog, true);
      w.keyword("WITH");
      SqlWriter.Frame parens = w.startList("(", ")");
      w.endList(parens);
      w.literal(";");
    }
  }

  /** Implements a CREATE DATABASE IF NOT EXISTS statement */
  class DatabaseImplementor implements ScriptImplementor {
    private final String database;
    private final String catalog;

    public DatabaseImplementor(@Nullable String catalog, String database) {
      this.catalog = catalog;
      this.database = database;
    }

    @Override
    public void implement(SqlWriter w) {
      w.keyword("CREATE DATABASE IF NOT EXISTS");
      (new CompoundIdentifierImplementor(catalog, database, null)).implement(w);
      w.keyword("WITH");
      SqlWriter.Frame parens = w.startList("(", ")");
      w.endList(parens);
      w.literal(";");
    }
  }

  /** Implements an identifier like TRACKING.'PageViewEvent' */
  class CompoundIdentifierImplementor implements ScriptImplementor {
    private final String catalog;
    private final String schema;
    private final String table;

    public CompoundIdentifierImplementor(@Nullable String catalog, @Nullable String schema, @Nullable String table) {
      this.catalog = catalog;
      this.schema = schema;
      this.table = table;
    }

    @Override
    public void implement(SqlWriter w) {
      SqlIdentifier identifier = null;
      if (catalog != null && schema != null && table != null) {
        identifier = new SqlIdentifier(Arrays.asList(catalog, schema, table), SqlParserPos.ZERO);
      } else if (catalog != null && schema != null) {
        identifier = new SqlIdentifier(Arrays.asList(catalog, schema), SqlParserPos.ZERO);
      } else if (catalog != null && table != null) {
        identifier = new SqlIdentifier(Arrays.asList(catalog, table), SqlParserPos.ZERO);
      } else if (schema != null && table != null) {
        identifier = new SqlIdentifier(Arrays.asList(schema, table), SqlParserPos.ZERO);
      } else if (catalog != null) {
        w.identifier(catalog, true);
      } else if (schema != null) {
        w.identifier(schema, true);
      } else if (table != null) {
        w.identifier(table, true);
      }
      if (identifier != null) {
        identifier.unparse(w, 0, 0);
      }
    }
  }

  /** Implements an identifier like 'PageViewEvent' */
  class IdentifierImplementor implements ScriptImplementor {
    private final String name;

    public IdentifierImplementor(String name) {
      this.name = name;
    }

    @Override
    public void implement(SqlWriter w) {
      SqlIdentifier identifier = new SqlIdentifier(Collections.singletonList(name), SqlParserPos.ZERO);
      identifier.unparse(w, 0, 0);
    }
  }

  /** Implements row type specs, e.g. `NAME VARCHAR(20), AGE INTEGER`.
   * <p>
   * N.B. the following magic:
   *  - NULL fields are promoted to BYTES
   *  - Flattened fields like FOO$BAR are renamed FOO_BAR
   */
  class RowTypeSpecImplementor implements ScriptImplementor {
    private final RelDataType dataType;

    public RowTypeSpecImplementor(RelDataType dataType) {
      this.dataType = dataType;
    }

    @Override
    public void implement(SqlWriter w) {
      RelDataType flattened = dataType;
      List<SqlIdentifier> fieldNames = flattened.getFieldList()
          .stream()
          .map(RelDataTypeField::getName)
          .map(x -> x.replaceAll("\\$", "_"))  // support FOO$BAR columns as FOO_BAR
          .map(x -> new SqlIdentifier(x, SqlParserPos.ZERO))
          .collect(Collectors.toList());
      List<SqlDataTypeSpec> fieldTypes =
          flattened.getFieldList().stream().map(RelDataTypeField::getType).map(RowTypeSpecImplementor::toSpec).collect(Collectors.toList());
      for (int i = 0; i < fieldNames.size(); i++) {
        w.sep(",");
        fieldNames.get(i).unparse(w, 0, 0);
        if (fieldTypes.get(i).getTypeName().getSimple().equals("NULL")) {
          w.literal("BYTES"); // promote NULL fields to BYTES
        } else {
          fieldTypes.get(i).unparse(w, 0, 0);
        }
      }
    }

    private static SqlDataTypeSpec toSpec(RelDataType dataType) {
      if (dataType.isStruct()) {
        List<SqlIdentifier> fieldNames = dataType.getFieldList()
            .stream()
            .map(RelDataTypeField::getName)
            .map(x -> new SqlIdentifier(x, SqlParserPos.ZERO))
            .collect(Collectors.toList());
        List<SqlDataTypeSpec> fieldTypes = dataType.getFieldList()
            .stream()
            .map(RelDataTypeField::getType)
            .map(RowTypeSpecImplementor::toSpec)
            .collect(Collectors.toList());
        return maybeNullable(dataType,
            new SqlDataTypeSpec(new SqlRowTypeNameSpec(SqlParserPos.ZERO, fieldNames, fieldTypes), SqlParserPos.ZERO));
      }
      RelDataType componentType = dataType.getComponentType();
      if (componentType != null) {
        // To handle ROW ARRAY types
        if (componentType.isStruct()) {
          List<SqlIdentifier> fieldNames = componentType.getFieldList()
              .stream()
              .map(RelDataTypeField::getName)
              .map(x -> new SqlIdentifier(x, SqlParserPos.ZERO))
              .collect(Collectors.toList());
          List<SqlDataTypeSpec> fieldTypes = componentType.getFieldList()
              .stream()
              .map(RelDataTypeField::getType)
              .map(RowTypeSpecImplementor::toSpec)
              .collect(Collectors.toList());
          return maybeNullable(dataType, new SqlDataTypeSpec(new SqlCollectionTypeNameSpec(
              new SqlRowTypeNameSpec(SqlParserPos.ZERO, fieldNames, fieldTypes),
              dataType.getSqlTypeName(), SqlParserPos.ZERO), SqlParserPos.ZERO));
        } else if (componentType instanceof BasicSqlType) {
          // To handle primitive ARRAY types, e.g. `FLOAT ARRAY`.
          return maybeNullable(dataType, new SqlDataTypeSpec(new SqlCollectionTypeNameSpec(new SqlBasicTypeNameSpec(
              componentType.getSqlTypeName(), SqlParserPos.ZERO), dataType.getSqlTypeName(), SqlParserPos.ZERO), SqlParserPos.ZERO));
        } else {
          // To handle nested arrays
          return maybeNullable(dataType, new SqlDataTypeSpec(new SqlCollectionTypeNameSpec(
              toSpec(componentType).getTypeNameSpec(),
              componentType.getSqlTypeName(), SqlParserPos.ZERO), SqlParserPos.ZERO));
        }
      } else if (dataType.getKeyType() != null && dataType.getValueType() != null) {
        return maybeNullable(dataType, new SqlDataTypeSpec(new SqlMapTypeNameSpec(
            toSpec(dataType.getKeyType()), toSpec(dataType.getValueType()), SqlParserPos.ZERO), SqlParserPos.ZERO));
      } else {
        return maybeNullable(dataType,
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(dataType.getSqlTypeName(), SqlParserPos.ZERO),
                SqlParserPos.ZERO));
      }
    }

    private static SqlDataTypeSpec maybeNullable(RelDataType dataType, SqlDataTypeSpec spec) {
      if (!dataType.isNullable()) {
        return spec.withNullable(false);
      } else {
        // we don't want "VARCHAR NULL", only "VARCHAR NOT NULL"
        return spec;
      }
    }
  }

  /** Implements column lists, e.g. `NAME, AGE` */
  class ColumnListImplementor implements ScriptImplementor {
    private final List<RelDataTypeField> fields;

    public ColumnListImplementor(RelDataType dataType) {
      this(dataType.getFieldList());
    }

    public ColumnListImplementor(List<RelDataTypeField> fields) {
      this.fields = fields;
    }

    @Override
    public void implement(SqlWriter w) {
      SqlWriter.Frame frame1 = w.startList("(", ")");
      List<SqlIdentifier> fieldNames = fields.stream()
          .map(RelDataTypeField::getName)
          .map(x -> x.replaceAll("\\$", "_"))  // support FOO$BAR columns as FOO_BAR
          .map(x -> new SqlIdentifier(x, SqlParserPos.ZERO))
          .collect(Collectors.toList());
      for (SqlIdentifier fieldName : fieldNames) {
        w.sep(",");
        fieldName.unparse(w, 0, 0);
      }
      w.endList(frame1);
    }
  }

  /** Implements Flink's `('key'='value', ...)` clause */
  class ConfigSpecImplementor implements ScriptImplementor {
    private final Map<String, String> config;

    public ConfigSpecImplementor(Map<String, String> config) {
      this.config = config;
    }

    @Override
    public void implement(SqlWriter w) {
      config.forEach((k, v) -> {
        w.sep(",");
        w.literal("'" + k + "'='" + v + "'");
      });
    }
  }
}
