package com.linkedin.hoptimator.util.planner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlRowTypeNameSpec;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.Optional;

/**
 * An abstract way to write SQL scripts.
 *
 * To generate a specific statement, implement this interface, or use one
 * of the provided implementations, e.g. `QueryImplementor`.
 *
 * To generate a script (more than one statement), start with `empty()`
 * and append subsequent ScriptImplementors with `with(...)` etc.
 *
 * e.g.
 *
 *   ScriptImplementor.empty()
 *     .database(db)
 *     .connector(db, name, rowType, configs)
 *
 * ... would produce something like
 *
 *   CREATE DATABASE IF NOT EXIST `FOO`;
 *   CREATE TABLE `BAR` (NAME VARCHAR) WITH ('key1' = 'value1');
 */
public interface ScriptImplementor {

  /** Writes arbitrary DDL/SQL */
  void implement(SqlWriter writer);

  /** Construct an empty ScriptImplementor */
  static ScriptImplementor empty() {
    return w -> { };
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
    return with(new QueryImplementor(relNode));
  }

  /** Append a connector definition, e.g. `CREATE TABLE ... WITH (...)` */
  default ScriptImplementor connector(String name, RelDataType rowType,
      Map<String, String> connectorConfig) {
    return with(new ConnectorImplementor(name, rowType, connectorConfig));
  }

  /** Append a database definition, e.g. `CREATE DATABASE ...` */
  default ScriptImplementor database(String database) {
    return with(new DatabaseImplementor(database));
  }

  /** Append an insert statement, e.g. `INSERT INTO ... SELECT ...` */
  default ScriptImplementor insert(String name, RelNode relNode) {
    return with(new InsertImplementor(name, relNode));
  }

  /** Render the script as DDL/SQL in the default dialect */ 
  default String sql() {
    return sql(AnsiSqlDialect.DEFAULT);
  }

  /** Render the script as DDL/SQL in the given dialect */
  default String sql(SqlDialect dialect) {
    SqlWriter w = new SqlPrettyWriter(SqlWriterConfig.of().withDialect(dialect));
    implement(w);
    return w.toSqlString().getSql()
      .replaceAll("\\n", " ").replaceAll("\\s*;\\s*", ";\n").trim();
  }

  /** Generate SQL for a given dialect */
  default Function<SqlDialect, String> seal() {
    return x -> sql(x);
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

    public QueryImplementor(RelNode relNode) {
      this.relNode = relNode;
    }

    @Override
    public void implement(SqlWriter w) {
      RelToSqlConverter converter = new RelToSqlConverter(w.getDialect());
      SqlImplementor.Result result = converter.visitRoot(relNode);
      SqlSelect select = result.asSelect();
      if (select.getSelectList() != null) {
        select.setSelectList((SqlNodeList) select.getSelectList().accept(REMOVE_ROW_CONSTRUCTOR));
      }
      select.unparse(w, 0, 0);
    }

    // A `ROW(...)` operator which will unparse as just `(...)`.
    private static final SqlRowOperator IMPLIED_ROW_OPERATOR = new SqlRowOperator(""); // empty string name

    // a shuttle that replaces `Row(...)` with just `(...)`
    private static final SqlShuttle REMOVE_ROW_CONSTRUCTOR = new SqlShuttle() {
      @Override
      public SqlNode visit(SqlCall call) {
        List<SqlNode> operands = call.getOperandList().stream().map(x -> x.accept(this)).collect(Collectors.toList());
        if ((call.getKind() == SqlKind.ROW || call.getKind() == SqlKind.COLUMN_LIST
              || call.getOperator() instanceof SqlRowOperator)
              && operands.size() > 1) {
          return IMPLIED_ROW_OPERATOR.createCall(call.getParserPosition(), operands);
        } else {
          return call.getOperator().createCall(call.getParserPosition(), operands);
        }
      }
    };
  }

  /**
   * Implements a CREATE TABLE...WITH... DDL statement.
   *
   * N.B. the following magic:
   *  - field 'PRIMARY_KEY' is treated as a PRIMARY KEY
   *  - NULL fields are promoted to BYTES
   */
  class ConnectorImplementor implements ScriptImplementor {
    private final String name;
    private final RelDataType rowType;
    private final Map<String, String> connectorConfig;

    public ConnectorImplementor(String name, RelDataType rowType,
        Map<String, String> connectorConfig) {
      this.name = name;
      this.rowType = rowType;
      this.connectorConfig = connectorConfig;
    }

    @Override
    public void implement(SqlWriter w) {
      w.keyword("CREATE TABLE IF NOT EXISTS");
      (new IdentifierImplementor(name)).implement(w);
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
   *
   * N.B. the following magic:
   *  - NULL columns (e.g. `NULL AS KEY`) are elided from the pipeline
   *
   * */
  class InsertImplementor implements ScriptImplementor {
    private final String name;
    private final RelNode relNode;

    public InsertImplementor(String name, RelNode relNode) {
      this.name = name;
      this.relNode = relNode;
    }

    @Override
    public void implement(SqlWriter w) {
      w.keyword("INSERT INTO");
      (new IdentifierImplementor(name)).implement(w);
      SqlWriter.Frame frame1 = w.startList("(", ")");
      RelNode project = dropNullFields(relNode);
      (new ColumnListImplementor(project.getRowType())).implement(w);
      w.endList(frame1);
      (new QueryImplementor(project)).implement(w);
      w.literal(";");
    }

    private static RelNode dropNullFields(RelNode relNode) {
      List<Integer> cols = new ArrayList<>();
      int i = 0;
      for (RelDataTypeField field : relNode.getRowType().getFieldList()) {
        if (!field.getType().getSqlTypeName().equals(SqlTypeName.NULL)) {
          cols.add(i);
        }
        i++;
      }
      return RelOptUtil.createProject(relNode, cols);
    }
  }

  /** Implements a CREATE DATABASE IF NOT EXISTS statement */
  class DatabaseImplementor implements ScriptImplementor {
    private final String database;

    public DatabaseImplementor(String database) {
      this.database = database;
    }

    @Override
    public void implement(SqlWriter w) {
      w.keyword("CREATE DATABASE IF NOT EXISTS");
      w.identifier(database, true);
      w.keyword("WITH");
      SqlWriter.Frame parens = w.startList("(", ")");
      w.endList(parens);
      w.literal(";");
    }
  }

  /** Implements an identifier like TRACKING.'PageViewEvent' */
  class CompoundIdentifierImplementor implements ScriptImplementor {
    private final String database;
    private final String name;

    public CompoundIdentifierImplementor(String database, String name) {
      this.database = database;
      this.name = name;
    }

    @Override
    public void implement(SqlWriter w) {
      SqlIdentifier identifier = new SqlIdentifier(Arrays.asList(new String[]{database, name}), SqlParserPos.ZERO);
      identifier.unparse(w, 0, 0);
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
      SqlIdentifier identifier = new SqlIdentifier(Arrays.asList(new String[]{name}), SqlParserPos.ZERO);
      identifier.unparse(w, 0, 0);
    }
  }


  /** Implements row type specs, e.g. `NAME VARCHAR(20), AGE INTEGER`.
   *
   * N.B. the following magic:
   *  - NULL fields are promoted to BYTES
   */
  class RowTypeSpecImplementor implements ScriptImplementor {
    private final RelDataType dataType;

    public RowTypeSpecImplementor(RelDataType dataType) {
      this.dataType = dataType;
    }

    @Override
    public void implement(SqlWriter w) {
      List<SqlIdentifier> fieldNames = dataType.getFieldList().stream()
          .map(x -> x.getName())
          .map(x -> new SqlIdentifier(x, SqlParserPos.ZERO))
          .collect(Collectors.toList());
      List<SqlDataTypeSpec> fieldTypes = dataType.getFieldList().stream()
          .map(x -> x.getType())
          .map(x -> toSpec(x))
          .collect(Collectors.toList());
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
        List<SqlIdentifier> fieldNames = dataType.getFieldList().stream()
            .map(x -> x.getName())
            .map(x -> new SqlIdentifier(x, SqlParserPos.ZERO))
            .collect(Collectors.toList());
        List<SqlDataTypeSpec> fieldTypes = dataType.getFieldList().stream()
            .map(x -> x.getType())
            .map(x -> toSpec(x))
            .collect(Collectors.toList());
        return maybeNullable(dataType, new SqlDataTypeSpec(new SqlRowTypeNameSpec(SqlParserPos.ZERO, fieldNames, fieldTypes), SqlParserPos.ZERO));
      } else if (dataType.getComponentType() != null) {
        return maybeNullable(dataType, new SqlDataTypeSpec(new SqlCollectionTypeNameSpec(
            new SqlBasicTypeNameSpec(Optional.ofNullable(dataType.getComponentType())
            .map(x -> x.getSqlTypeName()).orElseThrow(
            () -> new IllegalArgumentException("not a collection?")), SqlParserPos.ZERO),
            dataType.getSqlTypeName(), SqlParserPos.ZERO),
            SqlParserPos.ZERO));
      } else {
        return maybeNullable(dataType, new SqlDataTypeSpec(new SqlBasicTypeNameSpec(dataType.getSqlTypeName(), SqlParserPos.ZERO), SqlParserPos.ZERO));
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
      List<SqlIdentifier> fieldNames = fields.stream()
          .map(x -> x.getName())
          .map(x -> new SqlIdentifier(x, SqlParserPos.ZERO))
          .collect(Collectors.toList());
     for (int i = 0; i < fieldNames.size(); i++) {
        w.sep(",");
        fieldNames.get(i).unparse(w, 0, 0);
      } 
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
