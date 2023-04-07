package com.linkedin.hoptimator.catalog;

import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlRowTypeNameSpec;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;

import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.stream.Collectors;

/** An abstract way to write SQL scripts. */
public interface ScriptImplementor {

  void implement(SqlWriter writer);

  static ScriptImplementor empty() {
    return w -> { };
  }

  default ScriptImplementor with(ScriptImplementor next) {
    return w -> {
      implement(w);
      next.implement(w);
    };
  }

  default ScriptImplementor query(RelNode relNode) {
    return with(new QueryImplementor(relNode));
  }

  default ScriptImplementor connector(String database, String name, RelDataType rowType,
      Map<String, String> connectorConfig) {
    return with(new ConnectorImplementor(database, name, rowType, connectorConfig));
  }

  default ScriptImplementor database(String database) {
    return with(new DatabaseImplementor(database));
  }

  default ScriptImplementor insert(String database, String name, RelNode relNode) {
    return with(new InsertImplementor(database, name, relNode));
  }
 
  default String sql() {
    return sql(AnsiSqlDialect.DEFAULT);
  }

  default String sql(SqlDialect dialect) {
    SqlWriter w = new SqlPrettyWriter(SqlWriterConfig.of().withDialect(dialect));
    implement(w);
    return w.toSqlString().getSql()
      .replaceAll("\\n", " ").replaceAll(";", ";\n").trim();
  }

  /** Implements an arbitrary RelNode as a statement */
  class RelStatementImplementor implements ScriptImplementor {
    private final RelNode relNode;

    public RelStatementImplementor(RelNode relNode) {
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
      w.literal(result.asSelect().toSqlString(w.getDialect()).getSql());
    }
  } 

  /** Implements a CREATE TABLE...PARTITIONED BY...WITH... Flink DDL statement */
  class ConnectorImplementor implements ScriptImplementor {
    private final String database;
    private final String name;
    private final RelDataType rowType;
    private final Map<String, String> connectorConfig;

    public ConnectorImplementor(String database, String name, RelDataType rowType,
        Map<String, String> connectorConfig) {
      this.database = database;
      this.name = name;
      this.rowType = rowType;
      this.connectorConfig = connectorConfig;
    }

    @Override
    public void implement(SqlWriter w) {
      RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      w.keyword("CREATE TABLE");
      (new CompoundIdentifierImplementor(database, name)).implement(w);
      SqlWriter.Frame frame1 = w.startList("(", ")");
      (new DataTypeSpecImplementor(rowType)).implement(w);
      w.endList(frame1);
      // TODO support PARTITIONED BY for Tables that support it
      w.keyword("WITH");
      SqlWriter.Frame frame2 = w.startList("(", ")");
      (new ConfigSpecImplementor(connectorConfig)).implement(w);
      w.endList(frame2);
      w.literal(";");
    }
  }

  /** Implements a CREATE TEMPORARY VIEW Flink DDL statement */
  class ViewImplementor implements ScriptImplementor {
    private final String database;
    private final String name;
    private final RelNode relNode;

    public ViewImplementor(String database, String name, RelNode relNode) {
      this.database = database;
      this.name = name;
      this.relNode = relNode;
    }

    @Override
    public void implement(SqlWriter w) {
      w.keyword("CREATE TEMPORARY VIEW");
      (new CompoundIdentifierImplementor(database, name)).implement(w);
      w.keyword("AS");
      (new QueryImplementor(relNode)).implement(w);
      w.literal(";");
    }
  }

  /** Implements an INSERT INTO statement */
  class InsertImplementor implements ScriptImplementor {
    private final String database;
    private final String name;
    private final RelNode relNode;

    public InsertImplementor(String database, String name, RelNode relNode) {
      this.database = database;
      this.name = name;
      this.relNode = relNode;
    }

    @Override
    public void implement(SqlWriter w) {
      w.keyword("INSERT INTO");
      (new CompoundIdentifierImplementor(database, name)).implement(w);
      SqlWriter.Frame frame1 = w.startList("(", ")");
      (new ColumnListImplementor(relNode.getRowType())).implement(w);
      w.endList(frame1);
      (new QueryImplementor(relNode)).implement(w);
      w.literal(";");
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

  /** Implements type specs, e.g. NAME VARCHAR(20) */
  class DataTypeSpecImplementor implements ScriptImplementor {
    private final RelDataType dataType;

    public DataTypeSpecImplementor(RelDataType dataType) {
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
        fieldTypes.get(i).unparse(w, 0, 0);
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
      } else {
        return maybeNullable(dataType, new SqlDataTypeSpec(new SqlBasicTypeNameSpec(dataType.getSqlTypeName(), SqlParserPos.ZERO), SqlParserPos.ZERO));
      }
    }

    private static SqlDataTypeSpec maybeNullable(RelDataType dataType, SqlDataTypeSpec spec) {
      if (!dataType.isNullable()) {
        return spec.withNullable(true);
      } else {
        // we don't want "VARCHAR NULL", only "VARCHAR NOT NULL"
        return spec;
      }
    }
  }

  /** Implements column lists, e.g. NAME, AGE */
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

  /** Implements Flink's ('key'='value', ...) clause */
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
