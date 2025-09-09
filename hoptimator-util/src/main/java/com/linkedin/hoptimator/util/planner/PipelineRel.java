package com.linkedin.hoptimator.util.planner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.hoptimator.ThrowingFunction;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.ImmutablePairList;

import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.util.ConnectionService;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;


/**
 * Calling convention which implements a data pipeline.
 * <p>
 * "Convention" here just means a target set of "traits" the planner should
 * aim for. We can ask the planner to convert a query into the PIPELINE
 * convention, and the result will be a PipelineRel. This in turn can be
 * implemented as a Pipeline.
 */
public interface PipelineRel extends RelNode {

  Convention CONVENTION = new Convention.Impl("PIPELINE", PipelineRel.class);
  ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  void implement(Implementor implementor) throws SQLException;

  /** Implements a deployable Pipeline. */
  class Implementor {
    private final Map<Source, RelDataType> sources = new LinkedHashMap<>();
    private final ImmutablePairList<Integer, String> targetFields;
    private final Map<String, String> hints;
    private RelNode query;
    private Sink sink = null;
    private RelDataType sinkRowType = null;

    public Implementor(ImmutablePairList<Integer, String> targetFields, Map<String, String> hints) {
      this.targetFields = targetFields;
      this.hints = hints;
    }

    public void visit(RelNode node) throws SQLException {
      if (this.query == null) {
        this.query = node;
      }
      for (RelNode input : node.getInputs()) {
        visit(input);
      }
      ((PipelineRel) node).implement(this);
    }

    /**
     * Adds a source to the pipeline.
     * <p>
     * This involves deploying any relevant objects, and configuring
     * a connector. The connector is configured via `CREATE TABLE...WITH(...)`.
     */
    public void addSource(String database, List<String> path, RelDataType rowType, Map<String, String> options) {
      Map<String, String> newOptions = new LinkedHashMap<>(options);
      newOptions.putAll(this.hints);
      sources.put(new Source(database, path, newOptions), rowType);
    }

    /**
     * Sets the sink to use for the pipeline.
     * <p>
     * By default, the sink is `PIPELINE.SINK`. An expected row type is required
     * for validation purposes.
     */
    public void setSink(String database, List<String> path, RelDataType rowType, Map<String, String> options) {
      this.sinkRowType = rowType;

      Map<String, String> newOptions = new LinkedHashMap<>(options);
      newOptions.putAll(this.hints);
      this.sink = new Sink(database, path, newOptions);
    }

    public void setQuery(RelNode query) {
      this.query = query;
    }

    /** Combine deployables into a Pipeline */
    public Pipeline pipeline(String name, Connection connection) throws SQLException {
      Map<String, ThrowingFunction<SqlDialect, String>> templateEvals = new HashMap<>();
      templateEvals.put("sql", sql(connection));
      templateEvals.put("query", query(connection));
      templateEvals.put("fieldMap", fieldMap());

      Job job = new Job(name, sink, templateEvals);
      return new Pipeline(sources.keySet(), sink, job);
    }

    private ScriptImplementor script(Connection connection) throws SQLException {
      ScriptImplementor script = ScriptImplementor.empty();
      for (Map.Entry<Source, RelDataType> source : sources.entrySet()) {
        script = script.database(source.getKey().schema());
        Map<String, String> configs = ConnectionService.configure(source.getKey(), connection);
        script = script.connector(source.getKey().schema(), source.getKey().table(), source.getValue(), configs);
      }
      return script;
    }

    /** SQL script ending in an INSERT INTO */
    public ThrowingFunction<SqlDialect, String> sql(Connection connection) throws SQLException {
      return wrap(x -> {
        ScriptImplementor script = script(connection);
        RelDataType targetRowType = sinkRowType;
        if (targetRowType == null) {
          targetRowType = query.getRowType();
        } else {
          validateFieldMapping(targetRowType);
        }
        Map<String, String> sinkConfigs = ConnectionService.configure(sink, connection);
        script = script.database(sink.schema());
        script = script.connector(sink.schema(), sink.table(), targetRowType, sinkConfigs);
        script = script.insert(sink.schema(), sink.table(), query, targetFields);
        return script.sql(x);
      });
    }

    /** SQL script ending in a SELECT */
    public ThrowingFunction<SqlDialect, String> query(Connection connection) throws SQLException {
      return wrap(x -> script(connection).query(query).sql(x));
    }

    public ThrowingFunction<SqlDialect, String> fieldMap() {
      return wrap(x -> {
        if (!TrivialQueryChecker.isTrivialQuery(query)) {
          throw new SQLNonTransientException("Field mapping is only supported for trivial queries with simple projections and aliasing.");
        }

        if (sinkRowType != null) {
          validateFieldMapping(sinkRowType);
        }

        RelToSqlConverter converter = new RelToSqlConverter(x);
        SqlImplementor.Result result = converter.visitRoot(query);
        SqlNodeList nodeList = result.asSelect().getSelectList();

        Map<String, String> fieldMap = new HashMap<>();
        for (SqlNode node : nodeList) {
          if (node instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) node;
            if (identifier.isStar()) {
              targetFields.rightList().forEach(f -> fieldMap.put(f, f));
            } else {
              fieldMap.put(identifier.toString(), identifier.toString());
            }
          } else if (node instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) node;
            if (!(call.getOperator() instanceof SqlAsOperator)
                || call.operandCount() != 2) {
              throw new SQLNonTransientException(String.format("Field mapping does not support SQL operator %s with %d operands",
                  call.getClass().getSimpleName(), call.operandCount()));
            }
            if (!(call.operand(0) instanceof SqlIdentifier)
                || !(call.operand(1) instanceof SqlIdentifier)) {
              throw new SQLNonTransientException(String.format("Field mapping does not support operand types %s and %s",
                  call.operand(0).getKind(), call.operand(1).getKind()));
            }
            SqlIdentifier original = call.operand(0);
            SqlIdentifier alias = call.operand(1);
            fieldMap.put(original.toString(), alias.toString());
          } else {
            throw new SQLNonTransientException("Unsupported SQL node for field mapping: " + node);
          }
        }
        try {
          return OBJECT_MAPPER.writeValueAsString(fieldMap);
        } catch (Exception e) {
        throw new SQLNonTransientException("Failed to serialize field map to JSON", e);
      }
      });
    }

    private void validateFieldMapping(RelDataType targetRowType) throws SQLException {
      // Assert target fields exist in the sink schema when the sink schema is known (partial view use case)
      for (String fieldName : targetFields.rightList()) {
        if (!targetRowType.getFieldNames().contains(fieldName)) {
          throw new SQLNonTransientException("Field " + fieldName + " not found in sink schema");
        }
      }
    }

    ThrowingFunction<SqlDialect, String> wrap(ThrowingFunction<org.apache.calcite.sql.SqlDialect, String> innerFunction) {
      return x -> {
        switch (x) {
          case ANSI:
          case FLINK:
            return innerFunction.apply(AnsiSqlDialect.DEFAULT);
          default:
            throw new IllegalStateException("Unknown SQL dialect: " + x);
        }
      };
    }
  }
}
