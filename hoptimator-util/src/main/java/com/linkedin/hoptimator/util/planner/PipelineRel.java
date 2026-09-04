package com.linkedin.hoptimator.util.planner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.MissingConnectorException;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.ThrowingFunction;
import com.linkedin.hoptimator.util.ConnectionService;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.ImmutablePairList;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.fun.SqlItemOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;


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
    private final TypeCoercion.CastMode castMode;
    private RelNode query;
    private Sink sink = null;
    private RelDataType sinkRowType = null;

    public Implementor(ImmutablePairList<Integer, String> targetFields, Map<String, String> hints) {
      this.targetFields = targetFields;
      this.hints = hints;
      this.castMode = TypeCoercion.CastMode.fromHints(hints);
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

    public boolean hasSink() {
      return sink != null;
    }

    public void setQuery(RelNode query) {
      this.query = query;
    }

    /** Combine deployables into a Pipeline */
    public Pipeline pipeline(String name, DeploymentContext context) throws SQLException {
      Map<String, ThrowingFunction<SqlDialect, String>> templateEvals = new HashMap<>();
      templateEvals.put("sql", sql(context));
      templateEvals.put("query", query(context));
      templateEvals.put("fieldMap", fieldMap());

      Job job = new Job(name, sources.keySet(), sink, templateEvals);
      return new Pipeline(sources.keySet(), sink, job);
    }

    private ScriptImplementor script(DeploymentContext context) throws SQLException {
      ScriptImplementor script = ScriptImplementor.empty();
      // Check if we need to add suffixes to avoid table name collisions
      boolean needsSuffixes = hasTableNameCollision();

      for (Map.Entry<Source, RelDataType> source : sources.entrySet()) {
        script = script.catalog(source.getKey().catalog());
        script = script.database(source.getKey().catalog(), source.getKey().schema());
        Map<String, String> configs = ConnectionService.configure(source.getKey(), context);
        // A source with no connector configuration cannot be read by a SQL job. As with a
        // connector-less sink, such a table is moved by a non-SQL job (with source and sink
        // reversed). Signal this to callers so they can skip SQL generation.
        if (configs.isEmpty()) {
          throw new MissingConnectorException(source.getKey().pathString());
        }
        String suffix = needsSuffixes ? "_source" : null;
        script = script.connector(source.getKey().catalog(), source.getKey().schema(), source.getKey().table(), suffix, source.getValue(), configs);
      }
      return script;
    }

    /**
     * Checks if there's a collision between source and sink table names.
     * A collision occurs when a source table has the same catalog, schema, and table name as the sink.
     */
    private boolean hasTableNameCollision() {
      if (sink == null) {
        return false;
      }
      for (Source source : sources.keySet()) {
        if (Objects.equals(source.catalog(), sink.catalog())
            && Objects.equals(source.schema(), sink.schema())
            && Objects.equals(source.table(), sink.table())) {
          return true;
        }
      }
      return false;
    }

    /** SQL script ending in an INSERT INTO */
    public ThrowingFunction<SqlDialect, String> sql(DeploymentContext context) throws SQLException {
      return wrap(x -> {
        ScriptImplementor script = script(context);
        RelDataType targetRowType = sinkRowType;
        Map<String, RelDataType> castTargets = Collections.emptyMap();
        if (targetRowType == null) {
          targetRowType = query.getRowType();
        } else {
          validateFieldMapping(targetRowType);
          castTargets = resolveCasts(targetRowType);
        }
        Map<String, String> sinkConfigs = ConnectionService.configure(sink, context);
        // A sink with no connector configuration cannot be materialized by a SQL job. Signal
        // this to callers so they can skip SQL generation while still emitting non-SQL jobs.
        if (sinkConfigs.isEmpty()) {
          throw new MissingConnectorException(sink.pathString());
        }
        script = script.catalog(sink.catalog());
        script = script.database(sink.catalog(), sink.schema());
        // Check if we need to add suffixes to avoid table name collisions
        boolean needsSuffixes = hasTableNameCollision();
        String sinkSuffix = needsSuffixes ? "_sink" : null;
        script = script.connector(sink.catalog(), sink.schema(), sink.table(), sinkSuffix, targetRowType, sinkConfigs);

        // Build table name replacement map for the query
        Map<String, String> tableNameReplacements = new HashMap<>();
        if (needsSuffixes) {
          for (Source source : sources.keySet()) {
            String qualifiedName = source.pathString();
            String suffixedTable = source.table() + "_source";
            tableNameReplacements.put(qualifiedName, suffixedTable);
          }
        }

        script = script.insert(sink.catalog(), sink.schema(), sink.table(), sinkSuffix, query, targetFields, tableNameReplacements, castTargets);
        return script.sql(x);
      });
    }

    /** SQL script ending in a SELECT */
    public ThrowingFunction<SqlDialect, String> query(DeploymentContext context) throws SQLException {
      return wrap(x -> script(context).query(query).sql(x));
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

        Map<String, String> fieldMap = buildFieldMappingFromSqlNodes(nodeList);
        try {
          return OBJECT_MAPPER.writeValueAsString(fieldMap);
        } catch (Exception e) {
        throw new SQLNonTransientException("Failed to serialize field map to JSON", e);
      }
      });
    }

    void validateFieldMapping(RelDataType targetRowType) throws SQLException {
      // Assert target fields exist in the sink schema when the sink schema is known (partial view use case)
      for (String fieldName : targetFields.rightList()) {
        if (!targetRowType.getFieldNames().contains(fieldName)) {
          throw new SQLNonTransientException("Field " + fieldName + " not found in sink schema");
        }
      }
    }

    /**
     * Compares the query output type against the sink schema column-by-column and decides, per the
     * active {@link TypeCoercion.CastMode}, whether each projected column needs an assignment cast.
     * Returns a map from sink column name to the type Hoptimator should {@code CAST} that column to
     * (only for columns that require injection). Raises a {@link SQLNonTransientException} for any
     * column whose types are incompatible, so the failure surfaces here rather than late at job
     * submission. An explicit user {@code CAST} is respected and never double-wrapped.
     */
    Map<String, RelDataType> resolveCasts(RelDataType targetRowType) throws SQLException {
      Map<String, RelDataType> castTargets = new LinkedHashMap<>();
      List<RelDataTypeField> queryFields = query.getRowType().getFieldList();
      for (int i = 0; i < targetFields.size(); i++) {
        int queryIndex = targetFields.leftList().get(i);
        String sinkName = targetFields.rightList().get(i);
        RelDataTypeField sinkField = targetRowType.getField(sinkName, true, false);
        if (sinkField == null || queryIndex >= queryFields.size()) {
          continue;
        }
        RelDataType sourceType = queryFields.get(queryIndex).getType();
        RelDataType targetType = sinkField.getType();
        // NULL-typed columns (e.g. `NULL AS KEY`) are elided from the pipeline downstream; skip them.
        if (sourceType.getSqlTypeName() == SqlTypeName.NULL) {
          continue;
        }
        TypeCoercion.Decision decision = TypeCoercion.decide(sourceType, targetType, castMode);
        switch (decision) {
          case NONE:
            break;
          case CAST:
            // Respect an explicit user CAST: never stack another level on top of it. If its result
            // is not already assignable to the sink column, fail early rather than defer to the engine.
            if (isUserProvidedCast(queryIndex)) {
              if (!TypeCoercion.isImplicitlyAssignable(sourceType, targetType)) {
                throw incompatibleColumn(sinkName, sourceType, targetType,
                    "an explicit CAST is present but its result type is not assignable to the sink column");
              }
            } else {
              castTargets.put(sinkName, targetType);
            }
            break;
          case INCOMPATIBLE:
          default:
            throw incompatibleColumn(sinkName, sourceType, targetType,
                "no safe conversion is available under castMode=" + castMode.name().toLowerCase(Locale.ROOT));
        }
      }
      return castTargets;
    }

    private boolean isUserProvidedCast(int queryIndex) {
      if (query instanceof Project) {
        List<RexNode> projects = ((Project) query).getProjects();
        if (queryIndex < projects.size()) {
          SqlKind kind = projects.get(queryIndex).getKind();
          return kind == SqlKind.CAST || kind == SqlKind.SAFE_CAST;
        }
      }
      return false;
    }

    private SQLNonTransientException incompatibleColumn(String column, RelDataType source, RelDataType target,
        String reason) {
      return new SQLNonTransientException(String.format(
          "Incompatible types for sink column '%s': query produces %s but sink expects %s (%s).",
          column, source.getFullTypeString(), target.getFullTypeString(), reason));
    }

    /**
     * Builds a field mapping from SQL node list, handling simple identifiers, aliases, and nested field access.
     *
     * @param nodeList The SQL node list from the SELECT clause
     * @return A map from source field names to target field names
     * @throws SQLNonTransientException if unsupported SQL constructs are encountered
     */
    Map<String, String> buildFieldMappingFromSqlNodes(SqlNodeList nodeList) throws SQLNonTransientException {
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
          if (call.getOperator() instanceof SqlAsOperator && call.operandCount() == 2) {
            // Handle AS operator (aliasing)
            SqlNode original = call.operand(0);
            SqlNode alias = call.operand(1);

            if (!(alias instanceof SqlIdentifier)) {
              throw new SQLNonTransientException(String.format("Field mapping alias must be an identifier, got: %s", alias.getKind()));
            }

            String originalFieldName = extractFieldName(original);
            String aliasName = alias.toString();
            fieldMap.put(originalFieldName, aliasName);
          } else {
            // Handle other operators like ITEM for nested field access
            String fieldName = extractFieldName(call);
            fieldMap.put(fieldName, fieldName);
          }
        } else {
          throw new SQLNonTransientException("Unsupported SQL node for field mapping: " + node);
        }
      }
      return fieldMap;
    }

    /**
     * Extracts field name from SqlNode, handling nested field access with ITEM operator.
     *
     * @param node The SqlNode to extract field name from
     * @return The field name, using dot notation for nested fields (e.g., "field.nestedField")
     * @throws SQLNonTransientException if the node type is not supported
     */
    private String extractFieldName(SqlNode node) throws SQLNonTransientException {
      if (node instanceof SqlIdentifier) {
        return node.toString();
      } else if (node instanceof SqlBasicCall) {
        SqlBasicCall call = (SqlBasicCall) node;

        // Handle ITEM operator for nested field access: ITEM($field, 'nestedField')
        if (call.getOperator() instanceof SqlItemOperator && call.operandCount() == 2) {
          SqlNode baseField = call.operand(0);
          SqlNode nestedFieldLiteral = call.operand(1);

          if (!(nestedFieldLiteral instanceof SqlLiteral)) {
            throw new SQLNonTransientException("ITEM operator second operand must be a literal, got: " + nestedFieldLiteral.getKind());
          }

          String baseFieldName = extractFieldName(baseField);
          String nestedFieldName = ((SqlLiteral) nestedFieldLiteral).getValueAs(String.class);

          // Use dot notation for nested fields
          return baseFieldName + "." + nestedFieldName;
        } else {
          throw new SQLNonTransientException("Unsupported SQL operator for field mapping: " + call.getOperator());
        }
      } else {
        throw new SQLNonTransientException("Unsupported SQL node type for field extraction: " + node.getKind());
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
