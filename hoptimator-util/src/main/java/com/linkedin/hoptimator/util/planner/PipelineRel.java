package com.linkedin.hoptimator.util.planner;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.ImmutablePairList;

import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.util.ConnectionService;


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

  void implement(Implementor implementor) throws SQLException;

  /** Implements a deployable Pipeline. */
  class Implementor {
    private final Map<Source, RelDataType> sources = new LinkedHashMap<>();
    private final ImmutablePairList<Integer, String> targetFields;
    private final Map<String, String> hints;
    private RelNode query;
    private Sink sink;
    private RelDataType sinkRowType = null;

    public Implementor(ImmutablePairList<Integer, String> targetFields, Map<String, String> hints) {
      this.targetFields = targetFields;
      this.hints = hints;
      this.sink = new Sink("PIPELINE", Arrays.asList("PIPELINE", "SINK"), hints);
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
    public Pipeline pipeline(String name, Properties connectionProperties) throws SQLException {
      Job job = new Job(name, sink, sql(connectionProperties));
      return new Pipeline(sources.keySet(), sink, job);
    }

    private ScriptImplementor script(Properties connectionProperties) throws SQLException {
      ScriptImplementor script = ScriptImplementor.empty();
      for (Map.Entry<Source, RelDataType> source : sources.entrySet()) {
        script = script.database(source.getKey().schema());
        Map<String, String> configs = ConnectionService.configure(source.getKey(), connectionProperties);
        script = script.connector(source.getKey().schema(), source.getKey().table(), source.getValue(), configs);
      }
      return script;
    }

    /** SQL script ending in an INSERT INTO */
    public Function<SqlDialect, String> sql(Properties connectionProperties) throws SQLException {
      ScriptImplementor script = script(connectionProperties);
      RelDataType targetRowType = sinkRowType;
      if (targetRowType == null) {
        targetRowType = query.getRowType();
      } else {
        // Assert target fields exist in the sink schema when the sink schema is known (partial view use case)
        for (String fieldName : targetFields.rightList()) {
          if (!targetRowType.getFieldNames().contains(fieldName)) {
            throw new SQLNonTransientException("Field " + fieldName + " not found in sink schema");
          }
        }
      }
      Map<String, String> sinkConfigs = ConnectionService.configure(sink, connectionProperties);
      script = script.database(sink.schema());
      script = script.connector(sink.schema(), sink.table(), targetRowType, sinkConfigs);
      script = script.insert(sink.schema(), sink.table(), query, targetFields);
      return script.seal();
    }

    /** SQL script ending in a SELECT */
    public Function<SqlDialect, String> query(Properties connectionProperties) throws SQLException {
      return script(connectionProperties).query(query).seal();
    }
  }
}
