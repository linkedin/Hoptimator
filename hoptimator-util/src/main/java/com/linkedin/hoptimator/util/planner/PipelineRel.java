package com.linkedin.hoptimator.util.planner;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.util.ConnectionService;
import com.linkedin.hoptimator.util.DataTypeUtils;
import com.linkedin.hoptimator.util.DeploymentService;


/**
 * Calling convention which implements a data pipeline.
 *
 * "Convention" here just means a target set of "traits" the planner should
 * aim for. We can ask the planner to convert a query into the PIPELINE
 * convention, and the result will be a PipelineRel. This in turn can be
 * implemented as a Pipeline.
 */
public interface PipelineRel extends RelNode {

  Convention CONVENTION = new Convention.Impl("PIPELINE", PipelineRel.class);
  String KEY_OPTION = "keys";
  String KEY_PREFIX_OPTION = "keyPrefix";
  String KEY_TYPE_OPTION = "keyType";
  String KEY_PREFIX = "KEY_";

  void implement(Implementor implementor) throws SQLException;

  /** Implements a deployable Pipeline. */
  class Implementor {
    private final ImmutableList<Pair<Integer, String>> targetFields;
    private final Map<Source, RelDataType> sources = new LinkedHashMap<>();
    private RelNode query;
    private String sinkDatabase = "pipeline";
    private List<String> sinkPath = Arrays.asList(new String[]{"PIPELINE", "SINK"});
    private RelDataType sinkRowType = null;
    private Map<String, String> sinkOptions = Collections.emptyMap();

    public Implementor(ImmutableList<Pair<Integer, String>> targetFields) {
      this.targetFields = targetFields;
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
     *
     * This involves deploying any relevant objects, and configuring
     * a connector. The connector is configured via `CREATE TABLE...WITH(...)`.
     */
    public void addSource(String database, List<String> path, RelDataType rowType, Map<String, String> options) {
      sources.put(new Source(database, path, addKeysAsOption(options, rowType)), rowType);
    }

    /**
     * Sets the sink to use for the pipeline.
     *
     * By default, the sink is `PIPELINE.SINK`. An expected row type is required
     * for validation purposes.
     */
    public void setSink(String database, List<String> path, RelDataType rowType, Map<String, String> options) {
      this.sinkDatabase = database;
      this.sinkPath = path;
      this.sinkRowType = rowType;
      this.sinkOptions = addKeysAsOption(options, rowType);
    }

    @VisibleForTesting
    static Map<String, String> addKeysAsOption(Map<String, String> options, RelDataType rowType) {
      Map<String, String> newOptions = new LinkedHashMap<>(options);

      RelDataType flattened = DataTypeUtils.flatten(rowType, new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));

      // If the keys are already set, don't overwrite them
      if (newOptions.containsKey(KEY_OPTION)) {
        return newOptions;
      }

      String keyString = flattened.getFieldList().stream()
          .map(x -> x.getName().replaceAll("\\$", "_"))
          .filter(name -> name.startsWith(KEY_PREFIX))
          .collect(Collectors.joining(";"));
      if (!keyString.isEmpty()) {
        newOptions.put(KEY_OPTION, keyString);
        newOptions.put(KEY_PREFIX_OPTION, KEY_PREFIX);
        newOptions.put(KEY_TYPE_OPTION, "RECORD");
      }
      return newOptions;
    }

    public void setQuery(RelNode query) {
      this.query = query;
    }

    /** Combine Deployables into a Pipeline */
    public Pipeline pipeline() throws SQLException {
      List<Deployable> deployables = new ArrayList<>();
      for (Source source : sources.keySet()) {
        deployables.addAll(DeploymentService.deployables(source, Source.class));
        ConnectionService.configure(source, Source.class);
      }
      RelDataType targetRowType = sinkRowType;
      if (targetRowType == null) {
        targetRowType = query.getRowType();
      }
      Sink sink = new Sink(sinkDatabase, sinkPath, sinkOptions);
      ConnectionService.configure(sink, Sink.class);
      Job job = new Job(sink, sql());
      RelOptUtil.equal(sink.table(), targetRowType, "pipeline", query.getRowType(), Litmus.THROW);
      deployables.addAll(DeploymentService.deployables(sink, Sink.class));
      deployables.addAll(DeploymentService.deployables(job, Job.class));
      return new Pipeline(deployables);
    }

    private ScriptImplementor script() throws SQLException {
      ScriptImplementor script = ScriptImplementor.empty();
      for (Map.Entry<Source, RelDataType> source : sources.entrySet()) {
        script = script.database(source.getKey().schema());
        Map<String, String> configs = ConnectionService.configure(source.getKey(), Source.class);
        script = script.connector(source.getKey().schema(), source.getKey().table(), source.getValue(), configs);
      }
      return script;
    }

    /** SQL script ending in an INSERT INTO */
    public Function<SqlDialect, String> sql() throws SQLException {
      ScriptImplementor script = script();
      RelDataType targetRowType = sinkRowType;
      if (targetRowType == null) {
        targetRowType = query.getRowType();
      }
      Sink sink = new Sink(sinkDatabase, sinkPath, sinkOptions);
      Map<String, String> sinkConfigs = ConnectionService.configure(sink, Sink.class);
      script = script.database(sink.schema());
      script = script.connector(sink.schema(), sink.table(), targetRowType, sinkConfigs);
      script = script.insert(sink.schema(), sink.table(), query, targetFields);
      RelOptUtil.equal(sink.table(), targetRowType, "pipeline", query.getRowType(), Litmus.THROW);
      return script.seal();
    }

    /** SQL script ending in a SELECT */
    public Function<SqlDialect, String> query() throws SQLException {
      return script().query(query).seal();
    }
  }
}
