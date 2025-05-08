package com.linkedin.hoptimator.util.planner;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableSet;

import com.linkedin.hoptimator.util.DataTypeUtils;


public final class PipelineRules {

  private PipelineRules() {
  }

  public static Collection<RelOptRule> rules() {
    return Arrays.asList(PipelineFilterRule.INSTANCE, PipelineProjectRule.INSTANCE, PipelineJoinRule.INSTANCE,
        PipelineCalcRule.INSTANCE, PipelineAggregateRule.INSTANCE, PipelineUncollectRule.INSTANCE);
  }

  public static class PipelineTableScanRule extends ConverterRule {

    private final String database;

    public static PipelineTableScanRule create(HoptimatorJdbcConvention inTrait) {
      return Config.INSTANCE.withConversion(TableScan.class, inTrait, PipelineRel.CONVENTION, "PipelineTableScanRule")
          .withRuleFactory(x -> new PipelineTableScanRule(x, inTrait.database()))
          .toRule(PipelineTableScanRule.class);
    }

    protected PipelineTableScanRule(Config config, String database) {
      super(config);
      this.database = database;
    }

    @Override
    public RelNode convert(RelNode rel) {
      TableScan scan = (TableScan) rel;
      RelTraitSet traitSet = scan.getTraitSet().replace(PipelineRel.CONVENTION);
      return new PipelineTableScan(rel.getCluster(), traitSet, scan.getHints(), database, scan.getTable());
    }
  }

  static class PipelineTableScan extends TableScan implements PipelineRel {

    private final String database;

    PipelineTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, String database, RelOptTable table) {
      super(cluster, traitSet, hints, table);
      assert getConvention() == PipelineRel.CONVENTION;
      this.database = database;
    }

    @Override
    public void implement(Implementor implementor) throws SQLException {
      implementor.addSource(database, table.getQualifiedName(), table.getRowType(),
          Collections.emptyMap()); // TODO pass in table scan hints
    }
  }

  public static class PipelineTableModifyRule extends ConverterRule {

    private final String database;

    public static PipelineTableModifyRule create(HoptimatorJdbcConvention inTrait) {
      return Config.INSTANCE.withConversion(TableModify.class, inTrait, PipelineRel.CONVENTION,
              "PipelineTableModifyRule")
          .withRuleFactory(x -> new PipelineTableModifyRule(x, inTrait.database()))
          .toRule(PipelineTableModifyRule.class);
    }

    protected PipelineTableModifyRule(Config config, String database) {
      super(config);
      this.database = database;
    }

    @Override
    public RelNode convert(RelNode rel) {
      TableModify mod = (TableModify) rel;
      RelTraitSet traitSet = mod.getTraitSet().replace(PipelineRel.CONVENTION);
      return new PipelineTableModify(database, rel.getCluster(), traitSet, mod.getTable(), mod.getCatalogReader(),
          convert(mod.getInput(), traitSet), mod.getOperation(), mod.getUpdateColumnList(),
          mod.getSourceExpressionList(), mod.isFlattened());
    }
  }

  static class PipelineTableModify extends TableModify implements PipelineRel {

    private final String database;

    PipelineTableModify(String database, RelOptCluster cluster, RelTraitSet traits, RelOptTable table,
        Prepare.CatalogReader catalogReader, RelNode input, Operation operation, List<String> updateColumnList,
        List<RexNode> sourceExpressionList, boolean flattened) {
      super(cluster, traits, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened);
      assert getConvention() == PipelineRel.CONVENTION;
      this.database = database;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new PipelineTableModify(database, getCluster(), traitSet, getTable(), getCatalogReader(), sole(inputs),
          getOperation(), getUpdateColumnList(), getSourceExpressionList(), isFlattened());
    }

    @Override
    public void implement(Implementor implementor) throws SQLException {
      RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType flattened = DataTypeUtils.flatten(table.getRowType(), typeFactory);
      RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
      for (RelDataTypeField field : flattened.getFieldList()) {
        // Rewrite FOO$BAR as FOO_BAR in output tables
        builder.add(field.getName().replaceAll("\\$", "_"), field.getType());
      }
      implementor.setSink(database, table.getQualifiedName(), builder.build(), Collections.emptyMap());
      implementor.setQuery(getInput());
    }
  }

  static class PipelineFilterRule extends ConverterRule {
    static final PipelineFilterRule INSTANCE =
        Config.INSTANCE.withConversion(LogicalFilter.class, Convention.NONE, PipelineRel.CONVENTION,
                "PipelineFilterRule")
            .withRuleFactory(PipelineFilterRule::new)
            .as(Config.class)
            .toRule(PipelineFilterRule.class);

    protected PipelineFilterRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      LogicalFilter filter = (LogicalFilter) rel;
      RelTraitSet traitSet = filter.getTraitSet().replace(PipelineRel.CONVENTION);
      return new PipelineFilter(rel.getCluster(), traitSet, filter.getHints(), convert(filter.getInput(), PipelineRel.CONVENTION), filter.getCondition());
    }
  }

  static class PipelineFilter extends Filter implements PipelineRel {

    PipelineFilter(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelNode input, RexNode condition) {
      super(cluster, traitSet, hints, input, condition);
    }

    @Override
    public PipelineFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
      return new PipelineFilter(getCluster(), traitSet, getHints(), input, condition);
    }

    @Override
    public void implement(Implementor implementor) {
    }
  }

  static class PipelineProjectRule extends ConverterRule {
    static final PipelineProjectRule INSTANCE =
        Config.INSTANCE.withConversion(LogicalProject.class, Convention.NONE, PipelineRel.CONVENTION,
                "PipelineProjectRule")
            .withRuleFactory(PipelineProjectRule::new)
            .as(Config.class)
            .toRule(PipelineProjectRule.class);

    protected PipelineProjectRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      Project project = (Project) rel;
      RelTraitSet traitSet = project.getTraitSet().replace(PipelineRel.CONVENTION);
      return new PipelineProject(rel.getCluster(), traitSet, project.getHints(), convert(project.getInput(), PipelineRel.CONVENTION),
          project.getProjects(), project.getRowType());
    }
  }

  static class PipelineProject extends Project implements PipelineRel {

    PipelineProject(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelNode input, List<? extends RexNode> projects,
        RelDataType rowType) {
      super(cluster, traitSet, hints, input, projects, rowType, ImmutableSet.of());
      assert getConvention() == PipelineRel.CONVENTION;
      assert input.getConvention() == PipelineRel.CONVENTION;
    }

    @Override
    public PipelineProject copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
      return new PipelineProject(input.getCluster(), traitSet, getHints(), input, projects, rowType);
    }

    @Override
    public void implement(Implementor implementor) {
    }
  }

  static class PipelineJoinRule extends ConverterRule {
    static final PipelineJoinRule INSTANCE =
        Config.INSTANCE.withConversion(LogicalJoin.class, Convention.NONE, PipelineRel.CONVENTION, "PipelineJoinRule")
            .withRuleFactory(PipelineJoinRule::new)
            .as(Config.class)
            .toRule(PipelineJoinRule.class);

    protected PipelineJoinRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      Join join = (Join) rel;
      RelTraitSet traitSet = join.getTraitSet().replace(PipelineRel.CONVENTION);
      return new PipelineJoin(rel.getCluster(), traitSet, join.getHints(), convert(join.getLeft(), PipelineRel.CONVENTION),
          convert(join.getRight(), PipelineRel.CONVENTION), join.getCondition(), join.getVariablesSet(),
          join.getJoinType());
    }
  }

  static class PipelineJoin extends Join implements PipelineRel {

    PipelineJoin(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelNode left, RelNode right,
        RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
      super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
    }

    @Override
    public PipelineJoin copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
        boolean semiJoinDone) {
      return new PipelineJoin(getCluster(), traitSet, getHints(), left, right, condition, getVariablesSet(), joinType);
    }

    @Override
    public void implement(Implementor implementor) {
    }
  }

  static class PipelineCalcRule extends ConverterRule {
    static final PipelineCalcRule INSTANCE =
        Config.INSTANCE.withConversion(LogicalCalc.class, Convention.NONE, PipelineRel.CONVENTION, "PipelineCalcRule")
            .withRuleFactory(PipelineCalcRule::new)
            .as(Config.class)
            .toRule(PipelineCalcRule.class);

    protected PipelineCalcRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      Calc calc = (Calc) rel;
      RelTraitSet traitSet = calc.getTraitSet().replace(PipelineRel.CONVENTION);
      return new PipelineCalc(rel.getCluster(), traitSet, calc.getHints(), convert(calc.getInput()), calc.getProgram());
    }
  }

  static class PipelineCalc extends Calc implements PipelineRel {

    PipelineCalc(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelNode child, RexProgram program) {
      super(cluster, traitSet, hints, child, program);
    }

    @Override
    public PipelineCalc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
      return new PipelineCalc(getCluster(), traitSet, getHints(), child, program);
    }

    @Override
    public void implement(Implementor implementor) {
    }
  }

  static class PipelineAggregateRule extends ConverterRule {
    static final PipelineAggregateRule INSTANCE =
        Config.INSTANCE.withConversion(LogicalAggregate.class, Convention.NONE, PipelineRel.CONVENTION, "PipelineAggregateRule")
            .withRuleFactory(PipelineAggregateRule::new)
            .as(Config.class)
            .toRule(PipelineAggregateRule.class);

    protected PipelineAggregateRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      Aggregate agg = (Aggregate) rel;
      RelTraitSet traitSet = agg.getTraitSet().replace(PipelineRel.CONVENTION);
      return new PipelineAggregate(rel.getCluster(), traitSet, agg.getHints(), convert(agg.getInput(), PipelineRel.CONVENTION),
          agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList());
    }
  }

  static class PipelineAggregate extends Aggregate implements PipelineRel {

    PipelineAggregate(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelNode input,
        ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public PipelineAggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      return new PipelineAggregate(getCluster(), traitSet, getHints(), input, groupSet, groupSets, aggCalls);
    }

    @Override
    public void implement(Implementor implementor) {
    }
  }

  static class PipelineUncollectRule extends ConverterRule {
    static final PipelineUncollectRule INSTANCE =
        Config.INSTANCE.withConversion(Uncollect.class, Convention.NONE, PipelineRel.CONVENTION, "PipelineUncollectRule")
            .withRuleFactory(PipelineUncollectRule::new)
            .as(Config.class)
            .toRule(PipelineUncollectRule.class);

    protected PipelineUncollectRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      Uncollect uncollect = (Uncollect) rel;
      RelTraitSet traitSet = uncollect.getTraitSet().replace(PipelineRel.CONVENTION);
      return new PipelineUncollect(rel.getCluster(), traitSet, convert(uncollect.getInput(), PipelineRel.CONVENTION),
          uncollect.withOrdinality, Collections.emptyList());
    }
  }

  static class PipelineUncollect extends Uncollect implements PipelineRel {
    PipelineUncollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, boolean withOrdinality,
        List<String> itemAliases) {
      super(cluster, traitSet, input, withOrdinality, itemAliases);
    }

    @Override
    public PipelineUncollect copy(RelTraitSet traitSet, RelNode input) {
      return new PipelineUncollect(getCluster(), traitSet, input, withOrdinality, Collections.emptyList());
    }

    @Override
    public void implement(Implementor implementor) {
    }
  }

  static Table findTable(CalciteSchema schema, List<String> qualifiedName) {
    if (qualifiedName.isEmpty()) {
      throw new IllegalArgumentException("Empty qualified name.");
    } else if (qualifiedName.size() == 1) {
      String name = qualifiedName.get(0);
      CalciteSchema.TableEntry table = schema.getTable(name, false);
      if (table == null) {
        throw new IllegalArgumentException("No table '" + name + "' in schema '" + schema.getName() + "'");
      }
      return table.getTable();
    } else {
      String head = qualifiedName.get(0);
      List<String> tail = qualifiedName.subList(1, qualifiedName.size());
      CalciteSchema subSchema = schema.getSubSchema(head, false);
      if (subSchema == null) {
        throw new IllegalArgumentException(
            "No schema '" + schema.getName() + "' found when looking for table '" + qualifiedName.get(
                qualifiedName.size() - 1) + "'");
      }
      return findTable(subSchema, tail);
    }
  }

  static Table findTable(CalciteSchema schema, String table) {
    return findTable(schema, Collections.singletonList(table));
  }

  static CalciteSchema schema(RelNode node) {
    return Optional.ofNullable(node.getTable())
        .map(x -> x.unwrap(CalciteSchema.class))
        .orElseThrow(() -> new IllegalArgumentException("null table?"));
  }

  static List<String> qualifiedName(RelNode node) {
    return Optional.ofNullable(node.getTable())
        .map(RelOptTable::getQualifiedName)
        .orElseThrow(() -> new IllegalArgumentException("null table?"));
  }

  static List<String> qualifiedName(RelOptTable table) {
    return table.getQualifiedName();
  }

  static String name(RelNode node) {
    List<String> names = qualifiedName(node);
    return names.get(names.size() - 1);
  }
}
