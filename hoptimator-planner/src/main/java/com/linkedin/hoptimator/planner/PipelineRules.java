package com.linkedin.hoptimator.planner;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.schema.Table;

import com.linkedin.hoptimator.catalog.HopRel;
import com.linkedin.hoptimator.catalog.HopTable;
import com.linkedin.hoptimator.catalog.HopTableScan;
import com.linkedin.hoptimator.catalog.ProtoTable;
import com.linkedin.hoptimator.catalog.RuleProvider;


public class PipelineRules implements RuleProvider {

  @Override
  public Collection<RelOptRule> rules() {
    return Arrays.asList(PipelineTableScanRule.INSTANCE, PipelineFilterRule.INSTANCE, PipelineProjectRule.INSTANCE,
        PipelineJoinRule.INSTANCE, PipelineCalcRule.INSTANCE);
  }

  static class PipelineTableScanRule extends ConverterRule {
    static final PipelineTableScanRule INSTANCE =
        Config.INSTANCE.withConversion(HopTableScan.class, HopRel.CONVENTION, PipelineRel.CONVENTION,
                "PipelineTableScanRule")
            .withRuleFactory(PipelineTableScanRule::new)
            .as(Config.class)
            .toRule(PipelineTableScanRule.class);

    protected PipelineTableScanRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      HopTableScan scan = (HopTableScan) rel;
      RelTraitSet traitSet = scan.getTraitSet().replace(PipelineRel.CONVENTION);
      return new PipelineTableScan(rel.getCluster(), traitSet, scan.getTable());
    }
  }

  static class PipelineTableScan extends TableScan implements PipelineRel {

    PipelineTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
      super(cluster, traitSet, Collections.emptyList(), table);
      assert getConvention() == PipelineRel.CONVENTION;
    }

    @Override
    public void implement(Implementor implementor) {
      Table table = findTable(schema(this), name(this));
      if (table instanceof ProtoTable) {
        // resolve to an actual table, which may involve talking to external systems.
        try {
          table = ((ProtoTable) table).table();
        } catch (Exception e) {
          // TODO consider having implement() throw something
          throw new RuntimeException(e);
        }
      }
      implementor.implement((HopTable) table);
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
      RelNode input = Objects.requireNonNull(filter.getInput(), "input");
      RexNode condition = Objects.requireNonNull(filter.getCondition(), "condition");
      RelTraitSet traitSet = filter.getTraitSet().replace(PipelineRel.CONVENTION);
      return new PipelineFilter(rel.getCluster(), traitSet, convert(input, PipelineRel.CONVENTION), condition);
    }
  }

  static class PipelineFilter extends Filter implements PipelineRel {

    PipelineFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode conditions) {
      super(cluster, traitSet, input, conditions);
    }

    @Override
    public PipelineFilter copy(RelTraitSet traitSet, RelNode input, RexNode conditions) {
      return new PipelineFilter(getCluster(), traitSet, input, conditions);
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
      return new PipelineProject(rel.getCluster(), traitSet, convert(project.getInput(), PipelineRel.CONVENTION),
          project.getProjects(), project.getRowType());
    }
  }

  static class PipelineProject extends Project implements PipelineRel {

    PipelineProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects,
        RelDataType rowType) {
      super(cluster, traitSet, Collections.emptyList(), input, projects, rowType);
      assert getConvention() == PipelineRel.CONVENTION;
      assert input.getConvention() == PipelineRel.CONVENTION;
    }

    @Override
    public PipelineProject copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
      return new PipelineProject(getCluster(), traitSet, input, projects, rowType);
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
      return new PipelineJoin(rel.getCluster(), traitSet, convert(join.getLeft(), PipelineRel.CONVENTION),
          convert(join.getRight(), PipelineRel.CONVENTION), join.getCondition(), join.getVariablesSet(),
          join.getJoinType());
    }
  }

  static class PipelineJoin extends Join implements PipelineRel {

    PipelineJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
        Set<CorrelationId> variablesSet, JoinRelType joinType) {
      super(cluster, traitSet, Collections.emptyList(), left, right, condition, variablesSet, joinType);
    }

    @Override
    public PipelineJoin copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
        boolean semiJoinDone) {
      return new PipelineJoin(getCluster(), traitSet, left, right, condition, getVariablesSet(), joinType);
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
      return new PipelineCalc(rel.getCluster(), traitSet, calc.getInput(), calc.getProgram());
    }
  }

  static class PipelineCalc extends Calc implements PipelineRel {

    PipelineCalc(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexProgram program) {
      super(cluster, traitSet, Collections.emptyList(), child, program);
    }

    @Override
    public PipelineCalc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
      return new PipelineCalc(getCluster(), traitSet, child, program);
    }

    @Override
    public void implement(Implementor implementor) {
    }
  }

  static Table findTable(CalciteSchema schema, List<String> qualifiedName) {
    if (qualifiedName.size() == 0) {
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
    return (CalciteSchema) node.getTable().unwrap(CalciteSchema.class);
  }

  static List<String> qualifiedName(RelNode node) {
    return node.getTable().getQualifiedName();
  }

  static List<String> qualifiedName(RelOptTable table) {
    return table.getQualifiedName();
  }

  static String name(RelNode node) {
    List<String> names = qualifiedName(node);
    return names.get(names.size() - 1);
  }
}
