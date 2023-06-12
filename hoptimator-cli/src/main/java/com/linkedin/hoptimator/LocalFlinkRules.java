package com.linkedin.hoptimator;

import com.linkedin.hoptimator.catalog.RuleProvider;
import com.linkedin.hoptimator.planner.PipelineRel;
import com.linkedin.hoptimator.planner.HoptimatorHook;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.util.BuiltInMethod;

import java.util.Collections;
import java.util.Collection;
import java.util.List;

public class LocalFlinkRules implements RuleProvider {

  /** Rule for running (parts of) pipelines locally */
  public static class PipelineToLocalFlinkConverterRule extends ConverterRule {
    public static final ConverterRule INSTANCE = Config.INSTANCE
        .withConversion(PipelineRel.class, PipelineRel.CONVENTION,
            EnumerableConvention.INSTANCE, "PipelineToLocalFlinkConverterRule")
        .withRuleFactory(PipelineToLocalFlinkConverterRule::new)
        .toRule(PipelineToLocalFlinkConverterRule.class);

    protected PipelineToLocalFlinkConverterRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
      return new PipelineToLocalFlinkConverter(rel.getCluster(), newTraitSet, rel);
    }
  } 

  /** Runs a Pipeline's FlinkJob locally */
  public static class PipelineToLocalFlinkConverter extends ConverterImpl implements EnumerableRel {

    protected PipelineToLocalFlinkConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
      super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new PipelineToLocalFlinkConverter(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      BlockBuilder builder = new BlockBuilder();
      PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.preferCustom());
      PipelineRel.Implementor impl = new PipelineRel.Implementor(getInput());
      String sql = impl.query().sql(MysqlSqlDialect.DEFAULT);
      Hook.QUERY_PLAN.run(sql);         // for script validation in tests
      HoptimatorHook.QUERY_PLAN.run(sql);  // ditto
      Expression iter = builder.append("iter", Expressions.new_(FlinkIterable.class, Expressions.constant(sql),
        Expressions.constant(20_000))); // timeout after 20s
      Expression enumerable = builder.append("enumerable", Expressions.call(BuiltInMethod.AS_ENUMERABLE.method, iter));
      builder.add(Expressions.return_(null, enumerable));
      return implementor.result(physType, builder.toBlock());
    }
  }

  @Override
  public Collection<RelOptRule> rules() {
    return Collections.singletonList(PipelineToLocalFlinkConverterRule.INSTANCE);
  }
}
