package com.linkedin.hoptimator.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import com.linkedin.hoptimator.util.planner.PipelineRules;


public final class DeploymentService {

  private DeploymentService() {
  }

  public static <T extends Deployable> void create(T obj, Properties connectionProperties)
      throws SQLException {
    for (Deployer deployer : deployers(obj, connectionProperties)) {
      deployer.create();
    }
  }

  public static <T extends Deployable> void delete(T obj, Properties connectionProperties)
      throws SQLException {
    for (Deployer deployer : deployers(obj, connectionProperties)) {
      deployer.delete();
    }
  }

  public static <T extends Deployable> void update(T obj, Properties connectionProperties)
      throws SQLException {
    for (Deployer deployer : deployers(obj, connectionProperties)) {
      deployer.update();
    }
  }

  public static <T extends Deployable> List<String> specify(T obj, Properties connectionProperties)
      throws SQLException {
    List<String> specs = new ArrayList<>();
    for (Deployer deployer : deployers(obj, connectionProperties)) {
      specs.addAll(deployer.specify());
    }
    return specs;
  }

  public static Collection<DeployerProvider> providers() {
    ServiceLoader<DeployerProvider> loader = ServiceLoader.load(DeployerProvider.class);
    List<DeployerProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(x -> providers.add(x));
    return providers;
  }

  public static <T extends Deployable> Collection<Deployer> deployers(T obj, Properties connectionProperties) {
    return providers().stream()
        .flatMap(x -> x.deployers(obj, connectionProperties).stream())
        .collect(Collectors.toList());
  }

  /** Plans a deployable Pipeline which implements the query. */
  public static PipelineRel.Implementor plan(RelRoot root, List<RelOptMaterialization> materializations)
      throws SQLException {
    RelTraitSet traitSet = root.rel.getTraitSet().simplify().replace(PipelineRel.CONVENTION);
    Program program = Programs.standard();
    RelOptPlanner planner = root.rel.getCluster().getPlanner();
    PipelineRules.rules().forEach(x -> planner.addRule(x));
    PipelineRel plan = (PipelineRel) program.run(planner, root.rel, traitSet, materializations,
        Collections.emptyList());
    PipelineRel.Implementor implementor = new PipelineRel.Implementor(root.fields);
    implementor.visit(plan);
    return implementor;
  }
}
