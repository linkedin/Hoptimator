package com.linkedin.hoptimator.util;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import com.linkedin.hoptimator.util.planner.PipelineRules;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.sql.SQLException;

public final class DeploymentService {

  private DeploymentService() {
  }

  public static <T> void create(T object, Class<T> clazz) throws SQLException {
    for (Deployer<T> deployer : deployers(clazz)) {
      deployer.create(object);
    }
  }

  public static <T> void delete(T object, Class<T> clazz) throws SQLException {
    for (Deployer<T> deployer : deployers(clazz)) {
      deployer.delete(object);
    }
  }

  public static <T> void update(T object, Class<T> clazz) throws SQLException {
    for (Deployer<T> deployer : deployers(clazz)) {
      deployer.update(object);
    }
  }

  public static <T> List<String> specify(T object, Class<T> clazz) throws SQLException {
    List<String> specs = new ArrayList<>();
    for (Deployer<T> deployer : deployers(clazz)) {
      specs.addAll(deployer.specify(object));
    }
    return specs;
  }

  public static Collection<DeployerProvider> providers() {
    ServiceLoader<DeployerProvider> loader = ServiceLoader.load(DeployerProvider.class);
    List<DeployerProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(x -> providers.add(x));
    return providers;
  }

  public static <T> Collection<Deployer<T>> deployers(Class<T> clazz) {
    return providers().stream().flatMap(x -> x.deployers(clazz).stream())
        .collect(Collectors.toList());
  }

  public static <T> List<Deployable> deployables(T object, Class<T> clazz) {
    return deployers(clazz).stream().map(x -> new Deployable() {

      @Override
      public void create() throws SQLException {
        x.create(object);
      }

      @Override
      public void update() throws SQLException {
        x.update(object);
      }

      @Override
      public void delete() throws SQLException {
        x.delete(object);
      }

      @Override
      public List<String> specify() throws SQLException {
        return x.specify(object);
      }
    }).collect(Collectors.toList());
  }

  /** Plans a deployable Pipeline which implements the query. */
  public static PipelineRel.Implementor plan(RelNode rel) throws SQLException {
    RelTraitSet traitSet = rel.getTraitSet().simplify().replace(PipelineRel.CONVENTION);
    Program program = Programs.standard();
    // TODO add materializations here (currently empty list)
    RelOptPlanner planner = rel.getCluster().getPlanner();
    PipelineRules.rules().forEach(x -> planner.addRule(x));
    PipelineRel plan = (PipelineRel) program.run(rel.getCluster().getPlanner(), rel,
        traitSet, Collections.emptyList(), Collections.emptyList());
    PipelineRel.Implementor implementor = new PipelineRel.Implementor(plan);
    return implementor;
  }
}
