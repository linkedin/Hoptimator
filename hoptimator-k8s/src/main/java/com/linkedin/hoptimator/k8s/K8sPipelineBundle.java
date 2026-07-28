package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.kubernetes.client.openapi.models.V1OwnerReference;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;


/**
 * Deploys a Pipeline custom resource together with its YAML elements (e.g. SqlJob), owned by an
 * externally provided owner (such as a LogicalTable custom resource rather than a View custom resource).
 *
 * <p>Analogous to the relationship between {@link K8sMaterializedViewDeployer} and
 * {@link K8sViewDeployer}: just as the materialized view deployer owns a view deployer
 * as a top-level field, this class owns its {@link K8sPipelineDeployer} at construction time.
 */
public class K8sPipelineBundle implements Deployer {

  private final String name;
  private final List<String> pipelineSpecs;
  private final K8sContext ownerContext;
  private final K8sPipelineDeployer pipelineDeployer;
  private final List<Deployer> deployers = new ArrayList<>();

  /**
   * {@code sources} and {@code sink} are stamped as {@code depends-on-*}
   * labels on the Pipeline custom resource so the delete-time guard in {@link DependencyChecker}
   * can find this pipeline by label selector.
   */
  public K8sPipelineBundle(String name, List<String> pipelineSpecs, String sql,
      Collection<Source> sources, Sink sink, K8sContext ownerContext) {
    this.name = name;
    this.pipelineSpecs = pipelineSpecs;
    this.ownerContext = ownerContext;
    this.pipelineDeployer = createPipelineDeployer(name, pipelineSpecs, sql, sources, sink, ownerContext);
  }

  K8sPipelineDeployer createPipelineDeployer(String n, List<String> specs, String sql,
      Collection<Source> sources, Sink sink, K8sContext ctx) {
    return new K8sPipelineDeployer(n, specs, sql, sources, sink, ctx);
  }

  K8sYamlDeployerImpl createYamlDeployerImpl(K8sContext pipelineContext, List<String> specs) {
    return new K8sYamlDeployerImpl(pipelineContext, specs);
  }

  @Override
  public void create() throws SQLException {
    deployers.add(pipelineDeployer);
    V1OwnerReference pipelineRef = pipelineDeployer.createAndReference();
    K8sContext pipelineContext = ownerContext.withLabel("pipeline", name).withOwner(pipelineRef);
    K8sYamlDeployerImpl yamlDeployer = createYamlDeployerImpl(pipelineContext, pipelineSpecs);
    deployers.add(yamlDeployer);
    // Use update because some elements may already exist
    yamlDeployer.update();
  }

  @Override
  public boolean exists() throws SQLException {
    // The bundle's identity is its Pipeline custom resource; the owned YAML elements cascade from it.
    return pipelineDeployer.exists();
  }

  @Override
  public void update() throws SQLException {
    deployers.add(pipelineDeployer);
    V1OwnerReference pipelineRef = pipelineDeployer.updateAndReference();
    K8sContext pipelineContext = ownerContext.withLabel("pipeline", name).withOwner(pipelineRef);
    K8sYamlDeployerImpl yamlDeployer = createYamlDeployerImpl(pipelineContext, pipelineSpecs);
    deployers.add(yamlDeployer);
    yamlDeployer.update();
  }

  @Override
  public void delete() throws SQLException {
    // Deleting the Pipeline custom resource causes K8s to cascade-delete its owned YAML elements.
    pipelineDeployer.delete();
  }

  @Override
  public void restore() {
    for (Deployer deployer : deployers) {
      deployer.restore();
    }
  }

  @Override
  public List<String> specify() throws SQLException {
    return pipelineSpecs;
  }
}
