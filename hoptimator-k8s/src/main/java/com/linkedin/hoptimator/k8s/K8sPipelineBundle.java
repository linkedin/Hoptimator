package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import io.kubernetes.client.openapi.models.V1OwnerReference;

import com.linkedin.hoptimator.Deployer;


/**
 * Deploys a Pipeline CRD together with its YAML elements (e.g. SqlJob), owned by an
 * externally provided owner (such as a LogicalTable CRD rather than a View CRD).
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

  public K8sPipelineBundle(String name, List<String> pipelineSpecs, String sql,
      K8sContext ownerContext) {
    this.name = name;
    this.pipelineSpecs = pipelineSpecs;
    this.ownerContext = ownerContext;
    this.pipelineDeployer = new K8sPipelineDeployer(name, pipelineSpecs, sql, ownerContext);
  }

  @Override
  public void create() throws SQLException {
    deployers.add(pipelineDeployer);
    V1OwnerReference pipelineRef = pipelineDeployer.createAndReference();
    K8sContext pipelineContext = ownerContext.withLabel("pipeline", name).withOwner(pipelineRef);
    K8sYamlDeployerImpl yamlDeployer = new K8sYamlDeployerImpl(pipelineContext, pipelineSpecs);
    deployers.add(yamlDeployer);
    // Use update because some elements may already exist
    yamlDeployer.update();
  }

  @Override
  public void update() throws SQLException {
    deployers.add(pipelineDeployer);
    V1OwnerReference pipelineRef = pipelineDeployer.updateAndReference();
    K8sContext pipelineContext = ownerContext.withLabel("pipeline", name).withOwner(pipelineRef);
    K8sYamlDeployerImpl yamlDeployer = new K8sYamlDeployerImpl(pipelineContext, pipelineSpecs);
    deployers.add(yamlDeployer);
    yamlDeployer.update();
  }

  @Override
  public void delete() throws SQLException {
    // Deleting the Pipeline CRD causes K8s to cascade-delete its owned YAML elements.
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
