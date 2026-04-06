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
 * <p>Unlike {@link K8sPipelineDeployer} which only creates the Pipeline CRD record,
 * this class creates both the Pipeline CRD and the child YAML elements as a bundle.
 * The Pipeline CRD's K8s ownerReference points to whatever external resource is passed
 * via {@code ownerContext}.
 */
public class K8sPipelineBundle implements Deployer {

  private final String name;
  private final List<String> pipelineSpecs;
  private final String sql;
  private final K8sContext ownerContext;
  private final List<Deployer> deployers = new ArrayList<>();

  public K8sPipelineBundle(String name, List<String> pipelineSpecs, String sql,
      K8sContext ownerContext) {
    this.name = name;
    this.pipelineSpecs = pipelineSpecs;
    this.sql = sql;
    this.ownerContext = ownerContext;
  }

  @Override
  public void create() throws SQLException {
    K8sPipelineDeployer pipelineDeployer = new K8sPipelineDeployer(name, pipelineSpecs, sql, ownerContext);
    deployers.add(pipelineDeployer);
    V1OwnerReference pipelineRef = pipelineDeployer.createAndReference();
    K8sContext pipelineContext = ownerContext.withLabel("pipeline", name).withOwner(pipelineRef);
    K8sYamlDeployerImpl yamlDeployer = new K8sYamlDeployerImpl(pipelineContext, pipelineSpecs);
    deployers.add(yamlDeployer);
    yamlDeployer.update();
  }

  @Override
  public void update() throws SQLException {
    K8sPipelineDeployer pipelineDeployer = new K8sPipelineDeployer(name, pipelineSpecs, sql, ownerContext);
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
    new K8sPipelineDeployer(name, pipelineSpecs, sql, ownerContext).delete();
  }

  @Override
  public void restore() {
    // Restore each deployer; no strict ordering required since the Pipeline CRD
    // cascade-deletes its owned elements on removal.
    for (Deployer deployer : deployers) {
      deployer.restore();
    }
  }

  @Override
  public List<String> specify() throws SQLException {
    return pipelineSpecs;
  }
}
