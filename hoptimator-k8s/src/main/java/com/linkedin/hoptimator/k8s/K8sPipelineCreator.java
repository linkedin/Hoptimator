package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import io.kubernetes.client.openapi.models.V1OwnerReference;

import com.linkedin.hoptimator.Deployer;


/**
 * Creates or updates a Pipeline CRD with its child YAML elements, owned by an
 * externally provided owner reference (e.g. a LogicalTable or View CRD).
 *
 * <p>This is the public API for pipeline creation that can be called from outside
 * the {@code hoptimator-k8s} package (e.g. from {@code hoptimator-logical}).
 */
public class K8sPipelineCreator implements Deployer {

  private final String name;
  private final List<String> pipelineSpecs;
  private final String sql;
  private final K8sContext ownerContext;
  private final List<Deployer> deployers = new ArrayList<>();

  public K8sPipelineCreator(String name, List<String> pipelineSpecs, String sql,
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
  public void restore() {
    for (int i = deployers.size() - 1; i >= 0; i--) {
      deployers.get(i).restore();
    }
  }

  @Override
  public void delete() throws SQLException {
    restore();
  }

  @Override
  public List<String> specify() throws SQLException {
    return pipelineSpecs;
  }
}
