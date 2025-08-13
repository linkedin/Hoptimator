package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import io.kubernetes.client.openapi.models.V1OwnerReference;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.MaterializedView;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.util.DeploymentService;


/** Deploys View and Pipeline objects, along with all the pipeline elements. */
class K8sMaterializedViewDeployer implements Deployer {

  private final MaterializedView view;
  private final K8sContext context;
  private final K8sViewDeployer viewDeployer;
  private final List<Deployer> deployers;

  private final Object crudLock = new Object();

  K8sMaterializedViewDeployer(MaterializedView view, K8sContext context) {
    this.view = view;
    this.context = context;
    this.viewDeployer = new K8sViewDeployer(view, true, context);
    this.deployers = new ArrayList<>();
  }

  @Override
  public void create() throws SQLException {
    synchronized (crudLock) {
      String name = name();
      List<String> pipelineSpecs = pipelineSpecs();
      V1OwnerReference viewRef = viewDeployer.createAndReference();
      K8sContext viewContext = context.withOwner(viewRef);
      K8sPipelineDeployer pipelineDeployer = new K8sPipelineDeployer(name, pipelineSpecs, sql(), viewContext);
      deployers.add(pipelineDeployer);
      V1OwnerReference pipelineRef = pipelineDeployer.createAndReference();
      K8sContext pipelineContext = viewContext.withLabel("pipeline", name).withOwner(pipelineRef);
      K8sYamlDeployerImpl yamlDeployer = new K8sYamlDeployerImpl(pipelineContext, pipelineSpecs);
      deployers.add(yamlDeployer);
      yamlDeployer.update();  // update, cuz some elements may already exist
    }
  }

  @Override
  public void update() throws SQLException {
    synchronized (crudLock) {
      String name = name();
      List<String> pipelineSpecs = pipelineSpecs();
      V1OwnerReference viewRef = viewDeployer.updateAndReference();
      K8sContext viewContext = context.withOwner(viewRef);
      K8sPipelineDeployer pipelineDeployer = new K8sPipelineDeployer(name, pipelineSpecs, sql(), viewContext);
      deployers.add(pipelineDeployer);
      V1OwnerReference pipelineRef = pipelineDeployer.updateAndReference();
      K8sContext pipelineContext = viewContext.withLabel("pipeline", name).withOwner(pipelineRef);
      K8sYamlDeployerImpl yamlDeployer = new K8sYamlDeployerImpl(pipelineContext, pipelineSpecs);
      deployers.add(yamlDeployer);
      yamlDeployer.update();
    }
  }

  @Override
  public void delete() throws SQLException {
    synchronized (crudLock) {
      // Delete will cascade to any owned objects, including the Pipeline object.
      viewDeployer.delete();
    }
  }

  @Override
  public void restore() {
    synchronized (crudLock) {
      // There may be a scenario where a view creates multiple pipelines.
      // Restoring the pipelines in reverse order ensures the original state is preserved.
      for (int i = deployers.size() - 1; i >= 0; i--) {
        deployers.get(i).restore();
      }
      viewDeployer.restore();
    }
  }

  List<String> pipelineSpecs() throws SQLException {
    List<String> specs = new ArrayList<>();
    for (Source source : view.pipeline().sources()) {
      specs.addAll(DeploymentService.specify(source, context.connection()));
    }
    specs.addAll(DeploymentService.specify(view.pipeline().sink(), context.connection()));
    specs.addAll(DeploymentService.specify(view.pipeline().job(), context.connection()));
    return specs;
  }

  String name() {
    return K8sUtils.canonicalizeName(view.path());
  }

  String sql() {
    return view.pipelineSql().apply(SqlDialect.ANSI);
  }

  @Override
  public List<String> specify() throws SQLException {
    // Users don't care about the MaterializedView and Pipeline objects.
    // We just return the pipeline elements here. This will be what
    // renders when the `!specify` command is called.
    return pipelineSpecs();
  }
}
