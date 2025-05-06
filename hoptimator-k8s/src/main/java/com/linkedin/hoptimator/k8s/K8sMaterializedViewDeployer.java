package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
  private final Properties connectionProperties;

  K8sMaterializedViewDeployer(MaterializedView view, K8sContext context, Properties connectionProperties) {
    this.view = view;
    this.context = context;
    this.connectionProperties = connectionProperties;
  }

  @Override
  public void create() throws SQLException {
    String name = name();
    List<String> pipelineSpecs = pipelineSpecs();
    V1OwnerReference viewRef = new K8sViewDeployer(view, true, context).createAndReference();
    K8sContext viewContext = context.withOwner(viewRef);
    V1OwnerReference pipelineRef = new K8sPipelineDeployer(name, pipelineSpecs, sql(), viewContext).createAndReference();
    K8sContext pipelineContext = viewContext.withLabel("pipeline", name).withOwner(pipelineRef);
    new K8sYamlDeployerImpl(pipelineContext, pipelineSpecs).update();  // update, cuz some elements may already exist
  }

  @Override
  public void update() throws SQLException {
    String name = name();
    List<String> pipelineSpecs = pipelineSpecs();
    V1OwnerReference viewRef = new K8sViewDeployer(view, true, context).updateAndReference();
    K8sContext viewContext = context.withOwner(viewRef);
    V1OwnerReference pipelineRef = new K8sPipelineDeployer(name, pipelineSpecs, sql(), viewContext).updateAndReference();
    K8sContext pipelineContext = viewContext.withLabel("pipeline", name).withOwner(pipelineRef);
    new K8sYamlDeployerImpl(pipelineContext, pipelineSpecs).update();
  }

  @Override
  public void delete() throws SQLException {
    // Delete will cascade to any owned objects, including the Pipeline object.
    new K8sViewDeployer(view, true, context).delete();
  }

  List<String> pipelineSpecs() throws SQLException {
    List<String> specs = new ArrayList<>();
    for (Source source : view.pipeline().sources()) {
      specs.addAll(DeploymentService.specify(source, connectionProperties));
    }
    specs.addAll(DeploymentService.specify(view.pipeline().sink(), connectionProperties));
    specs.addAll(DeploymentService.specify(view.pipeline().job(), connectionProperties));
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
