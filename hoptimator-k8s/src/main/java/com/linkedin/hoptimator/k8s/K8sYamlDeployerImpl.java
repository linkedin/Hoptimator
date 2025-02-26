package com.linkedin.hoptimator.k8s;

import java.util.List;


/** Deploys a set of objects specified in YAML */
public class K8sYamlDeployerImpl extends K8sYamlDeployer {

  private final List<String> specs;

  public K8sYamlDeployerImpl(K8sContext context, List<String> specs) {
    super(context); 
    this.specs = specs;
  }

  @Override
  public List<String> specify() {
    return specs;
  }
}
