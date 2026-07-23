package com.linkedin.hoptimator.logical;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.K8sContext;


/**
 * Activates {@link LogicalTableDeployer} for sources backed by a logical Database CRD.
 *
 * <p>Detection uses {@link DeploymentContext#databaseProperties} to look up the
 * schema by name and check if its underlying JDBC URL starts with
 * {@link LogicalTableDriver#CONNECT_STRING_PREFIX}. No K8s API calls needed for activation.
 * The returned Properties contain the tier params (e.g. nearline=kafka-database, online=venice).
 */
public class LogicalTableDeployerProvider implements DeployerProvider {

  private static final Logger log = LoggerFactory.getLogger(LogicalTableDeployerProvider.class);

  @Override
  public <T extends Deployable> Collection<Deployer> deployers(T obj, DeploymentContext context) {
    if (!(obj instanceof Source)) {
      return Collections.emptyList();
    }
    Source source = (Source) obj;

    Properties tierProps = context.databaseProperties(
        source.catalog(), source.schema(), LogicalTableDriver.CONNECT_STRING_PREFIX);

    if (tierProps == null) {
      return Collections.emptyList();
    }

    log.debug("LogicalTableDeployerProvider activating for source {}", source);
    K8sContext k8sContext = K8sContext.create(context);
    return List.of(new LogicalTableDeployer(source, tierProps, k8sContext));
  }

  @Override
  public int priority() {
    return 2;
  }
}
