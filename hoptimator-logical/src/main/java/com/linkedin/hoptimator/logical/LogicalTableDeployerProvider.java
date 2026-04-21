package com.linkedin.hoptimator.logical;

import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.DeployerUtils;
import com.linkedin.hoptimator.k8s.K8sContext;


/**
 * Activates {@link LogicalTableDeployer} for sources backed by a logical Database CRD.
 *
 * <p>Detection uses {@link DeployerUtils#extractPropertiesFromJdbcSchema} to look up the
 * schema by name in the connection and check if its underlying JDBC URL starts with
 * {@link LogicalTableDriver#CONNECT_STRING_PREFIX}. No K8s API calls needed for activation.
 * The returned Properties contain the tier params (e.g. nearline=kafka-database, online=venice).
 */
public class LogicalTableDeployerProvider implements DeployerProvider {

  private static final Logger log = LoggerFactory.getLogger(LogicalTableDeployerProvider.class);

  @Override
  public <T extends Deployable> Collection<Deployer> deployers(T obj, Connection connection) {
    if (!(obj instanceof Source)) {
      return Collections.emptyList();
    }
    Source source = (Source) obj;

    Properties tierProps = DeployerUtils.extractPropertiesFromJdbcSchema(
        source.catalog(), source.schema(), connection,
        LogicalTableDriver.CONNECT_STRING_PREFIX, log);

    if (tierProps == null) {
      return Collections.emptyList();
    }

    log.debug("LogicalTableDeployerProvider activating for source {}", source);
    K8sContext context = K8sContext.create(connection);
    return List.of(new LogicalTableDeployer(source, tierProps, context));
  }

  @Override
  public int priority() {
    return 2;
  }
}
