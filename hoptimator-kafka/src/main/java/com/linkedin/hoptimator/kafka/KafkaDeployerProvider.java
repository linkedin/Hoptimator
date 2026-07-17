package com.linkedin.hoptimator.kafka;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcSchema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;


/**
 * Provides {@link KafkaDeployer} instances for Kafka-backed tables.
 *
 * <p>Detection works by looking up the source's schema in the deployment context,
 * checking if it is a {@link HoptimatorJdbcSchema} backed by a {@code jdbc:kafka://} URL.
 * The Kafka config (bootstrap.servers) is read from the JDBC URL properties stored on the schema.
 */
public class KafkaDeployerProvider implements DeployerProvider {

  @Override
  public <T extends Deployable> Collection<Deployer> deployers(T obj, DeploymentContext context) {
    List<Deployer> deployers = new ArrayList<>();
    if (obj instanceof Source) {
      Source source = (Source) obj;

      Properties properties = context.databaseProperties(
          source.catalog(), source.schema(), KafkaDriver.CONNECTION_PREFIX);

      if (properties == null) {
        return deployers;
      }

      deployers.add(new KafkaDeployer(source, properties));
    }
    return deployers;
  }

  @Override
  public int priority() {
    return 2;
  }
}
