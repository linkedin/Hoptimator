package com.linkedin.hoptimator.kafka;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.DeployerUtils;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;


/**
 * Provides {@link KafkaDeployer} instances for Kafka-backed tables.
 *
 * <p>Detection works by looking up the source's schema in the Calcite connection,
 * checking if it is a {@link HoptimatorJdbcSchema} backed by a {@code jdbc:kafka://} URL.
 * The Kafka config (bootstrap.servers) is read from the JDBC URL properties stored on the schema.
 */
public class KafkaDeployerProvider implements DeployerProvider {

  private static final Logger log = LoggerFactory.getLogger(KafkaDeployerProvider.class);
  private static final String KAFKA_DB_PREFIX = "kafka";

  @Override
  public <T extends Deployable> Collection<Deployer> deployers(T obj, Connection connection) {
    List<Deployer> deployers = new ArrayList<>();
    if (obj instanceof Source) {
      Source source = (Source) obj;

      String database = source.database();
      if (database == null || !database.startsWith(KAFKA_DB_PREFIX)) {
        return deployers;
      }

      Properties properties = DeployerUtils.extractPropertiesFromJdbcSchema(
          source.catalog(),
          source.schema(),
          connection,
          KafkaDriver.CONNECTION_PREFIX,
          log);
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
