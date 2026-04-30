package com.linkedin.hoptimator.logical;

import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.PendingDelete;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;
import com.linkedin.hoptimator.jdbc.DeployerUtils;
import com.linkedin.hoptimator.k8s.K8sContext;


/**
 * Returns a tier-aware dependency-guard validator for logical-table sources. A logical table's
 * physical resources live across multiple tiers (Kafka, Venice, etc.); a downstream pipeline
 * may reference any tier directly. The default {@code K8sPipelineDependencyValidator}
 * (registered in hoptimator-k8s) only scans for the logical {@code LOGICAL.table} reference,
 * which pipelines never use — so without this provider, the DROP would silently bypass the
 * guard for cross-tier dependents.
 *
 * <p>Detection mirrors {@link LogicalTableDeployerProvider}: the source's schema must be
 * backed by a {@code jdbc:logicaltable://} URL.
 */
public class LogicalValidatorProvider implements ValidatorProvider {

  private static final Logger log = LoggerFactory.getLogger(LogicalValidatorProvider.class);

  @Override
  public <T> Collection<Validator> validators(T obj, Connection connection) {
    if (connection == null || !(obj instanceof PendingDelete)) {
      return Collections.emptyList();
    }
    Object target = ((PendingDelete<?>) obj).target();
    if (!(target instanceof Source)) {
      return Collections.emptyList();
    }
    Source source = (Source) target;

    Properties tierProps = DeployerUtils.extractPropertiesFromJdbcSchema(
        source.catalog(), source.schema(), connection,
        LogicalTableDriver.CONNECT_STRING_PREFIX, log);
    if (tierProps == null) {
      return Collections.emptyList();
    }

    K8sContext context = K8sContext.create(connection);
    return Collections.singletonList(new LogicalTableDependencyValidator(source, tierProps, context));
  }
}
