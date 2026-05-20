package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.SQLException;

import com.linkedin.hoptimator.graph.GraphProvider;
import com.linkedin.hoptimator.graph.GraphTarget;
import com.linkedin.hoptimator.graph.PipelineGraph;


/**
 * {@link GraphProvider} backed by Hoptimator's K8s state. Wraps {@link PipelineGraphBuilder} —
 * the existing entry-point methods become routes for each {@link GraphTarget} subtype:
 *
 * <ul>
 *   <li>{@link GraphTarget.View} → {@link PipelineGraphBuilder#forView(String)} (single-hop;
 *       depth is ignored — see method Javadoc)
 *   <li>{@link GraphTarget.LogicalTable} → {@link PipelineGraphBuilder#forLogicalTable(String)}
 *       (single-hop; depth is ignored — see method Javadoc)
 *   <li>{@link GraphTarget.Resource} → {@link PipelineGraphBuilder#forResource(String, java.util.List, int)}.
 *       SQL-identifier-to-CRD-name resolution happens in {@link com.linkedin.hoptimator.jdbc.GraphService}
 *       before dispatch — by the time the target reaches us, {@code Resource.database()} is the
 *       K8s Database CRD's {@code metadata.name}.
 * </ul>
 *
 * <p>Registered via {@code META-INF/services/com.linkedin.hoptimator.graph.GraphProvider} so callers
 * reach it through {@link com.linkedin.hoptimator.jdbc.GraphService}.
 */
public class K8sGraphProvider implements GraphProvider {

  @Override
  public PipelineGraph forTarget(GraphTarget target, int depth, Connection connection)
      throws SQLException {
    if (connection == null) {
      throw new SQLException("K8sGraphProvider.forTarget requires a non-null JDBC connection; "
          + "K8sContext can't be derived from null.");
    }
    K8sContext context = K8sContext.create(connection);
    PipelineGraphBuilder builder = createBuilder(context);
    if (target instanceof GraphTarget.View) {
      GraphTarget.View v = (GraphTarget.View) target;
      return builder.forView(v.name());
    }
    if (target instanceof GraphTarget.LogicalTable) {
      GraphTarget.LogicalTable lt = (GraphTarget.LogicalTable) target;
      return builder.forLogicalTable(lt.name());
    }
    if (target instanceof GraphTarget.Resource) {
      GraphTarget.Resource r = (GraphTarget.Resource) target;
      return builder.forResource(r.database(), r.path(), depth);
    }
    throw new SQLException("K8sGraphProvider does not support target: " + target);
  }

  @Override
  public boolean supports(GraphTarget target) {
    return target instanceof GraphTarget.View
        || target instanceof GraphTarget.LogicalTable
        || target instanceof GraphTarget.Resource;
  }

  /** Factory method so tests can substitute a mock builder. */
  PipelineGraphBuilder createBuilder(K8sContext context) {
    return new PipelineGraphBuilder(context);
  }
}
