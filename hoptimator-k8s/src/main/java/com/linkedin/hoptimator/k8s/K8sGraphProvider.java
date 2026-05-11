package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.linkedin.hoptimator.GraphProvider;
import com.linkedin.hoptimator.GraphTarget;
import com.linkedin.hoptimator.PipelineGraph;


/**
 * {@link GraphProvider} backed by Hoptimator's K8s state. Wraps {@link PipelineGraphBuilder} —
 * the existing entry-point methods become routes for each {@link GraphTarget} subtype:
 *
 * <ul>
 *   <li>{@link GraphTarget.View} → {@link PipelineGraphBuilder#forView(String, String, int)}
 *   <li>{@link GraphTarget.LogicalTable} → {@link PipelineGraphBuilder#forLogicalTable(String, String, int)}
 *   <li>{@link GraphTarget.Resource} → {@link PipelineGraphBuilder#forResource(String, List, int)}
 * </ul>
 *
 * <p>Registered via {@code META-INF/services/com.linkedin.hoptimator.GraphProvider} so callers
 * reach it through {@link com.linkedin.hoptimator.graph.GraphService}.
 */
public final class K8sGraphProvider implements GraphProvider {

  @Override
  public PipelineGraph forTarget(GraphTarget target, int depth, Connection connection)
      throws SQLException {
    K8sContext context = K8sContext.create(connection);
    PipelineGraphBuilder builder = new PipelineGraphBuilder(context);
    if (target instanceof GraphTarget.View) {
      GraphTarget.View v = (GraphTarget.View) target;
      return builder.forView(defaultNamespace(v.namespace(), context), v.name(), depth);
    }
    if (target instanceof GraphTarget.LogicalTable) {
      GraphTarget.LogicalTable lt = (GraphTarget.LogicalTable) target;
      return builder.forLogicalTable(defaultNamespace(lt.namespace(), context), lt.name(), depth);
    }
    if (target instanceof GraphTarget.Resource) {
      GraphTarget.Resource r = (GraphTarget.Resource) target;
      return builder.forResource(r.database(), r.path(), depth);
    }
    throw new SQLException("K8sGraphProvider does not support target: " + target);
  }

  /** When the caller didn't supply a namespace, fall back to the K8s context's default. */
  private static String defaultNamespace(String requested, K8sContext context) {
    return requested != null ? requested : context.namespace();
  }

  @Override
  public boolean supports(GraphTarget target) {
    return target instanceof GraphTarget.View
        || target instanceof GraphTarget.LogicalTable
        || target instanceof GraphTarget.Resource;
  }
}
