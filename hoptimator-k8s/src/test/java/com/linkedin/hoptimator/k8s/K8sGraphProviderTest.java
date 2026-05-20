package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linkedin.hoptimator.graph.GraphTarget;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


/**
 * Dispatch tests for {@link K8sGraphProvider}. By the time the provider runs, the resolver in
 * {@link com.linkedin.hoptimator.jdbc.GraphService} has already turned the user identifier into
 * a typed {@link GraphTarget}; the provider's only job is to route each subtype to the right
 * {@link PipelineGraphBuilder} entry point and to advertise what it supports.
 */
@ExtendWith(MockitoExtension.class)
class K8sGraphProviderTest {

  @Mock
  private MockedStatic<K8sContext> contextStatic;
  @Mock
  private Connection connection;
  @Mock
  private K8sContext context;

  /** Subclass that hands out a single mock builder so we can verify which entry point fires. */
  private static K8sGraphProvider providerWith(PipelineGraphBuilder builder) {
    return new K8sGraphProvider() {
      @Override
      PipelineGraphBuilder createBuilder(K8sContext context) {
        return builder;
      }
    };
  }

  @Test
  void supportsAcceptsAllThreeGraphTargetSubtypes() {
    K8sGraphProvider provider = new K8sGraphProvider();
    assertTrue(provider.supports(new GraphTarget.View("audience")));
    assertTrue(provider.supports(new GraphTarget.LogicalTable("members")));
    assertTrue(provider.supports(new GraphTarget.Resource("kafka", List.of("topic"))));
  }

  @Test
  void forTargetRoutesViewToBuilderForView() throws SQLException {
    PipelineGraphBuilder builder = mock(PipelineGraphBuilder.class);
    contextStatic.when(() -> K8sContext.create(connection)).thenReturn(context);

    providerWith(builder).forTarget(new GraphTarget.View("audience"), 3, connection);

    verify(builder).forView(eq("audience"));
  }

  @Test
  void forTargetRoutesLogicalTableToBuilderForLogicalTable() throws SQLException {
    PipelineGraphBuilder builder = mock(PipelineGraphBuilder.class);
    contextStatic.when(() -> K8sContext.create(connection)).thenReturn(context);

    providerWith(builder).forTarget(new GraphTarget.LogicalTable("members"), 2, connection);

    verify(builder).forLogicalTable(eq("members"));
  }

  @Test
  void forTargetRoutesResourceToBuilderForResource() throws SQLException {
    PipelineGraphBuilder builder = mock(PipelineGraphBuilder.class);
    contextStatic.when(() -> K8sContext.create(connection)).thenReturn(context);

    providerWith(builder).forTarget(
        new GraphTarget.Resource("ads-database", List.of("ADS", "AD_CLICKS")), 1, connection);

    verify(builder).forResource(eq("ads-database"), eq(List.of("ADS", "AD_CLICKS")), eq(1));
  }
}
