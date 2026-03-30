package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Connector;
import com.linkedin.hoptimator.Source;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;


@ExtendWith(MockitoExtension.class)
class K8sConnectorProviderTest {

  @Mock
  private Connection connection;

  @Mock
  private K8sContext context;

  @Mock
  private MockedStatic<K8sContext> contextStatic;

  @Test
  void connectorsForSourceReturnsK8sConnector() {
    contextStatic.when(() -> K8sContext.create(any(Connection.class))).thenReturn(context);
    K8sConnectorProvider provider = new K8sConnectorProvider();
    Source source = mock(Source.class);

    Collection<Connector> connectors = provider.connectors(source, connection);

    assertEquals(1, connectors.size());
    assertTrue(connectors.iterator().next() instanceof K8sConnector);
  }

  @Test
  void connectorsForNonSourceReturnsEmpty() {
    contextStatic.when(() -> K8sContext.create(any(Connection.class))).thenReturn(context);
    K8sConnectorProvider provider = new K8sConnectorProvider();

    Collection<Connector> connectors = provider.connectors("not a source", connection);

    assertTrue(connectors.isEmpty());
  }
}
