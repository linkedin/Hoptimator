package com.linkedin.hoptimator.util;

import com.linkedin.hoptimator.Connector;
import com.linkedin.hoptimator.ConnectorProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class ConnectionServiceTest {

  @Mock
  private Connection mockConnection;

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  private MockedStatic<ConnectionService> mockedConnectionService;

  @Test
  void testProvidersReturnsCollection() {
    Collection<ConnectorProvider> providers = ConnectionService.providers();

    assertNotNull(providers);
  }

  @Test
  void testConnectorsReturnsCollectionForObject() {
    Collection<?> connectors = ConnectionService.connectors("test", mockConnection);

    assertNotNull(connectors);
  }

  @Test
  void testConfigureReturnsMapFromConnectors() throws SQLException {
    // With no ServiceLoader providers registered, should return empty map
    Map<String, String> configs = ConnectionService.configure("test", mockConnection);

    assertNotNull(configs);
    assertTrue(configs.isEmpty());
  }

  @Mock
  private Connector mockConnector;

  @Test
  void testConfigureCollectsConfigsFromConnectors() throws SQLException {
    Map<String, String> connectorConfigs = new LinkedHashMap<>();
    connectorConfigs.put("bootstrap.servers", "localhost:9092");
    connectorConfigs.put("topic", "test-topic");
    when(mockConnector.configure()).thenReturn(connectorConfigs);

    ConnectorProvider provider = new ConnectorProvider() {
      @Override
      public <T> Collection<Connector> connectors(T obj, Connection conn) {
        return Collections.singletonList(mockConnector);
      }
    };

    mockedConnectionService.when(ConnectionService::providers).thenReturn(Collections.singletonList(provider));

    Map<String, String> result = ConnectionService.configure("test", mockConnection);

    assertEquals(2, result.size());
    assertEquals("localhost:9092", result.get("bootstrap.servers"));
    assertEquals("test-topic", result.get("topic"));
  }
}
