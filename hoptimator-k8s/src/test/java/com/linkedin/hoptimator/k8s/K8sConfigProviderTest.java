package com.linkedin.hoptimator.k8s;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class K8sConfigProviderTest {

  @Mock
  private Connection connection;

  @Mock
  private MockedStatic<K8sContext> k8sContextStatic;

  @Mock
  private K8sContext mockContext;

  @Test
  void hoptimatorConfigMapConstant() {
    assertEquals("hoptimator-configmap", K8sConfigProvider.HOPTIMATOR_CONFIG_MAP);
  }

  @Test
  void canBeInstantiated() {
    K8sConfigProvider provider = new K8sConfigProvider();
    assertNotNull(provider);
  }

  @Test
  void loadConfigReturnsProperties() throws SQLException {
    Map<String, String> data = new HashMap<>();
    data.put("key1", "value1");
    data.put("key2", "value2");

    V1ConfigMap configMap = new V1ConfigMap()
        .metadata(new V1ObjectMeta().name("hoptimator-configmap"))
        .data(data);

    @SuppressWarnings("unchecked")
    K8sApi<V1ConfigMap, V1ConfigMapList> mockApi = mock(K8sApi.class);
    when(mockContext.namespace()).thenReturn("test-ns");
    when(mockApi.get("test-ns", "hoptimator-configmap")).thenReturn(configMap);
    k8sContextStatic.when(() -> K8sContext.create(any(Connection.class))).thenReturn(mockContext);

    K8sConfigProvider provider = new K8sConfigProvider() {
      @Override
      K8sApi<V1ConfigMap, V1ConfigMapList> createConfigMapApi(K8sContext context) {
        return mockApi;
      }
    };

    Properties result = provider.loadConfig(connection);

    assertNotNull(result);
    assertEquals("value1", result.getProperty("key1"));
    assertEquals("value2", result.getProperty("key2"));
  }

  @Test
  void loadConfigWithEmptyNamespaceUsesNameOnly() throws SQLException {
    Map<String, String> data = new HashMap<>();
    data.put("prop", "val");

    V1ConfigMap configMap = new V1ConfigMap()
        .metadata(new V1ObjectMeta().name("hoptimator-configmap"))
        .data(data);

    @SuppressWarnings("unchecked")
    K8sApi<V1ConfigMap, V1ConfigMapList> mockApi = mock(K8sApi.class);
    when(mockContext.namespace()).thenReturn("");
    when(mockApi.get("hoptimator-configmap")).thenReturn(configMap);
    k8sContextStatic.when(() -> K8sContext.create(any(Connection.class))).thenReturn(mockContext);

    K8sConfigProvider provider = new K8sConfigProvider() {
      @Override
      K8sApi<V1ConfigMap, V1ConfigMapList> createConfigMapApi(K8sContext context) {
        return mockApi;
      }
    };

    Properties result = provider.loadConfig(connection);

    assertEquals("val", result.getProperty("prop"));
  }

  @Test
  void loadConfigThrowsWhenConfigMapNotFound() throws SQLException {
    @SuppressWarnings("unchecked")
    K8sApi<V1ConfigMap, V1ConfigMapList> mockApi = mock(K8sApi.class);
    when(mockContext.namespace()).thenReturn("test-ns");
    when(mockApi.get("test-ns", "hoptimator-configmap")).thenThrow(new SQLException("Not found"));
    k8sContextStatic.when(() -> K8sContext.create(any(Connection.class))).thenReturn(mockContext);

    K8sConfigProvider provider = new K8sConfigProvider() {
      @Override
      K8sApi<V1ConfigMap, V1ConfigMapList> createConfigMapApi(K8sContext context) {
        return mockApi;
      }
    };

    assertThrows(SQLException.class, () -> provider.loadConfig(connection));
  }

  @Test
  void loadConfigReturnsEmptyPropertiesWhenDataEmpty() throws SQLException {
    V1ConfigMap configMap = new V1ConfigMap()
        .metadata(new V1ObjectMeta().name("hoptimator-configmap"))
        .data(new HashMap<>());

    @SuppressWarnings("unchecked")
    K8sApi<V1ConfigMap, V1ConfigMapList> mockApi = mock(K8sApi.class);
    when(mockContext.namespace()).thenReturn("test-ns");
    when(mockApi.get("test-ns", "hoptimator-configmap")).thenReturn(configMap);
    k8sContextStatic.when(() -> K8sContext.create(any(Connection.class))).thenReturn(mockContext);

    K8sConfigProvider provider = new K8sConfigProvider() {
      @Override
      K8sApi<V1ConfigMap, V1ConfigMapList> createConfigMapApi(K8sContext context) {
        return mockApi;
      }
    };

    Properties result = provider.loadConfig(connection);

    assertTrue(result.isEmpty());
  }
}
