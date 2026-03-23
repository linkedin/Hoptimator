package com.linkedin.hoptimator.k8s;

import com.google.gson.JsonObject;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineSpec;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatus;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatusEstimator;
import io.kubernetes.client.common.KubernetesType;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for K8sPipelineElementApi
 */
@ExtendWith(MockitoExtension.class)
class K8sPipelineElementApiTest {

    @Mock
    private K8sContext mockContext;

    @Mock
    private KubernetesApiResponse<DynamicKubernetesObject> mockApiResponse;

    @Mock
    private DynamicKubernetesObject mockDynamicObject;

    @Mock
    private MockedStatic<K8sUtils> k8sUtilsMockedStatic;

    private K8sPipelineElementApi api;

    @BeforeEach
    void setUp() {
        api = new K8sPipelineElementApi(mockContext);
    }

    @Test
    void testGetPipelineElementsSingleElement() {
        // Given
        V1alpha1Pipeline pipeline = createPipeline("test-pipeline", "default",
            "apiVersion: apps/v1\n"
                + "kind: Deployment\n"
                + "metadata:\n"
                + "  name: test-deployment");

        // When
        List<String> elements = api.getPipelineElements(pipeline);

        // Then
        assertEquals(1, elements.size());
        assertEquals("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: test-deployment", elements.get(0));
    }

    @Test
    void testGetPipelineElementsMultipleElements() {
        // Given
        String yaml = "apiVersion: apps/v1\n"
            + "kind: Deployment\n"
            + "metadata:\n"
            + "  name: test-deployment\n"
            + "---\n"
            + "apiVersion: v1\n"
            + "kind: Service\n"
            + "metadata:\n"
            + "  name: test-service\n"
            + "---\n"
            + "apiVersion: v1\n"
            + "kind: ConfigMap\n"
            + "metadata:\n"
            + "  name: test-config";

        V1alpha1Pipeline pipeline = createPipeline("test-pipeline", "default", yaml);

        // When
        List<String> elements = api.getPipelineElements(pipeline);

        // Then
        assertEquals(3, elements.size());
        assertTrue(elements.get(0).contains("kind: Deployment"));
        assertTrue(elements.get(1).contains("kind: Service"));
        assertTrue(elements.get(2).contains("kind: ConfigMap"));
    }

    @Test
    void testGetPipelineElementsWithEmptyElements() {
        // Given
        String yaml = "apiVersion: apps/v1\n"
            + "kind: Deployment\n"
            + "metadata:\n"
            + "  name: test-deployment\n"
            + "---\n"
            + "\n"
            + "---\n"
            + "apiVersion: v1\n"
            + "kind: Service\n"
            + "metadata:\n"
            + "  name: test-service";

        V1alpha1Pipeline pipeline = createPipeline("test-pipeline", "default", yaml);

        // When
        List<String> elements = api.getPipelineElements(pipeline);

        // Then
        assertEquals(2, elements.size()); // Empty elements should be filtered out
        assertTrue(elements.get(0).contains("kind: Deployment"));
        assertTrue(elements.get(1).contains("kind: Service"));
    }

    @Test
    void testGetPipelineElementsNullYaml() {
        // Given
        V1alpha1Pipeline pipeline = createPipeline("test-pipeline", "default", null);

        // When & Then
        assertThrows(NullPointerException.class, () -> api.getPipelineElements(pipeline));
    }

    @Test
    void testGetElementConfigurationWithConfigs() {
        // Given
        String elementYaml = "apiVersion: hoptimator.linkedin.com/v1alpha1\n"
            + "kind: SqlJob\n"
            + "metadata:\n"
            + "  name: test-sqljob\n"
            + "  namespace: default";

        JsonObject configsJson = new JsonObject();
        configsJson.addProperty("key1", "value1");
        configsJson.addProperty("key2", "value2");

        JsonObject specJson = new JsonObject();
        specJson.add("configs", configsJson);

        JsonObject rootJson = new JsonObject();
        rootJson.add("spec", specJson);

        setupMockForElementConfiguration(rootJson, true);

        // When
        Map<String, String> result = api.getElementConfiguration(elementYaml, "default");

        // Then
        assertEquals(2, result.size());
        assertEquals("value1", result.get("key1"));
        assertEquals("value2", result.get("key2"));
    }

    @Test
    void testGetElementConfigurationNoConfigs() {
        // Given
        String elementYaml = "apiVersion: apps/v1\n"
            + "kind: Deployment\n"
            + "metadata:\n"
            + "  name: test-deployment\n"
            + "  namespace: default";

        JsonObject specJson = new JsonObject();
        JsonObject rootJson = new JsonObject();
        rootJson.add("spec", specJson);

        setupMockForElementConfiguration(rootJson, true);

        // When
        Map<String, String> result = api.getElementConfiguration(elementYaml, "default");

        // Then
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetElementConfigurationNoSpec() {
        // Given
        String elementYaml = "apiVersion: apps/v1\n"
            + "kind: Deployment\n"
            + "metadata:\n"
            + "  name: test-deployment\n"
            + "  namespace: default";

        JsonObject rootJson = new JsonObject();

        setupMockForElementConfiguration(rootJson, true);

        // When
        Map<String, String> result = api.getElementConfiguration(elementYaml, "default");

        // Then
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetElementConfigurationApiCallFails() {
        // Given
        String elementYaml = "apiVersion: apps/v1\n"
            + "kind: Deployment\n"
            + "metadata:\n"
            + "  name: test-deployment\n"
            + "  namespace: default";

        setupMockForElementConfiguration(null, false);

        // When
        Map<String, String> result = api.getElementConfiguration(elementYaml, "default");

        // Then
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetElementConfigurationNullElement() {
        // Given
        String elementYaml = "apiVersion: apps/v1\n"
            + "kind: Deployment\n"
            + "metadata:\n"
            + "  name: test-deployment\n"
            + "  namespace: default";

        k8sUtilsMockedStatic.when(() -> K8sUtils.guessPlural(any(DynamicKubernetesObject.class))).thenReturn("deployments");

        when(mockApiResponse.isSuccess()).thenReturn(true);
        when(mockApiResponse.getObject()).thenReturn(null);

        DynamicKubernetesApi mockDynamicApi = mock(DynamicKubernetesApi.class);
        when(mockDynamicApi.get(anyString(), anyString())).thenReturn(mockApiResponse);
        when(mockContext.dynamic(anyString(), anyString())).thenReturn(mockDynamicApi);

        // When
        Map<String, String> result = api.getElementConfiguration(elementYaml, "default");

        // Then
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetElementConfigurationMalformedYaml() {
      // snakeyaml throws IndexOutOfBoundsException for malformed YAML.
      // getElementConfiguration() must catch this and return an empty map rather than propagating.
      String malformedYaml = "malformed: yaml: [unclosed";
      Map<String, String> result = api.getElementConfiguration(malformedYaml, "default");
      assertTrue(result.isEmpty());
    }

    @Test
    void testGetElementConfigurationExceptionThrown() {
        // Given
        String elementYaml = "apiVersion: apps/v1\n"
            + "kind: Deployment\n"
            + "metadata:\n"
            + "  name: test-deployment\n"
            + "  namespace: default";

        k8sUtilsMockedStatic.when(() -> K8sUtils.guessPlural(any(DynamicKubernetesObject.class))).thenReturn("deployments");

        when(mockContext.dynamic(anyString(), anyString())).thenThrow(new RuntimeException("API error"));

        // When
        Map<String, String> result = api.getElementConfiguration(elementYaml, "default");

        // Then
        assertTrue(result.isEmpty());
    }

    // Helper methods

    private V1alpha1Pipeline createPipeline(String name, String namespace, String yaml) {
        V1alpha1Pipeline pipeline = new V1alpha1Pipeline();

        V1ObjectMeta metadata = new V1ObjectMeta();
        metadata.setName(name);
        metadata.setNamespace(namespace);
        pipeline.setMetadata(metadata);

        V1alpha1PipelineSpec spec = new V1alpha1PipelineSpec();
        spec.setYaml(yaml);
        pipeline.setSpec(spec);

        return pipeline;
    }

    @Mock
    private K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> mockPipelineApi;

    @Mock
    private K8sPipelineElementStatusEstimator mockStatusEstimator;

    @Test
    void testListReturnsEmptyWhenNoPipelines() throws SQLException {
        K8sPipelineElementApi testApi = new K8sPipelineElementApi(mockContext) {
            @Override
            K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> createPipelineApi(K8sContext context) {
                return mockPipelineApi;
            }

            @Override
            K8sPipelineElementStatusEstimator createStatusEstimator(K8sContext context) {
                return mockStatusEstimator;
            }
        };
        when(mockPipelineApi.list()).thenReturn(Collections.emptyList());

        Collection<K8sPipelineElement> elements = testApi.list();

        assertTrue(elements.isEmpty());
    }

    @Test
    void testListReturnsPipelineElements() throws SQLException {
        K8sPipelineElementApi testApi = new K8sPipelineElementApi(mockContext) {
            @Override
            K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> createPipelineApi(K8sContext context) {
                return mockPipelineApi;
            }

            @Override
            K8sPipelineElementStatusEstimator createStatusEstimator(K8sContext context) {
                return mockStatusEstimator;
            }
        };

        String yaml = "apiVersion: apps/v1\n"
            + "kind: Deployment\n"
            + "metadata:\n"
            + "  name: test-deployment\n"
            + "  namespace: default";
        V1alpha1Pipeline pipeline = createPipeline("test-pipeline", "default", yaml);
        when(mockPipelineApi.list()).thenReturn(Collections.singletonList(pipeline));

        K8sPipelineElementStatus status = new K8sPipelineElementStatus("Deployment/test-deployment", true, false, "OK");
        when(mockStatusEstimator.estimateElementStatus(anyString(), anyString())).thenReturn(status);

        k8sUtilsMockedStatic.when(() -> K8sUtils.guessPlural(any(DynamicKubernetesObject.class))).thenReturn("deployments");

        DynamicKubernetesApi mockDynamicApi = mock(DynamicKubernetesApi.class);
        when(mockDynamicApi.get(anyString(), anyString())).thenReturn(mockApiResponse);
        when(mockContext.dynamic(anyString(), anyString())).thenReturn(mockDynamicApi);
        when(mockApiResponse.isSuccess()).thenReturn(false);

        Collection<K8sPipelineElement> elements = testApi.list();

        assertEquals(1, elements.size());
        K8sPipelineElement element = elements.iterator().next();
        assertEquals("Deployment/test-deployment", element.name());
    }

    @Test
    void testListDeduplicatesElementsByName() throws SQLException {
        K8sPipelineElementApi testApi = new K8sPipelineElementApi(mockContext) {
            @Override
            K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> createPipelineApi(K8sContext context) {
                return mockPipelineApi;
            }

            @Override
            K8sPipelineElementStatusEstimator createStatusEstimator(K8sContext context) {
                return mockStatusEstimator;
            }
        };

        String yaml = "apiVersion: apps/v1\n"
            + "kind: Deployment\n"
            + "metadata:\n"
            + "  name: shared-deployment\n"
            + "  namespace: default";
        V1alpha1Pipeline pipeline1 = createPipeline("pipeline-1", "default", yaml);
        V1alpha1Pipeline pipeline2 = createPipeline("pipeline-2", "default", yaml);
        when(mockPipelineApi.list()).thenReturn(Arrays.asList(pipeline1, pipeline2));

        K8sPipelineElementStatus status = new K8sPipelineElementStatus("Deployment/shared-deployment", true, false, "OK");
        when(mockStatusEstimator.estimateElementStatus(anyString(), anyString())).thenReturn(status);

        k8sUtilsMockedStatic.when(() -> K8sUtils.guessPlural(any(DynamicKubernetesObject.class))).thenReturn("deployments");

        DynamicKubernetesApi mockDynamicApi = mock(DynamicKubernetesApi.class);
        when(mockDynamicApi.get(anyString(), anyString())).thenReturn(mockApiResponse);
        when(mockContext.dynamic(anyString(), anyString())).thenReturn(mockDynamicApi);
        when(mockApiResponse.isSuccess()).thenReturn(false);

        Collection<K8sPipelineElement> elements = testApi.list();

        // Same element name across two pipelines should be deduplicated
        assertEquals(1, elements.size());
        K8sPipelineElement element = elements.iterator().next();
        // But both pipelines should be tracked
        assertEquals(2, element.pipelineNames().size());
    }

    @Test
    void createPipelineApiReturnsNonNull() {
        K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi = api.createPipelineApi(mockContext);
        assertNotNull(pipelineApi);
    }

    @Test
    void createStatusEstimatorReturnsNonNull() {
        K8sPipelineElementStatusEstimator estimator = api.createStatusEstimator(mockContext);
        assertNotNull(estimator);
    }

    private void setupMockForElementConfiguration(JsonObject rootJson, boolean success) {
        k8sUtilsMockedStatic.when(() -> K8sUtils.guessPlural(any(KubernetesType.class))).thenReturn("deployments");

        when(mockApiResponse.isSuccess()).thenReturn(success);
        if (success && rootJson != null) {
            when(mockApiResponse.getObject()).thenReturn(mockDynamicObject);
            when(mockDynamicObject.getRaw()).thenReturn(rootJson);
        }

        DynamicKubernetesApi mockDynamicApi = mock(DynamicKubernetesApi.class);
        when(mockDynamicApi.get(anyString(), anyString())).thenReturn(mockApiResponse);
        when(mockContext.dynamic(anyString(), anyString())).thenReturn(mockDynamicApi);
    }
}
