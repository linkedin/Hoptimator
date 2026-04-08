package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.options.DeleteOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;


@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"},
    justification = "Mocked AutoCloseable and return values not needed in tests")
@ExtendWith(MockitoExtension.class)
class K8sApiErrorResponseTest {

  @Mock
  private K8sContext mockErrContext;

  @Mock
  private GenericKubernetesApi<V1alpha1Pipeline, V1alpha1PipelineList> mockErrApi;

  @Mock
  private KubernetesApiResponse<V1alpha1Pipeline> mockErrResponse;

  private K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> errApi;

  @BeforeEach
  void setUp() {
    errApi = new K8sApi<>(mockErrContext, K8sApiEndpoints.PIPELINES);
    lenient().when(mockErrContext.generic(K8sApiEndpoints.PIPELINES)).thenReturn(mockErrApi);
    lenient().when(mockErrContext.namespace()).thenReturn("test-ns");
  }

  @Test
  void getThrowsWhenResponseIsErrorStatus() throws ApiException {
    ApiException apiEx = new ApiException(500, "Internal Server Error");
    when(mockErrApi.get(eq("test-ns"), eq("bad-pipeline"))).thenReturn(mockErrResponse);
    when(mockErrResponse.getHttpStatusCode()).thenReturn(500);
    // throwsApiException() throws when response is not success
    lenient().doThrow(apiEx).when(mockErrResponse).throwsApiException();

    assertThrows(SQLException.class, () -> errApi.get("test-ns", "bad-pipeline"));
  }

  @Test
  void createThrowsWhenResponseIsErrorStatus() throws ApiException {
    V1alpha1Pipeline pipeline = makePipeline("bad-pipeline", null);
    ApiException apiEx = new ApiException(500, "Internal Server Error");
    when(mockErrApi.create(any(V1alpha1Pipeline.class))).thenReturn(mockErrResponse);
    when(mockErrResponse.getHttpStatusCode()).thenReturn(500);
    lenient().doThrow(apiEx).when(mockErrResponse).throwsApiException();

    assertThrows(SQLException.class, () -> errApi.create(pipeline));
  }

  @Test
  void deleteThrowsWhenResponseIsErrorStatus() throws ApiException {
    ApiException apiEx = new ApiException(500, "Internal Server Error");
    when(mockErrApi.delete(eq("test-ns"), eq("bad-pipeline"), any(DeleteOptions.class)))
        .thenReturn(mockErrResponse);
    when(mockErrResponse.getHttpStatusCode()).thenReturn(500);
    lenient().doThrow(apiEx).when(mockErrResponse).throwsApiException();

    assertThrows(SQLException.class, () -> errApi.delete("test-ns", "bad-pipeline"));
  }

  @Test
  void updateThrowsWhenResponseIsErrorStatusOnFinalUpdate() throws ApiException {
    V1alpha1Pipeline pipeline = makePipeline("bad-pipeline", "test-ns");
    V1alpha1Pipeline existing = makePipeline("bad-pipeline", "test-ns");
    existing.getMetadata().setResourceVersion("rv1");

    KubernetesApiResponse<V1alpha1Pipeline> existingResp = mock(KubernetesApiResponse.class);
    when(existingResp.isSuccess()).thenReturn(true);
    when(existingResp.getObject()).thenReturn(existing);
    when(mockErrApi.get(eq("test-ns"), eq("bad-pipeline"))).thenReturn(existingResp);

    ApiException apiEx = new ApiException(500, "Internal Server Error");
    when(mockErrApi.update(any(V1alpha1Pipeline.class))).thenReturn(mockErrResponse);
    when(mockErrResponse.getHttpStatusCode()).thenReturn(500);
    lenient().doThrow(apiEx).when(mockErrResponse).throwsApiException();

    assertThrows(SQLException.class, () -> errApi.update(pipeline));
  }

  @Test
  void updateStatusThrowsWhenResponseIsErrorStatus() throws ApiException {
    V1alpha1Pipeline pipeline = makePipeline("bad-pipeline", "test-ns");
    ApiException apiEx = new ApiException(500, "Internal Server Error");
    when(mockErrApi.updateStatus(any(V1alpha1Pipeline.class), any())).thenReturn(mockErrResponse);
    when(mockErrResponse.getHttpStatusCode()).thenReturn(500);
    lenient().doThrow(apiEx).when(mockErrResponse).throwsApiException();

    assertThrows(SQLException.class, () -> errApi.updateStatus(pipeline, new Object()));
  }

  @Test
  void getByObjectThrowsWhenResponseIsErrorStatus() throws ApiException {
    V1alpha1Pipeline pipeline = makePipeline("bad-pipeline", "test-ns");
    ApiException apiEx = new ApiException(500, "Internal Server Error");
    when(mockErrApi.get(eq("test-ns"), eq("bad-pipeline"))).thenReturn(mockErrResponse);
    when(mockErrResponse.getHttpStatusCode()).thenReturn(500);
    lenient().doThrow(apiEx).when(mockErrResponse).throwsApiException();

    assertThrows(SQLException.class, () -> errApi.get(pipeline));
  }

  @Test
  void getIfExistsThrowsWhenNon404ErrorStatus() throws ApiException {
    ApiException apiEx = new ApiException(500, "Internal Server Error");
    when(mockErrApi.get(eq("test-ns"), eq("bad-pipeline"))).thenReturn(mockErrResponse);
    when(mockErrResponse.getHttpStatusCode()).thenReturn(500);
    lenient().doThrow(apiEx).when(mockErrResponse).throwsApiException();

    assertThrows(SQLException.class, () -> errApi.getIfExists("test-ns", "bad-pipeline"));
  }

  private V1alpha1Pipeline makePipeline(String name, String namespace) {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline();
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(name);
    meta.setNamespace(namespace);
    pipeline.setMetadata(meta);
    return pipeline;
  }
}
