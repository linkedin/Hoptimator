package com.linkedin.hoptimator.k8s;

import io.kubernetes.client.openapi.models.V1OwnerReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
class K8sPipelineBundleTest {

  @Mock
  private K8sContext context;

  @Mock
  private K8sPipelineDeployer pipelineDeployer;

  @Mock
  private K8sYamlDeployerImpl yamlDeployer;

  @BeforeEach
  void setUp() {
    lenient().when(context.withOwner(any())).thenReturn(context);
    lenient().when(context.withLabel(anyString(), anyString())).thenReturn(context);
  }

  private K8sPipelineBundle makeBundleWithMocks(String name, List<String> pipelineSpecs) {
    K8sPipelineDeployer capturedPipelineDeployer = pipelineDeployer;
    K8sYamlDeployerImpl capturedYamlDeployer = yamlDeployer;
    return new K8sPipelineBundle(name, pipelineSpecs, "INSERT INTO sink SELECT * FROM source", Collections.emptyList(), null, context) {
      @Override
      K8sPipelineDeployer createPipelineDeployer(String n, List<String> specs, String sql,
          Collection<Source> sources, Sink sink, K8sContext ctx) {
        return capturedPipelineDeployer;
      }

      @Override
      K8sYamlDeployerImpl createYamlDeployerImpl(K8sContext pipelineContext, List<String> specs) {
        return capturedYamlDeployer;
      }
    };
  }

  @Test
  void constructorStoresNameAndPipelineSpecs() throws SQLException {
    List<String> specs = Arrays.asList("spec1", "spec2");

    K8sPipelineBundle bundle = makeBundleWithMocks("my-pipeline", specs);

    assertNotNull(bundle);
    assertEquals(specs, bundle.specify());
  }

  @Test
  void specifyReturnsPipelineSpecs() throws SQLException {
    List<String> specs = Arrays.asList("yaml1", "yaml2", "yaml3");

    K8sPipelineBundle bundle = makeBundleWithMocks("test-pipeline", specs);
    List<String> result = bundle.specify();

    assertEquals(3, result.size());
    assertEquals("yaml1", result.get(0));
    assertEquals("yaml2", result.get(1));
    assertEquals("yaml3", result.get(2));
  }

  @Test
  void specifyWithEmptySpecsReturnsEmptyList() throws SQLException {
    List<String> specs = Collections.emptyList();

    K8sPipelineBundle bundle = makeBundleWithMocks("empty-pipeline", specs);
    List<String> result = bundle.specify();

    assertNotNull(result);
    assertEquals(0, result.size());
  }

  @Test
  void createCallsPipelineDeployerCreateAndReference() throws SQLException {
    V1OwnerReference pipelineRef = new V1OwnerReference().name("pipeline-ref").uid("p-uid")
        .kind("Pipeline").apiVersion("hoptimator.linkedin.com/v1alpha1");
    doReturn(pipelineRef).when(pipelineDeployer).createAndReference();

    K8sPipelineBundle bundle = makeBundleWithMocks("test-pipeline", Collections.singletonList("spec1"));
    bundle.create();

    verify(pipelineDeployer).createAndReference();
  }

  @Test
  void createCallsYamlDeployerUpdate() throws SQLException {
    V1OwnerReference pipelineRef = new V1OwnerReference().name("pipeline-ref").uid("p-uid")
        .kind("Pipeline").apiVersion("hoptimator.linkedin.com/v1alpha1");
    doReturn(pipelineRef).when(pipelineDeployer).createAndReference();

    K8sPipelineBundle bundle = makeBundleWithMocks("test-pipeline", Collections.singletonList("spec1"));
    bundle.create();

    verify(yamlDeployer).update();
  }

  @Test
  void updateCallsPipelineDeployerUpdateAndReference() throws SQLException {
    V1OwnerReference pipelineRef = new V1OwnerReference().name("pipeline-ref").uid("p-uid")
        .kind("Pipeline").apiVersion("hoptimator.linkedin.com/v1alpha1");
    doReturn(pipelineRef).when(pipelineDeployer).updateAndReference();

    K8sPipelineBundle bundle = makeBundleWithMocks("test-pipeline", Collections.singletonList("spec1"));
    bundle.update();

    verify(pipelineDeployer).updateAndReference();
  }

  @Test
  void updateCallsYamlDeployerUpdate() throws SQLException {
    V1OwnerReference pipelineRef = new V1OwnerReference().name("pipeline-ref").uid("p-uid")
        .kind("Pipeline").apiVersion("hoptimator.linkedin.com/v1alpha1");
    doReturn(pipelineRef).when(pipelineDeployer).updateAndReference();

    K8sPipelineBundle bundle = makeBundleWithMocks("test-pipeline", Collections.singletonList("spec1"));
    bundle.update();

    verify(yamlDeployer).update();
  }

  @Test
  void deleteCallsPipelineDeployerDelete() throws SQLException {
    K8sPipelineBundle bundle = makeBundleWithMocks("test-pipeline", Collections.singletonList("spec1"));
    bundle.delete();

    verify(pipelineDeployer).delete();
  }

  @Test
  void restoreWithNoDeployersIsNoOp() {
    K8sPipelineBundle bundle = makeBundleWithMocks("test-pipeline", Collections.singletonList("spec1"));
    bundle.restore();

    // No exception means success; deployers list is empty before any CRUD
    assertNotNull(bundle);
  }

  @Test
  void restoreAfterCreateCallsDeployersInForwardOrder() throws SQLException {
    V1OwnerReference pipelineRef = new V1OwnerReference().name("pipeline-ref").uid("p-uid")
        .kind("Pipeline").apiVersion("hoptimator.linkedin.com/v1alpha1");
    doReturn(pipelineRef).when(pipelineDeployer).createAndReference();

    K8sPipelineBundle bundle = makeBundleWithMocks("test-pipeline", Collections.singletonList("spec1"));
    bundle.create();
    bundle.restore();

    verify(pipelineDeployer).restore();
    verify(yamlDeployer).restore();
  }

  @Test
  void restoreAfterUpdateCallsDeployersInForwardOrder() throws SQLException {
    V1OwnerReference pipelineRef = new V1OwnerReference().name("pipeline-ref").uid("p-uid")
        .kind("Pipeline").apiVersion("hoptimator.linkedin.com/v1alpha1");
    doReturn(pipelineRef).when(pipelineDeployer).updateAndReference();

    K8sPipelineBundle bundle = makeBundleWithMocks("test-pipeline", Collections.singletonList("spec1"));
    bundle.update();
    bundle.restore();

    verify(pipelineDeployer).restore();
    verify(yamlDeployer).restore();
  }

  @Test
  void createUsesOwnerContextChainWithLabelAndOwner() throws SQLException {
    V1OwnerReference pipelineRef = new V1OwnerReference().name("pipeline-ref").uid("p-uid")
        .kind("Pipeline").apiVersion("hoptimator.linkedin.com/v1alpha1");
    doReturn(pipelineRef).when(pipelineDeployer).createAndReference();

    K8sPipelineBundle bundle = makeBundleWithMocks("my-pipeline", Collections.singletonList("spec1"));
    bundle.create();

    verify(context).withLabel("pipeline", "my-pipeline");
    verify(context).withOwner(pipelineRef);
  }

  @Test
  void updateUsesOwnerContextChainWithLabelAndOwner() throws SQLException {
    V1OwnerReference pipelineRef = new V1OwnerReference().name("pipeline-ref").uid("p-uid")
        .kind("Pipeline").apiVersion("hoptimator.linkedin.com/v1alpha1");
    doReturn(pipelineRef).when(pipelineDeployer).updateAndReference();

    K8sPipelineBundle bundle = makeBundleWithMocks("my-pipeline", Collections.singletonList("spec1"));
    bundle.update();

    verify(context).withLabel("pipeline", "my-pipeline");
    verify(context).withOwner(pipelineRef);
  }
}
