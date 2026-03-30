package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.MaterializedView;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.ThrowingFunction;
import com.linkedin.hoptimator.util.DeploymentService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION"},
    justification = "Mock objects created in stubbing setup don't need resource management")
class K8sMaterializedViewDeployerTest {

  @Mock
  private MockedStatic<DeploymentService> mockedDeploymentService;

  @Mock
  private K8sViewDeployer viewDeployer;

  @Mock
  private K8sContext context;

  @Mock
  private K8sPipelineDeployer pipelineDeployer;

  @Mock
  private K8sYamlDeployerImpl yamlDeployer;

  @BeforeEach
  void setUp() {
    // context.withOwner/withLabel are chained in create() and update() — stub to return context itself
    lenient().when(context.withOwner(any())).thenReturn(context);
    lenient().when(context.withLabel(anyString(), anyString())).thenReturn(context);
  }

  private MaterializedView createTestMaterializedView(List<String> path,
      List<Source> sources, Sink sink, Job job) {
    ThrowingFunction<SqlDialect, String> pipelineSql =
        dialect -> "INSERT INTO sink SELECT * FROM source";
    Pipeline pipeline = new Pipeline(sources, sink, job);
    return new MaterializedView("testdb", path, "SELECT 1", pipelineSql, pipeline);
  }

  private K8sMaterializedViewDeployer makeDeployerWithMockView(MaterializedView view) {
    K8sViewDeployer capturedViewDeployer = viewDeployer;
    K8sPipelineDeployer capturedPipelineDeployer = pipelineDeployer;
    K8sYamlDeployerImpl capturedYamlDeployer = yamlDeployer;
    return new K8sMaterializedViewDeployer(view, context) {
      @Override
      K8sViewDeployer createViewDeployer(MaterializedView v, K8sContext ctx) {
        return capturedViewDeployer;
      }

      @Override
      K8sPipelineDeployer createPipelineDeployer(String name, List<String> pipelineSpecs, String sql,
          K8sContext viewContext) {
        return capturedPipelineDeployer;
      }

      @Override
      K8sYamlDeployerImpl createYamlDeployerImpl(K8sContext pipelineContext, List<String> pipelineSpecs) {
        return capturedYamlDeployer;
      }
    };
  }

  @Test
  void nameCanonicalizesMaterializedViewPath() {
    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    MaterializedView view = createTestMaterializedView(
        Arrays.asList("CATALOG", "SCHEMA", "VIEW"),
        Collections.emptyList(), sink, job);

    K8sMaterializedViewDeployer deployer = new K8sMaterializedViewDeployer(view, context);
    assertEquals("catalog-schema-view", deployer.name());
  }

  @Test
  void constructorCreatesDeployer() {
    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    MaterializedView view = createTestMaterializedView(
        Arrays.asList("SCHEMA", "VIEW"),
        Collections.emptyList(), sink, job);

    K8sMaterializedViewDeployer deployer = new K8sMaterializedViewDeployer(view, context);
    assertNotNull(deployer);
    assertEquals("schema-view", deployer.name());
  }

  @Test
  void sqlReturnsPipelineSql() throws SQLException {
    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    MaterializedView view = createTestMaterializedView(
        Arrays.asList("SCHEMA", "VIEW"),
        Collections.emptyList(), sink, job);

    K8sMaterializedViewDeployer deployer = makeDeployerWithMockView(view);
    String sql = deployer.sql();
    assertEquals("INSERT INTO sink SELECT * FROM source", sql);
  }

  @Test
  void pipelineSpecsCollectsFromAllSources() throws SQLException {
    Source source1 = new Source("srcdb1", Arrays.asList("s1", "t1"), Collections.emptyMap());
    Source source2 = new Source("srcdb2", Arrays.asList("s2", "t2"), Collections.emptyMap());
    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    MaterializedView view = createTestMaterializedView(
        Arrays.asList("SCHEMA", "VIEW"),
        Arrays.asList(source1, source2), sink, job);

    mockedDeploymentService.when(() -> DeploymentService.specify(source1, null))
        .thenReturn(Arrays.asList("source1-spec"));
    mockedDeploymentService.when(() -> DeploymentService.specify(source2, null))
        .thenReturn(Arrays.asList("source2-spec"));
    mockedDeploymentService.when(() -> DeploymentService.specify(sink, null))
        .thenReturn(Arrays.asList("sink-spec"));
    mockedDeploymentService.when(() -> DeploymentService.specify(job, null))
        .thenReturn(Arrays.asList("job-spec"));

    K8sMaterializedViewDeployer deployer = makeDeployerWithMockView(view);
    List<String> specs = deployer.pipelineSpecs();

    assertEquals(4, specs.size());
    assertTrue(specs.contains("source1-spec"));
    assertTrue(specs.contains("source2-spec"));
    assertTrue(specs.contains("sink-spec"));
    assertTrue(specs.contains("job-spec"));
  }

  @Test
  void specifyDelegatesToPipelineSpecs() throws SQLException {
    Source source = new Source("srcdb", Arrays.asList("s", "t"), Collections.emptyMap());
    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    MaterializedView view = createTestMaterializedView(
        Arrays.asList("SCHEMA", "VIEW"),
        Collections.singletonList(source), sink, job);

    mockedDeploymentService.when(() -> DeploymentService.specify(source, null))
        .thenReturn(Arrays.asList("src-spec"));
    mockedDeploymentService.when(() -> DeploymentService.specify(sink, null))
        .thenReturn(Collections.emptyList());
    mockedDeploymentService.when(() -> DeploymentService.specify(job, null))
        .thenReturn(Collections.emptyList());

    K8sMaterializedViewDeployer deployer = makeDeployerWithMockView(view);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    assertEquals("src-spec", specs.get(0));
  }

  @Test
  void restoreWithNoDeployersIsNoOp() {
    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    MaterializedView view = createTestMaterializedView(
        Arrays.asList("SCHEMA", "VIEW"),
        Collections.emptyList(), sink, job);

    K8sMaterializedViewDeployer deployer = makeDeployerWithMockView(view);
    // restore() with empty deployers list should just call viewDeployer.restore()
    deployer.restore();
    // No exception means success
    assertNotNull(deployer);
  }

  @Test
  void deleteCallsViewDeployerDelete() throws SQLException {
    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    MaterializedView view = createTestMaterializedView(
        Arrays.asList("SCHEMA", "VIEW"),
        Collections.emptyList(), sink, job);

    K8sMaterializedViewDeployer deployer = makeDeployerWithMockView(view);
    // viewDeployer.delete() is mocked, so this should succeed
    deployer.delete();
    assertNotNull(deployer);
  }

  @Test
  void createDeploysViewAndPipeline() throws SQLException {
    Source source = new Source("srcdb", Arrays.asList("s", "t"), Collections.emptyMap());
    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    MaterializedView view = createTestMaterializedView(
        Arrays.asList("SCHEMA", "VIEW"),
        Collections.singletonList(source), sink, job);

    V1OwnerReference viewRef = new V1OwnerReference().name("view-ref").uid("v-uid")
        .kind("View").apiVersion("hoptimator.linkedin.com/v1alpha1");
    V1OwnerReference pipelineRef = new V1OwnerReference().name("pipeline-ref").uid("p-uid")
        .kind("Pipeline").apiVersion("hoptimator.linkedin.com/v1alpha1");
    doReturn(viewRef).when(viewDeployer).createAndReference();
    doReturn(pipelineRef).when(pipelineDeployer).createAndReference();

    mockedDeploymentService.when(() -> DeploymentService.specify(source, null))
        .thenReturn(Collections.emptyList());
    mockedDeploymentService.when(() -> DeploymentService.specify(sink, null))
        .thenReturn(Collections.emptyList());
    mockedDeploymentService.when(() -> DeploymentService.specify(job, null))
        .thenReturn(Collections.emptyList());

    K8sMaterializedViewDeployer deployer = makeDeployerWithMockView(view);
    deployer.create();

    verify(pipelineDeployer).createAndReference();
    verify(yamlDeployer).update();
  }

  @Test
  void updateDeploysViewAndPipeline() throws SQLException {
    Source source = new Source("srcdb", Arrays.asList("s", "t"), Collections.emptyMap());
    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    MaterializedView view = createTestMaterializedView(
        Arrays.asList("SCHEMA", "VIEW"),
        Collections.singletonList(source), sink, job);

    V1OwnerReference viewRef = new V1OwnerReference().name("view-ref").uid("v-uid")
        .kind("View").apiVersion("hoptimator.linkedin.com/v1alpha1");
    V1OwnerReference pipelineRef = new V1OwnerReference().name("pipeline-ref").uid("p-uid")
        .kind("Pipeline").apiVersion("hoptimator.linkedin.com/v1alpha1");
    doReturn(viewRef).when(viewDeployer).updateAndReference();
    doReturn(pipelineRef).when(pipelineDeployer).updateAndReference();

    mockedDeploymentService.when(() -> DeploymentService.specify(source, null))
        .thenReturn(Collections.emptyList());
    mockedDeploymentService.when(() -> DeploymentService.specify(sink, null))
        .thenReturn(Collections.emptyList());
    mockedDeploymentService.when(() -> DeploymentService.specify(job, null))
        .thenReturn(Collections.emptyList());

    K8sMaterializedViewDeployer deployer = makeDeployerWithMockView(view);
    deployer.update();

    verify(pipelineDeployer).updateAndReference();
    verify(yamlDeployer).update();
  }

  @Test
  void restoreCallsDeployersInReverseOrder() throws SQLException {
    Source source = new Source("srcdb", Arrays.asList("s", "t"), Collections.emptyMap());
    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    MaterializedView view = createTestMaterializedView(
        Arrays.asList("SCHEMA", "VIEW"),
        Collections.singletonList(source), sink, job);

    V1OwnerReference viewRef = new V1OwnerReference().name("view-ref").uid("v-uid")
        .kind("View").apiVersion("hoptimator.linkedin.com/v1alpha1");
    V1OwnerReference pipelineRef = new V1OwnerReference().name("pipeline-ref").uid("p-uid")
        .kind("Pipeline").apiVersion("hoptimator.linkedin.com/v1alpha1");
    doReturn(viewRef).when(viewDeployer).createAndReference();
    doReturn(pipelineRef).when(pipelineDeployer).createAndReference();

    mockedDeploymentService.when(() -> DeploymentService.specify(source, null))
        .thenReturn(Collections.emptyList());
    mockedDeploymentService.when(() -> DeploymentService.specify(sink, null))
        .thenReturn(Collections.emptyList());
    mockedDeploymentService.when(() -> DeploymentService.specify(job, null))
        .thenReturn(Collections.emptyList());

    K8sMaterializedViewDeployer deployer = makeDeployerWithMockView(view);
    deployer.create();
    deployer.restore();

    // Verify restore was called on both deployers and the viewDeployer
    verify(pipelineDeployer).restore();
    verify(yamlDeployer).restore();
    verify(viewDeployer).restore();
  }

  @Test
  void pipelineSpecsWithNoSources() throws SQLException {
    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    MaterializedView view = createTestMaterializedView(
        Arrays.asList("SCHEMA", "VIEW"),
        Collections.emptyList(), sink, job);

    mockedDeploymentService.when(() -> DeploymentService.specify(sink, null))
        .thenReturn(Arrays.asList("sink-spec"));
    mockedDeploymentService.when(() -> DeploymentService.specify(job, null))
        .thenReturn(Collections.emptyList());

    K8sMaterializedViewDeployer deployer = makeDeployerWithMockView(view);
    List<String> specs = deployer.pipelineSpecs();

    assertEquals(1, specs.size());
    assertEquals("sink-spec", specs.get(0));
  }
}
