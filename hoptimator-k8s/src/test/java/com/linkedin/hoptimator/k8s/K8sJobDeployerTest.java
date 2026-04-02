package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.ThrowingFunction;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateSpec;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"},
    justification = "Mockito doReturn().when() stubs — framework captures the return value")
class K8sJobDeployerTest {

  @Mock
  private HoptimatorConnection connection;

  @Mock
  private K8sContext mockContext;

  private List<V1alpha1JobTemplate> templates;
  private FakeK8sApi<V1alpha1JobTemplate, V1alpha1JobTemplateList> jobTemplateApi;
  private K8sSnapshot snapshot;
  private FakeK8sYamlApi fakeYamlApi;

  @BeforeEach
  void setUp() {
    templates = new ArrayList<>();
    jobTemplateApi = new FakeK8sApi<>(templates);
    Map<String, String> yamls = new HashMap<>();
    fakeYamlApi = new FakeK8sYamlApi(yamls);
    snapshot = new K8sSnapshot(null) {
      @Override
      K8sYamlApi createYamlApi(K8sContext context) {
        return fakeYamlApi;
      }
    };
    when(mockContext.connection()).thenReturn(connection);
  }

  private Job createTestJob(Sink sink) {
    Map<String, ThrowingFunction<SqlDialect, String>> lazyEvals = new HashMap<>();
    lazyEvals.put("sql", dialect -> "INSERT INTO sink SELECT * FROM source");
    lazyEvals.put("fieldMap", dialect -> "{\"a\":\"b\"}");
    Source source = new Source("srcdb", Arrays.asList("schema", "src_table"), Collections.emptyMap());
    return new Job("test-job", new HashSet<>(Collections.singleton(source)), sink, lazyEvals);
  }

  private K8sJobDeployer makeDeployer(Job job) {
    FakeK8sApi<V1alpha1JobTemplate, V1alpha1JobTemplateList> capturedTemplateApi = jobTemplateApi;
    FakeK8sYamlApi capturedYamlApi = fakeYamlApi;
    K8sSnapshot capturedSnapshot = snapshot;
    return new K8sJobDeployer(job, mockContext) {
      @Override
      K8sApi<V1alpha1JobTemplate, V1alpha1JobTemplateList> createJobTemplateApi(K8sContext context) {
        return capturedTemplateApi;
      }

      @Override
      K8sYamlApi createYamlApi(K8sContext context) {
        return capturedYamlApi;
      }

      @Override
      K8sSnapshot createSnapshot(K8sContext context) {
        return capturedSnapshot;
      }
    };
  }

  @Test
  void specifyWithNoTemplatesReturnsEmpty() throws SQLException {
    doReturn(new Properties()).when(connection).connectionProperties();

    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"),
        Collections.emptyMap());
    Job job = createTestJob(sink);

    K8sJobDeployer deployer = makeDeployer(job);

    List<String> specs = deployer.specify();

    assertNotNull(specs);
    assertTrue(specs.isEmpty());
  }

  @Test
  void specifyRendersMatchingTemplate() throws SQLException {
    doReturn(new Properties()).when(connection).connectionProperties();

    templates.add(new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("template1"))
        .spec(new V1alpha1JobTemplateSpec()
            .databases(Collections.singletonList("sinkdb"))
            .yaml("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{name}}")));

    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"),
        Collections.emptyMap());
    Job job = createTestJob(sink);

    K8sJobDeployer deployer = makeDeployer(job);

    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    assertTrue(specs.get(0).contains("sinkdb-test-job"));
  }

  @Test
  void specifyFiltersOutNonMatchingDatabases() throws SQLException {
    doReturn(new Properties()).when(connection).connectionProperties();

    templates.add(new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("template1"))
        .spec(new V1alpha1JobTemplateSpec()
            .databases(Collections.singletonList("otherdb"))
            .yaml("should-not-render")));

    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"),
        Collections.emptyMap());
    Job job = createTestJob(sink);

    K8sJobDeployer deployer = makeDeployer(job);

    List<String> specs = deployer.specify();

    assertTrue(specs.isEmpty());
  }

  @Test
  void specifyWithNullDatabasesMatchesAll() throws SQLException {
    doReturn(new Properties()).when(connection).connectionProperties();

    templates.add(new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("template1"))
        .spec(new V1alpha1JobTemplateSpec()
            .databases(null)
            .yaml("name: {{name}}")));

    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"),
        Collections.emptyMap());
    Job job = createTestJob(sink);

    K8sJobDeployer deployer = makeDeployer(job);

    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
  }

  @Test
  void specifyRendersTemplateVariables() throws SQLException {
    doReturn(new Properties()).when(connection).connectionProperties();

    templates.add(new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("template1"))
        .spec(new V1alpha1JobTemplateSpec()
            .yaml("sql: {{sql}}\nflinksql: {{flinksql}}\nfieldMap: {{fieldMap}}\n"
                + "sources: {{sourceDatabases}}\ncatalogs: {{sourceCatalogs}}\n"
                + "schemas: {{sourceSchemas}}\ntables: {{sourceTables}}")));

    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"),
        Collections.emptyMap());
    Job job = createTestJob(sink);

    K8sJobDeployer deployer = makeDeployer(job);

    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    assertTrue(specs.get(0).contains("INSERT INTO sink SELECT * FROM source"));
    assertTrue(specs.get(0).contains("srcdb"));
  }

  @Test
  void specifyLambdasReturnNonEmptyValues() throws SQLException {
    // Verify each key field is non-empty.
    doReturn(new Properties()).when(connection).connectionProperties();

    templates.add(new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("template1"))
        .spec(new V1alpha1JobTemplateSpec()
            .databases(Collections.singletonList("sinkdb"))
            .yaml("name: {{name}}\ndatabase: {{database}}\n"
                + "table: {{table}}\n"
                + "sourceDatabases: {{sourceDatabases}}\nsourceSchemas: {{sourceSchemas}}\n"
                + "sourceTables: {{sourceTables}}\nsql: {{sql}}\nflinksql: {{flinksql}}\n"
                + "fieldMap: {{fieldMap}}")));

    Sink sink = new Sink("sinkdb", Arrays.asList("myschema", "mytable"),
        Collections.emptyMap());
    Job job = createTestJob(sink);
    K8sJobDeployer deployer = makeDeployer(job);

    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    String yaml = specs.get(0);
    assertFalse(yaml.isEmpty());
    // Each lambda must produce a non-empty value
    assertTrue(yaml.contains("srcdb"), "sourceDatabases lambda must return non-empty");
    assertTrue(yaml.contains("src_table"), "sourceTables lambda must return non-empty");
    assertTrue(yaml.contains("INSERT INTO sink SELECT * FROM source"), "sql lambda must return non-empty");
    assertTrue(yaml.contains("sinkdb"), "database must appear");
    assertTrue(yaml.contains("mytable"), "table must appear");
    assertTrue(yaml.contains("{\"a\":\"b\"}"), "fieldMap lambda must return non-empty");
  }

  @Test
  void specifyWithFlinkConfigPropertiesIncludesThem() throws SQLException {
    // Verify that sink options ARE merged into the environment
    Properties connProps = new Properties();
    connProps.setProperty("flinkConfig1", "value1");
    doReturn(connProps).when(connection).connectionProperties();

    Map<String, String> sinkOptions = new HashMap<>();
    sinkOptions.put("sinkOption", "sinkVal");
    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"), sinkOptions);

    templates.add(new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("template1"))
        .spec(new V1alpha1JobTemplateSpec()
            .yaml("option: {{sinkOption}}")));

    Job job = createTestJob(sink);
    K8sJobDeployer deployer = makeDeployer(job);

    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    // sinkOption comes from sink options putAll — if putAll is removed, "sinkVal" won't appear
    assertTrue(specs.get(0).contains("sinkVal"),
        "sink options must be merged into template environment via putAll");
  }

  @Test
  void specifyConditionalRenderedTemplateNotNull() throws SQLException {
    // Verify null templates are skipped
    doReturn(new Properties()).when(connection).connectionProperties();

    templates.add(new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("template1"))
        .spec(new V1alpha1JobTemplateSpec()
            .yaml("name: {{name}}")));

    Sink sink = new Sink("sinkdb", Arrays.asList("schema", "sink_table"),
        Collections.emptyMap());
    Job job = createTestJob(sink);
    K8sJobDeployer deployer = makeDeployer(job);

    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    assertFalse(specs.get(0).isEmpty(), "rendered template must be non-empty");
    // The name should be canonicalized from "sinkdb" + "test-job"
    assertTrue(specs.get(0).contains("sinkdb"), "rendered template must contain database name");
  }
}
