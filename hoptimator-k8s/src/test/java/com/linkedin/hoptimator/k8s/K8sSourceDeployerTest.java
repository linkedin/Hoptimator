package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateSpec;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"},
    justification = "Mockito doReturn().when() stubs — framework captures the return value")
class K8sSourceDeployerTest {

  @Mock
  private HoptimatorConnection connection;

  @Mock
  private K8sContext mockContext;

  private List<V1alpha1TableTemplate> templates;
  private FakeK8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> tableTemplateApi;
  private K8sSnapshot snapshot;
  private FakeK8sYamlApi fakeYamlApi;

  @BeforeEach
  void setUp() {
    templates = new ArrayList<>();
    tableTemplateApi = new FakeK8sApi<>(templates);
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

  private K8sSourceDeployer makeDeployer(Source source) {
    FakeK8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> capturedTemplateApi = tableTemplateApi;
    FakeK8sYamlApi capturedYamlApi = fakeYamlApi;
    K8sSnapshot capturedSnapshot = snapshot;
    return new K8sSourceDeployer(source, mockContext) {
      @Override
      K8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> createTableTemplateApi(K8sContext context) {
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

    Source source = new Source("testdb", Arrays.asList("schema", "table"),
        Collections.emptyMap());

    K8sSourceDeployer deployer = makeDeployer(source);

    List<String> specs = deployer.specify();

    assertNotNull(specs);
    assertTrue(specs.isEmpty());
  }

  @Test
  void specifyRendersMatchingTemplate() throws SQLException {
    doReturn(new Properties()).when(connection).connectionProperties();

    templates.add(new V1alpha1TableTemplate()
        .metadata(new V1ObjectMeta().name("template1"))
        .spec(new V1alpha1TableTemplateSpec()
            .databases(Collections.singletonList("testdb"))
            .yaml("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{name}}")));

    Source source = new Source("testdb", Arrays.asList("schema", "table"),
        Collections.emptyMap());

    K8sSourceDeployer deployer = makeDeployer(source);

    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    assertTrue(specs.get(0).contains("testdb-table"));
  }

  @Test
  void specifyFiltersOutNonMatchingDatabases() throws SQLException {
    doReturn(new Properties()).when(connection).connectionProperties();

    templates.add(new V1alpha1TableTemplate()
        .metadata(new V1ObjectMeta().name("template1"))
        .spec(new V1alpha1TableTemplateSpec()
            .databases(Collections.singletonList("otherdb"))
            .yaml("should-not-render")));

    Source source = new Source("testdb", Arrays.asList("schema", "table"),
        Collections.emptyMap());

    K8sSourceDeployer deployer = makeDeployer(source);

    List<String> specs = deployer.specify();

    assertTrue(specs.isEmpty());
  }

  @Test
  void specifyWithJobPropertiesInOptions() throws SQLException {
    doReturn(new Properties()).when(connection).connectionProperties();

    templates.add(new V1alpha1TableTemplate()
        .metadata(new V1ObjectMeta().name("template1"))
        .spec(new V1alpha1TableTemplateSpec()
            .yaml("name: {{name}}")));

    Map<String, String> options = new HashMap<>();
    options.put("job.properties.parallelism", "4");
    Source source = new Source("testdb", Arrays.asList("schema", "table"), options);

    K8sSourceDeployer deployer = makeDeployer(source);

    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
  }

  @Test
  void specifyWithNullDatabasesMatchesAll() throws SQLException {
    doReturn(new Properties()).when(connection).connectionProperties();

    templates.add(new V1alpha1TableTemplate()
        .metadata(new V1ObjectMeta().name("template1"))
        .spec(new V1alpha1TableTemplateSpec()
            .databases(null)
            .yaml("name: {{name}}")));

    Source source = new Source("anydb", Arrays.asList("schema", "table"),
        Collections.emptyMap());

    K8sSourceDeployer deployer = makeDeployer(source);

    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
  }
}
