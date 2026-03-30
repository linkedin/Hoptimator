package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.hoptimator.k8s.K8sConnector.addKeysAsOption;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;


@ExtendWith(MockitoExtension.class)
class K8sConnectorTest {

  @Mock
  private MockedStatic<HoptimatorDriver> hoptimatorDriverMock;

  @Mock
  private HoptimatorConnection connection;

  @Mock
  private K8sContext mockContext;

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  private K8sConnector makeConnector(Source source,
      FakeK8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> templateApi) {
    return new K8sConnector(source, mockContext) {
      @Override
      K8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> createTableTemplateApi(K8sContext context) {
        return templateApi;
      }
    };
  }

  @Test
  void addKeysAsOptionWithNoKeyFields() {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("name", SqlTypeName.VARCHAR);
    builder.add("value", SqlTypeName.INTEGER);
    RelDataType rowType = builder.build();

    Map<String, String> result = addKeysAsOption(new HashMap<>(), rowType);

    assertTrue(result.isEmpty());
  }

  @Test
  void addKeysAsOptionWithKeyPrefixFields() {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", SqlTypeName.INTEGER);
    builder.add("KEY_name", SqlTypeName.VARCHAR);
    builder.add("value", SqlTypeName.VARCHAR);
    RelDataType rowType = builder.build();

    Map<String, String> result = addKeysAsOption(new HashMap<>(), rowType);

    assertEquals("KEY_id;KEY_name", result.get("keys"));
    assertEquals("KEY_", result.get("keyPrefix"));
    assertEquals("RECORD", result.get("keyType"));
  }

  @Test
  void addKeysAsOptionPreservesExistingKeys() {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", SqlTypeName.INTEGER);
    RelDataType rowType = builder.build();

    Map<String, String> options = new HashMap<>();
    options.put("keys", "existing-keys");

    Map<String, String> result = addKeysAsOption(options, rowType);

    assertEquals("existing-keys", result.get("keys"));
    assertFalse(result.containsKey("keyPrefix"));
  }

  @Test
  void addKeysAsOptionPreservesExistingOptions() {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("value", SqlTypeName.VARCHAR);
    RelDataType rowType = builder.build();

    Map<String, String> options = new LinkedHashMap<>();
    options.put("existing", "option");

    Map<String, String> result = addKeysAsOption(options, rowType);

    assertEquals("option", result.get("existing"));
    assertFalse(result.containsKey("keys"));
  }

  @Test
  void addKeysAsOptionWithSingleKeyField() {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", SqlTypeName.INTEGER);
    builder.add("name", SqlTypeName.VARCHAR);
    RelDataType rowType = builder.build();

    Map<String, String> result = addKeysAsOption(new HashMap<>(), rowType);

    assertEquals("KEY_id", result.get("keys"));
  }

  @Test
  void addKeysAsOptionIgnoresKeyWithoutPrefix() {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY", SqlTypeName.VARCHAR);
    builder.add("keyfield", SqlTypeName.VARCHAR);
    RelDataType rowType = builder.build();

    Map<String, String> result = addKeysAsOption(new HashMap<>(), rowType);

    assertTrue(result.isEmpty());
  }

  @Test
  void configureWithNoTemplatesReturnsEmpty() throws SQLException {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("value", SqlTypeName.VARCHAR);
    RelDataType rowType = builder.build();

    hoptimatorDriverMock.when(() -> HoptimatorDriver.rowType(any(Source.class), any()))
        .thenReturn(rowType);

    List<V1alpha1TableTemplate> templates = new ArrayList<>();
    FakeK8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> templateApi =
        new FakeK8sApi<>(templates);

    Source source = new Source("testdb", Arrays.asList("schema", "table"),
        Collections.emptyMap());

    K8sConnector connector = makeConnector(source, templateApi);
    Map<String, String> config = connector.configure();

    assertNotNull(config);
    assertTrue(config.isEmpty());
  }

  @Test
  void configureRendersMatchingTemplate() throws SQLException {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("value", SqlTypeName.VARCHAR);
    RelDataType rowType = builder.build();

    hoptimatorDriverMock.when(() -> HoptimatorDriver.rowType(any(Source.class), any()))
        .thenReturn(rowType);

    List<V1alpha1TableTemplate> templates = new ArrayList<>();
    templates.add(new V1alpha1TableTemplate()
        .metadata(new V1ObjectMeta().name("tpl"))
        .spec(new V1alpha1TableTemplateSpec()
            .databases(Collections.singletonList("testdb"))
            .connector("connector=kafka\ntopic={{table}}")));

    FakeK8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> templateApi =
        new FakeK8sApi<>(templates);

    Source source = new Source("testdb", Arrays.asList("schema", "table"),
        Collections.emptyMap());

    K8sConnector connector = makeConnector(source, templateApi);
    Map<String, String> config = connector.configure();

    assertEquals("kafka", config.get("connector"));
    assertEquals("table", config.get("topic"));
  }

  @Test
  void configureFiltersOutNonMatchingDatabases() throws SQLException {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("value", SqlTypeName.VARCHAR);
    RelDataType rowType = builder.build();

    hoptimatorDriverMock.when(() -> HoptimatorDriver.rowType(any(Source.class), any()))
        .thenReturn(rowType);

    List<V1alpha1TableTemplate> templates = new ArrayList<>();
    templates.add(new V1alpha1TableTemplate()
        .metadata(new V1ObjectMeta().name("tpl"))
        .spec(new V1alpha1TableTemplateSpec()
            .databases(Collections.singletonList("otherdb"))
            .connector("connector=kafka")));

    FakeK8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> templateApi =
        new FakeK8sApi<>(templates);

    Source source = new Source("testdb", Arrays.asList("schema", "table"),
        Collections.emptyMap());

    K8sConnector connector = makeConnector(source, templateApi);
    Map<String, String> config = connector.configure();

    assertTrue(config.isEmpty());
  }

  @Test
  void configureWithConnectorHints() throws SQLException {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("value", SqlTypeName.VARCHAR);
    RelDataType rowType = builder.build();

    hoptimatorDriverMock.when(() -> HoptimatorDriver.rowType(any(Source.class), any()))
        .thenReturn(rowType);

    List<V1alpha1TableTemplate> templates = new ArrayList<>();
    templates.add(new V1alpha1TableTemplate()
        .metadata(new V1ObjectMeta().name("tpl"))
        .spec(new V1alpha1TableTemplateSpec()
            .connector("connector=kafka\ntopic=default-topic")));

    FakeK8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> templateApi =
        new FakeK8sApi<>(templates);

    Map<String, String> options = new HashMap<>();
    options.put("kafka.source.topic", "custom-topic");
    Source source = new Source("testdb", Arrays.asList("schema", "table"), options);

    K8sConnector connector = makeConnector(source, templateApi);
    Map<String, String> config = connector.configure();

    assertEquals("kafka", config.get("connector"));
  }

  @Test
  void configureWithSink() throws SQLException {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("value", SqlTypeName.VARCHAR);
    RelDataType rowType = builder.build();

    hoptimatorDriverMock.when(() -> HoptimatorDriver.rowType(any(Source.class), any()))
        .thenReturn(rowType);

    List<V1alpha1TableTemplate> templates = new ArrayList<>();
    templates.add(new V1alpha1TableTemplate()
        .metadata(new V1ObjectMeta().name("tpl"))
        .spec(new V1alpha1TableTemplateSpec()
            .methods(Collections.singletonList(V1alpha1TableTemplateSpec.MethodsEnum.MODIFY))
            .connector("connector=kafka")));

    FakeK8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> templateApi =
        new FakeK8sApi<>(templates);

    Sink sink = new Sink("testdb", Arrays.asList("schema", "table"),
        Collections.emptyMap());

    K8sConnector connector = new K8sConnector(sink, mockContext) {
      @Override
      K8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> createTableTemplateApi(K8sContext context) {
        return templateApi;
      }
    };
    Map<String, String> config = connector.configure();

    assertEquals("kafka", config.get("connector"));
  }
}
