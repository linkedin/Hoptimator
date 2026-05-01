package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.avro.AvroSchemaSource;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


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
  void configureHintsExactKeyAfterStrippingPrefix() throws SQLException {
    // Tests getConnectorHints: the substring(connectorHintPrefix.length() + 1)
    // change +1 to something else, altering the stripped key
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("value", SqlTypeName.VARCHAR);
    RelDataType rowType = builder.build();

    hoptimatorDriverMock.when(() -> HoptimatorDriver.rowType(any(Source.class), any()))
        .thenReturn(rowType);

    List<V1alpha1TableTemplate> templates = new ArrayList<>();
    templates.add(new V1alpha1TableTemplate()
        .metadata(new V1ObjectMeta().name("tpl"))
        .spec(new V1alpha1TableTemplateSpec()
            .connector("connector=kafka\ntopic=base-topic")));

    FakeK8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> templateApi =
        new FakeK8sApi<>(templates);

    // hint key: "kafka.source.bootstrap-servers" → should become "bootstrap-servers"
    Map<String, String> options = new HashMap<>();
    options.put("kafka.source.bootstrap-servers", "broker:9092");
    Source source = new Source("testdb", Arrays.asList("schema", "table"), options);

    K8sConnector connector = makeConnector(source, templateApi);
    Map<String, String> config = connector.configure();

    // The hint key stripped of "kafka.source." prefix should be exactly "bootstrap-servers"
    assertEquals("broker:9092", config.get("bootstrap-servers"));
  }

  @Test
  void configureOnlyIncludesMatchingHints() throws SQLException {
    // Filter should exclude non-matching entries
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("value", SqlTypeName.VARCHAR);
    RelDataType rowType = builder.build();

    hoptimatorDriverMock.when(() -> HoptimatorDriver.rowType(any(Source.class), any()))
        .thenReturn(rowType);

    List<V1alpha1TableTemplate> templates = new ArrayList<>();
    templates.add(new V1alpha1TableTemplate()
        .metadata(new V1ObjectMeta().name("tpl"))
        .spec(new V1alpha1TableTemplateSpec()
            .connector("connector=kafka\ntopic=base-topic")));

    FakeK8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> templateApi =
        new FakeK8sApi<>(templates);

    Map<String, String> options = new HashMap<>();
    options.put("kafka.source.group-id", "my-group");
    options.put("otherconnector.source.something", "should-not-appear");
    Source source = new Source("testdb", Arrays.asList("schema", "table"), options);

    K8sConnector connector = makeConnector(source, templateApi);
    Map<String, String> config = connector.configure();

    assertEquals("my-group", config.get("group-id"));
    // Non-matching hint should not appear in config
    assertFalse(config.containsKey("something"));
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

  @Test
  void configureAvroValueSchemaUsesSourceWhenAvailable() throws SQLException {
    // When the resolved table implements AvroSchemaSource, its native Avro schema is rendered
    // into the template as-is — no round-trip through RelDataType.
    RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
        .add("KEY_id", SqlTypeName.VARCHAR)
        .add("name", SqlTypeName.VARCHAR).build();
    hoptimatorDriverMock.when(() -> HoptimatorDriver.rowType(any(Source.class), any()))
        .thenReturn(rowType);

    Schema avroSchema = SchemaBuilder.record("User").namespace("com.linkedin.foo").fields()
        .requiredString("KEY_id")
        .requiredString("name")
        .endRecord();
    Source source = new Source("testdb", Arrays.asList("schema", "table"),
        Collections.emptyMap());
    installRootSchemaWithTable(source, new SourceTable(avroSchema));

    FakeK8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> templateApi = new FakeK8sApi<>(
        List.of(new V1alpha1TableTemplate()
            .metadata(new V1ObjectMeta().name("tpl"))
            .spec(new V1alpha1TableTemplateSpec()
                .databases(List.of("testdb"))
                .connector("avroValueSchema={{avroValueSchema}}"))));

    K8sConnector connector = makeConnector(source, templateApi);
    Map<String, String> config = connector.configure();

    String rendered = config.get("avroValueSchema");
    assertNotNull(rendered);
    assertTrue(rendered.contains("\"namespace\":\"com.linkedin.foo\""),
        "provider namespace preserved; got " + rendered);
    assertTrue(rendered.contains("\"name\":\"User\""),
        "provider record name preserved; got " + rendered);
  }

  @Test
  void configureAvroValueSchemaFallsBackToRowTypeSynthesisWhenNoSource() throws SQLException {
    // No AvroSchemaSource on the resolved table → synthesize from the row type using AvroConverter.avro.
    RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
        .add("name", SqlTypeName.VARCHAR).build();
    hoptimatorDriverMock.when(() -> HoptimatorDriver.rowType(any(Source.class), any()))
        .thenReturn(rowType);

    Source source = new Source("testdb", Arrays.asList("schema", "table"),
        Collections.emptyMap());
    installRootSchemaWithTable(source, new PlainTable());

    FakeK8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> templateApi = new FakeK8sApi<>(
        List.of(new V1alpha1TableTemplate()
            .metadata(new V1ObjectMeta().name("tpl"))
            .spec(new V1alpha1TableTemplateSpec()
                .databases(List.of("testdb"))
                .connector("avroValueSchema={{avroValueSchema}}"))));

    K8sConnector connector = makeConnector(source, templateApi);
    Map<String, String> config = connector.configure();

    assertTrue(config.get("avroValueSchema").contains("\"namespace\":\"com.linkedin.hoptimator.table\""),
        "synthesized fallback produces the legacy namespace pattern; got " + config.get("avroValueSchema"));
  }

  private void installRootSchemaWithTable(Source source, Table table) {
    SchemaPlus root = CalciteSchema.createRootSchema(false).plus();
    SchemaPlus parent = root;
    for (String part : source.path().subList(0, source.path().size() - 1)) {
      parent = parent.add(part, new org.apache.calcite.schema.impl.AbstractSchema());
    }
    parent.add(source.table(), table);

    CalciteConnection calciteConn = mock(CalciteConnection.class);
    when(calciteConn.getRootSchema()).thenReturn(root);
    when(connection.calciteConnection()).thenReturn(calciteConn);
    when(mockContext.connection()).thenReturn(connection);
  }

  private static final class PlainTable extends AbstractTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class SourceTable extends AbstractTable implements AvroSchemaSource {
    private final Schema value;

    SourceTable(Schema value) {
      this.value = value;
    }

    @Override
    public Schema valueSchema() {
      return value;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      throw new UnsupportedOperationException();
    }
  }
}
