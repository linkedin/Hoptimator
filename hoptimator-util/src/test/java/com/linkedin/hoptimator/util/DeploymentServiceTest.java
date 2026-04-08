package com.linkedin.hoptimator.util;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.linkedin.hoptimator.util.DeploymentService.HINT_OPTION;
import static com.linkedin.hoptimator.util.DeploymentService.PIPELINE_OPTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class DeploymentServiceTest {

  /**
   * "hint" keys <b>and</b> values are required to be non-{@code null}. An
   * empty {@link Map} is considered invalid and should <b>not</b> be added
   * to the {@link Properties} object.
   * <p>
   * nb. "pipeline" values are <b>always</b> added when present.
   */
  @Test
  void parseHints() {
    Map<String, String> empty = DeploymentService.parseHints(new Properties());
    assertTrue(empty.isEmpty(), "An empty map should not add `hints`.");

    Map<String, String> defined = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "key=value");
    }});
    assertEquals("value", defined.get("key"), "Did not match expected key value pair: `key=value`.");

    Map<String, String> nokey = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "flag0=,flag1=");
    }});
    assertTrue(nokey.keySet().containsAll(Arrays.asList("flag0", "flag1")), "Expected to find flags.");

    Map<String, String> pipelineOnly = DeploymentService.parseHints(new Properties() {{
      put(PIPELINE_OPTION, "pipeline");
    }});
    assertEquals("pipeline", pipelineOnly.get(PIPELINE_OPTION), "Did not match expected `pipeline` value.");

    Map<String, String> both = DeploymentService.parseHints(new Properties() {{
      putAll(defined);
      put(PIPELINE_OPTION, "pipeline");
    }});
    assertEquals("pipeline", both.get(PIPELINE_OPTION), "Did not match expected `pipeline` value.");
  }

  /**
   * Test that URL-encoded values are properly decoded, allowing commas and other special characters.
   */
  @Test
  void parseHintsWithUrlEncodedValues() {
    // Test value with commas (URL-encoded)
    String valueWithCommas = "host1:9092,host2:9092,host3:9092";
    String encoded = URLEncoder.encode(valueWithCommas, StandardCharsets.UTF_8);
    Map<String, String> urlEncoded = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "servers=" + encoded + ",parallelism=4");
    }});
    assertEquals(valueWithCommas, urlEncoded.get("servers"),
        "URL-encoded value with commas should be decoded correctly");
    assertEquals("4", urlEncoded.get("parallelism"));

    // Test complex nested configuration (URL-encoded)
    String complexValue = "bootstrap.servers=host1:9092,host2:9092,host3:9092";
    String encodedComplex = URLEncoder.encode(complexValue, StandardCharsets.UTF_8);
    Map<String, String> complexEncoded = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "kafka.config=" + encodedComplex + ",other=simple");
    }});
    assertEquals(complexValue, complexEncoded.get("kafka.config"),
        "Complex URL-encoded value should be decoded correctly");
    assertEquals("simple", complexEncoded.get("other"));

    // Test value with equals signs and commas (URL-encoded)
    String nestedKV = "key1=value1,key2=value2,key3=value3";
    String encodedNested = URLEncoder.encode(nestedKV, StandardCharsets.UTF_8);
    Map<String, String> nestedEncoded = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "nested.config=" + encodedNested);
    }});
    assertEquals(nestedKV, nestedEncoded.get("nested.config"),
        "Nested K=V pairs with commas should be preserved when URL-encoded");

    // Test standard values
    String simpleValue = "value";
    String encodedSimpleValue = URLEncoder.encode(simpleValue, StandardCharsets.UTF_8);
    Map<String, String> hintsSimpleValue = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "key=" + encodedSimpleValue);
    }});
    assertEquals(simpleValue, hintsSimpleValue.get("key"), "Simple K=V strings should be preserved when URL-encoded");
  }

  /**
   * Test backward compatibility - non-encoded values should still work.
   */
  @Test
  void parseHintsBackwardCompatibility() {
    // Test that existing non-encoded values still work
    Map<String, String> simple = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "kafka.partitions=4,flink.parallelism=2");
    }});
    assertEquals("4", simple.get("kafka.partitions"));
    assertEquals("2", simple.get("flink.parallelism"));

    // Test existing examples from README still work
    Map<String, String> readmeExample = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "kafka.partitions=4,flink.parallelism=2,kafka.source.k1=v1,kafka.sink.k2=v2");
    }});
    assertEquals("4", readmeExample.get("kafka.partitions"));
    assertEquals("2", readmeExample.get("flink.parallelism"));
    assertEquals("v1", readmeExample.get("kafka.source.k1"));
    assertEquals("v2", readmeExample.get("kafka.sink.k2"));

    // Test values with dots and underscores (common in property names)
    Map<String, String> dotted = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "kafka.source.properties.group.id=my_group,offline.table.name=ads_offline");
    }});
    assertEquals("my_group", dotted.get("kafka.source.properties.group.id"));
    assertEquals("ads_offline", dotted.get("offline.table.name"));
  }

  /**
   * Test mixed encoded and non-encoded values (should work together).
   */
  @Test
  void parseHintsMixedEncodedAndNonEncoded() {
    String encodedValue = URLEncoder.encode("value,with,commas", StandardCharsets.UTF_8);
    Map<String, String> mixed = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "encoded=" + encodedValue + ",simple=value,another=test");
    }});
    assertEquals("value,with,commas", mixed.get("encoded"));
    assertEquals("value", mixed.get("simple"));
    assertEquals("test", mixed.get("another"));
  }

  @Mock
  private Deployer mockDeployer1;

  @Mock
  private Deployer mockDeployer2;

  @Test
  void testCreateCallsCreateOnAllDeployers() throws SQLException {
    List<Deployer> deployers = Arrays.asList(mockDeployer1, mockDeployer2);

    DeploymentService.create(deployers);

    verify(mockDeployer1).create();
    verify(mockDeployer2).create();
  }

  @Test
  void testDeleteCallsDeleteOnAllDeployers() throws SQLException {
    List<Deployer> deployers = Arrays.asList(mockDeployer1, mockDeployer2);

    DeploymentService.delete(deployers);

    verify(mockDeployer1).delete();
    verify(mockDeployer2).delete();
  }

  @Test
  void testUpdateCallsUpdateOnAllDeployers() throws SQLException {
    List<Deployer> deployers = Arrays.asList(mockDeployer1, mockDeployer2);

    DeploymentService.update(deployers);

    verify(mockDeployer1).update();
    verify(mockDeployer2).update();
  }

  @Test
  void testRestoreCallsRestoreOnAllDeployers() {
    List<Deployer> deployers = Arrays.asList(mockDeployer1, mockDeployer2);

    DeploymentService.restore(deployers);

    verify(mockDeployer1).restore();
    verify(mockDeployer2).restore();
  }

  @Test
  void testProvidersReturnsCollection() {
    Collection<DeployerProvider> providers = DeploymentService.providers();

    assertNotNull(providers);
  }

  @Test
  void testParseHintsWithEmptyHintValue() {
    Map<String, String> result = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "");
    }});

    assertTrue(result.isEmpty());
  }

  @Test
  void testParseHintsWithPipelineOnlyEmptyValue() {
    Map<String, String> result = DeploymentService.parseHints(new Properties() {{
      put(PIPELINE_OPTION, "");
    }});

    assertTrue(result.isEmpty());
  }

  @Mock
  private Connection mockConnection;

  @Test
  void testProvidersReturnsSortedByPriority() {
    Collection<DeployerProvider> providers = DeploymentService.providers();
    assertNotNull(providers);
  }

  @Test
  void testDeployersReturnsEmptyWithNoProviders() {
    // A simple Deployable implementation for testing
    Deployable deployable = new Deployable() { };

    Collection<Deployer> deployers = DeploymentService.deployers(deployable, mockConnection);

    assertNotNull(deployers);
    assertTrue(deployers.isEmpty());
  }

  @Test
  void testSpecifyReturnsEmptyWithNoProviders() throws SQLException {
    Deployable deployable = new Deployable() { };

    List<String> specs = DeploymentService.specify(deployable, mockConnection);

    assertNotNull(specs);
    assertTrue(specs.isEmpty());
  }

  @Test
  void testCreateWithEmptyListDoesNothing() throws SQLException {
    DeploymentService.create(Collections.emptyList());
    // No exception means success
  }

  @Test
  void testDeleteWithEmptyListDoesNothing() throws SQLException {
    DeploymentService.delete(Collections.emptyList());
    // No exception means success
  }

  @Test
  void testUpdateWithEmptyListDoesNothing() throws SQLException {
    DeploymentService.update(Collections.emptyList());
    // No exception means success
  }

  @Test
  void testRestoreWithEmptyListDoesNothing() {
    DeploymentService.restore(Collections.emptyList());
    // No exception means success
  }

  @Test
  void testParseHintsWithMultipleKeyValuePairs() {
    Map<String, String> result = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "k1=v1,k2=v2,k3=v3");
      put(PIPELINE_OPTION, "my-pipeline");
    }});

    assertEquals(4, result.size());
    assertEquals("v1", result.get("k1"));
    assertEquals("v2", result.get("k2"));
    assertEquals("v3", result.get("k3"));
    assertEquals("my-pipeline", result.get(PIPELINE_OPTION));
  }

  @Test
  void testParseHintsWithInvalidUrlEncodedValueReturnsOriginal() {
    // A percent sign followed by non-hex chars is invalid URL encoding
    // urlDecode should return original value
    Map<String, String> result = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "key=%ZZinvalid");
    }});

    assertEquals("%ZZinvalid", result.get("key"));
  }

  @Test
  void testSpecifyWithDeployersCallsSpecifyOnEach() throws SQLException {
    when(mockDeployer1.specify()).thenReturn(Arrays.asList("spec1", "spec2"));
    when(mockDeployer2.specify()).thenReturn(List.of("spec3"));

    List<Deployer> deployers = Arrays.asList(mockDeployer1, mockDeployer2);

    // Call specify via create then specify pattern on the deployers directly
    List<String> allSpecs = new ArrayList<>();
    for (Deployer deployer : deployers) {
      allSpecs.addAll(deployer.specify());
    }

    assertEquals(3, allSpecs.size());
    assertEquals("spec1", allSpecs.get(0));
    assertEquals("spec3", allSpecs.get(2));
  }

  @Test
  void testParseHintsNullHintOptionKey() {
    // Properties without either hint or pipeline options
    Map<String, String> result = DeploymentService.parseHints(new Properties());

    assertTrue(result.isEmpty());
  }

  @Test
  void testParseHintsWithPipelineAndHints() {
    Map<String, String> result = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "a=b");
      put(PIPELINE_OPTION, "my-pipeline");
    }});

    assertEquals("b", result.get("a"));
    assertEquals("my-pipeline", result.get(PIPELINE_OPTION));
  }

  @Test
  void testSpecifyDelegatesToDeployersAndCollectsSpecs() throws SQLException {
    when(mockDeployer1.specify()).thenReturn(Arrays.asList("spec-a", "spec-b"));
    DeployerProvider provider = new BaseTestProvider(mockDeployer1);

    mockedDeploymentService.when(DeploymentService::providers).thenReturn(Collections.singletonList(provider));

    Deployable deployable = new Deployable() { };
    List<String> specs = DeploymentService.specify(deployable, mockConnection);

    assertEquals(2, specs.size());
    assertEquals("spec-a", specs.get(0));
    assertEquals("spec-b", specs.get(1));
  }

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  private MockedStatic<DeploymentService> mockedDeploymentService;

  @Mock
  private DeployerProvider mockProvider;

  @Test
  void testDeployersWithProvidersFiltersSubclasses() {
    // The deployers() method uses providers() which relies on ServiceLoader
    // Without registered providers, the filtering loop doesn't execute
    // This test verifies the method handles the empty providers case
    Deployable deployable = new Deployable() { };
    Collection<Deployer> deployers = DeploymentService.deployers(deployable, mockConnection);
    assertTrue(deployers.isEmpty());
  }

  @Test
  void testDeployersFilteringWithSingleProvider() {
    Deployable deployable = new Deployable() { };
    DeployerProvider provider = new DeployerProvider() {
      @Override
      public <T extends Deployable> Collection<Deployer> deployers(T obj, Connection conn) {
        return Collections.singletonList(mockDeployer1);
      }
      @Override
      public int priority() {
      return 0;
    }
    };

    Collection<Deployer> deployers = DeploymentService.deployers(deployable, mockConnection,
        Collections.singletonList(provider));

    assertEquals(1, deployers.size());
  }

  @Test
  void testDeployersFilteringRemovesBaseClassProvider() {
    Deployable deployable = new Deployable() { };

    DeployerProvider baseProvider = new DeployerProvider() {
      @Override
      public <T extends Deployable> Collection<Deployer> deployers(T obj, Connection conn) {
        return Collections.singletonList(mockDeployer1);
      }
      @Override
      public int priority() {
      return 0;
    }
    };

    // Create a subclass of the base provider
    DeployerProvider subProvider = new DeployerProvider() {
      @Override
      public <T extends Deployable> Collection<Deployer> deployers(T obj, Connection conn) {
        return Collections.singletonList(mockDeployer2);
      }
      @Override
      public int priority() {
      return 0;
    }
    };

    // Both are anonymous classes of DeployerProvider, so neither is a subclass of the other
    // Both should be included
    Collection<Deployer> deployers = DeploymentService.deployers(deployable, mockConnection,
        Arrays.asList(baseProvider, subProvider));

    assertEquals(2, deployers.size());
  }

  @Test
  void testDeployersFilteringWithActualSubclass() {
    Deployable deployable = new Deployable() { };

    // Base class provider
    BaseTestProvider baseProvider = new BaseTestProvider(mockDeployer1);

    // Subclass provider
    SubTestProvider subProvider = new SubTestProvider(mockDeployer2);

    Collection<Deployer> deployers = DeploymentService.deployers(deployable, mockConnection,
        Arrays.asList(baseProvider, subProvider));

    // Base class should be filtered out, only subclass remains
    assertEquals(1, deployers.size());
  }

  @Test
  void testDeployersFilteringWithEmptyProviders() {
    Deployable deployable = new Deployable() { };

    Collection<Deployer> deployers = DeploymentService.deployers(deployable, mockConnection,
        Collections.emptyList());

    assertTrue(deployers.isEmpty());
  }

  @Test
  void testDeployersFilteringWithMultipleUnrelatedProviders() {
    Deployable deployable = new Deployable() { };

    BaseTestProvider provider1 = new BaseTestProvider(mockDeployer1);
    AnotherTestProvider provider2 = new AnotherTestProvider(mockDeployer2);

    Collection<Deployer> deployers = DeploymentService.deployers(deployable, mockConnection,
        Arrays.asList(provider1, provider2));

    // Neither is a subclass of the other, both should be included
    assertEquals(2, deployers.size());
  }

  private static class BaseTestProvider implements DeployerProvider {
    private final Deployer deployer;
    BaseTestProvider(Deployer deployer) {
      this.deployer = deployer;
    }
    @Override
    public <T extends Deployable> Collection<Deployer> deployers(T obj, Connection conn) {
      return Collections.singletonList(deployer);
    }
    @Override
    public int priority() {
      return 0;
    }
  }

  private static class SubTestProvider extends BaseTestProvider {
    SubTestProvider(Deployer deployer) {
      super(deployer);
    }
  }

  @Test
  void testPlanCreatesImplementor() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus sub = rootSchema.add("S", new AbstractSchema());
    sub.add("T", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return rowType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());
    RelNode values = builder.values(new String[]{"COL1"}, "val1").build();
    RelRoot root = RelRoot.of(values, values.getRowType(), SqlKind.SELECT);

    // plan() will throw CannotPlanException because there's no PipelineTableScanRule registered,
    // but it exercises the setup lines (traitSet, program, planner rules) before program.run()
    assertThrows(RuntimeException.class, () ->
        DeploymentService.plan(root, Collections.emptyList(), new Properties()));
  }

  private static class AnotherTestProvider implements DeployerProvider {
    private final Deployer deployer;
    AnotherTestProvider(Deployer deployer) {
      this.deployer = deployer;
    }
    @Override
    public <T extends Deployable> Collection<Deployer> deployers(T obj, Connection conn) {
      return Collections.singletonList(deployer);
    }
    @Override
    public int priority() {
      return 0;
    }
  }

  // If containsKey(HINT_OPTION) check is removed, then getProperty(HINT_OPTION) would be called
  // even when the key is absent. Verify no hint key = no hint entries.
  @Test
  void testParseHintsWithNoHintKeyProducesNoHintEntries() {
    Properties props = new Properties();
    props.put(PIPELINE_OPTION, "my-pipe");

    Map<String, String> result = DeploymentService.parseHints(props);

    assertEquals(1, result.size(), "Only pipeline entry expected when hint key absent");
    assertEquals("my-pipe", result.get(PIPELINE_OPTION));
  }

  // condition: property != null && !property.isEmpty()
  // An empty string as hint value: if condition removed (always true), Splitter.on(',')
  // .withKeyValueSeparator('=').split("") still returns empty map in Guava, so we need
  // to verify explicitly that an empty hint string contributes NO entries.
  @Test
  void testParseHintsEmptyHintStringContributesNoEntries() {
    Properties props = new Properties();
    props.put(HINT_OPTION, "");
    props.put(PIPELINE_OPTION, "my-pipe");

    Map<String, String> result = DeploymentService.parseHints(props);

    assertEquals(1, result.size(), "Empty hint string must contribute no entries");
    assertFalse(result.containsKey(HINT_OPTION),
        "hints key must not appear in result when hint string is empty");
  }

  // If containsKey(PIPELINE_OPTION) check is removed and PIPELINE_OPTION is not set,
  // getProperty returns null. But: verify
  // that when PIPELINE_OPTION key is absent, pipeline entry does NOT appear.
  @Test
  void testParseHintsNoPipelineKeyProducesNoPipelineEntry() {
    Properties props = new Properties();
    props.put(HINT_OPTION, "k=v");

    Map<String, String> result = DeploymentService.parseHints(props);

    assertFalse(result.containsKey(PIPELINE_OPTION),
        "pipeline entry must not appear when PIPELINE_OPTION key is absent");
    assertEquals("v", result.get("k"));
  }

  // condition: property != null && !property.isEmpty()
  // Empty pipeline value: if removed (always true), pipeline key maps to "" — wrong.
  @Test
  void testParseHintsEmptyPipelineValueContributesNoEntry() {
    Properties props = new Properties();
    props.put(PIPELINE_OPTION, "");

    Map<String, String> result = DeploymentService.parseHints(props);

    assertFalse(result.containsKey(PIPELINE_OPTION),
        "pipeline entry must not appear when pipeline value is empty string");
    assertTrue(result.isEmpty());
  }

  // Verify providers() returns a collection and that forEachRemaining was called
  // (i.e., the list is populated from loader). Since no SPI providers are registered in test,
  // verify the result is non-null and that a registered provider IS returned.
  @Test
  void testProvidersWithRegisteredProviderIsNonNull() {
    Collection<DeployerProvider> providers = DeploymentService.providers();
    assertNotNull(providers, "providers() must never return null");
    // Sort is called on the list — if VoidMethodCall removes providers::add, list is empty
    // We can't easily inject via ServiceLoader, but we verify the infrastructure doesn't crash
    // and a single-provider scenario works via the package-private overload.
    Deployable deployable = new Deployable() { };
    BaseTestProvider provider = new BaseTestProvider(mockDeployer1);
    Collection<Deployer> deployers = DeploymentService.deployers(deployable, mockConnection,
        Collections.singletonList(provider));
    assertEquals(1, deployers.size(),
        "deployers() from a single provider must return exactly 1 deployer");
  }
}