package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Trigger;
import com.linkedin.hoptimator.UserJob;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class K8sTriggerDeployerTest {

  @Mock
  private K8sContext mockContext;

  private List<V1alpha1TableTrigger> triggers;
  private FakeK8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi;
  private List<V1alpha1JobTemplate> jobTemplates;
  private FakeK8sApi<V1alpha1JobTemplate, V1alpha1JobTemplateList> jobTemplateApi;
  private K8sSnapshot snapshot;

  @BeforeEach
  void setUp() {
    triggers = new ArrayList<>();
    triggerApi = new FakeK8sApi<>(triggers);
    jobTemplates = new ArrayList<>();
    jobTemplateApi = new FakeK8sApi<>(jobTemplates);
    Map<String, String> yamls = new HashMap<>();
    FakeK8sYamlApi fakeYamlApi = new FakeK8sYamlApi(yamls);
    snapshot = new K8sSnapshot(null) {
      @Override
      K8sYamlApi createYamlApi(K8sContext context) {
        return fakeYamlApi;
      }
    };
  }

  private K8sTriggerDeployer makeDeployer(Trigger trigger, K8sContext context) {
    FakeK8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> capturedTriggerApi = triggerApi;
    FakeK8sApi<V1alpha1JobTemplate, V1alpha1JobTemplateList> capturedJobTemplateApi = jobTemplateApi;
    K8sSnapshot capturedSnapshot = snapshot;
    return new K8sTriggerDeployer(trigger, context) {
      @Override
      K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> createApi(K8sContext ctx,
          K8sApiEndpoint<V1alpha1TableTrigger, V1alpha1TableTriggerList> endpoint) {
        return capturedTriggerApi;
      }

      @Override
      K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> createTriggerApi(K8sContext ctx) {
        return capturedTriggerApi;
      }

      @Override
      K8sApi<V1alpha1JobTemplate, V1alpha1JobTemplateList> createJobTemplateApi(K8sContext ctx) {
        return capturedJobTemplateApi;
      }

      @Override
      K8sSnapshot createSnapshot(K8sContext ctx) {
        return capturedSnapshot;
      }
    };
  }

  // K8sUtils.canonicalizeName("MY_TRIGGER") => "mytrigger"
  // K8sUtils.canonicalizeName("MY_JOB") => "myjob"

  @Test
  void updateWithPausedOptionPausesTrigger() throws SQLException {
    V1alpha1TableTrigger existing = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("mytrigger"))
        .spec(new V1alpha1TableTriggerSpec().paused(false));
    triggers.add(existing);

    Map<String, String> options = new HashMap<>();
    options.put(Trigger.PAUSED_OPTION, "true");
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"), "0 * * * *", options, new Source(null, Arrays.asList("schema", "table"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);

    deployer.update();

    assertTrue(existing.getSpec().getPaused());
  }

  @Test
  void updateWithPausedOptionUnpausesTrigger() throws SQLException {
    V1alpha1TableTrigger existing = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("mytrigger"))
        .spec(new V1alpha1TableTriggerSpec().paused(true));
    triggers.add(existing);

    Map<String, String> options = new HashMap<>();
    options.put(Trigger.PAUSED_OPTION, "false");
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"), "0 * * * *", options, new Source(null, Arrays.asList("schema", "table"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);

    deployer.update();

    assertFalse(existing.getSpec().getPaused());
  }

  @Test
  void updateWithPausedOptionCreatesSpecIfNull() throws SQLException {
    V1alpha1TableTrigger existing = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("mytrigger"))
        .spec(null);
    triggers.add(existing);

    Map<String, String> options = new HashMap<>();
    options.put(Trigger.PAUSED_OPTION, "true");
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"), "0 * * * *", options, new Source(null, Arrays.asList("schema", "table"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);

    deployer.update();

    assertNotNull(existing.getSpec());
    assertTrue(existing.getSpec().getPaused());
  }

  @Test
  void updateWithPausedOptionAlsoStampsDependsOnLabelsWhenDatabaseSet() throws SQLException {
    // The partial-update path (paused-only) used to skip toK8sObject and never refresh the
    // depends-on metadata. Pin down that re-applying the LogicalTable through this path now
    // stamps the labels/annotation so visualization (and the dep-guard reverse lookup) works.
    V1alpha1TableTrigger existing = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("mytrigger"))
        .spec(new V1alpha1TableTriggerSpec().paused(true));
    triggers.add(existing);

    Map<String, String> options = new HashMap<>();
    options.put(Trigger.PAUSED_OPTION, "true");
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"), null, options, new Source("mysql-db", Arrays.asList("MYSQL", "testdb", "events"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);

    deployer.update();

    String expectedLabel = PipelineDependencyLabels.labelKey(
        "mysql-db", Arrays.asList("MYSQL", "testdb", "events"));
    String expectedIdentifier = PipelineDependencyLabels.identifier(
        "mysql-db", Arrays.asList("MYSQL", "testdb", "events"));
    assertNotNull(existing.getMetadata().getLabels(), "labels must be set after partial update");
    assertTrue(existing.getMetadata().getLabels().containsKey(expectedLabel),
        "depends-on label must be stamped on the partial-update path: " + existing.getMetadata().getLabels());
    assertNotNull(existing.getMetadata().getAnnotations(), "annotations must be set");
    assertEquals(expectedIdentifier,
        existing.getMetadata().getAnnotations().get(PipelineDependencyLabels.ANNOTATION_KEY_SOURCES));
  }

  @Test
  void updateStampsSinkLabelWhenTriggerCarriesASink() throws SQLException {
    // Bridging-tier triggers (LogicalTableDeployer's offline→online flow) carry a Sink in
    // addition to their source. Pin that the partial-update path stamps both source and sink
    // depends-on labels so the dep-guard reverse lookup finds the trigger from either end.
    V1alpha1TableTrigger existing = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("mytrigger"))
        .spec(new V1alpha1TableTriggerSpec().paused(true));
    triggers.add(existing);

    Map<String, String> options = new HashMap<>();
    options.put(Trigger.PAUSED_OPTION, "true");
    Source source = new Source("hdfs-db", Arrays.asList("HDFS", "events"), Collections.emptyMap());
    Sink sink = new Sink("venice-db", Arrays.asList("VENICE", "events"), Collections.emptyMap());
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"), null, options, source, sink);

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    deployer.update();

    String sourceLabel = PipelineDependencyLabels.labelKey(
        "hdfs-db", Arrays.asList("HDFS", "events"));
    String sinkLabel = PipelineDependencyLabels.labelKey(
        "venice-db", Arrays.asList("VENICE", "events"));
    String sinkIdentifier = PipelineDependencyLabels.identifier(
        "venice-db", Arrays.asList("VENICE", "events"));

    assertTrue(existing.getMetadata().getLabels().containsKey(sourceLabel),
        "source-side depends-on label must be stamped: " + existing.getMetadata().getLabels());
    assertTrue(existing.getMetadata().getLabels().containsKey(sinkLabel),
        "sink-side depends-on label must be stamped: " + existing.getMetadata().getLabels());
    assertEquals(sinkIdentifier,
        existing.getMetadata().getAnnotations().get(PipelineDependencyLabels.ANNOTATION_KEY_SINK),
        "depends-on-sink annotation must record the sink identifier verbatim");
  }

  @Test
  void updateWithPausedOptionThrowsWhenTriggerNotFound() {
    Map<String, String> options = new HashMap<>();
    options.put(Trigger.PAUSED_OPTION, "true");
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"), "0 * * * *", options, new Source(null, Arrays.asList("schema", "table"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);

    assertThrows(SQLException.class, deployer::update);
  }

  @Test
  void deleteRemovesTrigger() throws SQLException {
    V1alpha1TableTrigger existing = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("mytrigger"))
        .spec(new V1alpha1TableTriggerSpec());
    triggers.add(existing);

    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"), "0 * * * *", Collections.emptyMap(), new Source(null, Arrays.asList("schema", "table"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);

    deployer.delete();

    assertTrue(triggers.isEmpty());
  }

  @Test
  void deleteThrowsWhenTriggerNotFound() {
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"), "0 * * * *", Collections.emptyMap(), new Source(null, Arrays.asList("schema", "table"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);

    assertThrows(SQLException.class, deployer::delete);
  }

  @Test
  void toK8sObjectBuildsCorrectTrigger() throws SQLException {
    V1alpha1JobTemplate jobTemplate = new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("myjob").namespace("test-ns"))
        .spec(new V1alpha1JobTemplateSpec().yaml("apiVersion: batch/v1\nkind: Job\n"
            + "metadata:\n  name: {{name}}"));
    jobTemplates.add(jobTemplate);

    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"), "0 * * * *", Collections.emptyMap(), new Source(null, Arrays.asList("SCHEMA", "TABLE"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);

    List<String> specs = deployer.specify();

    assertNotNull(specs);
    assertEquals(1, specs.size());
  }

  @Test
  void toK8sObjectWithNoJobPropertiesHasNullJobProperties() throws SQLException {
    V1alpha1JobTemplate jobTemplate = new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("myjob").namespace("test-ns"))
        .spec(new V1alpha1JobTemplateSpec().yaml("template: {{name}}"));
    jobTemplates.add(jobTemplate);

    // No job.properties.* options — spec should NOT have jobProperties set
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"), "0 * * * *", Collections.emptyMap(), new Source(null, Arrays.asList("SCHEMA", "TABLE"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    List<String> specs = deployer.specify();

    assertNotNull(specs);
    // The YAML for the trigger should not have jobProperties section since it was not set
    assertFalse(specs.get(0).contains("jobProperties"));
  }

  @Test
  void toK8sObjectWithJobPropertiesIncludesThemInSpec() throws SQLException {
    V1alpha1JobTemplate jobTemplate = new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("myjob").namespace("test-ns"))
        .spec(new V1alpha1JobTemplateSpec().yaml("template: {{name}}"));
    jobTemplates.add(jobTemplate);

    Map<String, String> options = new HashMap<>();
    options.put("job.properties.parallelism", "4");
    options.put("job.properties.restart-strategy", "never");
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"), "0 * * * *", options, new Source(null, Arrays.asList("SCHEMA", "TABLE"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    List<String> specs = deployer.specify();

    assertNotNull(specs);
    assertFalse(specs.isEmpty());
    // The YAML should include jobProperties since we set job.properties.* options
    assertTrue(specs.get(0).contains("jobProperties"));
    assertTrue(specs.get(0).contains("parallelism"));
    assertTrue(specs.get(0).contains("restart-strategy"));
  }

  @Test
  void toK8sObjectOptionsPutAllIncludedInEnvironment() throws SQLException {
    // Verify options ARE in template rendering
    V1alpha1JobTemplate jobTemplate = new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("myjob").namespace("test-ns"))
        .spec(new V1alpha1JobTemplateSpec().yaml("table: {{table}}\nschedule: {{schedule}}"));
    jobTemplates.add(jobTemplate);

    Map<String, String> options = new HashMap<>();
    options.put("someKey", "someValue");
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"), "5 4 * * *", options, new Source(null, Arrays.asList("SCHEMA", "MY_TABLE"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    List<String> specs = deployer.specify();

    assertFalse(specs.isEmpty());
    // schedule and table ARE rendered — proves the environment was set up (putAll had effect)
    assertTrue(specs.get(0).contains("5 4 * * *"), "schedule must appear in rendered YAML");
    assertTrue(specs.get(0).contains("MY_TABLE"), "table must appear in rendered YAML");
  }

  @Test
  void toK8sObjectForEachAppliesJobPropertiesFilter() throws SQLException {
    // Verify ALL matching hints ARE applied
    V1alpha1JobTemplate jobTemplate = new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("myjob").namespace("test-ns"))
        .spec(new V1alpha1JobTemplateSpec().yaml("template: {{name}}"));
    jobTemplates.add(jobTemplate);

    Map<String, String> options = new HashMap<>();
    options.put("job.properties.key1", "val1");
    options.put("job.properties.key2", "val2");
    options.put("other.option", "ignored");
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"), "0 * * * *", options, new Source(null, Arrays.asList("SCHEMA", "TABLE"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    List<String> specs = deployer.specify();

    assertFalse(specs.isEmpty());
    String yaml = specs.get(0);
    assertTrue(yaml.contains("key1"), "job property key1 must be in spec");
    assertTrue(yaml.contains("key2"), "job property key2 must be in spec");
    assertFalse(yaml.contains("other.option"), "non-job.properties option must not appear as job property");
  }

  @Test
  void updateWithPausedOptionCallsApiUpdate() throws SQLException {
    // Verify update IS called
    V1alpha1TableTrigger existing = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("mytrigger"))
        .spec(new V1alpha1TableTriggerSpec().paused(false));
    triggers.add(existing);

    Map<String, String> options = new HashMap<>();
    options.put(Trigger.PAUSED_OPTION, "true");
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"), "0 * * * *", options, new Source(null, Arrays.asList("schema", "table"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    deployer.update();

    // FakeK8sApi.update() removes and re-adds. Verify the object is still present.
    assertEquals(1, triggers.size());
    assertTrue(triggers.get(0).getSpec().getPaused(),
        "api.update() must have been called — paused state must be persisted");
  }

  @Test
  void updateWithChangedSpecCallsSuperUpdate() throws SQLException {
    // spec HAS changed (no PAUSED_OPTION) → super.update() is triggered
    V1alpha1JobTemplate jobTemplate = new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("myjob").namespace("test-ns"))
        .spec(new V1alpha1JobTemplateSpec().yaml("template: {{name}}"));
    jobTemplates.add(jobTemplate);

    // No PAUSED_OPTION → should fall through to super.update()
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"), "0 * * * *", Collections.emptyMap(), new Source(null, Arrays.asList("SCHEMA", "TABLE"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    // super.update() calls api.update() via K8sDeployer; FakeK8sApi.update adds to list
    deployer.update();

    assertEquals(1, triggers.size(), "update() must have added the object via api.update()");
  }

  @Test
  void deleteOnNonExistingTriggerThrowsSqlException() {
    // Graceful handling when trigger not found
    Trigger trigger = new Trigger("NONEXISTENT", new UserJob(null, "myjob"), "0 * * * *", Collections.emptyMap(), new Source(null, Arrays.asList("schema", "table"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);

    // FakeK8sApi.get() throws SQLException(404) for unknown names
    assertThrows(SQLException.class, deployer::delete,
        "delete() on a non-existing trigger must throw");
  }

  // ───────── update() paused-state-preservation tests ─────────

  @Test
  void updatePreservesPausedWhenOptionsHaveNoPausedOption() throws SQLException {
    // Scenario: CREATE OR REPLACE TABLE with no explicit pause/resume — an existing paused
    // CRD must remain paused even though the caller passes no PAUSED_OPTION.
    V1alpha1TableTrigger existing = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("mytrigger"))
        .spec(new V1alpha1TableTriggerSpec().paused(true));
    triggers.add(existing);

    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "myjob"), "0 * * * *", Collections.emptyMap(), new Source(null, Arrays.asList("schema", "table"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    deployer.update();

    assertTrue(triggers.get(0).getSpec().getPaused(),
        "Existing paused=true must be preserved when options omit PAUSED_OPTION");
  }

  @Test
  void updatePreservesUnpausedWhenOptionsHaveNoPausedOption() throws SQLException {
    // Scenario: existing CRD is unpaused and the caller passes no PAUSED_OPTION — stays unpaused.
    V1alpha1TableTrigger existing = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("mytrigger"))
        .spec(new V1alpha1TableTriggerSpec().paused(false));
    triggers.add(existing);

    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "myjob"), "0 * * * *", Collections.emptyMap(), new Source(null, Arrays.asList("schema", "table"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    deployer.update();

    assertFalse(triggers.get(0).getSpec().getPaused(),
        "Existing paused=false must be preserved (short-circuit path sets false)");
  }

  @Test
  void updateWithPausedOptionFalseUnpausesAlreadyPausedTrigger() throws SQLException {
    // Scenario: RESUME TRIGGER DDL — PAUSED_OPTION="false" must unpause the existing trigger
    // even though it was paused. Regression check for a bug where currentlyPaused forced true.
    V1alpha1TableTrigger existing = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("mytrigger"))
        .spec(new V1alpha1TableTriggerSpec().paused(true));
    triggers.add(existing);

    Map<String, String> options = new HashMap<>();
    options.put(Trigger.PAUSED_OPTION, "false");
    Trigger trigger = new Trigger("MY_TRIGGER", null, null, options, new Source(null, new ArrayList<>(), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    deployer.update();

    assertFalse(triggers.get(0).getSpec().getPaused(),
        "Explicit PAUSED_OPTION=false must override a currently-paused CRD");
  }

  @Test
  void updateFallsThroughToSuperUpdateWhenNoExistingAndNoPausedOption() throws SQLException {
    // Scenario: first-time CREATE OR REPLACE TABLE — no existing trigger, no PAUSED_OPTION.
    // Must go through super.update() so api.update() upserts (creates) instead of throwing
    // "Trigger not found".
    V1alpha1JobTemplate jobTemplate = new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("myjob").namespace("test-ns"))
        .spec(new V1alpha1JobTemplateSpec().yaml("template: {{name}}"));
    jobTemplates.add(jobTemplate);

    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"), "0 * * * *", Collections.emptyMap(), new Source(null, Arrays.asList("SCHEMA", "TABLE"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    deployer.update();

    assertEquals(1, triggers.size(),
        "super.update() must upsert the trigger when none exists");
  }

  @Test
  void updateFallsThroughToSuperUpdateWhenExistingHasNullPaused() throws SQLException {
    // Scenario: existing CRD has spec.paused=null — neither the explicit-option nor the
    // preserve-existing path fires, so super.update() re-renders (which calls toK8sObject,
    // which needs a JobTemplate).
    V1alpha1TableTrigger existing = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("mytrigger"))
        .spec(new V1alpha1TableTriggerSpec()); // paused not set → null
    triggers.add(existing);

    V1alpha1JobTemplate jobTemplate = new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("myjob").namespace("test-ns"))
        .spec(new V1alpha1JobTemplateSpec().yaml("template: {{name}}"));
    jobTemplates.add(jobTemplate);

    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"), "0 * * * *", Collections.emptyMap(), new Source(null, Arrays.asList("SCHEMA", "TABLE"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    deployer.update();

    // If super.update() ran, toK8sObject() populated spec.table on the re-rendered CRD.
    // The short-circuit would instead leave spec.table untouched (null on the existing).
    assertTrue(triggers.stream().anyMatch(t -> "TABLE".equals(t.getSpec().getTable())),
        "super.update() must have run toK8sObject() — expected a CRD with spec.table='TABLE'");
  }

  // ───────── toK8sObject() paused rendering test ─────────

  @Test
  void toK8sObjectSetsSpecPausedWhenPausedOptionTrue() throws SQLException {
    V1alpha1JobTemplate jobTemplate = new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("myjob").namespace("test-ns"))
        .spec(new V1alpha1JobTemplateSpec().yaml("template: {{name}}"));
    jobTemplates.add(jobTemplate);

    Map<String, String> options = new HashMap<>();
    options.put(Trigger.PAUSED_OPTION, "true");
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"), "0 * * * *", options, new Source(null, Arrays.asList("SCHEMA", "TABLE"), Collections.emptyMap()));

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    List<String> specs = deployer.specify();

    assertFalse(specs.isEmpty());
    assertTrue(specs.get(0).contains("paused: true"),
        "spec.paused=true must be present in rendered YAML when PAUSED_OPTION=true — got: " + specs.get(0));
  }
}
