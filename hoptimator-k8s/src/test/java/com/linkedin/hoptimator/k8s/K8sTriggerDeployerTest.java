package com.linkedin.hoptimator.k8s;

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
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"),
        Arrays.asList("schema", "table"), "0 * * * *", options);

    K8sTriggerDeployer deployer = makeDeployer(trigger, null);

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
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"),
        Arrays.asList("schema", "table"), "0 * * * *", options);

    K8sTriggerDeployer deployer = makeDeployer(trigger, null);

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
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"),
        Arrays.asList("schema", "table"), "0 * * * *", options);

    K8sTriggerDeployer deployer = makeDeployer(trigger, null);

    deployer.update();

    assertNotNull(existing.getSpec());
    assertTrue(existing.getSpec().getPaused());
  }

  @Test
  void updateWithPausedOptionThrowsWhenTriggerNotFound() {
    Map<String, String> options = new HashMap<>();
    options.put(Trigger.PAUSED_OPTION, "true");
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"),
        Arrays.asList("schema", "table"), "0 * * * *", options);

    K8sTriggerDeployer deployer = makeDeployer(trigger, null);

    assertThrows(SQLException.class, deployer::update);
  }

  @Test
  void deleteRemovesTrigger() throws SQLException {
    V1alpha1TableTrigger existing = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("mytrigger"))
        .spec(new V1alpha1TableTriggerSpec());
    triggers.add(existing);

    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"),
        Arrays.asList("schema", "table"), "0 * * * *", Collections.emptyMap());

    K8sTriggerDeployer deployer = makeDeployer(trigger, null);

    deployer.delete();

    assertTrue(triggers.isEmpty());
  }

  @Test
  void deleteThrowsWhenTriggerNotFound() {
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"),
        Arrays.asList("schema", "table"), "0 * * * *", Collections.emptyMap());

    K8sTriggerDeployer deployer = makeDeployer(trigger, null);

    assertThrows(SQLException.class, deployer::delete);
  }

  @Test
  void toK8sObjectBuildsCorrectTrigger() throws SQLException {
    V1alpha1JobTemplate jobTemplate = new V1alpha1JobTemplate()
        .metadata(new V1ObjectMeta().name("myjob").namespace("test-ns"))
        .spec(new V1alpha1JobTemplateSpec().yaml("apiVersion: batch/v1\nkind: Job\n"
            + "metadata:\n  name: {{name}}"));
    jobTemplates.add(jobTemplate);

    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"),
        Arrays.asList("SCHEMA", "TABLE"), "0 * * * *", Collections.emptyMap());

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
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"),
        Arrays.asList("SCHEMA", "TABLE"), "0 * * * *", Collections.emptyMap());

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
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"),
        Arrays.asList("SCHEMA", "TABLE"), "0 * * * *", options);

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
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"),
        Arrays.asList("SCHEMA", "MY_TABLE"), "5 4 * * *", options);

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
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"),
        Arrays.asList("SCHEMA", "TABLE"), "0 * * * *", options);

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
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob(null, "myjob"),
        Arrays.asList("schema", "table"), "0 * * * *", options);

    K8sTriggerDeployer deployer = makeDeployer(trigger, null);
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
    Trigger trigger = new Trigger("MY_TRIGGER", new UserJob("test-ns", "MY_JOB"),
        Arrays.asList("SCHEMA", "TABLE"), "0 * * * *", Collections.emptyMap());

    K8sTriggerDeployer deployer = makeDeployer(trigger, mockContext);
    // super.update() calls api.update() via K8sDeployer; FakeK8sApi.update adds to list
    deployer.update();

    assertEquals(1, triggers.size(), "update() must have added the object via api.update()");
  }

  @Test
  void deleteOnNonExistingTriggerThrowsSqlException() {
    // Graceful handling when trigger not found
    Trigger trigger = new Trigger("NONEXISTENT", new UserJob(null, "myjob"),
        Arrays.asList("schema", "table"), "0 * * * *", Collections.emptyMap());

    K8sTriggerDeployer deployer = makeDeployer(trigger, null);

    // FakeK8sApi.get() throws SQLException(404) for unknown names
    assertThrows(SQLException.class, deployer::delete,
        "delete() on a non-existing trigger must throw");
  }
}
