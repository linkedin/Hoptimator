package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.Trigger;
import com.linkedin.hoptimator.UserJob;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;

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
}
