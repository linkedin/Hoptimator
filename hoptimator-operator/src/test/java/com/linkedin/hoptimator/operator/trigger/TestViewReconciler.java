package com.linkedin.hoptimator.operator.trigger;

import io.kubernetes.client.extended.controller.reconciler.Result;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.k8s.FakeK8sApi;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewList;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewStatus;


class TestViewReconciler {

  private List<V1alpha1TableTrigger> triggers = new ArrayList<>();
  private List<V1alpha1View> views = new ArrayList<>();
  private final ViewReconciler reconciler = new ViewReconciler(
      new FakeK8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList>(triggers),
      new FakeK8sApi<V1alpha1View, V1alpha1ViewList>(views));

  @BeforeEach
  void beforeEach() {
    triggers.clear();
    views.clear();
  }

  @Test
  void firesDownstreamTrigger() {
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("view-1"))
        .status(new V1alpha1ViewStatus().watermark(OffsetDateTime.now()));
    Map<String, String> labels = new HashMap<>();
    labels.put("view", "view-1");
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("table-trigger").labels(labels));
    triggers.add(trigger);
    views.add(view);
    Result result =  reconciler.reconcile(new Request("namespace", "view-1"));
    Assertions.assertFalse(result.isRequeue());
    Assertions.assertNotNull(trigger.getStatus(), "Trigger was not fired");
    Assertions.assertNotNull(trigger.getStatus().getTimestamp(), "Trigger was not fired");
    Assertions.assertEquals(trigger.getStatus().getTimestamp(),
        view.getStatus().getWatermark(), "Trigger timestamp doesn't match view watermark");
  }
}
