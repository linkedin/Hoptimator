package com.linkedin.hoptimator.operator.kafka;

import com.linkedin.hoptimator.operator.Operator;
import com.linkedin.hoptimator.models.V1alpha1KafkaConnector;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.Response;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaConnectorReconciler implements Reconciler {
  private final static Logger log = LoggerFactory.getLogger(KafkaConnectorReconciler.class);
  private final static String KAFKACONNECTOR = "hoptimator.linkedin.com/v1alpha1/KafkaConnector";
  private final static MediaType JSON = MediaType.get("application/json; charset=utf-8");
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final OkHttpClient httpClient = new OkHttpClient.Builder().build();

  private final Operator operator;

  public KafkaConnectorReconciler(Operator operator) {
    this.operator = operator;
  }

  private int putConfig(String url, Map<String, String> config) throws IOException {
    String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(config);
    RequestBody body = RequestBody.create(json, JSON);
    okhttp3.Request request = new okhttp3.Request.Builder().url(url).put(body).build();
    try (Response response = httpClient.newCall(request).execute()) {
      log.info("Response: {}.", response.body().string());
      return response.code();
    }
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();

    try {
      V1alpha1KafkaConnector object = operator.<V1alpha1KafkaConnector>fetch(KAFKACONNECTOR, namespace, name);

      if (object == null) {
        log.info("Object {}/{} deleted. Skipping.", namespace, name);
        return new Result(false);
      }

      String connectorName = object.getSpec().getConnectorName();
      String baseUrl = object.getSpec().getClusterBaseUrl();
      String url = baseUrl + "/connectors/" + connectorName + "/config";
      Map<String, String> connectorConfig = object.getSpec().getConnectorConfig();

      int status = putConfig(url, connectorConfig);
      switch(status) {
      case 201:
        log.info("Created new connector {} with config {}.", connectorName, connectorConfig);
        break;
      case 200:
        log.info("Updated existing connector {} with config {}.", connectorName, connectorConfig);
        break;
      default:
        log.error("{} PUT {}.", status, url);
        return new Result(true, operator.failureRetryDuration());
      }
    } catch (Exception e) {
      log.error("Encountered exception while reconciling KafkaConnector {}/{}", namespace, name, e);
      return new Result(true, operator.failureRetryDuration());
    }
    log.info("Done reconciling {}/{}", namespace, name);
    return new Result(false);
  }

  public static Controller controller(Operator operator) {
    Reconciler reconciler = new KafkaConnectorReconciler(operator);
    return ControllerBuilder.defaultBuilder(operator.informerFactory())
      .withReconciler(reconciler)
      .withName("kafka-connector-controller")
      .withWorkerCount(1)
      //.withReadyFunc(resourceInformer::hasSynced) // optional, only starts controller when the
      // cache has synced up
      //.withWorkQueue(resourceWorkQueue)
      //.watch()
      .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1KafkaConnector.class, x).build())
      .build();
  }
}

