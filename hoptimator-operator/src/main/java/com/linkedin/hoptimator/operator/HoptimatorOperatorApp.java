package com.linkedin.hoptimator.operator;

import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.ControllerManager;

import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.models.V1alpha1Subscription;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionList;
import com.linkedin.hoptimator.operator.subscription.SubscriptionReconciler;
import com.linkedin.hoptimator.planner.HoptimatorPlanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HoptimatorOperatorApp {
  private static final Logger log = LoggerFactory.getLogger(HoptimatorOperatorApp.class);

  final String modelPath;
  final String namespace;
  final ApiClient apiClient;
  final Properties properties;
  final Resource.Environment environment;

  /** This constructor is likely to evolve and break. */
  public HoptimatorOperatorApp(String modelPath, String namespace, ApiClient apiClient, Properties properties) {
    this.modelPath = modelPath;
    this.namespace = namespace;
    this.apiClient = apiClient;
    this.properties = properties;
    this.environment = new Resource.SimpleEnvironment(properties);
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      throw new IllegalArgumentException("Missing model file argument.");
    }

    Options options = new Options();

    Option namespace = new Option("n", "namespace", true, "specified namespace");
    namespace.setRequired(false);
    options.addOption(namespace);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("utility-name", options);

      System.exit(1);
      return;
    }

    String modelFileInput = cmd.getArgs()[0];
    String namespaceInput = cmd.getOptionValue("namespace", "default");

    new HoptimatorOperatorApp(modelFileInput, namespaceInput, Config.defaultClient(),
      new Properties()).run();
  }

  public void run() throws Exception {
    HoptimatorPlanner.Factory plannerFactory = HoptimatorPlanner.Factory.fromModelFile(modelPath,
        properties);

    // ensure model file works, and that static classes are initialized in the main thread
    HoptimatorPlanner planner = plannerFactory.makePlanner();

    apiClient.setHttpClient(apiClient.getHttpClient().newBuilder()
      .readTimeout(0, TimeUnit.SECONDS).build());
    SharedInformerFactory informerFactory = new SharedInformerFactory(apiClient);
    Operator operator = new Operator(namespace, apiClient, informerFactory, properties);

    operator.registerApi("Subscription", "subscription", "subscriptions", "hoptimator.linkedin.com",
      "v1alpha1", V1alpha1Subscription.class, V1alpha1SubscriptionList.class);

    List<Controller> controllers = new ArrayList<>();
    controllers.addAll(ControllerService.controllers(operator));
    controllers.add(SubscriptionReconciler.controller(operator, plannerFactory, environment));

    ControllerManager controllerManager = new ControllerManager(operator.informerFactory(),
      controllers.toArray(new Controller[0]));
  
    log.info("Starting operator with {} controllers.", controllers.size());
    controllerManager.run();  
  }
}
