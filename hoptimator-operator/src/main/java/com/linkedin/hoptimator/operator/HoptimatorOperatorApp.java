package com.linkedin.hoptimator.operator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.ControllerManager;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;

import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.models.V1alpha1Subscription;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionList;
import com.linkedin.hoptimator.operator.pipeline.PipelineReconciler;
import com.linkedin.hoptimator.operator.subscription.SubscriptionReconciler;
import com.linkedin.hoptimator.planner.HoptimatorPlanner;


public class HoptimatorOperatorApp {
  private static final Logger log = LoggerFactory.getLogger(HoptimatorOperatorApp.class);

  final String url;
  final String watchNamespace;
  final Predicate<V1alpha1Subscription> subscriptionFilter;
  final Properties properties;
  final Resource.Environment environment;

  /** This constructor is likely to evolve and break. */
  public HoptimatorOperatorApp(String url, String watchNamespace,
      Predicate<V1alpha1Subscription> subscriptionFilter, Properties properties) {
    this.url = url;
    this.watchNamespace = watchNamespace;
    this.subscriptionFilter = subscriptionFilter;
    this.properties = properties;
    this.environment = new Resource.SimpleEnvironment(properties);
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      throw new IllegalArgumentException("Missing JDBC URL argument.");
    }

    Options options = new Options();

    Option watchNamespace = new Option("w", "watch", true,
        "namespace to watch for resource operations, empty string indicates all namespaces");
    watchNamespace.setRequired(false);
    options.addOption(watchNamespace);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("hoptimator-operator", options);

      System.exit(1);
      return;
    }

    String urlInput = cmd.getArgs()[0];
    String watchNamespaceInput = cmd.getOptionValue("watch", "");

    new HoptimatorOperatorApp(urlInput, watchNamespaceInput,
        null, new Properties()).run();
  }

  public void run() throws Exception {
    HoptimatorPlanner.Factory plannerFactory = HoptimatorPlanner.Factory.fromJdbc(url, properties);

    // ensure JDBC connection works, and that static classes are initialized in the main thread
    HoptimatorPlanner planner = plannerFactory.makePlanner();

    Properties connectionProperties = new Properties();
    connectionProperties.putAll(properties);
    connectionProperties.put("k8s.namespace", watchNamespace);
    K8sContext context = new K8sContext(connectionProperties);

    ApiClient apiClient = context.apiClient();
    apiClient.setHttpClient(apiClient.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build());
    SharedInformerFactory informerFactory = context.informerFactory();
    Operator operator = new Operator(watchNamespace, apiClient, informerFactory, properties);
    operator.registerApi("Subscription", "subscription", "subscriptions", "hoptimator.linkedin.com", "v1alpha1",
        V1alpha1Subscription.class, V1alpha1SubscriptionList.class);

    List<Controller> controllers = new ArrayList<>();
    controllers.addAll(ControllerService.controllers(operator));
    controllers.add(SubscriptionReconciler.controller(operator, plannerFactory, environment, subscriptionFilter));

    context.registerInformer(K8sApiEndpoints.PIPELINES, Duration.ofMinutes(5), watchNamespace);
    controllers.add(PipelineReconciler.controller(context));

    ControllerManager controllerManager =
        new ControllerManager(operator.informerFactory(), controllers.toArray(new Controller[0]));

    log.info("Starting operator with {} controllers.", controllers.size());
    controllerManager.run();
  }
}
