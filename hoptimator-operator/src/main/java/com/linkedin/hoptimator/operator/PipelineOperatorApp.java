package com.linkedin.hoptimator.operator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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

import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.operator.pipeline.PipelineReconciler;


public class PipelineOperatorApp {
  private static final Logger log = LoggerFactory.getLogger(PipelineOperatorApp.class);

  final String watchNamespace;

  public PipelineOperatorApp(String watchNamespace) {
    this.watchNamespace = watchNamespace;
  }

  public static void main(String[] args) throws Exception {
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
      formatter.printHelp("pipeline-operator", options);

      System.exit(1);
      return;
    }

    String watchNamespaceInput = cmd.getOptionValue("watch", "");

    new PipelineOperatorApp(watchNamespaceInput).run();
  }

  public void run() throws Exception {
    K8sContext context = K8sContext.currentContext();

    // register informers
    context.registerInformer(K8sApiEndpoints.PIPELINES, Duration.ofMinutes(5), watchNamespace);

    List<Controller> controllers = new ArrayList<>();
    // TODO: add additional controllers from ControllerProvider SPI
    controllers.add(PipelineReconciler.controller(context));

    ControllerManager controllerManager =
        new ControllerManager(context.informerFactory(), controllers.toArray(new Controller[0]));

    log.info("Starting operator with {} controllers.", controllers.size());
    controllerManager.run();
  }
}
