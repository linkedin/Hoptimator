package com.linkedin.hoptimator.operator;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.operator.pipeline.PipelineReconciler;
import com.linkedin.hoptimator.operator.trigger.TableTriggerReconciler;
import com.linkedin.hoptimator.operator.trigger.ViewReconciler;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.ControllerManager;
import io.kubernetes.client.informer.SharedInformerFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


/**
 * Hosts the pipeline operator's {@link ControllerManager}. Provides a non-blocking
 * {@link #start(List)} / {@link #stop} pair so callers driven by an external lifecycle
 * (Spring, Guava, etc.) are not blocked by {@code ControllerManager.run()}'s
 * indefinite {@code CountDownLatch.await()}.
 *
 * <p>Failure handling: the caller supplies a {@link Consumer} that's invoked once if any
 * controller exits unexpectedly or the runner thread dies with an uncaught exception.
 * The handler should propagate the failure however the caller wants but a controller will
 * not auto recover. The handler fires at most once per service instance and is suppressed
 * during an intentional {@link #stop} sequence.
 */
public class PipelineOperatorApp {
  private static final Logger log = LoggerFactory.getLogger(PipelineOperatorApp.class);

  private final K8sContext context;
  private final Consumer<Throwable> failureHandler;
  private final AtomicBoolean stopRequested = new AtomicBoolean(false);
  private final AtomicBoolean failureReported = new AtomicBoolean(false);
  private ControllerManager controllerManager;
  private Thread runnerThread;
  private boolean shutdownHookInstalled;

  /** Constructor for callers that don't care about runtime failures (e.g. {@link #main(String[])}). */
  public PipelineOperatorApp(K8sContext context) {
    this(context, t -> { /* no-op */ });
  }

  /**
   * @param failureHandler invoked once if a controller exits unexpectedly or the runner
   *     thread dies with an uncaught exception. The throwable argument is the captured cause,
   *     or {@code null} if the controller's {@code run()} returned cleanly without
   *     a stop signal (no exception to capture).
   */
  public PipelineOperatorApp(K8sContext context, Consumer<Throwable> failureHandler) {
    this.context = context;
    this.failureHandler = Objects.requireNonNull(failureHandler);
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
    Properties connectionProperties = new Properties();
    connectionProperties.put("k8s.watch.namespace", watchNamespaceInput);
    K8sContext context = K8sContext.create(new HoptimatorConnection(null, connectionProperties));
    PipelineOperatorApp app = new PipelineOperatorApp(context);
    app.installShutdownHook(Duration.ofMinutes(1));
    app.start(Collections.emptyList());
    app.awaitTermination();
  }

  /**
   * Registers informers and controllers, then starts the {@link ControllerManager} on a
   * daemon thread named {@code hoptimator-controller-manager}. Returns promptly.
   *
   * <p>If a controller's {@code run()} method exits unexpectedly (without a corresponding
   * {@link #stop} call), the configured {@link Consumer} failure handler is invoked once.
   * Subsequent failures (e.g. other controllers also exiting) are dropped.
   */
  public synchronized void start(List<Controller> initialControllers) {
    if (controllerManager != null) {
      log.info("PipelineOperatorApp.start() ignored — already started");
      return;
    }

    context.registerInformer(K8sApiEndpoints.PIPELINES, Duration.ofMinutes(5));
    context.registerInformer(K8sApiEndpoints.TABLE_TRIGGERS, Duration.ofMinutes(5));
    context.registerInformer(K8sApiEndpoints.VIEWS, Duration.ofMinutes(5));

    List<Controller> rawControllers = buildControllers(initialControllers);
    if (rawControllers.isEmpty()) {
      log.warn("PipelineOperatorApp.start() called with no controllers; staying idle.");
      return;
    }

    controllerManager = newControllerManager(
        context.informerFactory(), rawControllers.stream()
            .map(this::wrapForFailFast).toArray(Controller[]::new));

    log.info("Starting operator with {} controllers.", rawControllers.size());
    runnerThread = new Thread(controllerManager::run, "hoptimator-controller-manager");
    runnerThread.setDaemon(true);
    runnerThread.setUncaughtExceptionHandler((t, ex) -> {
      log.error("Controller-manager runner thread died unexpectedly", ex);
      reportFailure(ex);
    });
    runnerThread.start();
  }

  /**
   * Builds the full controller list by appending the standard reconcilers to the caller-provided
   * initial set. Extracted so tests can substitute a stub list without invoking the real
   * reconciler factories.
   */
  @VisibleForTesting
  List<Controller> buildControllers(List<Controller> initialControllers) {
    List<Controller> controllers = new ArrayList<>(initialControllers);
    controllers.addAll(ControllerService.controllers(context));
    controllers.add(PipelineReconciler.controller(context));
    controllers.add(TableTriggerReconciler.controller(context));
    controllers.add(ViewReconciler.controller(context));
    return controllers;
  }

  /**
   * Constructs the {@link ControllerManager}. Extracted so tests can substitute a mock without
   * invoking the real upstream constructor.
   */
  @VisibleForTesting
  ControllerManager newControllerManager(SharedInformerFactory factory, Controller[] controllers) {
    return new ControllerManager(factory, controllers);
  }

  /**
   * Wraps a {@link Controller} so an unexpected exit from its {@code run()} method
   * (without {@link #stop} having been called) invokes the configured failure handler.
   * Captures the thrown cause if the exit was via exception, otherwise reports {@code null}.
   */
  private Controller wrapForFailFast(Controller inner) {
    String name = inner.getClass().getName();
    return new Controller() {
      @Override
      public void run() {
        Throwable cause = null;
        try {
          inner.run();
        } catch (Throwable t) {
          cause = t;
          throw t;
        } finally {
          if (!stopRequested.get()) {
            if (cause != null) {
              log.error("Controller {} exited unexpectedly with throwable", name, cause);
            } else {
              log.error("Controller {} exited unexpectedly", name);
            }
            reportFailure(cause);
          }
        }
      }

      @Override
      public void shutdown() {
        inner.shutdown();
      }
    };
  }

  /**
   * Invokes the failure handler at most once. Suppressed during intentional stop. Catches any
   * throwable from the handler itself so a misbehaving handler can't corrupt our shutdown path.
   */
  private void reportFailure(Throwable cause) {
    if (stopRequested.get()) {
      return;
    }
    if (failureReported.compareAndSet(false, true)) {
      try {
        failureHandler.accept(cause);
      } catch (Throwable t) {
        log.error("Failure handler itself threw", t);
      }
    }
  }

  /**
   * Signals the {@link ControllerManager} to stop and waits up to {@code timeout} for the
   * runner thread to exit. No-op if {@link #start(List)} was never called.
   */
  public synchronized void stop(Duration timeout) {
    if (controllerManager == null) {
      // Either start() was never called, or start() short-circuited because controllers list
      // was empty. Either way nothing to stop.
      return;
    }
    // Mark intentional-stop FIRST so wrappers don't fire failureHandler on their way out.
    stopRequested.set(true);
    log.info("Stopping operator.");
    controllerManager.shutdown();
    if (runnerThread != null) {
      try {
        runnerThread.join(timeout.toMillis());
        if (runnerThread.isAlive()) {
          log.warn("Controller-manager runner thread did not terminate within {}", timeout);
          runnerThread.interrupt();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Registers a JVM shutdown hook that calls {@link #stop(Duration)} with the given timeout.
   * Idempotent — a second call is a no-op. Useful when running from {@link #main(String[])} or
   * any other entry point that doesn't have an external lifecycle manager driving shutdown.
   */
  public synchronized void installShutdownHook(Duration gracefulShutdownTimeout) {
    if (shutdownHookInstalled) {
      return;
    }
    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> stop(gracefulShutdownTimeout), "hoptimator-shutdown-hook"));
    shutdownHookInstalled = true;
  }

  /**
   * Blocks until the controller-manager runner thread exits, which happens on
   * {@link #stop} or when all controllers terminate naturally. Returns immediately if
   * {@link #start(List)} was called with an empty controller list (idle mode) — there's no
   * runner to wait on and {@link #main(String[])} should be allowed to exit cleanly.
   */
  public void awaitTermination() throws InterruptedException {
    Thread thread;
    synchronized (this) {
      thread = runnerThread;
    }
    if (thread == null) {
      // Either never started or idle mode (no controllers). Either way, nothing to await.
      // Idle mode lets main() exit cleanly; calling awaitTermination without start is a
      // caller bug, but throwing here would punish the expected idle-mode path.
      return;
    }
    thread.join();
  }
}
