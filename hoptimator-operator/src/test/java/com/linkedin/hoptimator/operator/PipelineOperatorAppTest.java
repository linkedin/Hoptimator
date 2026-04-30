package com.linkedin.hoptimator.operator;

import com.linkedin.hoptimator.k8s.K8sContext;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.ControllerManager;
import io.kubernetes.client.informer.SharedInformerFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


/**
 * Tests for {@link PipelineOperatorApp}.
 *
 * <p>Uses {@link TestablePipelineOperatorApp} which substitutes the {@link ControllerManager}
 * construction and the controller list with mocks, letting us drive {@code start()} →
 * {@code stop()} → {@code awaitTermination()} transitions without needing a real Kubernetes
 * API server.
 *
 * <p>Failure-mode tests use an "interactive" fake {@link ControllerManager} that actually
 * invokes each wrapped controller's {@code run()} method, exercising the fail-fast wrapper
 * and verifying the failure handler is invoked exactly once with the right cause.
 */
@ExtendWith(MockitoExtension.class)
class PipelineOperatorAppTest {

  @Mock
  private K8sContext context;

  // --- State guards ---

  @Test
  void stopIsNoOpWhenNeverStarted() {
    PipelineOperatorApp app = new PipelineOperatorApp(context);

    assertDoesNotThrow(() -> app.stop(Duration.ofSeconds(1)));
  }

  @Test
  void awaitTerminationReturnsWhenNeverStarted() {
    // Idle mode (empty controllers) and never-started both result in a null runnerThread;
    // awaitTermination treats both as "nothing to wait for" and returns. This is the right
    // call for the empty-controllers path because main() expects to exit cleanly there.
    PipelineOperatorApp app = new PipelineOperatorApp(context);

    assertDoesNotThrow(app::awaitTermination);
  }

  @Test
  void installShutdownHookIsIdempotent() {
    PipelineOperatorApp app = new PipelineOperatorApp(context);

    app.installShutdownHook(Duration.ofSeconds(1));
    assertDoesNotThrow(() -> app.installShutdownHook(Duration.ofSeconds(1)));
  }

  // --- Lifecycle (start/stop with a fake manager) ---

  @Test
  void startSpawnsRunnerThreadThatBlocksOnControllerManagerRun() throws Exception {
    CountDownLatch runEntered = new CountDownLatch(1);
    CountDownLatch shutdownSignal = new CountDownLatch(1);
    ControllerManager fakeManager = newFakeManager(runEntered, shutdownSignal);

    TestablePipelineOperatorApp app = new TestablePipelineOperatorApp(context, fakeManager, t -> { });
    app.start(List.of(mock(Controller.class)));

    assertTrue(runEntered.await(2, TimeUnit.SECONDS),
        "ControllerManager.run() should have been invoked on the runner thread");

    shutdownSignal.countDown();
  }

  @Test
  void startIsIdempotentSecondCallDoesNotConstructAnotherManager() {
    AtomicInteger constructionCount = new AtomicInteger(0);
    CountDownLatch shutdownSignal = new CountDownLatch(1);

    TestablePipelineOperatorApp app = new TestablePipelineOperatorApp(context, null, t -> { }) {
      @Override
      ControllerManager newControllerManager(SharedInformerFactory factory, Controller[] controllers) {
        constructionCount.incrementAndGet();
        return newFakeManager(new CountDownLatch(1), shutdownSignal);
      }
    };

    Controller dummy = mock(Controller.class);
    app.start(List.of(dummy));
    app.start(List.of(dummy)); // should be a no-op

    assertEquals(1, constructionCount.get(),
        "second start() must not construct another ControllerManager");

    shutdownSignal.countDown();
  }

  @Test
  void stopShutsDownControllerManagerAndUnblocksAwaitTermination() throws Exception {
    CountDownLatch runEntered = new CountDownLatch(1);
    CountDownLatch shutdownSignal = new CountDownLatch(1);
    ControllerManager fakeManager = newFakeManager(runEntered, shutdownSignal);

    TestablePipelineOperatorApp app = new TestablePipelineOperatorApp(context, fakeManager, t -> { });
    app.start(List.of(mock(Controller.class)));
    assertTrue(runEntered.await(2, TimeUnit.SECONDS));

    Thread waiter = new Thread(() -> {
      try {
        app.awaitTermination();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, "test-await-waiter");
    waiter.start();
    Thread.sleep(50);
    assertTrue(waiter.isAlive(), "awaitTermination() should still be blocking");

    app.stop(Duration.ofSeconds(2));

    verify(fakeManager).shutdown();
    waiter.join(2_000);
    assertFalse(waiter.isAlive(), "awaitTermination() should have unblocked after stop()");
  }

  @Test
  void stopInterruptsRunnerThreadIfShutdownDoesNotWakeItWithinTimeout() throws Exception {
    CountDownLatch runEntered = new CountDownLatch(1);
    CountDownLatch unreachable = new CountDownLatch(1);
    ControllerManager fakeManager = mock(ControllerManager.class);
    doAnswer(inv -> {
      runEntered.countDown();
      try {
        unreachable.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return null;
    }).when(fakeManager).run();
    doAnswer(inv -> null).when(fakeManager).shutdown();

    TestablePipelineOperatorApp app = new TestablePipelineOperatorApp(context, fakeManager, t -> { });
    app.start(List.of(mock(Controller.class)));
    assertTrue(runEntered.await(2, TimeUnit.SECONDS));

    app.stop(Duration.ofMillis(100));

    app.assertRunnerThreadTerminated(2_000);
  }

  @Test
  void emptyControllerListPutsServiceInIdleMode() throws Exception {
    // No controllers → no manager, no runner. stop() is a no-op; awaitTermination returns.
    TestablePipelineOperatorApp app = new TestablePipelineOperatorApp(context, null, t -> { });

    app.start(Collections.emptyList());
    app.stop(Duration.ofSeconds(1));

    long start = System.currentTimeMillis();
    app.awaitTermination();
    assertTrue(System.currentTimeMillis() - start < 500,
        "awaitTermination() in idle mode should return immediately");
  }

  // --- Failure handling via callback ---

  @Test
  void runnerThreadCrashInvokesFailureHandler() throws Exception {
    CountDownLatch handlerInvoked = new CountDownLatch(1);
    AtomicReference<Throwable> received = new AtomicReference<>();
    Consumer<Throwable> handler = cause -> {
      received.set(cause);
      handlerInvoked.countDown();
    };

    RuntimeException boom = new RuntimeException("boom");
    ControllerManager fakeManager = mock(ControllerManager.class);
    doThrow(boom).when(fakeManager).run();

    TestablePipelineOperatorApp app = new TestablePipelineOperatorApp(context, fakeManager, handler);
    app.start(List.of(mock(Controller.class)));

    assertTrue(handlerInvoked.await(2, TimeUnit.SECONDS),
        "failure handler should fire when the runner thread dies");
    assertSame(boom, received.get());
  }

  @Test
  void controllerCleanExitInvokesFailureHandlerWithNullCause() throws Exception {
    // A controller's run() returns cleanly without anyone calling stop(). The
    // wrapper observes the unexpected exit and reports failure with no captured throwable.
    CountDownLatch handlerInvoked = new CountDownLatch(1);
    AtomicReference<Throwable> received = new AtomicReference<>();
    Consumer<Throwable> handler = cause -> {
      received.set(cause);
      handlerInvoked.countDown();
    };

    Controller quickExit = mock(Controller.class);
    doNothing().when(quickExit).run();

    TestablePipelineOperatorApp app = new TestablePipelineOperatorApp(context, null, handler) {
      @Override
      ControllerManager newControllerManager(SharedInformerFactory factory, Controller[] controllers) {
        return interactiveFakeManager(controllers);
      }
    };

    app.start(List.of(quickExit));

    assertTrue(handlerInvoked.await(2, TimeUnit.SECONDS));
    assertNull(received.get(), "clean exit should report null cause");
  }

  @Test
  void controllerThrowingInvokesFailureHandlerWithCause() throws Exception {
    CountDownLatch handlerInvoked = new CountDownLatch(1);
    AtomicReference<Throwable> received = new AtomicReference<>();
    Consumer<Throwable> handler = cause -> {
      received.set(cause);
      handlerInvoked.countDown();
    };

    Controller throwing = mock(Controller.class);
    RuntimeException boom = new RuntimeException("controller boom");
    doThrow(boom).when(throwing).run();

    TestablePipelineOperatorApp app = new TestablePipelineOperatorApp(context, null, handler) {
      @Override
      ControllerManager newControllerManager(SharedInformerFactory factory, Controller[] controllers) {
        return interactiveFakeManager(controllers);
      }
    };

    app.start(List.of(throwing));

    assertTrue(handlerInvoked.await(2, TimeUnit.SECONDS));
    assertSame(boom, received.get(), "throwing controller should report the captured throwable");
  }

  @Test
  void multipleControllerExitsFireHandlerOnlyOnce() throws Exception {
    AtomicInteger handlerCalls = new AtomicInteger(0);
    Consumer<Throwable> handler = cause -> handlerCalls.incrementAndGet();

    Controller exit1 = mock(Controller.class);
    Controller exit2 = mock(Controller.class);
    doNothing().when(exit1).run();
    doNothing().when(exit2).run();

    TestablePipelineOperatorApp app = new TestablePipelineOperatorApp(context, null, handler) {
      @Override
      ControllerManager newControllerManager(SharedInformerFactory factory, Controller[] controllers) {
        return interactiveFakeManager(controllers);
      }
    };

    app.start(List.of(exit1, exit2));
    app.awaitTermination();
    Thread.sleep(50); // let any straggling wrappers finish

    assertEquals(1, handlerCalls.get(),
        "failure handler should fire at most once even when multiple controllers exit");
  }

  @Test
  void stopSuppressesFailureHandler() throws Exception {
    // When stop() is called, controller wrappers fire on the way out — but the handler must
    // not be invoked because the exit was intentional.
    AtomicInteger handlerCalls = new AtomicInteger(0);
    Consumer<Throwable> handler = cause -> handlerCalls.incrementAndGet();

    Controller longRunning = mock(Controller.class);
    CountDownLatch holdLongRunning = new CountDownLatch(1);
    doAnswer(inv -> {
      holdLongRunning.await();
      return null;
    }).when(longRunning).run();

    TestablePipelineOperatorApp app = new TestablePipelineOperatorApp(context, null, handler) {
      @Override
      ControllerManager newControllerManager(SharedInformerFactory factory, Controller[] controllers) {
        return interactiveFakeManager(controllers);
      }
    };

    app.start(List.of(longRunning));
    Thread.sleep(50); // let the controller enter run()

    // Release the long-running controller from a separate thread so it can exit cleanly
    // after stop() flips stopRequested.
    new Thread(holdLongRunning::countDown).start();
    app.stop(Duration.ofSeconds(2));
    Thread.sleep(50); // let the wrapper's finally block observe stopRequested

    assertEquals(0, handlerCalls.get(),
        "failure handler must not fire during intentional stop");
  }

  // --- Helpers ---

  /**
   * Builds a fake {@link ControllerManager} whose {@code run()} signals it entered, then blocks on
   * {@code shutdownSignal} until released. {@code shutdown()} releases the signal so {@code run()}
   * returns and the runner thread can exit naturally.
   */
  private static ControllerManager newFakeManager(CountDownLatch runEntered, CountDownLatch shutdownSignal) {
    ControllerManager fake = mock(ControllerManager.class);
    lenient().doAnswer(inv -> {
      runEntered.countDown();
      shutdownSignal.await();
      return null;
    }).when(fake).run();
    lenient().doAnswer(inv -> {
      shutdownSignal.countDown();
      return null;
    }).when(fake).shutdown();
    return fake;
  }

  /**
   * Builds a fake {@link ControllerManager} that actually invokes each controller's {@code run()}
   * on a worker thread, mimicking the real upstream behavior. Returns from {@code run()} only
   * once every controller has returned.
   */
  private static ControllerManager interactiveFakeManager(Controller[] controllers) {
    ControllerManager fake = mock(ControllerManager.class);
    lenient().doAnswer(inv -> {
      ExecutorService pool = Executors.newCachedThreadPool();
      try {
        CountDownLatch latch = new CountDownLatch(controllers.length);
        for (Controller c : controllers) {
          pool.submit(() -> {
            try {
              c.run();
            } catch (Throwable ignored) {
              // upstream ControllerManager catches and logs; mirror that behavior so the latch
              // still counts down and run() can return.
            } finally {
              latch.countDown();
            }
          });
        }
        latch.await();
      } finally {
        pool.shutdown();
      }
      return null;
    }).when(fake).run();
    lenient().doAnswer(inv -> null).when(fake).shutdown();
    return fake;
  }

  /**
   * Subclass that substitutes the seams so tests don't need a real K8s context. If
   * {@code presetManager} is non-null, every {@link #newControllerManager} call returns it.
   */
  private static class TestablePipelineOperatorApp extends PipelineOperatorApp {
    private final ControllerManager presetManager;

    TestablePipelineOperatorApp(K8sContext context, ControllerManager presetManager,
        Consumer<Throwable> failureHandler) {
      super(context, failureHandler);
      this.presetManager = presetManager;
    }

    @Override
    List<Controller> buildControllers(List<Controller> initialControllers) {
      return initialControllers;
    }

    @Override
    ControllerManager newControllerManager(SharedInformerFactory factory, Controller[] controllers) {
      return presetManager;
    }

    void assertRunnerThreadTerminated(long timeoutMillis) throws InterruptedException {
      Thread waiter = new Thread(() -> {
        try {
          awaitTermination();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      waiter.setDaemon(true);
      waiter.start();
      waiter.join(timeoutMillis);
      assertFalse(waiter.isAlive(), "runner thread should have terminated within " + timeoutMillis + "ms");
    }
  }
}
