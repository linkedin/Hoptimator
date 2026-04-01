package com.linkedin.hoptimator.operator;

import com.linkedin.hoptimator.k8s.K8sContext;
import io.kubernetes.client.extended.controller.Controller;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class ControllerServiceTest {

  @Mock
  private K8sContext context;

  @Test
  void providersReturnsNonNullCollection() {
    Collection<ControllerProvider> providers = ControllerService.providers();
    assertNotNull(providers);
  }

  /**
   * The test SPI file registers ControllerProviderTest so ServiceLoader finds at least one provider.
   * If forEachRemaining() is removed (VoidMethodCall), providers() returns an empty list.
   * If providers() returns empty object (EmptyObjectReturnVals), size is 0.
   */
  @Test
  void providersReturnsAtLeastOneRegisteredProvider() {
    Collection<ControllerProvider> providers = ControllerService.providers();
    assertNotNull(providers);
    // ControllerProviderTest is registered via META-INF/services in test resources
    assertFalse(providers.isEmpty(), "ServiceLoader must find at least one ControllerProvider in test classpath");
  }

  @Test
  void allProvidersAreControllerProviderInstances() {
    Collection<ControllerProvider> providers = ControllerService.providers();
    for (ControllerProvider p : providers) {
      assertNotNull(p, "All loaded providers must be non-null");
    }
  }

  @Test
  void controllersReturnsNonNullCollection() {
    Collection<Controller> controllers = ControllerService.controllers(context);
    assertNotNull(controllers);
  }

  /**
   * ControllerProviderTest returns empty list, so controllers() maps to an empty stream.
   * We verify that controllers() returns exactly the flat-mapped result of all providers.
   */
  @Test
  void controllersCountMatchesExpectedFromProviders() {
    Collection<ControllerProvider> providers = ControllerService.providers();
    int expectedCount = providers.stream().mapToInt(p -> p.controllers(context).size()).sum();

    Collection<Controller> controllers = ControllerService.controllers(context);

    assertNotNull(controllers);
    // controllers() must return exactly what providers produce — not an empty placeholder
    assertTrue(controllers.size() == expectedCount,
        "controllers() must return the flat-mapped result of all providers, got: "
            + controllers.size() + " expected: " + expectedCount);
  }

  @Test
  void controllersReturnsSameCountAsProviders() {
    assertNotNull(ControllerService.providers());
    Collection<Controller> controllers = ControllerService.controllers(context);
    assertNotNull(controllers);
  }
}
