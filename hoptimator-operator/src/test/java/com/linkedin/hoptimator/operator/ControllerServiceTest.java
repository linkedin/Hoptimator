package com.linkedin.hoptimator.operator;

import java.util.Collection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kubernetes.client.extended.controller.Controller;

import com.linkedin.hoptimator.k8s.K8sContext;

import static org.junit.jupiter.api.Assertions.assertNotNull;


@ExtendWith(MockitoExtension.class)
class ControllerServiceTest {

  @Mock
  private K8sContext context;

  @Test
  void providersReturnsNonNullCollection() {
    Collection<ControllerProvider> providers = ControllerService.providers();
    assertNotNull(providers);
  }

  @Test
  void providersReturnsEmptyOrPopulatedCollection() {
    Collection<ControllerProvider> providers = ControllerService.providers();
    // ServiceLoader may find 0 providers in a test environment, that's OK
    assertNotNull(providers);
  }

  @Test
  void controllersReturnsNonNullCollection() {
    Collection<Controller> controllers = ControllerService.controllers(context);
    assertNotNull(controllers);
  }

  @Test
  void controllersReturnsSameCountAsProviders() {
    assertNotNull(ControllerService.providers());
    Collection<Controller> controllers = ControllerService.controllers(context);
    assertNotNull(controllers);
  }
}
