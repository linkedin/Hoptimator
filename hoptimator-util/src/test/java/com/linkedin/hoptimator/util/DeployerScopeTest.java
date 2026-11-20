package com.linkedin.hoptimator.util;

import com.linkedin.hoptimator.Deployer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.sql.SQLException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
class DeployerScopeTest {
  @Mock private Deployer first;
  @Mock private Deployer second;

  @Test
  void closesUniqueDeployersInReverseAcquisitionOrderOnlyOnce() {
    Collection<Deployer> acquired = List.of(first, second, first);
    DeployerScope scope = new DeployerScope(acquired);
    assertThat(scope.deployers()).containsExactly(first, second, first);
    verify(first, never()).close();
    verify(second, never()).close();

    scope.close();
    scope.close();

    InOrder order = inOrder(first, second);
    order.verify(first).close();
    order.verify(second).close();
    order.verifyNoMoreInteractions();
  }

  @Test
  void closesRemainingDeployersAndAggregatesFailures() {
    RuntimeException firstFailure = new IllegalStateException("first close");
    RuntimeException secondFailure = new IllegalArgumentException("second close");
    doThrow(firstFailure).when(first).close();
    doThrow(secondFailure).when(second).close();
    DeployerScope scope = new DeployerScope(List.of(first, second));

    assertDoesNotThrow(scope::close);

    assertThat(secondFailure.getSuppressed()).containsExactly(firstFailure);
    verify(first).close();
    verify(second).close();
    scope.close();
  }

  @ParameterizedTest
  @MethodSource("operationFailures")
  void preservesOperationFailureWhenCleanupFails(Throwable expected) {
    RuntimeException firstFailure = new IllegalStateException("first close");
    RuntimeException secondFailure = new IllegalArgumentException("second close");
    doThrow(firstFailure).when(first).close();
    doThrow(secondFailure).when(second).close();
    Throwable failure = assertThrows(expected.getClass(), () -> {
      try (DeployerScope scope = new DeployerScope(List.of(first, second))) {
        assertThat(scope.deployers()).containsExactly(first, second);
        throw expected;
      }
    });

    assertSame(expected, failure);
    assertThat(failure.getSuppressed()).isEmpty();
    assertThat(secondFailure.getSuppressed()).containsExactly(firstFailure);
    verify(first).close();
    verify(second).close();
  }

  private static Stream<Throwable> operationFailures() {
    return Stream.of(new SQLException("operation failed"),
        new IllegalStateException("operation failed"), new AssertionError("operation failed"));
  }

  @Test
  void rejectsAccessAfterScopeClosed() {
    DeployerScope scope = new DeployerScope(List.of(first));
    scope.close();

    assertThatThrownBy(scope::deployers)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("scope is closed");
    verify(first).close();
  }

  @Test
  void snapshotsAcquiredDeployersAndRejectsExternalMutation() {
    Collection<Deployer> acquired = new ArrayList<>(List.of(first));
    try (DeployerScope scope = new DeployerScope(acquired)) {
      acquired.clear();
      assertThat(scope.deployers()).containsExactly(first);
      assertThatThrownBy(() -> scope.deployers().clear()).isInstanceOf(UnsupportedOperationException.class);
    }
    verify(first).close();
  }
}
