package com.linkedin.hoptimator.operator.subscription;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.operator.Operator;
import com.linkedin.hoptimator.planner.HoptimatorPlanner;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;


/**
 * Tests for SubscriptionReconciler.fetchAttributes() YAML parsing exception handling.
 * Reproduces EXC-475778 where ClassCastException from malformed YAML in fetchAttributes()
 * was unhandled and propagated up through reconcile().
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SubscriptionReconcilerFetchAttributesTest {

  @Mock
  private Operator operator;

  @Mock
  private HoptimatorPlanner.Factory plannerFactory;

  @Mock
  private Resource.Environment environment;

  private SubscriptionReconciler reconciler;

  // YAML that triggers ClassCastException in snakeyaml Composer
  private static final String CLASS_CAST_EXCEPTION_YAML = "!!java.util.Date 2021-01-01";

  // YAML that triggers ScannerException
  private static final String SCANNER_EXCEPTION_YAML = "key: value: with: colons: everywhere:";

  // Valid YAML with no metadata — triggers null metadata in fetchAttributes
  private static final String NO_METADATA_YAML = "apiVersion: v1\nkind: ConfigMap";

  @BeforeEach
  void setUp() throws Exception {
    Constructor<SubscriptionReconciler> ctor = SubscriptionReconciler.class.getDeclaredConstructor(
        Operator.class, HoptimatorPlanner.Factory.class, Resource.Environment.class, java.util.function.Predicate.class);
    ctor.setAccessible(true);
    reconciler = ctor.newInstance(operator, plannerFactory, environment, null);
  }

  @Test
  void testFetchAttributesDoesNotThrowClassCastException() throws Exception {
    // Reproduce EXC-475778: SubscriptionReconciler.fetchAttributes() threw ClassCastException
    // when snakeyaml produced an unexpected node type during YAML parsing.
    // fetchAttributes() must catch the exception and return an empty map.
    Method fetchAttributes = SubscriptionReconciler.class.getDeclaredMethod("fetchAttributes", String.class);
    fetchAttributes.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, String> result = assertDoesNotThrow(
        () -> (Map<String, String>) fetchAttributes.invoke(reconciler, CLASS_CAST_EXCEPTION_YAML));
  }

  @Test
  void testFetchAttributesDoesNotThrowScannerException() throws Exception {
    // Same protection: fetchAttributes() must not throw for YAML that triggers ScannerException.
    Method fetchAttributes = SubscriptionReconciler.class.getDeclaredMethod("fetchAttributes", String.class);
    fetchAttributes.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, String> result = assertDoesNotThrow(
        () -> (Map<String, String>) fetchAttributes.invoke(reconciler, SCANNER_EXCEPTION_YAML));
  }

  @Test
  void testFetchAttributesDoesNotThrowOnNullMetadata() throws Exception {
    // Reproduce null metadata scenario: fetchAttributes() must not throw NPE when
    // the parsed YAML has no metadata field.
    Method fetchAttributes = SubscriptionReconciler.class.getDeclaredMethod("fetchAttributes", String.class);
    fetchAttributes.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, String> result = assertDoesNotThrow(
        () -> (Map<String, String>) fetchAttributes.invoke(reconciler, NO_METADATA_YAML));
  }
}
