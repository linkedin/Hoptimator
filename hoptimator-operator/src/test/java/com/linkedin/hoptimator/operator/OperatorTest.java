package com.linkedin.hoptimator.operator;

import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kubernetes.client.openapi.ApiClient;

import static org.junit.jupiter.api.Assertions.assertFalse;


/**
 * Tests for Operator focusing on YAML parsing exception handling.
 * Verifies that malformed YAML in isReady() and isFailed() returns false rather than throwing.
 */
@ExtendWith(MockitoExtension.class)
public class OperatorTest {

  @Mock
  private ApiClient apiClient;

  private Operator operator;

  // A valid minimal YAML for a K8s object
  private static final String VALID_YAML =
      "apiVersion: foo.org/v1beta1\n" + "kind: FakeJob\n" + "metadata:\n" + "  name: fake-job\n"
          + "  namespace: fake-ns\n";

  // YAML that triggers ScannerException: colon in unquoted value
  private static final String SCANNER_EXCEPTION_YAML = "key: value: with: colons: everywhere:";

  // YAML that triggers ParserException: invalid document structure
  private static final String PARSER_EXCEPTION_YAML = "--- invalid\n--- also invalid\n---";

  // Valid YAML with no namespace — triggers null namespace in isReady/isFailed
  private static final String NO_NAMESPACE_YAML =
      "apiVersion: foo.org/v1beta1\n" + "kind: FakeJob\n" + "metadata:\n" + "  name: fake-job\n";

  // Valid YAML with no metadata at all — triggers null metadata
  private static final String NO_METADATA_YAML = "apiVersion: v1\nkind: ConfigMap";

  @BeforeEach
  void setUp() {
    operator = new Operator("fake-namespace", apiClient, new Properties());
    operator.registerApi("FakeJob", "fakejob", "fakejobs", "foo.org", "v1beta1");
  }

  @Test
  void testIsReadyReturnsFalseOnScannerException() {
    // isReady(yaml) must return false rather than throw when YAML triggers a ScannerException.
    boolean result = operator.isReady(SCANNER_EXCEPTION_YAML);
    assertFalse(result);
  }

  @Test
  void testIsReadyReturnsFalseOnParserException() {
    // isReady(yaml) must return false rather than throw when YAML triggers a ParserException.
    boolean result = operator.isReady(PARSER_EXCEPTION_YAML);
    assertFalse(result);
  }

  @Test
  void testIsFailedReturnsFalseOnScannerException() {
    // Same protection needed for isFailed(): should not throw on malformed YAML.
    boolean result = operator.isFailed(SCANNER_EXCEPTION_YAML);
    assertFalse(result);
  }

  @Test
  void testIsFailedReturnsFalseOnParserException() {
    // Same protection needed for isFailed(): should not throw on malformed YAML.
    boolean result = operator.isFailed(PARSER_EXCEPTION_YAML);
    assertFalse(result);
  }

  @Test
  void testIsReadyReturnsFalseOnNullNamespace() {
    // isReady(yaml) must return false rather than throw when the YAML has no namespace.
    boolean result = operator.isReady(NO_NAMESPACE_YAML);
    assertFalse(result);
  }

  @Test
  void testIsFailedReturnsFalseOnNullNamespace() {
    boolean result = operator.isFailed(NO_NAMESPACE_YAML);
    assertFalse(result);
  }

  @Test
  void testIsReadyReturnsFalseOnNullMetadata() {
    boolean result = operator.isReady(NO_METADATA_YAML);
    assertFalse(result);
  }

  @Test
  void testIsFailedReturnsFalseOnNullMetadata() {
    boolean result = operator.isFailed(NO_METADATA_YAML);
    assertFalse(result);
  }
}
