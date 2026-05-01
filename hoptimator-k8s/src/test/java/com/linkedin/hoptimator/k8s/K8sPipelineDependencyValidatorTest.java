package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;


/**
 * Unit tests for {@link K8sPipelineDependencyValidator}. The validator is a thin adapter that
 * forwards to {@link PipelineDependencyChecker#assertNoExternalDependents} — these tests use
 * {@link MockedStatic} on both K8sContext and PipelineDependencyChecker to verify that source
 * fields and self-owner fields are forwarded correctly, and that thrown SQLException becomes a
 * validation issue rather than propagating.
 */
@ExtendWith(MockitoExtension.class)
class K8sPipelineDependencyValidatorTest {

  @Mock
  private MockedStatic<K8sContext> contextStatic;

  @Mock
  private MockedStatic<PipelineDependencyChecker> checkerStatic;

  @Mock
  private Connection connection;

  @Mock
  private K8sContext context;

  private static Source source() {
    return new Source("kafka1", List.of("KAFKA", "my-topic"), Map.of());
  }

  @Test
  void validateForwardsSourceFieldsAndSelfOwnerToChecker() {
    contextStatic.when(() -> K8sContext.create(connection)).thenReturn(context);

    K8sPipelineDependencyValidator validator =
        new K8sPipelineDependencyValidator(source(), "LogicalTable", "my-table");
    Validator.Issues issues = new Validator.Issues("test");

    validator.validate(issues, connection);

    ArgumentCaptor<String> dbCaptor = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<String>> pathCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<String> kindCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);

    checkerStatic.verify(() -> PipelineDependencyChecker.assertNoExternalDependents(
        eq(context), dbCaptor.capture(), pathCaptor.capture(),
        kindCaptor.capture(), nameCaptor.capture()));

    assertEquals("kafka1", dbCaptor.getValue());
    assertEquals(List.of("KAFKA", "my-topic"), pathCaptor.getValue());
    assertEquals("LogicalTable", kindCaptor.getValue());
    assertEquals("my-table", nameCaptor.getValue());
    assertTrue(issues.valid());
  }

  @Test
  @SuppressWarnings("unchecked")
  void validatePassesNullSelfOwnerWhenUnset() {
    contextStatic.when(() -> K8sContext.create(connection)).thenReturn(context);

    K8sPipelineDependencyValidator validator =
        new K8sPipelineDependencyValidator(source(), null, null);
    Validator.Issues issues = new Validator.Issues("test");

    validator.validate(issues, connection);

    ArgumentCaptor<String> kindCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
    checkerStatic.verify(() -> PipelineDependencyChecker.assertNoExternalDependents(
        eq(context), nullable(String.class), nullable(List.class),
        kindCaptor.capture(), nameCaptor.capture()));

    assertNull(kindCaptor.getValue());
    assertNull(nameCaptor.getValue());
  }

  @Test
  @SuppressWarnings("unchecked")
  void validateRecordsCheckerSqlExceptionAsIssue() {
    contextStatic.when(() -> K8sContext.create(connection)).thenReturn(context);
    checkerStatic.when(() -> PipelineDependencyChecker.assertNoExternalDependents(
            nullable(K8sContext.class), nullable(String.class), nullable(List.class),
            nullable(String.class), nullable(String.class)))
        .thenThrow(new SQLException("3 active pipeline(s) depend on it: p1, p2, p3"));

    K8sPipelineDependencyValidator validator =
        new K8sPipelineDependencyValidator(source(), null, null);
    Validator.Issues issues = new Validator.Issues("test");

    validator.validate(issues, connection);

    assertFalse(issues.valid(), "blocking pipelines should surface as a validation error");
    assertTrue(issues.toString().contains("3 active pipeline"),
        "issue message should include the SQLException's text: " + issues);
  }
}
