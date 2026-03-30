package com.linkedin.hoptimator;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class ValidatorTest {

  // --- Issues tests ---

  @Test
  void testIssuesStartsValid() {
    Validator.Issues issues = new Validator.Issues("test");
    assertTrue(issues.valid());
  }

  @Test
  void testErrorInvalidatesIssues() {
    Validator.Issues issues = new Validator.Issues("test");
    issues.error("something went wrong");
    assertFalse(issues.valid());
  }

  @Test
  void testWarnDoesNotInvalidate() {
    Validator.Issues issues = new Validator.Issues("test");
    issues.warn("minor issue");
    assertTrue(issues.valid());
  }

  @Test
  void testInfoDoesNotInvalidate() {
    Validator.Issues issues = new Validator.Issues("test");
    issues.info("informational");
    assertTrue(issues.valid());
  }

  @Test
  void testChildErrorInvalidatesParent() {
    Validator.Issues parent = new Validator.Issues("parent");
    Validator.Issues child = parent.child("child");
    child.error("child error");
    assertFalse(parent.valid());
    assertFalse(child.valid());
  }

  @Test
  void testChildReturnsSameInstanceForSameContext() {
    Validator.Issues issues = new Validator.Issues("root");
    Validator.Issues child1 = issues.child("ctx");
    Validator.Issues child2 = issues.child("ctx");
    assertEquals(child1, child2);
  }

  @Test
  void testClosePreventsFurtherEmissions() {
    Validator.Issues issues = new Validator.Issues("test");
    issues.close();
    assertThrows(IllegalStateException.class, () -> issues.error("after close"));
  }

  @Test
  void testToStringContainsContext() {
    Validator.Issues issues = new Validator.Issues("myContext");
    issues.error("my error");
    String result = issues.toString();
    assertTrue(result.contains("myContext"));
    assertTrue(result.contains("ERROR: my error"));
  }

  @Test
  void testToStringEmptyWhenNoIssues() {
    Validator.Issues issues = new Validator.Issues("test");
    assertEquals("", issues.toString());
  }

  @Test
  void testToStringIncludesChildIssues() {
    Validator.Issues parent = new Validator.Issues("parent");
    Validator.Issues child = parent.child("child");
    child.warn("child warning");
    String result = parent.toString();
    assertTrue(result.contains("child"));
    assertTrue(result.contains("WARNING: child warning"));
  }

  // --- validateSubdomainName tests ---

  @Test
  void testValidateSubdomainNamePassesForValid() {
    Validator.Issues issues = new Validator.Issues("test");
    Validator.validateSubdomainName("my-name", issues);
    assertTrue(issues.valid());
  }

  @Test
  void testValidateSubdomainNameRejectsUppercase() {
    Validator.Issues issues = new Validator.Issues("test");
    Validator.validateSubdomainName("MyName", issues);
    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("uppercase"));
  }

  @Test
  void testValidateSubdomainNameRejectsUnderscore() {
    Validator.Issues issues = new Validator.Issues("test");
    Validator.validateSubdomainName("my_name", issues);
    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("underscore"));
  }

  @Test
  void testValidateSubdomainNameRejectsPeriod() {
    Validator.Issues issues = new Validator.Issues("test");
    Validator.validateSubdomainName("my.name", issues);
    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("period"));
  }

  // --- validateUnique tests ---

  @Test
  void testValidateUniquePassesForUniqueValues() {
    Validator.Issues issues = new Validator.Issues("test");
    List<String> items = List.of("a", "b", "c");
    Validator.validateUnique(items, s -> s, issues);
    assertTrue(issues.valid());
  }

  @Test
  void testValidateUniqueRejectsDuplicates() {
    Validator.Issues issues = new Validator.Issues("test");
    List<String> items = List.of("a", "b", "a");
    Validator.validateUnique(items, s -> s, issues);
    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Duplicate"));
  }

  // --- notYetImplemented ---

  @Test
  void testNotYetImplementedAddsWarning() {
    Validator.Issues issues = new Validator.Issues("test");
    Validator.notYetImplemented(issues);
    assertTrue(issues.valid());
    assertTrue(issues.toString().contains("not implemented"));
  }

  // --- checkClosed tests ---

  @Test
  void testCheckClosedThrowsWhenChildNotClosed() {
    Validator.Issues parent = new Validator.Issues("parent");
    assertNotNull(parent.child("unclosed-child"));
    assertThrows(IllegalStateException.class, () -> parent.close());
  }

  @Test
  void testCheckClosedSucceedsWhenChildIsClosed() {
    Validator.Issues parent = new Validator.Issues("parent");
    Validator.Issues child = parent.child("closed-child");
    child.close();
    parent.close(); // should not throw
  }

  @Test
  void testCheckClosedMessageContainsPath() {
    Validator.Issues parent = new Validator.Issues("parent");
    Validator.Issues child = parent.child("bad-child");
    assertNotNull(child);
    IllegalStateException ex = assertThrows(IllegalStateException.class, () -> parent.close());
    assertTrue(ex.getMessage().contains("bad-child"));
    assertTrue(ex.getMessage().contains("was not closed"));
  }

  // --- DefaultValidator tests ---

  @Test
  void testDefaultValidatorDelegatesToTarget() {
    Validated target = issues -> issues.error("target error");
    Validator.DefaultValidator<Validated> validator = new Validator.DefaultValidator<>(target);
    Validator.Issues issues = new Validator.Issues("root");
    validator.validate(issues);
    assertFalse(issues.valid());
  }
}
