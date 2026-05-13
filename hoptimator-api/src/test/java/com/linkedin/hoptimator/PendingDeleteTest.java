package com.linkedin.hoptimator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class PendingDeleteTest {

  @Test
  void singleArgConstructorLeavesSelfOwnerNull() {
    Object target = new Object();
    PendingDelete<Object> pd = new PendingDelete<>(target);

    assertSame(target, pd.target());
    assertNull(pd.selfOwnerKind());
    assertNull(pd.selfOwnerName());
  }

  @Test
  void threeArgConstructorStoresSelfOwner() {
    Object target = new Object();
    PendingDelete<Object> pd = new PendingDelete<>(target, "LogicalTable", "my-table");

    assertSame(target, pd.target());
    assertEquals("LogicalTable", pd.selfOwnerKind());
    assertEquals("my-table", pd.selfOwnerName());
  }

  @Test
  void threeArgConstructorAcceptsNullSelfOwner() {
    Object target = new Object();
    PendingDelete<Object> pd = new PendingDelete<>(target, null, null);

    assertNull(pd.selfOwnerKind());
    assertNull(pd.selfOwnerName());
  }

  @Test
  void nullTargetThrows() {
    assertThrows(NullPointerException.class, () -> new PendingDelete<>(null));
    assertThrows(NullPointerException.class,
        () -> new PendingDelete<>(null, "LogicalTable", "my-table"));
  }

  @Test
  void toStringIncludesTargetAndSelfOwnerWhenPresent() {
    PendingDelete<String> pd = new PendingDelete<>("the-target", "LogicalTable", "my-table");
    String s = pd.toString();
    assertTrue(s.contains("the-target"), "toString should include target: " + s);
    assertTrue(s.contains("LogicalTable/my-table"), "toString should include kind/name: " + s);
  }

  @Test
  void toStringOmitsSelfOwnerWhenNull() {
    PendingDelete<String> pd = new PendingDelete<>("the-target");
    String s = pd.toString();
    assertTrue(s.contains("the-target"));
    assertFalse(s.contains("self="), "toString should not include self= when not set: " + s);
  }

  @Test
  void toStringOmitsSelfOwnerWhenOnlyOneFieldSet() {
    // The toString contract says self= appears only when both kind and name are non-null.
    // (The K8sPipelineDependencyChecker.isSelfOwned guard also requires both.)
    PendingDelete<String> kindOnly = new PendingDelete<>("t", "LogicalTable", null);
    assertFalse(kindOnly.toString().contains("self="));

    PendingDelete<String> nameOnly = new PendingDelete<>("t", null, "my-table");
    assertFalse(nameOnly.toString().contains("self="));
  }

  @Test
  void targetGenericTypeIsPreserved() {
    Source source = new Source("db", java.util.List.of("schema", "tbl"), java.util.Map.of());
    PendingDelete<Source> pd = new PendingDelete<>(source);
    Source unwrapped = pd.target();
    assertEquals("tbl", unwrapped.table());
  }
}
