package com.linkedin.hoptimator.logical;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class LogicalTierTest {

  // ── tierName() ───────────────────────────────────────────────────────────

  @Test
  public void nearlineTierNameIsNearline() {
    assertEquals("nearline", LogicalTier.NEARLINE.tierName());
  }

  @Test
  public void offlineTierNameIsOffline() {
    assertEquals("offline", LogicalTier.OFFLINE.tierName());
  }

  @Test
  public void onlineTierNameIsOnline() {
    assertEquals("online", LogicalTier.ONLINE.tierName());
  }

  // ── of() ─────────────────────────────────────────────────────────────────

  @Test
  public void ofNearlineReturnsNearline() {
    assertTrue(LogicalTier.of("nearline").isPresent());
    assertEquals(LogicalTier.NEARLINE, LogicalTier.of("nearline").get());
  }

  @Test
  public void ofOfflineReturnsOffline() {
    assertTrue(LogicalTier.of("offline").isPresent());
    assertEquals(LogicalTier.OFFLINE, LogicalTier.of("offline").get());
  }

  @Test
  public void ofOnlineReturnsOnline() {
    assertTrue(LogicalTier.of("online").isPresent());
    assertEquals(LogicalTier.ONLINE, LogicalTier.of("online").get());
  }

  @Test
  public void ofUnknownNameReturnsEmpty() {
    assertFalse(LogicalTier.of("unknown").isPresent());
  }

  @Test
  public void ofEmptyStringReturnsEmpty() {
    assertFalse(LogicalTier.of("").isPresent());
  }

  @Test
  public void ofMixedCaseReturnsMatch() {
    assertTrue(LogicalTier.of("NEARLINE").isPresent());
    assertEquals(LogicalTier.NEARLINE, LogicalTier.of("NEARLINE").get());
    assertTrue(LogicalTier.of("Offline").isPresent());
    assertEquals(LogicalTier.OFFLINE, LogicalTier.of("Offline").get());
    assertTrue(LogicalTier.of("ONLINE").isPresent());
    assertEquals(LogicalTier.ONLINE, LogicalTier.of("ONLINE").get());
  }

  // ── isTier() ─────────────────────────────────────────────────────────────

  @Test
  public void isTierReturnsTrueForNearline() {
    assertTrue(LogicalTier.isTier("nearline"));
  }

  @Test
  public void isTierReturnsTrueForOffline() {
    assertTrue(LogicalTier.isTier("offline"));
  }

  @Test
  public void isTierReturnsTrueForOnline() {
    assertTrue(LogicalTier.isTier("online"));
  }

  @Test
  public void isTierReturnsTrueForMixedCase() {
    assertTrue(LogicalTier.isTier("NEARLINE"));
    assertTrue(LogicalTier.isTier("Offline"));
    assertTrue(LogicalTier.isTier("ONLINE"));
  }

  @Test
  public void isTierReturnsFalseForUnknown() {
    assertFalse(LogicalTier.isTier("realtime"));
  }

  @Test
  public void isTierReturnsFalseForEmpty() {
    assertFalse(LogicalTier.isTier(""));
  }
}
