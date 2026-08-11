package com.linkedin.hoptimator.util;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class IdentifierUtilsTest {

  @Test
  void quotedDottedSegmentStaysWhole() {
    // The dot inside "my.table" is part of the name, not a separator.
    assertThat(IdentifierUtils.parseIdentifier("\"VENICE\".\"my.table\""))
        .containsExactly("VENICE", "my.table");
  }

  @Test
  void quotedDottedTopicStaysWhole() {
    assertThat(IdentifierUtils.parseIdentifier("\"KAFKA\".\"my.event\""))
        .containsExactly("KAFKA", "my.event");
  }

  @Test
  void unquotedIdentifierSplitsOnDots() {
    assertThat(IdentifierUtils.parseIdentifier("ADS.AD_CLICKS"))
        .containsExactly("ADS", "AD_CLICKS");
  }

  @Test
  void threeLevelUnquotedIdentifierSplits() {
    assertThat(IdentifierUtils.parseIdentifier("MYSQL.testdb.orders"))
        .containsExactly("MYSQL", "testdb", "orders");
  }

  @Test
  void unquotedHyphenatedNameSplitsOnDots() {
    // Calcite would read "LOGICAL.testevent-graph" as arithmetic (subtraction), but unquoted input
    // never reaches the parser: a plain dot-split is exact because an unquoted segment cannot
    // contain a dot. This is the form the !graph/!resolve/!describe CLI commands accept.
    assertThat(IdentifierUtils.parseIdentifier("LOGICAL.testevent-graph"))
        .containsExactly("LOGICAL", "testevent-graph");
  }

  @Test
  void malformedQuotedIdentifierThrows() {
    // Quoted input must be a well-formed identifier. A mixed form — quoted schema with an unquoted
    // hyphenated segment — is illegal (the SQL grammar reads it as subtraction), so surface it
    // rather than silently mis-splitting.
    assertThatThrownBy(() -> IdentifierUtils.parseIdentifier("\"KAFKA\".my-topic"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("\"KAFKA\".my-topic");
  }

  @Test
  void unterminatedQuoteThrows() {
    assertThatThrownBy(() -> IdentifierUtils.parseIdentifier("\"KAFKA\".\"my.event"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void quotedHyphenDollarNamePreservesSpecialChars() {
    assertThat(IdentifierUtils.parseIdentifier("VENICE.\"test-store$insert-partial\""))
        .containsExactly("VENICE", "test-store$insert-partial");
  }

  @Test
  void casePreservedForUnquotedAndQuoted() {
    assertThat(IdentifierUtils.parseIdentifier("profile.Members"))
        .containsExactly("profile", "Members");
  }

  @Test
  void singleSegmentReturnsOneElement() {
    assertThat(IdentifierUtils.parseIdentifier("audience")).containsExactly("audience");
  }

  @Test
  void singleQuotedSegmentWithDotStaysWhole() {
    assertThat(IdentifierUtils.parseIdentifier("\"my.event\"")).containsExactly("my.event");
  }

  @Test
  void escapedQuotesInsideSegmentAreUnescaped() {
    // "a""b" is the SQL-quoted form of the identifier a"b.
    assertThat(IdentifierUtils.parseIdentifier("\"a\"\"b\".\"c.d\""))
        .containsExactly("a\"b", "c.d");
  }

  @Test
  void nullReturnsEmptyList() {
    List<String> parts = IdentifierUtils.parseIdentifier(null);
    assertThat(parts).isEmpty();
  }
}
