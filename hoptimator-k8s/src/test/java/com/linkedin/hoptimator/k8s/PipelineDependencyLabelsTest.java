package com.linkedin.hoptimator.k8s;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class PipelineDependencyLabelsTest {

  private static Source src(String db, String... path) {
    return new Source(db, Arrays.asList(path), Collections.emptyMap());
  }

  private static Sink sink(String db, String... path) {
    return new Sink(db, Arrays.asList(path), Collections.emptyMap());
  }

  @Test
  void identifierJoinsDatabaseAndPath() {
    // Separator is "_" so the identifier is also a valid K8s label value out of the box.
    assertEquals("mydb_a.b.c", PipelineDependencyLabels.identifier("mydb", Arrays.asList("a", "b", "c")));
  }

  @Test
  void slugIsDeterministic() {
    String s1 = PipelineDependencyLabels.slug("db", Arrays.asList("foo", "bar"));
    String s2 = PipelineDependencyLabels.slug("db", Arrays.asList("foo", "bar"));
    assertEquals(s1, s2);
  }

  @Test
  void slugVariesByDatabase() {
    String a = PipelineDependencyLabels.slug("db1", Collections.singletonList("t"));
    String b = PipelineDependencyLabels.slug("db2", Collections.singletonList("t"));
    assertNotEquals(a, b);
  }

  @Test
  void slugVariesByPath() {
    String a = PipelineDependencyLabels.slug("db", Arrays.asList("schema", "t"));
    String b = PipelineDependencyLabels.slug("db", Arrays.asList("schema", "u"));
    assertNotEquals(a, b);
  }

  @Test
  void labelKeyFitsKubernetesNameLimit() {
    // Long path stressing the slug — name portion (after the /) must be ≤ 63 chars.
    String key = PipelineDependencyLabels.labelKey(
        "a-really-long-database-name",
        Arrays.asList("catalog", "schema", "a_very_long_table_name_that_exceeds_sixty_three_chars"));
    String namePortion = key.substring(key.indexOf('/') + 1);
    assertTrue(namePortion.length() <= 63, "name portion must be ≤63 chars, got " + namePortion.length());
    assertTrue(namePortion.matches("[a-z0-9]([-a-z0-9_.]*[a-z0-9])?"),
        "name portion must match K8s label-name regex, got: " + namePortion);
  }

  @Test
  void labelsForIncludesOnlySources() {
    // Sinks are deliberately excluded — the guard is about readers, not writers.
    Source s1 = src("kafka1", "events");
    Source s2 = src("venice1", "store");
    Sink sink = sink("mysql1", "outbox");
    Map<String, String> labels = PipelineDependencyLabels.labelsFor(Arrays.asList(s1, s2), sink);

    assertEquals(2, labels.size());
    assertTrue(labels.containsKey(PipelineDependencyLabels.labelKey("kafka1", Collections.singletonList("events"))));
    assertTrue(labels.containsKey(PipelineDependencyLabels.labelKey("venice1", Collections.singletonList("store"))));
    assertFalse(labels.containsKey(PipelineDependencyLabels.labelKey("mysql1", Collections.singletonList("outbox"))),
        "sink must NOT appear as a depends-on label — partial-views share sinks");
  }

  @Test
  void labelsForHandlesNullSink() {
    Map<String, String> labels = PipelineDependencyLabels.labelsFor(
        Collections.singletonList(src("db", "t")), null);
    assertEquals(1, labels.size());
  }

  @Test
  void labelsForOmitsSinkEvenWhenSourceCoincides() {
    // Self-loop pipeline: the source IS labeled (it's a read), but the sink alone wouldn't be.
    Source s = src("db", "t");
    Sink k = sink("db", "t");
    Map<String, String> labels = PipelineDependencyLabels.labelsFor(Collections.singletonList(s), k);
    assertEquals(1, labels.size());
  }

  @Test
  void labelValueTruncatedAtSixtyThreeChars() {
    String longPath = "this_is_a_really_long_table_name_that_exceeds_sixty_three_chars_by_a_lot";
    Map<String, String> labels = PipelineDependencyLabels.labelsFor(
        Collections.singletonList(src("db", longPath)), null);
    String value = labels.values().iterator().next();
    assertTrue(value.length() <= 63);
  }

  @Test
  void labelValueIsKubernetesLabelValueCompliant() {
    // K8s label values must match (([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?
    // — the identifier separator is "_" precisely so this holds out of the box for typical
    // (database, path) shapes seen in production.
    Map<String, String> labels = PipelineDependencyLabels.labelsFor(
        Collections.singletonList(src("ads-database", "ADS", "PAGE_VIEWS")), null);
    String value = labels.values().iterator().next();

    assertTrue(value.length() <= 63);
    assertTrue(value.matches("(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?"),
        "value must satisfy K8s label-value regex, got: " + value);
    assertFalse(value.contains("/"), "no '/' separator should leak into the label value");
  }

  @Test
  void annotationForListsAllSourceIdentifiers() {
    // Sink is excluded for the same reason as labels — this is a sources-only annotation.
    String annotation = PipelineDependencyLabels.annotationFor(
        Arrays.asList(src("kafka", "a"), src("venice", "b")),
        sink("mysql", "c"));
    assertTrue(annotation.contains("kafka_a"));
    assertTrue(annotation.contains("venice_b"));
    assertFalse(annotation.contains("mysql_c"),
        "sink must NOT appear in the annotation — sources-only");
  }

  @Test
  void annotationForDeduplicatesAndOmitsNullSink() {
    String annotation = PipelineDependencyLabels.annotationFor(
        Arrays.asList(src("db", "t"), src("db", "t")), null);
    assertEquals("db_t", annotation);
  }

  @Test
  void parseAnnotationRoundtrip() {
    // Sources only — sink is omitted by annotationFor by design.
    String annotation = PipelineDependencyLabels.annotationFor(
        Arrays.asList(src("a", "1"), src("b", "2")), sink("c", "3"));
    Set<String> parsed = PipelineDependencyLabels.parseAnnotation(annotation);
    assertEquals(2, parsed.size());
    assertTrue(parsed.contains("a_1"));
    assertTrue(parsed.contains("b_2"));
  }

  @Test
  void parseAnnotationHandlesNullAndEmpty() {
    assertTrue(PipelineDependencyLabels.parseAnnotation(null).isEmpty());
    assertTrue(PipelineDependencyLabels.parseAnnotation("").isEmpty());
    assertTrue(PipelineDependencyLabels.parseAnnotation("  ,  ").isEmpty());
  }

  @Test
  void stripDependencyLabelsRemovesOnlyPrefixedEntries() {
    Map<String, String> existing = new LinkedHashMap<>();
    existing.put("app", "hoptimator");                                       // unrelated label, keep
    existing.put(PipelineDependencyLabels.labelKey("db", List.of("t")), "db/t");  // strip
    existing.put("pipeline", "my-pipeline");                                 // keep

    Map<String, String> stripped = PipelineDependencyLabels.stripDependencyLabels(existing);

    assertEquals(2, stripped.size());
    assertTrue(stripped.containsKey("app"));
    assertTrue(stripped.containsKey("pipeline"));
    assertFalse(stripped.containsKey(PipelineDependencyLabels.labelKey("db", List.of("t"))));
  }

  @Test
  void stripDependencyLabelsHandlesNull() {
    Map<String, String> result = PipelineDependencyLabels.stripDependencyLabels(null);
    assertNotNull(result, "null input must return a non-null empty map, not propagate the null");
    assertTrue(result.isEmpty());
  }
}
