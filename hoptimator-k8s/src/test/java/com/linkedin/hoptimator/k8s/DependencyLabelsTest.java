package com.linkedin.hoptimator.k8s;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class DependencyLabelsTest {

  private static Source src(String db, String... path) {
    return new Source(db, Arrays.asList(path), Collections.emptyMap());
  }

  private static Sink sink(String db, String... path) {
    return new Sink(db, Arrays.asList(path), Collections.emptyMap());
  }

  @Test
  void identifierJoinsDatabaseAndPath() {
    // Separator is "_" so the identifier is also a valid K8s label value out of the box.
    assertEquals("mydb_a.b.c", DependencyLabels.identifier("mydb", Arrays.asList("a", "b", "c")));
  }

  @Test
  void slugIsDeterministic() {
    String s1 = DependencyLabels.slug("db", Arrays.asList("foo", "bar"));
    String s2 = DependencyLabels.slug("db", Arrays.asList("foo", "bar"));
    assertEquals(s1, s2);
  }

  @Test
  void slugVariesByDatabase() {
    String a = DependencyLabels.slug("db1", Collections.singletonList("t"));
    String b = DependencyLabels.slug("db2", Collections.singletonList("t"));
    assertNotEquals(a, b);
  }

  @Test
  void slugVariesByPath() {
    String a = DependencyLabels.slug("db", Arrays.asList("schema", "t"));
    String b = DependencyLabels.slug("db", Arrays.asList("schema", "u"));
    assertNotEquals(a, b);
  }

  @Test
  void labelKeyFitsKubernetesNameLimit() {
    // Long path stressing the slug — name portion (after the /) must be ≤ 63 chars.
    String key = DependencyLabels.labelKey(
        "a-really-long-database-name",
        Arrays.asList("catalog", "schema", "a_very_long_table_name_that_exceeds_sixty_three_chars"));
    String namePortion = key.substring(key.indexOf('/') + 1);
    assertTrue(namePortion.length() <= 63, "name portion must be ≤63 chars, got " + namePortion.length());
    assertTrue(namePortion.matches("[a-z0-9]([-a-z0-9_.]*[a-z0-9])?"),
        "name portion must match K8s label-name regex, got: " + namePortion);
  }

  private static V1ObjectMeta stamp(java.util.List<Source> sources, Sink sink) {
    V1ObjectMeta meta = new V1ObjectMeta();
    DependencyLabels.stamp(meta, sources,
        sink == null ? Collections.emptyList() : Collections.singletonList(sink));
    return meta;
  }

  @Test
  void stampIncludesSourcesAndSink() {
    // Both edges matter: dropping a source orphans readers; dropping a sink orphans writers.
    V1ObjectMeta meta = stamp(
        Arrays.asList(src("kafka1", "events"), src("venice1", "store")),
        sink("mysql1", "outbox"));

    Map<String, String> labels = meta.getLabels();
    assertEquals(3, labels.size());
    assertTrue(labels.containsKey(DependencyLabels.labelKey("kafka1", Collections.singletonList("events"))));
    assertTrue(labels.containsKey(DependencyLabels.labelKey("venice1", Collections.singletonList("store"))));
    assertTrue(labels.containsKey(DependencyLabels.labelKey("mysql1", Collections.singletonList("outbox"))));
  }

  @Test
  void stampHandlesNullSink() {
    V1ObjectMeta meta = stamp(Collections.singletonList(src("db", "t")), null);
    assertEquals(1, meta.getLabels().size());
    assertNull(meta.getAnnotations().get(DependencyLabels.ANNOTATION_KEY_SINKS));
  }

  @Test
  void stampCollapsesSelfLoopIntoOneLabel() {
    // Self-loop pipeline: source and sink share a slug, so the map collapses to one entry
    // rather than producing duplicate keys.
    V1ObjectMeta meta = stamp(Collections.singletonList(src("db", "t")), sink("db", "t"));
    assertEquals(1, meta.getLabels().size());
  }

  @Test
  void stampLabelValueTruncatedAtSixtyThreeChars() {
    String longPath = "this_is_a_really_long_table_name_that_exceeds_sixty_three_chars_by_a_lot";
    V1ObjectMeta meta = stamp(Collections.singletonList(src("db", longPath)), null);
    String value = meta.getLabels().values().iterator().next();
    assertTrue(value.length() <= 63);
  }

  @Test
  void stampLabelValueStrippedWhenTruncationLandsOnSeparator() {
    // Regression: a real identifier truncated at exactly 63 chars ended on '_', which K8s
    // rejects. This dummy path is sized so truncation lands on a '_' too; it must be stripped.
    V1ObjectMeta meta = stamp(
        Collections.singletonList(
            src("__dummy-database", "SCHEMA", "dummy_table_name_for_internal_testing____")),
        null);
    String value = meta.getLabels().values().iterator().next();

    assertTrue(value.length() <= 63);
    assertTrue(value.matches("(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?"),
        "value must satisfy K8s label-value regex, got: " + value);
    assertEquals("dummy-database_SCHEMA.dummy_table_name_for_internal_testing", value,
        "leading/trailing separator must be stripped, got: " + value);
  }

  @Test
  void stampLabelValueIsKubernetesLabelValueCompliant() {
    // K8s label values must match (([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?
    // — the identifier separator is "_" precisely so this holds out of the box for typical
    // (database, path) shapes seen in production.
    V1ObjectMeta meta = stamp(
        Collections.singletonList(src("ads-database", "ADS", "PAGE_VIEWS")), null);
    String value = meta.getLabels().values().iterator().next();

    assertTrue(value.length() <= 63);
    assertTrue(value.matches("(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?"),
        "value must satisfy K8s label-value regex, got: " + value);
    assertFalse(value.contains("/"), "no '/' separator should leak into the label value");
  }

  @Test
  void stampSourcesAnnotationListsOnlySources() {
    V1ObjectMeta meta = stamp(
        Arrays.asList(src("kafka", "a"), src("venice", "b")), sink("mysql", "c"));
    String annotation = meta.getAnnotations().get(DependencyLabels.ANNOTATION_KEY_SOURCES);
    assertTrue(annotation.contains("kafka_a"));
    assertTrue(annotation.contains("venice_b"));
    assertFalse(annotation.contains("mysql_c"), "sinks must not appear in sources annotation");
  }

  @Test
  void stampSourcesAnnotationDeduplicatesIdenticalSources() {
    V1ObjectMeta meta = stamp(Arrays.asList(src("db", "t"), src("db", "t")), null);
    assertEquals("db_t", meta.getAnnotations().get(DependencyLabels.ANNOTATION_KEY_SOURCES));
  }

  @Test
  void stampSinkAnnotationCarriesSinkIdentifier() {
    V1ObjectMeta meta = stamp(Collections.emptyList(), sink("mysql", "c"));
    assertEquals("mysql_c", meta.getAnnotations().get(DependencyLabels.ANNOTATION_KEY_SINKS));
  }

  @Test
  void stampSkipsNullDatabaseSource() {
    // Triggers may carry a source with a null database (e.g. table-name-only DROP); the stamp
    // helper drops those rather than producing a "null_<path>" identifier.
    V1ObjectMeta meta = stamp(
        Collections.singletonList(src(null, "t")), null);
    assertTrue(meta.getLabels() == null || meta.getLabels().isEmpty(),
        "no depends-on labels expected when source has null database");
    assertNull(meta.getAnnotations() == null ? null
        : meta.getAnnotations().get(DependencyLabels.ANNOTATION_KEY_SOURCES));
  }

  @Test
  void parseAnnotationRoundtripFromSourcesValue() {
    V1ObjectMeta meta = stamp(Arrays.asList(src("a", "1"), src("b", "2")), null);
    String annotation = meta.getAnnotations().get(DependencyLabels.ANNOTATION_KEY_SOURCES);
    Set<String> parsed = DependencyLabels.parseAnnotation(annotation);
    assertEquals(2, parsed.size());
    assertTrue(parsed.contains("a_1"));
    assertTrue(parsed.contains("b_2"));
  }

  @Test
  void parseAnnotationHandlesNullAndEmpty() {
    assertTrue(DependencyLabels.parseAnnotation(null).isEmpty());
    assertTrue(DependencyLabels.parseAnnotation("").isEmpty());
    assertTrue(DependencyLabels.parseAnnotation("  ,  ").isEmpty());
  }
}
