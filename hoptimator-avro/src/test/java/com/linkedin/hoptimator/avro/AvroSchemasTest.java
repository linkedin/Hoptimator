package com.linkedin.hoptimator.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class AvroSchemasTest {

  // --- cloneField ---

  @Test
  void cloneFieldProducesUnownedCopy() {
    Schema.Field original = new Schema.Field("a", Schema.create(Schema.Type.STRING), "doc", null);
    // Add to a record so position is set (this is what setFields would do).
    Schema parent = Schema.createRecord("P", null, "ns", false);
    parent.setFields(List.of(original));
    assertFalse(original.pos() == -1, "precondition: position assigned by setFields");

    Schema.Field clone = AvroSchemas.cloneField("a", original);

    // Cloned Field is unowned — can be installed in another record without Avro rejecting it.
    Schema other = Schema.createRecord("Q", null, "ns", false);
    other.setFields(List.of(clone));
    assertEquals("a", other.getField("a").name());
  }

  @Test
  void cloneFieldRenamesField() {
    Schema.Field original = new Schema.Field("a", Schema.create(Schema.Type.STRING), null, null);
    Schema.Field clone = AvroSchemas.cloneField("KEY_a", original);
    assertEquals("KEY_a", clone.name());
    assertSame(original.schema(), clone.schema(), "type schema shared by reference");
  }

  @Test
  void cloneFieldPreservesOrderAliasesPropsAndDefault() {
    Schema.Field original = new Schema.Field("a", Schema.create(Schema.Type.STRING), "doc",
        "fallback", Schema.Field.Order.DESCENDING);
    original.addProp("java", Map.of("class", "com.linkedin.common.urn.Urn"));
    original.addProp("compliance", "NONE");
    original.addAlias("legacyName");

    Schema.Field clone = AvroSchemas.cloneField("a", original);

    assertEquals("doc", clone.doc());
    assertEquals("fallback", clone.defaultVal());
    assertEquals(Schema.Field.Order.DESCENDING, clone.order());
    assertTrue(clone.aliases().contains("legacyName"));
    assertEquals(Map.of("class", "com.linkedin.common.urn.Urn"), clone.getObjectProp("java"));
    assertEquals("NONE", clone.getObjectProp("compliance"));
  }

  @Test
  void cloneFieldHandlesNoDefaultValue() {
    Schema.Field original = new Schema.Field("a", Schema.create(Schema.Type.STRING), null, null);
    // `original` has no default (the null passed above is the default-value arg; field has no
    // default because it's not a nullable union). hasDefaultValue() is false.
    assertFalse(original.hasDefaultValue());

    Schema.Field clone = AvroSchemas.cloneField("a", original);

    assertFalse(clone.hasDefaultValue(), "no-default fields clone without inventing one");
  }

  // --- mergeKeyIntoValue ---

  @Test
  void mergeKeyIntoValueInheritsValueSchemaIdentityAndProps() {
    Schema keySchema = SchemaBuilder.record("Key").fields().requiredString("id").endRecord();
    Schema valueSchema = SchemaBuilder.record("User").namespace("com.linkedin.foo")
        .aliases("com.linkedin.foo.UserV1").doc("User doc")
        .prop("owningTeam", "urn:li:internalTeam:feed")
        .fields().requiredString("name").endRecord();

    Schema merged = AvroSchemas.mergeKeyIntoValue(keySchema, valueSchema, "KEY_", "KEY");

    assertEquals("User doc", merged.getDoc());
    assertTrue(merged.getAliases().contains("com.linkedin.foo.UserV1"));
    assertEquals("urn:li:internalTeam:feed", merged.getObjectProp("owningTeam"));
  }

  @Test
  void mergeKeyIntoValueThrowsForNonRecordValue() {
    assertThrows(IllegalArgumentException.class,
        () -> AvroSchemas.mergeKeyIntoValue(
            Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.STRING), "KEY_", "KEY"));
  }

  @Test
  void mergeKeyIntoValueStructKeyGetsPrefix() {
    Schema keySchema = SchemaBuilder.record("Key").namespace("com.linkedin.k").fields()
        .requiredString("id").requiredInt("partition").endRecord();
    Schema valueSchema = SchemaBuilder.record("User").namespace("com.linkedin.v").fields()
        .requiredString("name").endRecord();

    Schema merged = AvroSchemas.mergeKeyIntoValue(keySchema, valueSchema, "KEY_", "KEY");

    assertEquals(List.of("KEY_id", "KEY_partition", "name"),
        merged.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()));
    assertEquals("com.linkedin.v", merged.getNamespace(), "merged inherits value namespace");
    assertEquals("User", merged.getName(), "merged inherits value name");
  }

  @Test
  void mergeKeyIntoValuePrimitiveKeyBecomesSingleNamedField() {
    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = SchemaBuilder.record("User").namespace("com.linkedin.v").fields()
        .requiredString("name").endRecord();

    Schema merged = AvroSchemas.mergeKeyIntoValue(keySchema, valueSchema, "KEY_", "KEY");

    assertEquals(2, merged.getFields().size());
    assertEquals("KEY", merged.getFields().get(0).name());
    assertEquals(Schema.Type.STRING, merged.getFields().get(0).schema().getType());
    assertEquals("name", merged.getFields().get(1).name());
  }

  @Test
  void mergeKeyIntoValuePreservesNestedRecordIdentities() {
    // Nested records in value (or key) keep their source namespaces because the Schema reference
    // is shared by cloneField — no round-trip through a type system.
    Schema address = SchemaBuilder.record("Address").namespace("com.linkedin.addr").fields()
        .requiredString("city").endRecord();
    Schema valueSchema = SchemaBuilder.record("User").namespace("com.linkedin.v").fields()
        .name("address").type(address).noDefault().endRecord();

    Schema merged = AvroSchemas.mergeKeyIntoValue(
        Schema.create(Schema.Type.STRING), valueSchema, "KEY_", "KEY");

    Schema addrField = merged.getField("address").schema();
    assertSame(address, addrField, "nested record schema shared by reference");
    assertEquals("com.linkedin.addr", addrField.getNamespace());
  }

  @Test
  void mergeKeyIntoValueReusedRecordsRenderAsNamedReferences() {
    // Same Schema instance referenced twice in the value → Avro serializer writes the record
    // definition once and references by FQN thereafter.
    Schema shared = SchemaBuilder.record("Shared").namespace("com.linkedin.s").fields()
        .requiredString("v").endRecord();
    Schema valueSchema = SchemaBuilder.record("User").namespace("com.linkedin.v").fields()
        .name("first").type(shared).noDefault()
        .name("second").type(shared).noDefault()
        .endRecord();

    Schema merged = AvroSchemas.mergeKeyIntoValue(
        Schema.create(Schema.Type.STRING), valueSchema, "KEY_", "KEY");
    String json = merged.toString(false);

    int firstDef = json.indexOf("\"name\":\"Shared\"");
    int secondDef = json.indexOf("\"name\":\"Shared\"", firstDef + 1);
    assertEquals(-1, secondDef, "reused record serialized once; got " + json);
  }

  @Test
  void mergeKeyIntoValueKeyFieldsKeepFieldProps() {
    // Custom props on key fields (common for LinkedIn schemas — "java", "validate", etc.) should
    // survive the key→KEY_ rename.
    Schema keySchema = SchemaBuilder.record("Key").fields()
        .name("id").type().stringType().noDefault()
        .endRecord();
    keySchema.getField("id").addProp("compliance", "NONE");
    Schema valueSchema = SchemaBuilder.record("V").namespace("v").fields()
        .requiredString("x").endRecord();

    Schema merged = AvroSchemas.mergeKeyIntoValue(keySchema, valueSchema, "KEY_", "KEY");

    assertEquals("NONE", merged.getField("KEY_id").getObjectProp("compliance"));
  }

  @Test
  void mergeKeyIntoValueSchemaRoundTripsThroughAvroParser() {
    // End-to-end sanity: the merged schema is parseable — ensures we don't produce anything that
    // would trip Avro's validation (e.g. duplicate names, unresolvable references).
    Schema keySchema = SchemaBuilder.record("Key").namespace("com.linkedin.k").fields()
        .requiredString("id").endRecord();
    Schema valueSchema = SchemaBuilder.record("User").namespace("com.linkedin.v").fields()
        .requiredString("name").endRecord();
    Schema merged = AvroSchemas.mergeKeyIntoValue(keySchema, valueSchema, "KEY_", "KEY");

    Schema reparsed = new Schema.Parser().parse(merged.toString(true));
    assertNotNull(reparsed);
    assertEquals("com.linkedin.v.User", reparsed.getFullName());
    assertEquals(2, reparsed.getFields().size());
  }
}
