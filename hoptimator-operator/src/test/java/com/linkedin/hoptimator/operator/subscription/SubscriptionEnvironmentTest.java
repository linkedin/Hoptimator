package com.linkedin.hoptimator.operator.subscription;

import com.linkedin.hoptimator.planner.Pipeline;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class SubscriptionEnvironmentTest {

  @Mock
  private Pipeline mockPipeline;

  private RelDataType buildSimpleRowType() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    return typeFactory.builder().add("col", SqlTypeName.VARCHAR).build();
  }

  @Test
  void exportsPipelineNamespace() {
    when(mockPipeline.outputType()).thenReturn(buildSimpleRowType());
    SubscriptionEnvironment env = new SubscriptionEnvironment("test-ns", "my-sub", mockPipeline);
    assertEquals("test-ns", env.getOrDefault("pipeline.namespace", () -> "missing"));
  }

  @Test
  void exportsPipelineName() {
    when(mockPipeline.outputType()).thenReturn(buildSimpleRowType());
    SubscriptionEnvironment env = new SubscriptionEnvironment("ns", "my-subscription", mockPipeline);
    assertEquals("my-subscription", env.getOrDefault("pipeline.name", () -> "missing"));
  }

  @Test
  void exportsPipelineAvroSchema() {
    when(mockPipeline.outputType()).thenReturn(buildSimpleRowType());
    SubscriptionEnvironment env = new SubscriptionEnvironment("ns", "my-sub", mockPipeline);
    String avroSchema = env.getOrDefault("pipeline.avroSchema", () -> "missing");
    assertNotNull(avroSchema);
    // Avro schema should be a JSON string containing the record name
    assertEquals(true, avroSchema.contains("OutputRecord") || avroSchema.contains("string"));
  }

  @Test
  void avroSchemaContainsFieldName() {
    when(mockPipeline.outputType()).thenReturn(buildSimpleRowType());
    SubscriptionEnvironment env = new SubscriptionEnvironment("ns", "my-sub", mockPipeline);
    String avroSchema = env.getOrDefault("pipeline.avroSchema", () -> "missing");
    // The field 'col' should appear in the Avro schema JSON
    assertEquals(true, avroSchema.contains("col"));
  }

  @Test
  void allThreePropertiesPresent() {
    when(mockPipeline.outputType()).thenReturn(buildSimpleRowType());
    SubscriptionEnvironment env = new SubscriptionEnvironment("prod-ns", "sub-name", mockPipeline);
    assertEquals("prod-ns", env.getOrDefault("pipeline.namespace", () -> "missing"));
    assertEquals("sub-name", env.getOrDefault("pipeline.name", () -> "missing"));
    assertNotNull(env.getOrDefault("pipeline.avroSchema", () -> null));
  }
}
