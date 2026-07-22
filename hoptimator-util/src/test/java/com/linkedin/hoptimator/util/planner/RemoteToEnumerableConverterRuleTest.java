package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.DeploymentContext;

import com.linkedin.hoptimator.Engine;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;


import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@ExtendWith(MockitoExtension.class)
class RemoteToEnumerableConverterRuleTest {

  @Mock
  private Engine mockEngine;

  @Mock
  private DeploymentContext mockConnection;

  // If create() returns null (NullReturnVals), assertNotNull fails.
  @Test
  void testCreateReturnsNonNull() {
    RemoteConvention convention = new RemoteConvention("test-remote", mockEngine);

    RemoteToEnumerableConverterRule rule = RemoteToEnumerableConverterRule.create(convention, mockConnection);

    assertNotNull(rule, "create() must return a non-null RemoteToEnumerableConverterRule");
  }

  @Test
  void testCreatedRuleHasCorrectDescription() {
    RemoteConvention convention = new RemoteConvention("test-remote", mockEngine);

    RemoteToEnumerableConverterRule rule = RemoteToEnumerableConverterRule.create(convention, mockConnection);

    assertNotNull(rule.toString(), "created rule must have a non-null description");
  }

  @Test
  void testConvertProducesRemoteToEnumerableConverter() {
    RemoteConvention convention = new RemoteConvention("test-remote", mockEngine);
    RemoteToEnumerableConverterRule rule =
        RemoteToEnumerableConverterRule.create(convention, mockConnection);

    // A trivial VALUES node gives convert() a real cluster + trait set to re-home.
    SchemaPlus root = Frameworks.createRootSchema(false);
    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(root)
        .typeSystem(RelDataTypeSystem.DEFAULT)
        .build();
    RelBuilder builder = RelBuilder.create(config);
    RelNode values = builder.values(new String[] {"C"}, 1).build();

    RelNode converted = rule.convert(values);

    assertNotNull(converted, "convert() must return a converter node");
    assertInstanceOf(RemoteToEnumerableConverter.class, converted);
  }
}
