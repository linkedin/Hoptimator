/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * N.B. this file copy-pasted from Apache Calcite with modifications.
 */
package com.linkedin.hoptimator.util.planner;

import java.util.Properties;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Rule to convert a relational expression from
 * {@link JdbcConvention} to
 * {@link EnumerableConvention}.
 */
public class RemoteToEnumerableConverterRule extends ConverterRule {

  private final Properties connectionProperties;

  /** Creates a RemoteToEnumerableConverterRule. */
  public static RemoteToEnumerableConverterRule create(RemoteConvention inTrait, Properties connectionProperties) {
    return Config.INSTANCE
        .withConversion(RelNode.class, inTrait, EnumerableConvention.INSTANCE,
            "RemoteToEnumerableConverterRule")
        .withRuleFactory(x -> new RemoteToEnumerableConverterRule(x, connectionProperties))
        .toRule(RemoteToEnumerableConverterRule.class);
  }

  /** Called from the Config. */
  protected RemoteToEnumerableConverterRule(Config config, Properties connectionProperties) {
    super(config);
    this.connectionProperties = connectionProperties;
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutTrait());
    return new RemoteToEnumerableConverter(rel.getCluster(), newTraitSet, rel, connectionProperties);
  }
}
