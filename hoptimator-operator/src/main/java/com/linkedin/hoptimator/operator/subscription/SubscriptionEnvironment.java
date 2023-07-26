package com.linkedin.hoptimator.operator.subscription;

import io.kubernetes.client.extended.controller.reconciler.Request;

import com.linkedin.hoptimator.catalog.AvroConverter;
import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.planner.Pipeline;

import java.util.Map;
import java.util.Collections;

/**
 * Exposes Subscription variables to resource templates.
 *
 * Variables have a `pipeline.` prefix (even though they come from the
 * Subscription object), because the planner is unaware of Subscriptions.
 * For example, the CLI constructs pipelines without any corresponding
 * Subscription object. In future, we may have additional K8s objects
 * that result in pipelines.
 *
 * The exported variables include:
 *
 *  - `pipeline.namespace`, the K8s namespace where the pipeline should be
 *     deployed. This is a recommendation -- templates may elect to ignore it.
 *  - `pipeline.name`, a unique name for the pipeline. Templates can use this
 *     as a basis for deriving K8s object names, Kafka topic names, etc. The
 *     name is guaranteed to be a valid K8s object name, e.g. `my-subscription`.
 *  - `pipeline.avroSchema`, an Avro schema for the pipeline's output type.
 *
 * In addition, any "hints" in the Subscription object (`.spec.hints`) are
 * exported as-is. These can be used to provide optional properties to
 * templates. When using such hints in a template, ensure that you provide a
 * default value, e.g. `{{numPartitions:null}``, since they will usually be
 * missing.
 */
public class SubscriptionEnvironment extends Resource.SimpleEnvironment {

  public SubscriptionEnvironment(String namespace, String name, Pipeline pipeline,
      Map<String, String> hints) {
    exportAll(hints);
    export("pipeline.namespace", namespace);
    export("pipeline.name", name);
    export("pipeline.avroSchema", AvroConverter.avro("com.linkedin.hoptimator", "OutputRecord",
      pipeline.outputType()).toString(false));
  }

  public SubscriptionEnvironment(String namespace, String name, Pipeline pipeline) {
    this(namespace, name, pipeline, Collections.emptyMap());
  }
}
