package com.linkedin.hoptimator.operator.subscription;

import io.kubernetes.client.extended.controller.reconciler.Request;

import com.linkedin.hoptimator.catalog.AvroConverter;
import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.planner.Pipeline;
import com.linkedin.hoptimator.models.V1alpha1Subscription;

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

  public SubscriptionEnvironment(V1alpha1Subscription subscription, Pipeline pipeline) {
    super(subscription.getSpec().getHints());
    export("pipeline.namespace", subscription.getMetadata().getNamespace());
    export("pipeline.name", subscription.getMetadata().getName());
    export("pipeline.avroSchema", AvroConverter.avro("com.linkedin.hoptimator", "OutputRecord",
      pipeline.outputType()).toString(false));
  }
}
