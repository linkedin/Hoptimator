package com.linkedin.hoptimator.operator.subscription;

import com.linkedin.hoptimator.catalog.AvroConverter;
import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.planner.Pipeline;


/**
 * Exposes Subscription variables to resource templates.
 * <p>
 * Variables have a `pipeline.` prefix (even though they come from the
 * Subscription object), because the planner is unaware of Subscriptions.
 * For example, the CLI constructs pipelines without any corresponding
 * Subscription object. In the future, we may have additional K8s objects
 * that result in pipelines.
 * <p>
 * The exported variables include:
 * <p>
 *  - `pipeline.namespace`, the K8s namespace where the pipeline should be
 *     deployed. This is a recommendation -- templates may elect to ignore it.
 *  - `pipeline.name`, a unique name for the pipeline. Templates can use this
 *     as a basis for deriving K8s object names, Kafka topic names, etc. The
 *     name is guaranteed to be a valid K8s object name, e.g. `my-subscription`.
 *  - `pipeline.avroSchema`, an Avro schema for the pipeline's output type.
 */
public class SubscriptionEnvironment extends Resource.SimpleEnvironment {

  public SubscriptionEnvironment(String namespace, String name, Pipeline pipeline) {
    export("pipeline.namespace", namespace);
    export("pipeline.name", name);
    export("pipeline.avroSchema",
        AvroConverter.avro("com.linkedin.hoptimator", "OutputRecord", pipeline.outputType()).toString(false));
  }
}
