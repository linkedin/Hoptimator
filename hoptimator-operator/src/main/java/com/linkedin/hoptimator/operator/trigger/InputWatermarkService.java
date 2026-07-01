package com.linkedin.hoptimator.operator.trigger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Loads {@link InputWatermarkProvider} plugins via the Service Provider Interface. */
public final class InputWatermarkService {

  private static final Logger LOG = LoggerFactory.getLogger(InputWatermarkService.class);

  private InputWatermarkService() {
  }

  public static Collection<InputWatermarkProvider> providers() {
    ServiceLoader<InputWatermarkProvider> loader = ServiceLoader.load(InputWatermarkProvider.class);
    List<InputWatermarkProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(providers::add);
    return providers;
  }

  /**
   * Returns the first data-time completeness watermark reported by any registered provider for the
   * input, or {@link Optional#empty()} if none handles it. A provider that throws is logged and
   * skipped, so one misbehaving source never blocks a trigger.
   */
  public static Optional<Instant> completeThrough(Collection<InputWatermarkProvider> providers, TriggerInput input) {
    for (InputWatermarkProvider provider : providers) {
      try {
        Optional<Instant> watermark = provider.completeThrough(input);
        if (watermark != null && watermark.isPresent()) {
          return watermark;
        }
      } catch (Exception e) {
        LOG.warn("InputWatermarkProvider {} failed for input {}; skipping.",
            provider.getClass().getName(), input, e);
      }
    }
    return Optional.empty();
  }
}
