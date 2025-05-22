package com.linkedin.hoptimator.util;

import com.linkedin.hoptimator.SnapshotProvider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;


public final class SnapshotService {

  private static List<SnapshotProvider> providers = null;

  private SnapshotService() {
  }

  public static void snapshot(List<String> specs, Properties connectionProperties) throws SQLException {
    for (SnapshotProvider provider : providers()) {
      provider.snapshot(specs, connectionProperties);
    }
  }

  public static void restore() throws SQLException {
    for (SnapshotProvider provider : providers()) {
      provider.restore();
    }
  }

  private static synchronized Collection<SnapshotProvider> providers() {
    if (providers != null) {
      return providers;
    }
    providers = new ArrayList<>();
    ServiceLoader<SnapshotProvider> loader = ServiceLoader.load(SnapshotProvider.class);
    loader.iterator().forEachRemaining(providers::add);
    return providers;
  }
}
