package com.linkedin.hoptimator.venice;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.security.SSLFactory;


public class LocalControllerClient extends ControllerClient  {

  private final List<String> controllerDiscoveryUrls;

  public LocalControllerClient(String clusterName, String discoveryUrls, Optional<SSLFactory> sslFactory) {
    super(clusterName, discoveryUrls, sslFactory);
    this.controllerDiscoveryUrls =
        Arrays.stream(discoveryUrls.split(",")).map(String::trim).collect(Collectors.toList());
  }

  @Override
  protected String discoverLeaderController() {
    return this.controllerDiscoveryUrls.get(0);
  }
}
