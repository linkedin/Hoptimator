package com.linkedin.hoptimator.venice;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.security.SSLFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;


public class LocalControllerClient extends ControllerClient  {

  public LocalControllerClient(String clusterName, String discoveryUrls, Optional<SSLFactory> sslFactory) {
    super(clusterName, discoveryUrls, sslFactory);
  }

  @Override
  protected String discoverLeaderController() {
    try {
      URL controllerUrl = new URL(super.discoverLeaderController());
      return controllerUrl.getProtocol() + "://localhost:" + controllerUrl.getPort();
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
