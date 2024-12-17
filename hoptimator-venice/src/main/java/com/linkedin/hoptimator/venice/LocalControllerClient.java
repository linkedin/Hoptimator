package com.linkedin.hoptimator.venice;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.security.SSLFactory;


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
      throw new RuntimeException(e);
    }
  }
}
