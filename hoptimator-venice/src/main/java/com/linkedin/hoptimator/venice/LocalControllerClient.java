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
      return transformUrl(super.discoverLeaderController());
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  protected static String transformUrl(String rawUrl) throws MalformedURLException {
    URL controllerUrl = new URL(rawUrl);
    return controllerUrl.getProtocol() + "://localhost:" + controllerUrl.getPort();
  }
}
