package com.linkedin.hoptimator.venice;

import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class LocalControllerClientTest {

  @Test
  void transformUrlReplacesHostWithLocalhost() throws MalformedURLException {
    String result = LocalControllerClient.transformUrl("http://remote-host.example.com:5555");
    assertEquals("http://localhost:5555", result);
  }

  @Test
  void transformUrlPreservesProtocolAndPort() throws MalformedURLException {
    String result = LocalControllerClient.transformUrl("https://venice-controller.prod:8443");
    assertEquals("https://localhost:8443", result);
  }

  @Test
  void transformUrlStripsPath() throws MalformedURLException {
    String result = LocalControllerClient.transformUrl("http://some-host:9000/leader");
    assertEquals("http://localhost:9000", result);
  }

  @Test
  void transformUrlThrowsMalformedUrlExceptionForInvalidUrl() {
    assertThrows(MalformedURLException.class, () -> LocalControllerClient.transformUrl("not-a-url"));
  }
}
