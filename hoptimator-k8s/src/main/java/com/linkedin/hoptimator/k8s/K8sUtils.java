package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateSpec.MethodsEnum;
import com.linkedin.hoptimator.util.IdentifierUtils;
import io.kubernetes.client.common.KubernetesType;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientException;
import java.util.Collection;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public final class K8sUtils {

  // Kubernetes limits most resource (DNS-subdomain) names to 253 characters. The names validated
  // here become the metadata.name of Hoptimator custom resources, so this is the relevant ceiling.
  // Note this is distinct from the stricter 63-character DNS-label limit, which applies to label
  // VALUES and to names of label-typed resources such as Services -- not to these resource names.
  // Kept equal to the parser's identifier limit so parse-time and name-validation limits stay aligned.
  public static final int MAX_NAME_LENGTH = IdentifierUtils.MAX_IDENTIFIER_LENGTH;

  private K8sUtils() {
  }

  public static String canonicalizeName(Collection<String> parts) {
    return parts.stream().filter(Objects::nonNull).map(K8sUtils::canonicalizeName).collect(Collectors.joining("-"));
  }

  // TODO: Robust and reversible canonicalization
  public static String canonicalizeName(String name) {
    return name.toLowerCase(Locale.ROOT).replace("_", "").replace("$", "-");
  }

  public static String canonicalizeName(String database, String name) {
    return Stream.of(database, name).filter(Objects::nonNull).map(K8sUtils::canonicalizeName)
        .collect(Collectors.joining("-"));
  }

  // see:
  // https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-subdomain-names
  public static void checkK8sName(String s) {

    if (s == null || s.isEmpty()) {
      throw new IllegalArgumentException("Name is empty.");
    }

    // contain at most 253 characters
    if (s.length() > MAX_NAME_LENGTH) {
      throw new IllegalArgumentException("Name is too long: " + s);
    }

    // contain only lowercase alphanumeric characters or '-'
    if (!s.matches("[a-z0-9\\-]+")) {
      throw new IllegalArgumentException("Name contains illegal characters: " + s);
    }

    // start with an alphabetic character {
    if (!s.matches("[a-z]+.*")) {
      throw new IllegalArgumentException("Name starts with illegal character: " + s);
    }

    // end with an alphanumeric character
    if (!s.matches(".*[a-z0-9]$")) {
      throw new IllegalArgumentException("Name ends with illegal character: " + s);
    }
  }

  public static String guessPlural(KubernetesType obj) {
    return guessPlural(obj.getKind());
  }

  public static String guessPlural(String kind) {
    String lower = kind.toLowerCase(Locale.ROOT);
    if (lower.endsWith("y")) {
      return lower.substring(0, lower.length() - 1) + "ies";
    } else {
      return lower + "s";
    }
  }

  static MethodsEnum method(Source source) {
    if (source instanceof Sink) {
      return MethodsEnum.MODIFY;  // sinks are modified
    } else {
      return MethodsEnum.SCAN;    // sources are scanned
    }
  }

  static void checkResponse(String msg, KubernetesApiResponse<?> resp) throws SQLException {
    checkResponse(() -> msg, resp);
  }

  /**
   * Executes a Kubernetes client call, normalizing an unchecked connectivity failure into a typed
   * {@link SQLException}.
   *
   * <p>The Kubernetes client reports a genuine connectivity failure (connection refused, unknown
   * host, socket timeout) as an unchecked {@link IllegalStateException} wrapping an
   * {@link IOException}, thrown from the terminal {@code generic()/dynamic().get/list/create/delete/
   * update} call — HTTP-status errors (404, 409, ...) instead come back in the response and are
   * classified by {@link #checkResponse}. Left unchecked, the connectivity failure would escape the
   * {@link SQLException} contract as an unclassified error, and — worse — a swallowed read could
   * report success while nothing was actually resolved or deployed. Every backend call (in
   * {@link K8sApi} and {@link K8sYamlApi} alike) routes through here so all SPIs get a uniform
   * classification: an IOException-caused failure is a {@link SQLTransientException} (a retryable
   * connectivity blip); any other {@link IllegalStateException} is surfaced as a
   * {@link SQLNonTransientException} rather than being masked as retryable.
   */
  static <R> R normalizingCall(String action, Supplier<R> request) throws SQLException {
    try {
      return request.get();
    } catch (IllegalStateException e) {
      if (e.getCause() instanceof IOException) {
        throw new SQLTransientException("Could not reach Kubernetes to " + action + ": " + e.getMessage(), e);
      }
      throw new SQLNonTransientException("Kubernetes call failed to " + action + ": " + e.getMessage(), e);
    }
  }

  static void checkResponse(Supplier<String> msgSupplier, KubernetesApiResponse<?> resp) throws SQLException {
    try {
      resp.throwsApiException();
    } catch (ApiException e) {
      switch (resp.getHttpStatusCode()) {
      case 408: // request timeout
      case 409: // conflict (e.g. optimistic-concurrency resourceVersion clash)
      case 410: // gone (stale resourceVersion)
      case 412: // precondition failed
      case 429: // too many requests (rate limited)
      case 500: // internal server error
      case 502: // bad gateway
      case 503: // service unavailable
      case 504: // gateway timeout
        // Retryable: server-side or optimistic-concurrency failures that may succeed on retry.
        throw new SQLTransientException(msgSupplier.get(), null, resp.getHttpStatusCode(), e);
      default:
        // Definitive client errors (e.g. 404 not found, 400 bad request, 403 forbidden): the
        // request won't succeed on retry, so surface a non-transient error rather than masking it
        // as retryable.
        throw new SQLNonTransientException(msgSupplier.get(), null, resp.getHttpStatusCode(), e);
      }
    }
  }

  public static DynamicKubernetesObject overrideNamespaceFromContext(K8sContext context, DynamicKubernetesObject obj) {
    if (obj.getMetadata().getNamespace() == null) {
      obj.setMetadata(obj.getMetadata().namespace(context.namespace()));
    }
    return obj;
  }
}
