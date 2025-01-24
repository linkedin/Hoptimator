package com.linkedin.hoptimator.k8s;

import java.util.Collection;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kubernetes.client.common.KubernetesType;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateSpec.MethodsEnum;


public final class K8sUtils {

  private K8sUtils() {
  }

  public static String canonicalizeName(Collection<String> parts) {
    return parts.stream().filter(x -> x != null).map(x -> canonicalizeName(x)).collect(Collectors.joining("-"));
  }

  // TODO: Robust and reversible canonicalization
  public static String canonicalizeName(String name) {
    return name.toLowerCase(Locale.ROOT).replace("_", "").replace("$", "-");
  }

  public static String canonicalizeName(String database, String name) {
    return Stream.of(database, name).filter(x -> x != null).map(x -> canonicalizeName(x))
        .collect(Collectors.joining("-"));
  }

  // see:
  // https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#rfc-1035-label-names
  public static void checkK8sName(String s) {

    if (s == null || s.isEmpty()) {
      throw new IllegalArgumentException("Name is empty.");
    }

    // contain at most 63 characters
    if (s.length() > 63) {
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
    String lower = obj.getKind().toLowerCase(Locale.ROOT);
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
}
