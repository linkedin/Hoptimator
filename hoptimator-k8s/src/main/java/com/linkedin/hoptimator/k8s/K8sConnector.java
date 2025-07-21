package com.linkedin.hoptimator.k8s;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.Connector;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateSpec;
import com.linkedin.hoptimator.util.Template;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;


/** Configures an abstract Source/Sink by applying TableTemplates */
class K8sConnector implements Connector {

  private final Source source;
  private final K8sContext context;
  private final K8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> tableTemplateApi;

  private static final String KEY_OPTION = "keys";
  private static final String KEY_PREFIX_OPTION = "keyPrefix";
  private static final String KEY_TYPE_OPTION = "keyType";
  private static final String KEY_PREFIX = "KEY_";

  K8sConnector(Source source, K8sContext context) {
    this.source = source;
    this.context = context;
    this.tableTemplateApi = new K8sApi<>(context, K8sApiEndpoints.TABLE_TEMPLATES);
  }

  @Override
  public Map<String, String> configure() throws SQLException {
    Map<String, String> options = new HashMap<>();
    // The PIPELINE schema is a placeholder used for select * type queries, no schema will be present
    if (!source.database().equals("PIPELINE")) {
      RelDataType sourceRowType = HoptimatorDriver.rowType(source, context.connection());
      options = addKeysAsOption(source.options(), sourceRowType);
    }

    Template.Environment env =
        new Template.SimpleEnvironment()
            .with("name", source.database() + "-" + source.table().toLowerCase(Locale.ROOT))
            .with("database", source.database())
            .with("table", source.table())
            .with(options);
    String configs = tableTemplateApi.list()
        .stream()
        .map(V1alpha1TableTemplate::getSpec)
        .filter(Objects::nonNull)
        .filter(x -> x.getDatabases() == null || x.getDatabases().contains(source.database()))
        .filter(x -> x.getMethods() == null || x.getMethods().contains(K8sUtils.method(source)))
        .map(V1alpha1TableTemplateSpec::getConnector)
        .filter(Objects::nonNull)
        .map(x -> new Template.SimpleTemplate(x).render(env))
        .collect(Collectors.joining("\n"));
    Properties props = new Properties();
    try {
      // Preload configs in order to check for 'connector'
      props.load(new StringReader(configs));

      // The order here is intentional. Connection options that allow overrides will have already been overridden above
      // by hints. Connection options that do not allow overrides, should not be overridden by hints.
      // If this were allowed, this can lead to incorrect behavior if hints attempt to override essential properties,
      // e.g. 'connector' or 'topic' for kafka
      if (props.containsKey("connector")) {
        props.putAll(getConnectorHints(options, props.getProperty("connector")));
      }
      props.load(new StringReader(configs));
    } catch (IOException e) {
      // This doesn't seem possible.
      throw new RuntimeException(e);
    }
    Map<String, String> map = new LinkedHashMap<>();
    props.stringPropertyNames().stream().sorted().forEach(k ->
        map.put(k, props.getProperty(k)));
    return map;
  }

  private Map<String, String> getConnectorHints(Map<String, String> options, String connectorName) {
    String connectorHintPrefix = connectorName + "." + (source instanceof Sink ? "sink" : "source");
    return options.entrySet().stream()
        .filter(e -> e.getKey().startsWith(connectorHintPrefix))
        .collect(Collectors.toMap(e -> e.getKey().substring(connectorHintPrefix.length() + 1), Map.Entry::getValue));
  }

  @VisibleForTesting
  static Map<String, String> addKeysAsOption(Map<String, String> options, RelDataType rowType) {
    Map<String, String> newOptions = new LinkedHashMap<>(options);

    // If the keys are already set, don't overwrite them
    if (newOptions.containsKey(KEY_OPTION)) {
      return newOptions;
    }

    String keyString = rowType.getFieldList().stream()
        .map(RelDataTypeField::getName)
        .filter(name -> name.startsWith(KEY_PREFIX))
        .collect(Collectors.joining(";"));
    if (!keyString.isEmpty()) {
      newOptions.put(KEY_OPTION, keyString);
      newOptions.put(KEY_PREFIX_OPTION, KEY_PREFIX);
      newOptions.put(KEY_TYPE_OPTION, "RECORD");
    }
    return newOptions;
  }
}
