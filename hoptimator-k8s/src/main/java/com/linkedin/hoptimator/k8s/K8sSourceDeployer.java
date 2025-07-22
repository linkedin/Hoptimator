package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateSpec;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.Template;
import io.kubernetes.client.util.Yaml;

/**
 * Specifies an abstract Source with concrete YAML by applying TableTemplates.
 */
class K8sSourceDeployer extends K8sYamlDeployer {

    private final Map<String, String> hints;
    private final Source source;
    private final K8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> tableTemplateApi;
    private static final String DELIMITER = "-";
    private static final String JOB_SUFFIX = "job";
    private static final String CONTAINER_SUFFIX = "container";
    private static final String JOB_YAML_CONFIG = "job.yaml";

    K8sSourceDeployer(Source source, K8sContext context, Properties connectionProperties) {
        super(context);
        this.hints = DeploymentService.parseHints(connectionProperties);
        this.source = source;
        this.tableTemplateApi = new K8sApi<>(context, K8sApiEndpoints.TABLE_TEMPLATES);
    }

    @Override
    public List<String> specify() throws SQLException {
        String name = K8sUtils.canonicalizeName(source.database(), source.table());
        Template.Environment env =
                new Template.SimpleEnvironment()
                        .with("name", name)
                        .with("database", source.database())
                        .with("schema", source.schema())
                        .with("table", source.table())
                        .with(source.options())
                        .with(hints)
                        .with(JOB_YAML_CONFIG, Yaml.dump(
                                K8sJob.builder()
                                        .jobName(String.join(DELIMITER, source.table(), JOB_SUFFIX))
                                        .containerName(String.join(DELIMITER, source.table(), CONTAINER_SUFFIX))
                                        .build()));

        return tableTemplateApi.list()
                .stream()
                .map(V1alpha1TableTemplate::getSpec)
                .filter(Objects::nonNull)
                .filter(x -> x.getDatabases() == null || x.getDatabases().contains(source.database()))
                .filter(x -> x.getMethods() == null || x.getMethods().contains(K8sUtils.method(source)))
                .map(V1alpha1TableTemplateSpec::getYaml)
                .filter(Objects::nonNull)
                .map(x -> new Template.SimpleTemplate(x).render(env))
                .filter(s -> !s.isEmpty()) // Filter out empty templates (which might have errored out due to missing hint expressions)
                .collect(Collectors.toList());
    }
}
