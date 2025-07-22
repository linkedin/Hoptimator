package com.linkedin.hoptimator.k8s;

import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;

import java.util.Collections;

public final class K8sJob {

    // Default values
    public static final String DEFAULT_IMAGE = "alpine/k8s:1.33.0";
    private static final String DEFAULT_JOB_NAME = "default-job";
    private static final String DEFAULT_CONTAINER_NAME = "default-container";
    private static final String DEFAULT_RESTART_POLICY = "Never";
    private static final int DEFAULT_BACKOFF_LIMIT = 4;
    private static final int DEFAULT_TTL_SECONDS = 90;

    private K8sJob() {
        // Private constructor to prevent instantiation
    }

    public static JobBuilder builder() {
        return new JobBuilder();
    }

    public static final class JobBuilder {
        private String jobName = DEFAULT_JOB_NAME;
        private String containerName = DEFAULT_CONTAINER_NAME;
        private String image = DEFAULT_IMAGE;
        private String restartPolicy = DEFAULT_RESTART_POLICY;
        private int backoffLimit = DEFAULT_BACKOFF_LIMIT;
        private int ttlSecondsAfterFinished = DEFAULT_TTL_SECONDS;

        private JobBuilder() {
            // Private constructor to prevent instantiation
        }

        public JobBuilder jobName(String jobName) {
            this.jobName = jobName;
            return this;
        }

        public JobBuilder containerName(String containerName) {
            this.containerName = containerName;
            return this;
        }

        public JobBuilder image(String image) {
            this.image = image;
            return this;
        }

        public JobBuilder restartPolicy(String restartPolicy) {
            this.restartPolicy = restartPolicy;
            return this;
        }

        public JobBuilder backoffLimit(int backoffLimit) {
            this.backoffLimit = backoffLimit;
            return this;
        }

        public JobBuilder ttlSecondsAfterFinished(int ttlSeconds) {
            this.ttlSecondsAfterFinished = ttlSeconds;
            return this;
        }

        public V1Job build() {
            return new V1Job()
                    .apiVersion(K8sApiEndpoints.JOBS.apiVersion())
                    .kind(K8sApiEndpoints.JOBS.kind())
                    .metadata(new V1ObjectMeta().name(jobName))
                    .spec(new V1JobSpec()
                            .template(new V1PodTemplateSpec()
                                    .spec(new V1PodSpec()
                                            .containers(Collections.singletonList(
                                                    new V1Container()
                                                            .name(containerName)
                                                            .image(image)
                                            ))
                                            .restartPolicy(restartPolicy)
                                    )
                            )
                            .backoffLimit(backoffLimit)
                            .ttlSecondsAfterFinished(ttlSecondsAfterFinished)
                    );
        }
    }
}
