/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.memiiso.lakeevents;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

import java.time.Duration;

public class TestS3 {
    static final int MINIO_DEFAULT_PORT = 9000;
    static final String DEFAULT_IMAGE = "minio/minio";
    static final String DEFAULT_TAG = "edge";
    static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    static final String HEALTH_ENDPOINT = "/minio/health/ready";
    static String MINIO_ACCESS_KEY;
    static String MINIO_SECRET_KEY;
    final Logger logger = LoggerFactory.getLogger(TestS3.class);
    private GenericContainer container = null;

    {
        ProfileCredentialsProvider pcred = ProfileCredentialsProvider.create("default");
        MINIO_ACCESS_KEY = pcred.resolveCredentials().accessKeyId();
        MINIO_SECRET_KEY = pcred.resolveCredentials().secretAccessKey();
    }

    public void start() {

        this.container = new FixedHostPortGenericContainer(DEFAULT_IMAGE + ':' + DEFAULT_TAG)
                .withFixedExposedPort(MINIO_DEFAULT_PORT, MINIO_DEFAULT_PORT)
                .waitingFor(new HttpWaitStrategy()
                        .forPath(HEALTH_ENDPOINT)
                        .forPort(MINIO_DEFAULT_PORT)
                        .withStartupTimeout(Duration.ofSeconds(30)))
                .withEnv("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
                .withEnv("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
                .withCommand("server " + DEFAULT_STORAGE_DIRECTORY);
        this.container.start();
        logger.info("Mino S3 Container is ready!");
    }

    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        } catch (Exception e) {
            // ignored
        }
    }

    public String getContainerIpAddress() {
        return this.container.getContainerIpAddress();
    }

    public Integer getMappedPort() {
        return this.container.getMappedPort(MINIO_DEFAULT_PORT);
    }

    public Integer getFirstMappedPort() {
        return this.container.getFirstMappedPort();
    }

}
