/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.memiiso.lakeevents;

import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.time.Duration;

public class TestS3 {
    public static int MINIO_MAPPED_PORT = 9001;
    public static String MINIO_ACCESS_KEY = "test";
    static final String DEFAULT_IMAGE = "minio/minio";
    static final String DEFAULT_TAG = "edge";
    static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    static final String HEALTH_ENDPOINT = "/minio/health/ready";
    public static String MINIO_SECRET_KEY = "testtest";
    static int MINIO_DEFAULT_PORT = 9000;

    final Logger logger = LoggerFactory.getLogger(TestS3.class);
    private GenericContainer container = null;

    public void start() {

        Assertions.assertNotNull(MINIO_ACCESS_KEY);
        Assertions.assertNotNull(MINIO_SECRET_KEY);
        Assertions.assertTrue(MINIO_SECRET_KEY.length() >= 8);

        this.container = new FixedHostPortGenericContainer(DEFAULT_IMAGE + ':' + DEFAULT_TAG)
                .withFixedExposedPort(MINIO_MAPPED_PORT, MINIO_DEFAULT_PORT)
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
